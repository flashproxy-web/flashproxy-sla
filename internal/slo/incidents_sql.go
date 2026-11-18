package slo

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// sqlStr quotes s as a ClickHouse string literal. Package names come from the
// service config, so this only has to be correct, not defensive.
func sqlStr(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// sqlNum formats f as a plain decimal literal (no exponent), so a large
// threshold never renders as e.g. 1e+06.
func sqlNum(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// IncidentsSQL computes the Down/Degraded incident windows entirely in ClickHouse
// and returns one row per incident: package, type, start, end (exclusive), minutes.
// It mirrors RollupPackagesAlts + IncidentsFromBars exactly:
//   - rows of an alt key are folded into its short key, the short row winning per
//     (vantage, minute) (MergeAlts);
//   - per-vantage minutes use the same rounding as MinutesSQL;
//   - a minute is Down when no vantage reaches DownSuccessPct, otherwise its
//     best available vantage latency drives Degraded (rollupPackageSeries);
//   - Degraded needs DegradedForMin consecutive over-threshold minutes, and a
//     Down/Degraded run ends at any other minute (Evaluator, IncidentsFromBars).
//
// Only the incident windows leave the server, instead of every minute of every
// vantage (~650k rows for 30 days).
func IncidentsSQL(db string, windowMin int, s SLO, alts []Package) string {
	s = s.withDefaults()

	// alt key -> short key, applied to raw package names.
	var mapping string
	am := AltMap(alts)
	var conds []string
	for short, alt := range am {
		if alt == short || alt == "" {
			continue
		}
		conds = append(conds, fmt.Sprintf("package = %s, %s", sqlStr(alt), sqlStr(short)))
	}
	sort.Strings(conds)
	if len(conds) == 0 {
		mapping = "package"
	} else {
		mapping = "multiIf(" + strings.Join(conds, ", ") + ", package)"
	}

	// per-package degraded threshold, as ForPackage resolves it.
	var thr []string
	keys := make([]string, 0, len(s.PackageDegradedMs))
	for k := range s.PackageDegradedMs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if ms := s.PackageDegradedMs[k]; TargetValid(ms) {
			thr = append(thr, fmt.Sprintf("pkg = %s, %s", sqlStr(k), sqlNum(ms)))
		}
	}
	thrExpr := sqlNum(s.DegradedAvgMs)
	if len(thr) > 0 {
		thrExpr = "multiIf(" + strings.Join(thr, ", ") + ", " + thrExpr + ")"
	}

	return fmt.Sprintf(`
WITH
  -- raw rows -> canonical package; is_alt = 1 for rows under an alt key
  per_vantage_raw AS (
    SELECT pkg, vantage, t, toUInt8(pkg != raw) AS is_alt,
           round(avg(ttfb_ms), 1) AS ms,
           round(100 * sum(success) / count(), 2) AS succ
    FROM (
      SELECT package AS raw, %[3]s AS pkg, vantage, ttfb_ms, success,
             toUInt32(toUnixTimestamp(toStartOfMinute(ts))) AS t
      FROM %[1]s.probe_raw
      WHERE scenario = 'connect'
        AND ts > now() - INTERVAL %[2]d MINUTE
    )
    -- the Grid: windowMin minutes ending at the start of the current minute
    WHERE t >= toUnixTimestamp(toStartOfMinute(now())) - 60 * %[2]d + 60
      AND t <= toUnixTimestamp(toStartOfMinute(now()))
    GROUP BY pkg, vantage, t, is_alt
  ),
  -- one row per (package, vantage, minute); the short key wins over an alt row
  per_vantage AS (
    SELECT pkg, vantage, t, argMin(ms, is_alt) AS ms, argMin(succ, is_alt) AS succ
    FROM per_vantage_raw
    GROUP BY pkg, vantage, t
  ),
  -- cross-vantage minute: down unless some vantage is available; latency is the
  -- best available vantage's average
  synth AS (
    SELECT pkg, t,
           toUInt8(max(succ >= %[4]s)) AS avail,
           minIf(ms, succ >= %[4]s) AS best_ms
    FROM per_vantage
    GROUP BY pkg, t
  ),
  cls AS (
    SELECT pkg, t,
           avail = 0 AS is_down,
           avail = 1 AND best_ms > %[5]s AS is_over
    FROM synth
  ),
  -- position inside each run of consecutive over-threshold minutes
  over_rn AS (
    SELECT pkg, t, toInt64(t / 60) - row_number() OVER (PARTITION BY pkg ORDER BY t) AS k
    FROM cls WHERE is_over
  ),
  over_pos AS (
    SELECT pkg, t, row_number() OVER (PARTITION BY pkg, k ORDER BY t) AS p
    FROM over_rn
  ),
  states AS (
    SELECT c.pkg AS pkg, c.t AS t,
           multiIf(c.is_down, 'down',
                   o.p >= %[6]d, 'degraded',
                   'operational') AS state
    FROM cls AS c
    LEFT JOIN over_pos AS o ON c.pkg = o.pkg AND c.t = o.t
  ),
  -- consecutive minutes with the same Down/Degraded state form one incident
  runs AS (
    SELECT pkg, state, t,
           toInt64(t / 60) - row_number() OVER (PARTITION BY pkg, state ORDER BY t) AS k
    FROM states WHERE state != 'operational'
  )
SELECT pkg AS package, state AS type,
       min(t) AS start, max(t) + 60 AS end, count() AS minutes
FROM runs
GROUP BY pkg, state, k
ORDER BY package, start`, db, windowMin, mapping, sqlNum(s.DownSuccessPct), thrExpr, s.DegradedForMin)
}

// FetchIncidentsAlts runs IncidentsSQL and returns the incidents, same as
// RollupIncidentsAlts does from the minute series.
func FetchIncidentsAlts(ctx context.Context, ch *chstore.Client, db string, windowMin int, s SLO, alts []Package) ([]Incident, error) {
	rows, err := ch.QueryJSON(ctx, IncidentsSQL(db, windowMin, s, alts))
	if err != nil {
		return nil, err
	}
	out := make([]Incident, 0, len(rows))
	var decodeErr error
	for _, m := range rows {
		inc := Incident{
			Package: chstore.Str(m, "package"),
			Type:    chstore.Str(m, "type"),
			Start:   chstore.Int64(m, "start", &decodeErr),
			End:     chstore.Int64(m, "end", &decodeErr),
			Minutes: int(chstore.Int64(m, "minutes", &decodeErr)),
		}
		out = append(out, inc)
	}
	if decodeErr != nil {
		return nil, fmt.Errorf("decode incident row: %w", decodeErr)
	}
	return out, nil
}
