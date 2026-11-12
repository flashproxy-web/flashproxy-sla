// Package slo holds the SLO evaluation shared by the website (rendering status +
// uptime) and the worker's monitor (firing alerts) — so both judge "operational /
// degraded / down / no_data" identically, and identically to the published SLA.
//
// The definitions mirror the SLA contract (web/sla.html.tmpl §2) EXACTLY, and are
// CROSS-VANTAGE — a package's verdict reduces over all of its vantages:
//   - Available  : the connect scenario succeeds (CONNECT -> 200) from at least
//     one vantage.
//   - Down       : a minute in which the package is unavailable from ALL vantages
//     simultaneously (connect success% < DownSuccessPct at every
//     vantage). A single-vantage failure is NOT Down — it isolates one
//     network path, not the proxy.
//   - Degraded   : Available, but the best (lowest-latency available) vantage's
//     AVERAGE round-trip response time (ttfb) exceeds DegradedAvgMs for DegradedForMin
//     CONSECUTIVE minutes, and stays Degraded until the average
//     recovers at/below the threshold. The run-up minutes before the
//     threshold is met are not yet Degraded ("for N consecutive
//     minutes" is a trigger, evaluated non-retroactively).
//   - Impact     : Down minutes + ½ × Degraded minutes (½-weight).
//   - Availability% = (minutes_with_data − Impact) / minutes_with_data × 100.
//
// RollupPackages()/Fetch are the authoritative cross-vantage path; rollupSeries/
// Rollup/Evaluator are the per-vantage building blocks they reduce, and Eval is the
// instant single-minute classifier. Thresholds are published at /api/meta so the
// verdict is reproducible from data alone — not just from this source.
package slo

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// SLO are the published thresholds.
type SLO struct {
	DegradedAvgMs  float64 `json:"degraded_avg_ms"`  // avg round-trip response-ms (ttfb) above which a minute is over threshold
	DegradedForMin int     `json:"degraded_for_min"` // consecutive over-threshold minutes that trigger Degraded
	DownSuccessPct float64 `json:"down_success_pct"` // connect success% below which a minute is Down
	StaleSeconds   int64   `json:"stale_seconds"`    // age of newest data beyond which status is no_data
	// Packages is the configured package list (top-level "packages" in worker.json
	// and website.json). It is not part of the "slo" JSON block: the config loaders
	// copy it here so the per-package Degraded threshold (target_ms) travels with
	// the thresholds. Nil/empty = every package uses DegradedAvgMs.
	Packages []Package `json:"-"`
}

// PackageKeyRe is the form of a package key (short or alt).
var PackageKeyRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

// Package is one entry of the "packages" config list: everything configured per
// package in one place.
type Package struct {
	// Short is the internal key: probe package name, ledger, rollup, routes,
	// data-pkg, ?package=, thresholds, alerts.
	Short string `json:"short"`
	// Alt is the old live key whose rows are merged into Short during the cutover
	// overlap (MergeAlts). nil (JSON null or absent) = no alt; equal to Short = no-op.
	Alt *string `json:"alt"`
	// Name is the display text; empty = Short is shown.
	Name string `json:"name,omitempty"`
	// TargetMs is the package's Degraded threshold in whole ms. Missing, below 1 or
	// not a whole number = ignored, the global DegradedAvgMs applies.
	TargetMs float64 `json:"target_ms,omitempty"`
	// Covered is carried in the data only; no verdict or page uses it yet.
	Covered bool `json:"covered"`
}

// UnmarshalJSON decodes a list entry and rejects a name that is present but empty
// (leave the field out to show the short key).
func (p *Package) UnmarshalJSON(b []byte) error {
	type plain Package
	aux := struct {
		*plain
		Name *string `json:"name"`
	}{plain: (*plain)(p)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if aux.Name != nil {
		if strings.TrimSpace(*aux.Name) == "" {
			return fmt.Errorf("packages: %q: name must not be empty (leave it out to show the short key)", p.Short)
		}
		p.Name = *aux.Name
	}
	return nil
}

// AltKey returns the package's alt key, or "" when it has none (null, empty or
// identical to Short).
func (p Package) AltKey() string {
	if p.Alt == nil || *p.Alt == p.Short {
		return ""
	}
	return *p.Alt
}

// TargetValid reports whether TargetMs is usable as a threshold: a whole number >= 1.
func (p Package) TargetValid() bool {
	return p.TargetMs >= 1 && p.TargetMs == math.Trunc(p.TargetMs)
}

// FindPackage returns the list entry whose short key is short.
func FindPackage(pkgs []Package, short string) (Package, bool) {
	for _, p := range pkgs {
		if p.Short == short {
			return p, true
		}
	}
	return Package{}, false
}

// AltMap returns short key -> alt key for the entries that name an alt (identity
// pairs included, as configured). Nil when no entry has one.
func AltMap(pkgs []Package) map[string]string {
	var m map[string]string
	for _, p := range pkgs {
		if p.Alt == nil {
			continue
		}
		if m == nil {
			m = map[string]string{}
		}
		m[p.Short] = *p.Alt
	}
	return m
}

// CheckPackages validates the "packages" list so keys and pairings are
// unambiguous: every short key must be a valid package name and appear once;
// every non-null alt must be a valid package name, must not be the short key of
// another entry (no chains), and two entries must not share one alt. Identity
// pairs (alt == short) mean "no alt key" and are allowed. target_ms is not
// checked here: an unusable value is ignored (see Package.TargetValid).
func CheckPackages(pkgs []Package) error {
	shorts := map[string]bool{}
	for i, p := range pkgs {
		if !PackageKeyRe.MatchString(p.Short) {
			return fmt.Errorf("packages[%d]: short key %q is not a valid package name", i, p.Short)
		}
		if shorts[p.Short] {
			return fmt.Errorf("packages: short key %q is listed more than once", p.Short)
		}
		shorts[p.Short] = true
	}
	owner := map[string]string{} // alt -> the short key that names it
	for _, p := range pkgs {
		if p.Alt == nil {
			continue
		}
		alt := *p.Alt
		if !PackageKeyRe.MatchString(alt) {
			return fmt.Errorf("packages[%q]: alt key %q is not a valid package name", p.Short, alt)
		}
		if alt == p.Short {
			continue
		}
		if shorts[alt] {
			return fmt.Errorf("packages[%q]: alt key %q is also a short key (chained pairs are not allowed)", p.Short, alt)
		}
		if other, ok := owner[alt]; ok {
			return fmt.Errorf("packages: alt key %q is used by both %q and %q", alt, other, p.Short)
		}
		owner[alt] = p.Short
	}
	return nil
}

// ForPackage returns s with DegradedAvgMs replaced by the package's own threshold
// (target_ms of its "packages" entry) when one is configured; otherwise s
// unchanged. Callers apply withDefaults after.
func (s SLO) ForPackage(pkg string) SLO {
	// Unusable values (below 1 ms, not whole) are ignored (global threshold
	// applies), same rule as the worker's alert gate.
	if p, ok := FindPackage(s.Packages, pkg); ok && p.TargetValid() {
		s.DegradedAvgMs = p.TargetMs
	}
	return s
}

// DegradedThresholdFor returns the effective Degraded threshold (ms) for pkg: its
// own entry when configured, else the global one, else the default. Display only.
func (s SLO) DegradedThresholdFor(pkg string) float64 {
	return s.ForPackage(pkg).withDefaults().DegradedAvgMs
}

func (s SLO) withDefaults() SLO {
	if s.DegradedAvgMs <= 0 {
		s.DegradedAvgMs = 50
	}
	if s.DegradedForMin <= 0 {
		s.DegradedForMin = 5
	}
	if s.DownSuccessPct <= 0 {
		s.DownSuccessPct = 90
	}
	if s.StaleSeconds <= 0 {
		s.StaleSeconds = 120
	}
	return s
}

// Status is the current rollup + verdict for one package (optionally one vantage).
type Status struct {
	Package       string  `json:"package"`
	Vantage       string  `json:"vantage"`
	Status        string  `json:"status"`
	ResponseMsAvg float64 `json:"response_ms_avg"` // avg over the most recent data minute
	SuccessPct    float64 `json:"success_pct"`
	Samples       uint64  `json:"samples"`
	AgeSeconds    int64   `json:"age_seconds"`
}

// Minute is one per-minute rollup of the connect scenario's round-trip response time (ttfb_ms).
type Minute struct {
	T             int64
	ResponseMsAvg float64
	SuccessPct    float64
	Samples       uint64
}

// MinutesSQL returns per-(package, vantage, minute) avg round-trip response time (ttfb)
// and success%% for the connect scenario over the window — the single source the
// uptime bars, and alerts all derive from.
func MinutesSQL(db string, windowMin int) string {
	return fmt.Sprintf(`SELECT package, vantage,
  toUInt32(toUnixTimestamp(toStartOfMinute(ts))) AS t,
  round(avg(ttfb_ms), 1)                 AS response_ms_avg,
  round(100 * sum(success) / count(), 2) AS success_pct,
  toUInt32(count())                      AS samples
FROM %s.probe_raw
WHERE scenario = 'connect' AND ts > now() - INTERVAL %d MINUTE
GROUP BY package, vantage, t ORDER BY package, vantage, t`, db, windowMin)
}

// Evaluator classifies a CONTIGUOUS, time-ordered sequence of minutes for one
// (package, vantage), carrying the consecutive-over-threshold run across calls.
type Evaluator struct {
	run int // consecutive over-threshold minutes so far
	s   SLO
}

func NewEvaluator(s SLO) *Evaluator { return &Evaluator{s: s.withDefaults()} }

// Next classifies one minute. hasData=false marks a gap (which breaks the
// consecutive run). It returns operational/degraded/down/no_data.
func (e *Evaluator) Next(hasData bool, responseMs, successPct float64) string {
	if !hasData {
		e.run = 0
		return "no_data"
	}
	if successPct < e.s.DownSuccessPct {
		e.run = 0
		return "down"
	}
	if responseMs > e.s.DegradedAvgMs {
		e.run++
		if e.run >= e.s.DegradedForMin {
			return "degraded"
		}
		return "operational" // over threshold but trigger not yet met
	}
	e.run = 0
	return "operational"
}

// Grid returns windowMin contiguous minute-start unix timestamps ending at the start
// of the current minute.
func Grid(now time.Time, windowMin int) []int64 {
	end := now.Unix()
	end -= end % 60
	g := make([]int64, windowMin)
	for i := range g {
		g[i] = end - int64(windowMin-1-i)*60
	}
	return g
}

// Eval is the simple boundary classifier used for a single already-aggregated
// minute where the consecutive rule does not apply (kept for callers that only need
// down/degraded-by-instant/operational). Prefer Evaluator for sequences.
func Eval(responseMs, successPct float64, ageSec int64, s SLO) string {
	s = s.withDefaults()
	if ageSec > s.StaleSeconds {
		return "no_data"
	}
	if successPct < s.DownSuccessPct {
		return "down"
	}
	if responseMs > s.DegradedAvgMs {
		return "degraded"
	}
	return "operational"
}

// Overall reduces per-component statuses to the headline banner state + label.
func Overall(statuses []string) (string, string) {
	rank := map[string]int{"no_data": 0, "operational": 1, "degraded": 2, "down": 3}
	worst := "no_data"
	for _, s := range statuses {
		if rank[s] > rank[worst] {
			worst = s
		}
	}
	label := map[string]string{
		"operational": "All Systems Operational",
		"degraded":    "Partial System Degradation",
		"down":        "Major System Outage",
		"no_data":     "Awaiting Data",
	}[worst]
	return worst, label
}

// Bar is one classified minute for the uptime strip.
type Bar struct {
	T             int64   `json:"t"`
	Status        string  `json:"status"`
	ResponseMsAvg float64 `json:"response_ms_avg"`
	SuccessPct    float64 `json:"success_pct"`
	Samples       uint64  `json:"samples"`
}

// VantageRollup is one (package, vantage) over the window: classified bars, the
// current status, and the ½-weight availability%.
type VantageRollup struct {
	Vantage   string
	Current   Status
	Bars      []Bar
	UptimePct float64
}

// rollupSeries classifies one (package, vantage) minute series over the grid with
// the consecutive-degraded rule, computing the bars, current status (verdict at the
// most recent data minute, or no_data if stale), and Availability% (Impact = Down +
// ½·Degraded; uptime = (withData − Impact) / withData × 100).
func rollupSeries(byMinute map[int64]Minute, grid []int64, now int64, s SLO) VantageRollup {
	s = s.withDefaults()
	ev := NewEvaluator(s)
	bars := make([]Bar, len(grid))
	var withData, down, deg int
	var lastDataT int64 = -1
	cur := Status{Status: "no_data"}
	for i, t := range grid {
		m, ok := byMinute[t]
		status := ev.Next(ok, m.ResponseMsAvg, m.SuccessPct)
		bars[i] = Bar{T: t, Status: status, ResponseMsAvg: m.ResponseMsAvg, SuccessPct: m.SuccessPct, Samples: m.Samples}
		if ok {
			withData++
			lastDataT = t
			cur = Status{Status: status, ResponseMsAvg: m.ResponseMsAvg, SuccessPct: m.SuccessPct, Samples: m.Samples}
			switch status {
			case "down":
				down++
			case "degraded":
				deg++
			}
		}
	}
	if lastDataT < 0 {
		cur.AgeSeconds = now - grid[0]
	} else {
		cur.AgeSeconds = now - lastDataT
		if cur.AgeSeconds > s.StaleSeconds {
			cur.Status = "no_data"
		}
	}
	uptime := 100.0
	if withData > 0 {
		uptime = (float64(withData) - float64(down) - 0.5*float64(deg)) / float64(withData) * 100
		uptime = float64(int(uptime*100+0.5)) / 100
	}
	return VantageRollup{Current: cur, Bars: bars, UptimePct: uptime}
}

// fetchMinutes pulls the per-(package, vantage) minute series for the window.
func fetchMinutes(ctx context.Context, ch *chstore.Client, db string, windowMin int) (map[string]map[string]map[int64]Minute, error) {
	rows, err := ch.QueryJSON(ctx, MinutesSQL(db, windowMin))
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]map[int64]Minute{} // package -> vantage -> t -> Minute
	for _, m := range rows {
		pkg, van := chstore.Str(m, "package"), chstore.Str(m, "vantage")
		if out[pkg] == nil {
			out[pkg] = map[string]map[int64]Minute{}
		}
		if out[pkg][van] == nil {
			out[pkg][van] = map[int64]Minute{}
		}
		t := chstore.Int64(m, "t", &err)
		out[pkg][van][t] = Minute{
			T: t, ResponseMsAvg: chstore.Float64(m, "response_ms_avg", &err),
			SuccessPct: chstore.Float64(m, "success_pct", &err), Samples: chstore.Uint[uint64](m, "samples", &err),
		}
	}
	if err != nil {
		return nil, fmt.Errorf("decode minute row: %w", err)
	}
	return out, nil
}

// MergeAlts folds rows stored under a package's alternate (old) key into its short
// (real) key, for the window in which a product's rows may sit under either key.
// pkgs is the "packages" list; each entry with an alt is a pair. For each pair, every (vantage, minute) of the alt
// key is moved under the short key; where the short key already has a row for the
// same vantage and minute, the short row is kept and the alt row is dropped, so a
// minute is never counted twice. The alt key is removed from the result. Pairs with
// an empty or identical alt are no-ops, and an alt named by more than one short key
// is merged into the first (by short key order) only. With no entry naming an alt
// (nil or empty list included) the input map is returned as is. The input is not
// modified.
func MergeAlts(data map[string]map[string]map[int64]Minute, pkgs []Package) map[string]map[string]map[int64]Minute {
	alts := AltMap(pkgs)
	if len(alts) == 0 {
		return data
	}
	out := make(map[string]map[string]map[int64]Minute, len(data))
	for pkg, byVantage := range data {
		out[pkg] = byVantage
	}
	used := map[string]bool{}
	for _, short := range sortedKeys(alts) {
		alt := alts[short]
		if alt == "" || alt == short || used[alt] {
			continue
		}
		altData, ok := data[alt]
		if !ok {
			continue
		}
		used[alt] = true
		merged := make(map[string]map[int64]Minute, len(altData))
		for _, src := range []map[string]map[int64]Minute{altData, data[short]} { // short last: it wins
			for van, mins := range src {
				if merged[van] == nil {
					merged[van] = make(map[int64]Minute, len(mins))
				}
				for t, m := range mins {
					merged[van][t] = m
				}
			}
		}
		delete(out, alt)
		out[short] = merged
	}
	return out
}

// PackageRollup is the CROSS-VANTAGE rollup for one package. A minute is Down only
// when the package is unavailable from EVERY vantage at once — a failure from a
// single vantage (e.g. one region's network path) is NOT Down. Degraded uses the
// best (lowest-latency) currently-available vantage's average, per the SLA.
type PackageRollup struct {
	Package       string
	Status        string
	BestVantage   string
	ResponseMsAvg float64
	SuccessPct    float64
	Samples       uint64
	AgeSeconds    int64
	UptimePct     float64
	Bars          []Bar
	Vantages      []VantageRollup
}

// rollupPackageSeries computes the package's authoritative CROSS-VANTAGE rollup plus
// the per-vantage detail for the UI.
//
// The PACKAGE's latency-Degraded verdict reads only the home (lowest-latency
// available) vantage each minute, and Down still requires unavailable-everywhere.
// The per-vantage detail is separate: each vantage's bars and Current show that
// vantage's own state (its own average against the package threshold, consecutive
// rule), so a far vantage measuring a product cross-region can read degraded there
// while the package stays operational.
func rollupPackageSeries(pkg string, byVantage map[string]map[int64]Minute, grid []int64, now int64, s SLO) PackageRollup {
	s = s.ForPackage(pkg).withDefaults()
	vans := make([]string, 0, len(byVantage))
	for v := range byVantage {
		vans = append(vans, v)
	}
	sort.Strings(vans)

	// 1) Collapse to one synthetic per-minute series (available-anywhere ⇒ up; the
	//    best available avg drives Degraded) and record the home vantage each minute.
	synth := make(map[int64]Minute, len(grid))
	bestVanAt := make(map[int64]string, len(grid))
	for _, t := range grid {
		var hasData, availAny bool
		var bestAvg float64
		var samples uint64
		bestVan := ""
		for _, v := range vans {
			m, ok := byVantage[v][t]
			if !ok {
				continue
			}
			hasData = true
			samples += m.Samples
			if m.SuccessPct >= s.DownSuccessPct { // available from this vantage this minute
				if !availAny || m.ResponseMsAvg < bestAvg {
					bestAvg, bestVan = m.ResponseMsAvg, v
				}
				availAny = true
			}
		}
		if !hasData {
			continue
		}
		succ := 0.0
		if availAny {
			succ = 100
		}
		synth[t] = Minute{T: t, ResponseMsAvg: bestAvg, SuccessPct: succ, Samples: samples}
		bestVanAt[t] = bestVan
	}

	// Package verdict + bars: Degraded on the home-vantage latency (consecutive rule).
	cross := rollupSeries(synth, grid, now, s)
	bestVan := ""
	for _, t := range grid { // home vantage at the most recent data minute (headline)
		if _, ok := synth[t]; ok {
			bestVan = bestVanAt[t]
		}
	}

	// 2) Per-vantage detail: every vantage shows ITS OWN state. Each one classifies
	//    its own minutes with the same consecutive rule as the package (one Evaluator
	//    per vantage, the package's effective SLO): down, degraded (own average above
	//    the threshold for DegradedForMin consecutive minutes), operational or no_data.
	//    Display detail only: the package verdict above does not read these.
	detail := make([]VantageRollup, 0, len(vans))
	for _, v := range vans {
		bars := make([]Bar, len(grid))
		var withData, down, deg int
		var lastT int64 = -1
		cur := Status{Status: "no_data", Vantage: v}
		ev := NewEvaluator(s)
		for i, t := range grid {
			m, ok := byVantage[v][t]
			st := ev.Next(ok, m.ResponseMsAvg, m.SuccessPct)
			if ok {
				withData++
				lastT = t
				switch st {
				case "down":
					down++
				case "degraded":
					deg++
				}
				cur = Status{Status: st, ResponseMsAvg: m.ResponseMsAvg, SuccessPct: m.SuccessPct, Samples: m.Samples, Vantage: v}
			}
			bars[i] = Bar{T: t, Status: st, ResponseMsAvg: m.ResponseMsAvg, SuccessPct: m.SuccessPct, Samples: m.Samples}
		}
		if lastT < 0 {
			cur.AgeSeconds = now - grid[0]
		} else {
			cur.AgeSeconds = now - lastT
			if cur.AgeSeconds > s.StaleSeconds {
				cur.Status = "no_data"
			}
		}
		uptime := 100.0
		if withData > 0 {
			uptime = (float64(withData) - float64(down) - 0.5*float64(deg)) / float64(withData) * 100
			uptime = float64(int(uptime*100+0.5)) / 100
		}
		detail = append(detail, VantageRollup{Vantage: v, Current: cur, Bars: bars, UptimePct: uptime})
	}

	return PackageRollup{
		Status: cross.Current.Status, BestVantage: bestVan,
		ResponseMsAvg: cross.Current.ResponseMsAvg, SuccessPct: cross.Current.SuccessPct,
		Samples: cross.Current.Samples, AgeSeconds: cross.Current.AgeSeconds,
		UptimePct: cross.UptimePct, Bars: cross.Bars, Vantages: detail,
	}
}

// RollupPackages returns the cross-vantage rollup per package (sorted). This is the
// authoritative status/uptime used by the banner, the monitor, and SLA accounting.
func RollupPackages(ctx context.Context, ch *chstore.Client, db string, windowMin int, s SLO) ([]PackageRollup, error) {
	return RollupPackagesAlts(ctx, ch, db, windowMin, s, nil)
}

// RollupPackagesAlts is RollupPackages over the fetched minutes after MergeAlts
// (alts: the "packages" list). With nil alts it is RollupPackages.
func RollupPackagesAlts(ctx context.Context, ch *chstore.Client, db string, windowMin int, s SLO, alts []Package) ([]PackageRollup, error) {
	s = s.withDefaults()
	data, err := fetchMinutes(ctx, ch, db, windowMin)
	if err != nil {
		return nil, err
	}
	data = MergeAlts(data, alts)
	now := time.Now()
	grid := Grid(now, windowMin)
	out := make([]PackageRollup, 0, len(data))
	for _, pkg := range sortedKeys(data) {
		pr := rollupPackageSeries(pkg, data[pkg], grid, now.Unix(), s)
		pr.Package = pkg
		out = append(out, pr)
	}
	return out, nil
}

// RollupPackagesTrimAlts is RollupPackagesAlts for a window of exactly `blocks`
// minute blocks chosen from blocks+1 fetched minutes (the overview: 91 fetched, 90
// used). The grid is blocks+1 minutes ending at the start of the current minute and
// is fetched and classified in full, so every Evaluator (package and per-vantage)
// runs over all blocks+1 minutes; only then is each package trimmed (trimRollup).
func RollupPackagesTrimAlts(ctx context.Context, ch *chstore.Client, db string, blocks int, s SLO, alts []Package) ([]PackageRollup, error) {
	s = s.withDefaults()
	data, err := fetchMinutes(ctx, ch, db, blocks+1)
	if err != nil {
		return nil, err
	}
	data = MergeAlts(data, alts)
	now := time.Now()
	grid := Grid(now, blocks+1)
	out := make([]PackageRollup, 0, len(data))
	for _, pkg := range sortedKeys(data) {
		pr := rollupPackageSeries(pkg, data[pkg], grid, now.Unix(), s)
		pr.Package = pkg
		out = append(out, trimRollup(pr, blocks, now.Unix(), s.ForPackage(pkg)))
	}
	return out, nil
}

// trimRollup cuts an already classified rollup of blocks+1 minutes down to exactly
// `blocks` minutes. The rule is decided once per package, on the PACKAGE bars: if the
// newest minute has no data (no vantage reported it), the newest block is dropped;
// otherwise the oldest block is dropped. The same block is dropped from the package
// bars and from every vantage's bars, so all series hold the same `blocks` timestamps
// in time order. Statuses are NOT re-classified (the consecutive-degraded run was
// computed over the full sequence); the current status, age and uptime of the
// package and of each vantage are recomputed from the kept bars only, so bars,
// uptime, impact minutes, incidents (IncidentsFromBars over Bars) and status all use
// the same blocks. A rollup that is not exactly blocks+1 long is returned as is.
func trimRollup(pr PackageRollup, blocks int, now int64, s SLO) PackageRollup {
	if blocks < 1 || len(pr.Bars) != blocks+1 {
		return pr
	}
	s = s.withDefaults()
	lo := 1 // newest minute has data: drop the oldest block
	if pr.Bars[blocks].Status == "no_data" {
		lo = 0 // newest minute has no data: drop the newest block
	}
	pr.Bars = pr.Bars[lo : lo+blocks : lo+blocks]
	cur, uptime := summarizeBars(pr.Bars, now, s)
	pr.Status, pr.ResponseMsAvg, pr.SuccessPct = cur.Status, cur.ResponseMsAvg, cur.SuccessPct
	pr.Samples, pr.AgeSeconds, pr.UptimePct = cur.Samples, cur.AgeSeconds, uptime
	if !barsHaveData(pr.Bars) {
		pr.BestVantage = "" // the only data minute was the dropped one
	}
	vans := make([]VantageRollup, len(pr.Vantages))
	for i, vr := range pr.Vantages {
		if len(vr.Bars) == blocks+1 {
			vr.Bars = vr.Bars[lo : lo+blocks : lo+blocks]
			vr.Current, vr.UptimePct = summarizeBars(vr.Bars, now, s)
			vr.Current.Vantage = vr.Vantage
		}
		vans[i] = vr
	}
	pr.Vantages = vans
	return pr
}

func barsHaveData(bars []Bar) bool {
	for _, b := range bars {
		if b.Status != "no_data" {
			return true
		}
	}
	return false
}

// summarizeBars recomputes, from classified bars alone, what rollupSeries derives
// while classifying: the current status (verdict at the most recent data minute, or
// no_data if stale) and Availability% ((withData − Down − ½·Degraded) / withData).
// A bar has data exactly when its status is not no_data.
func summarizeBars(bars []Bar, now int64, s SLO) (Status, float64) {
	var withData, down, deg int
	var lastDataT int64 = -1
	cur := Status{Status: "no_data"}
	for _, b := range bars {
		if b.Status == "no_data" {
			continue
		}
		withData++
		lastDataT = b.T
		cur = Status{Status: b.Status, ResponseMsAvg: b.ResponseMsAvg, SuccessPct: b.SuccessPct, Samples: b.Samples}
		switch b.Status {
		case "down":
			down++
		case "degraded":
			deg++
		}
	}
	if lastDataT < 0 {
		if len(bars) > 0 {
			cur.AgeSeconds = now - bars[0].T
		}
	} else {
		cur.AgeSeconds = now - lastDataT
		if cur.AgeSeconds > s.StaleSeconds {
			cur.Status = "no_data"
		}
	}
	uptime := 100.0
	if withData > 0 {
		uptime = (float64(withData) - float64(down) - 0.5*float64(deg)) / float64(withData) * 100
		uptime = float64(int(uptime*100+0.5)) / 100
	}
	return cur, uptime
}

// Incident is one contiguous Down or Degraded window for a package, derived from the
// cross-vantage minute classification. This is the SAME definition the credit engine
// (flash-staff-dash internal SLA port) uses, so the public incident history and the
// credit accounting identify each incident identically (same type, same window).
type Incident struct {
	Package string `json:"package"`
	Type    string `json:"type"`  // "down" | "degraded"
	Start   int64  `json:"start"` // unix, inclusive first impacted minute
	End     int64  `json:"end"`   // unix, exclusive (last impacted minute + 60)
	Minutes int    `json:"minutes"`
}

// IncidentsFromBars groups a package's classified minute Bars (already time-ordered)
// into contiguous Down/Degraded windows. A direct→degraded (or vice-versa) transition
// closes one incident and opens another; operational/no_data minutes end the current
// one. Mirrors flash-staff-dash slo.ts rollupPackage's incident flush exactly so both
// systems produce identical (type, start, end) for the same outage.
func IncidentsFromBars(pkg string, bars []Bar) []Incident {
	var incs []Incident
	var curType string
	var curStart, curEnd int64
	flush := func() {
		if curType != "" {
			incs = append(incs, Incident{
				Package: pkg, Type: curType,
				Start: curStart, End: curEnd + 60,
				Minutes: int((curEnd-curStart)/60) + 1,
			})
		}
	}
	for _, b := range bars {
		if b.Status == "down" || b.Status == "degraded" {
			if curType == b.Status {
				curEnd = b.T
			} else {
				flush()
				curType, curStart, curEnd = b.Status, b.T, b.T
			}
		} else if curType != "" {
			flush()
			curType = ""
		}
	}
	flush()
	return incs
}

// RollupIncidents enumerates every Down/Degraded incident across all packages over
// the window, using the authoritative cross-vantage rollup — the same source and
// algorithm as the live status and as the credit engine.
func RollupIncidents(ctx context.Context, ch *chstore.Client, db string, windowMin int, s SLO) ([]Incident, error) {
	return RollupIncidentsAlts(ctx, ch, db, windowMin, s, nil)
}

// RollupIncidentsAlts is RollupIncidents over RollupPackagesAlts.
func RollupIncidentsAlts(ctx context.Context, ch *chstore.Client, db string, windowMin int, s SLO, alts []Package) ([]Incident, error) {
	prs, err := RollupPackagesAlts(ctx, ch, db, windowMin, s, alts)
	if err != nil {
		return nil, err
	}
	var out []Incident
	for _, pr := range prs {
		out = append(out, IncidentsFromBars(pr.Package, pr.Bars)...)
	}
	return out, nil
}

// Fetch returns the current cross-vantage status per package (Down only when ALL
// vantages are down). Used by /api/status (complete=false) and the monitor
// (complete=true, see statuses).
func Fetch(ctx context.Context, ch *chstore.Client, db string, s SLO, complete bool) ([]Status, error) {
	return FetchAlts(ctx, ch, db, s, complete, nil)
}

// FetchAlts is Fetch over the fetched minutes after MergeAlts (alts: the "packages"
// list). With nil alts it is Fetch.
func FetchAlts(ctx context.Context, ch *chstore.Client, db string, s SLO, complete bool, alts []Package) ([]Status, error) {
	w := s.withDefaults().DegradedForMin + 5
	data, err := fetchMinutes(ctx, ch, db, w)
	if err != nil {
		return nil, err
	}
	return statuses(MergeAlts(data, alts), time.Now(), w, s, complete), nil
}

// statuses reduces fetched minutes to one Status per package, as RollupPackages does.
// complete=true (monitor) evaluates as of 90s ago, i.e. at the newest minute that closed
// >= 30s ago: a minute still filling (rows land seconds late) can hold only a slow
// cross-region vantage and misreport the best-vantage response. The grid shrinks by 2
// so it stays inside the fetched window.
func statuses(data map[string]map[string]map[int64]Minute, now time.Time, windowMin int, s SLO, complete bool) []Status {
	if complete {
		now, windowMin = now.Add(-90*time.Second), windowMin-2
	}
	grid := Grid(now, windowMin)
	out := make([]Status, 0, len(data))
	for _, pkg := range sortedKeys(data) {
		pr := rollupPackageSeries(pkg, data[pkg], grid, now.Unix(), s)
		out = append(out, Status{
			Package: pkg, Vantage: pr.BestVantage, Status: pr.Status,
			ResponseMsAvg: pr.ResponseMsAvg, SuccessPct: pr.SuccessPct,
			Samples: pr.Samples, AgeSeconds: pr.AgeSeconds,
		})
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
