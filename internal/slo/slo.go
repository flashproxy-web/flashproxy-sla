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
	"fmt"
	"sort"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// SLO are the published thresholds.
type SLO struct {
	DegradedAvgMs  float64 `json:"degraded_avg_ms"`  // avg round-trip response-ms (ttfb) above which a minute is over threshold
	DegradedForMin int     `json:"degraded_for_min"` // consecutive over-threshold minutes that trigger Degraded
	DownSuccessPct float64 `json:"down_success_pct"` // connect success% below which a minute is Down
	StaleSeconds   int64   `json:"stale_seconds"`    // age of newest data beyond which status is no_data
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
	Samples       float64 `json:"samples"`
	AgeSeconds    int64   `json:"age_seconds"`
}

// Minute is one per-minute rollup of the connect scenario's round-trip response time (ttfb_ms).
type Minute struct {
	T             int64
	ResponseMsAvg float64
	SuccessPct    float64
	Samples       float64
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
	Samples       float64 `json:"samples"`
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
		t := int64(chstore.Num(m, "t"))
		out[pkg][van][t] = Minute{
			T: t, ResponseMsAvg: chstore.Num(m, "response_ms_avg"),
			SuccessPct: chstore.Num(m, "success_pct"), Samples: chstore.Num(m, "samples"),
		}
	}
	return out, nil
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
	Samples       float64
	AgeSeconds    int64
	UptimePct     float64
	Bars          []Bar
	Vantages      []VantageRollup
}

// rollupPackageSeries computes the package's authoritative CROSS-VANTAGE rollup plus
// the per-vantage detail for the UI.
//
// The latency-Degraded threshold is applied ONLY to the home (lowest-latency
// available) vantage each minute. A vantage measuring a product cross-region has a
// geographic latency floor (e.g. ~200 ms EU→US) that would always exceed the 50 ms
// threshold — so non-home vantages are classified on AVAILABILITY only (reachable ⇒
// operational), never falsely "degraded". This matches the SLA, where Degraded is a
// best-vantage concept. Down still requires unavailable-everywhere.
func rollupPackageSeries(byVantage map[string]map[int64]Minute, grid []int64, now int64, s SLO) PackageRollup {
	s = s.withDefaults()
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
		var bestAvg, samples float64
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
	crossAt := make(map[int64]string, len(cross.Bars))
	for _, b := range cross.Bars {
		crossAt[b.T] = b.Status
	}
	bestVan := ""
	for _, t := range grid { // home vantage at the most recent data minute (headline)
		if _, ok := synth[t]; ok {
			bestVan = bestVanAt[t]
		}
	}

	// 2) Per-vantage detail: the home vantage mirrors the package verdict (may be
	//    Degraded); every other vantage is availability-only, so cross-region latency
	//    never reads as degradation.
	detail := make([]VantageRollup, 0, len(vans))
	for _, v := range vans {
		bars := make([]Bar, len(grid))
		var withData, down, deg int
		var lastT int64 = -1
		cur := Status{Status: "no_data", Vantage: v}
		for i, t := range grid {
			m, ok := byVantage[v][t]
			st := "no_data"
			if ok {
				switch {
				case m.SuccessPct < s.DownSuccessPct:
					st = "down"
				case bestVanAt[t] == v:
					st = crossAt[t] // home vantage: full verdict (may be degraded)
				default:
					st = "operational" // reachable but not the home vantage ⇒ not degraded
				}
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
	s = s.withDefaults()
	data, err := fetchMinutes(ctx, ch, db, windowMin)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	grid := Grid(now, windowMin)
	out := make([]PackageRollup, 0, len(data))
	for _, pkg := range sortedKeys(data) {
		pr := rollupPackageSeries(data[pkg], grid, now.Unix(), s)
		pr.Package = pkg
		out = append(out, pr)
	}
	return out, nil
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
	prs, err := RollupPackages(ctx, ch, db, windowMin, s)
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
	w := s.withDefaults().DegradedForMin + 5
	data, err := fetchMinutes(ctx, ch, db, w)
	if err != nil {
		return nil, err
	}
	return statuses(data, time.Now(), w, s, complete), nil
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
		pr := rollupPackageSeries(data[pkg], grid, now.Unix(), s)
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
