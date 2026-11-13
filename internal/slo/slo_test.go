package slo

import (
	"reflect"
	"testing"
	"time"
)

var testSLO = SLO{DegradedAvgMs: 50, DegradedForMin: 5, DownSuccessPct: 90, StaleSeconds: 120}

// TestEvaluatorConsecutiveDegraded verifies the contract trigger: avg > 50ms must
// persist for 5 CONSECUTIVE minutes before the state is Degraded; the run-up
// minutes are operational, and recovery (avg <= 50) resets immediately.
func TestEvaluatorConsecutiveDegraded(t *testing.T) {
	ev := NewEvaluator(testSLO)
	want := []string{
		"operational", // 1: over threshold, run=1
		"operational", // 2: run=2
		"operational", // 3: run=3
		"operational", // 4: run=4
		"degraded",    // 5: run=5 -> trigger
		"degraded",    // 6: stays degraded while over threshold
		"operational", // 7: recovered (<=50) -> reset
		"operational", // 8: over threshold again, run=1 (not retroactive)
	}
	avgs := []float64{60, 60, 60, 60, 60, 70, 40, 55}
	for i, a := range avgs {
		got := ev.Next(true, a, 100)
		if got != want[i] {
			t.Errorf("minute %d (avg %.0f): got %s want %s", i+1, a, got, want[i])
		}
	}
}

// TestEvaluatorDownAndGap: a sub-threshold success rate is Down immediately and
// resets the degraded run; a data gap also breaks the run.
func TestEvaluatorDownAndGap(t *testing.T) {
	ev := NewEvaluator(testSLO)
	for i := 0; i < 4; i++ {
		if got := ev.Next(true, 80, 100); got != "operational" {
			t.Fatalf("buildup %d: got %s", i, got)
		}
	}
	if got := ev.Next(true, 80, 50); got != "down" { // 50% success < 90 -> down
		t.Fatalf("down: got %s", got)
	}
	// run was reset by the down minute; need 5 fresh consecutive over-threshold.
	for i := 0; i < 4; i++ {
		if got := ev.Next(true, 80, 100); got != "operational" {
			t.Fatalf("post-down buildup %d: got %s", i, got)
		}
	}
	// a gap resets again
	if got := ev.Next(false, 0, 0); got != "no_data" {
		t.Fatalf("gap: got %s", got)
	}
	if got := ev.Next(true, 80, 100); got != "operational" {
		t.Fatalf("post-gap: got %s", got)
	}
}

func TestEvalSeriesCurrentStatus(t *testing.T) {
	now := int64(1_700_000_000)
	now = now - now%60
	grid := make([]int64, 8)
	for i := range grid {
		grid[i] = now - int64(7-i)*60
	}
	// All minutes over threshold and recent -> degraded at the latest data minute.
	bm := map[int64]Minute{}
	for _, t0 := range grid {
		bm[t0] = Minute{T: t0, ResponseMsAvg: 70, SuccessPct: 100, Samples: 10}
	}
	st := rollupSeries(bm, grid, now+10, testSLO).Current
	if st.Status != "degraded" {
		t.Fatalf("expected degraded, got %s", st.Status)
	}

	// Stale: newest data minute far in the past -> no_data regardless of values.
	st = rollupSeries(bm, grid, now+10_000, testSLO).Current
	if st.Status != "no_data" {
		t.Fatalf("expected no_data when stale, got %s (age %d)", st.Status, st.AgeSeconds)
	}
}

// TestUptimeHalfWeight: Availability% = (withData − down − ½·degraded)/withData.
// 10 minutes: 6 operational, 1 down, then 5 consecutive over-threshold of which the
// last yields the degraded trigger... so we craft a clean case: 8 operational,
// 1 down, plus a sustained degraded tail to get exactly N degraded minutes.
func TestUptimeHalfWeight(t *testing.T) {
	now := int64(1_700_000_000)
	now -= now % 60
	const n = 12
	grid := make([]int64, n)
	for i := range grid {
		grid[i] = now - int64(n-1-i)*60
	}
	bm := map[int64]Minute{}
	// minutes 0..5: operational (avg 10, 100%)
	for i := 0; i <= 5; i++ {
		bm[grid[i]] = Minute{T: grid[i], ResponseMsAvg: 10, SuccessPct: 100, Samples: 10}
	}
	// minute 6: down (success 0)
	bm[grid[6]] = Minute{T: grid[6], ResponseMsAvg: 10, SuccessPct: 0, Samples: 10}
	// minutes 7..11: avg 70 (over threshold). With DegradedForMin=5, only the 5th
	// consecutive over-threshold minute (index 11) is "degraded"; 7..10 are
	// operational run-up. So degraded=1, down=1, withData=12.
	for i := 7; i <= 11; i++ {
		bm[grid[i]] = Minute{T: grid[i], ResponseMsAvg: 70, SuccessPct: 100, Samples: 10}
	}
	vr := rollupSeries(bm, grid, now+10, testSLO)
	// uptime = (12 - 1 - 0.5*1)/12*100 = 10.5/12*100 = 87.5
	if vr.UptimePct != 87.5 {
		t.Fatalf("uptime: got %v want 87.5", vr.UptimePct)
	}
	if vr.Current.Status != "degraded" {
		t.Fatalf("current: got %s want degraded", vr.Current.Status)
	}
}

// TestCrossVantageDownRequiresAll: a package is Down only when EVERY vantage is
// down; if any vantage is available, the package is not Down.
func TestCrossVantageDownRequiresAll(t *testing.T) {
	now := int64(1_700_000_000)
	now -= now % 60
	grid := make([]int64, 8)
	for i := range grid {
		grid[i] = now - int64(7-i)*60
	}
	bv := map[string]map[int64]Minute{"us": {}, "eu": {}}
	for _, tt := range grid {
		bv["us"][tt] = Minute{T: tt, ResponseMsAvg: 200, SuccessPct: 0, Samples: 10}  // down
		bv["eu"][tt] = Minute{T: tt, ResponseMsAvg: 30, SuccessPct: 100, Samples: 10} // up
	}
	pr := rollupPackageSeries("pkg", bv, grid, now+10, testSLO)
	if pr.Status != "operational" {
		t.Fatalf("one vantage down, one up -> package must NOT be down; got %s", pr.Status)
	}
	if pr.BestVantage != "eu" {
		t.Fatalf("best available vantage should be eu, got %q", pr.BestVantage)
	}
	if pr.UptimePct != 100 {
		t.Fatalf("with one vantage always up, uptime should be 100, got %v", pr.UptimePct)
	}

	// Now BOTH vantages down at every minute -> package Down, uptime 0.
	for _, tt := range grid {
		bv["eu"][tt] = Minute{T: tt, ResponseMsAvg: 200, SuccessPct: 0, Samples: 10}
	}
	pr = rollupPackageSeries("pkg", bv, grid, now+10, testSLO)
	if pr.Status != "down" {
		t.Fatalf("all vantages down -> package must be down; got %s", pr.Status)
	}
	if pr.UptimePct != 0 {
		t.Fatalf("all-down uptime should be 0, got %v", pr.UptimePct)
	}
}

// TestNonHomeVantageNotDegraded: a far vantage measuring a product cross-region (high
// latency but reachable) must NOT degrade the PACKAGE — only the home (lowest-latency)
// vantage's latency feeds the package verdict. The far vantage's own detail shows
// its own state (degraded by the consecutive rule on its own series).
func TestNonHomeVantageNotDegraded(t *testing.T) {
	now := int64(1_700_000_000)
	now -= now % 60
	grid := make([]int64, 8)
	for i := range grid {
		grid[i] = now - int64(7-i)*60
	}
	bv := map[string]map[int64]Minute{"us": {}, "eu": {}}
	for _, tt := range grid {
		bv["us"][tt] = Minute{T: tt, ResponseMsAvg: 4, SuccessPct: 100, Samples: 10}   // home, fast
		bv["eu"][tt] = Minute{T: tt, ResponseMsAvg: 200, SuccessPct: 100, Samples: 10} // cross-region, far
	}
	pr := rollupPackageSeries("pkg", bv, grid, now+10, testSLO)
	if pr.Status != "operational" {
		t.Fatalf("package should be operational (home=4ms), got %s", pr.Status)
	}
	byVan := map[string]VantageRollup{}
	for _, v := range pr.Vantages {
		byVan[v.Vantage] = v
	}
	if pr.UptimePct != 100 {
		t.Fatalf("package uptime should be 100 (far vantage does not count), got %v", pr.UptimePct)
	}
	for _, b := range pr.Bars {
		if b.Status != "operational" {
			t.Fatalf("package bar at %d should be operational, got %s", b.T, b.Status)
		}
	}
	// Far vantage, own state: 200ms on all 8 minutes -> minutes 5..8 degraded
	// (consecutive rule), uptime = (8 - 0.5*4)/8 = 75.
	if byVan["eu"].Current.Status != "degraded" {
		t.Fatalf("far EU vantage (200ms sustained) shows its own state degraded, got %s", byVan["eu"].Current.Status)
	}
	for i, b := range byVan["eu"].Bars {
		want := "operational"
		if i >= testSLO.DegradedForMin-1 {
			want = "degraded"
		}
		if b.Status != want {
			t.Fatalf("far EU bar %d: got %s want %s", i, b.Status, want)
		}
	}
	if byVan["eu"].UptimePct != 75 {
		t.Fatalf("far vantage uptime: got %v want 75", byVan["eu"].UptimePct)
	}
	if byVan["us"].UptimePct != 100 {
		t.Fatalf("home vantage uptime should be 100, got %v", byVan["us"].UptimePct)
	}
	if byVan["us"].Current.Status != "operational" {
		t.Fatalf("home US vantage should be operational, got %s", byVan["us"].Current.Status)
	}

	// Genuine degradation: home vantage itself slow (>50ms) for the whole window ->
	// home vantage AND package degraded; far vantage degraded on its own series.
	for _, tt := range grid {
		bv["us"][tt] = Minute{T: tt, ResponseMsAvg: 80, SuccessPct: 100, Samples: 10}
	}
	pr = rollupPackageSeries("pkg", bv, grid, now+10, testSLO)
	if pr.Status != "degraded" {
		t.Fatalf("home vantage 80ms sustained -> package degraded, got %s", pr.Status)
	}
	for _, v := range pr.Vantages {
		if v.Vantage == "us" && v.Current.Status != "degraded" {
			t.Fatalf("home US vantage should be degraded, got %s", v.Current.Status)
		}
		if v.Vantage == "eu" && v.Current.Status != "degraded" {
			t.Fatalf("far EU vantage (200ms sustained) should be degraded, got %s", v.Current.Status)
		}
	}
}

func TestOverallWorstWins(t *testing.T) {
	if s, _ := Overall([]string{"operational", "degraded", "operational"}); s != "degraded" {
		t.Fatalf("got %s", s)
	}
	if s, _ := Overall([]string{"operational", "down", "degraded"}); s != "down" {
		t.Fatalf("got %s", s)
	}
	if s, _ := Overall([]string{"no_data", "operational"}); s != "operational" {
		t.Fatalf("no_data should rank below operational, got %s", s)
	}
}

func TestDefaults(t *testing.T) {
	got := SLO{}.withDefaults()
	if got.DegradedAvgMs != 50 || got.DegradedForMin != 5 || got.DownSuccessPct != 90 || got.StaleSeconds != 120 {
		t.Fatalf("unexpected defaults: %+v", got)
	}
}

// TestIncidentsFromBars verifies incident-window enumeration mirrors the staff-dash
// port: contiguous down/degraded runs become one incident each (start inclusive, end
// exclusive = last impacted minute + 60), a type change splits them, and
// operational/no_data minutes end the current incident.
func TestIncidentsFromBars(t *testing.T) {
	b := func(t0 int64, st string) Bar { return Bar{T: t0, Status: st} }
	// minutes at 0,60,120,...: op, down, down, op, degraded, degraded, no_data
	bars := []Bar{
		b(0, "operational"),
		b(60, "down"), b(120, "down"),
		b(180, "operational"),
		b(240, "degraded"), b(300, "degraded"),
		b(360, "no_data"),
	}
	got := IncidentsFromBars("isp", bars)
	if len(got) != 2 {
		t.Fatalf("want 2 incidents, got %d: %+v", len(got), got)
	}
	if got[0] != (Incident{Package: "isp", Type: "down", Start: 60, End: 180, Minutes: 2}) {
		t.Errorf("down incident wrong: %+v", got[0])
	}
	if got[1] != (Incident{Package: "isp", Type: "degraded", Start: 240, End: 360, Minutes: 2}) {
		t.Errorf("degraded incident wrong: %+v", got[1])
	}

	// Adjacent down→degraded with no operational gap must split into two incidents.
	split := IncidentsFromBars("x", []Bar{b(0, "down"), b(60, "degraded")})
	if len(split) != 2 || split[0].Type != "down" || split[1].Type != "degraded" {
		t.Errorf("adjacent down->degraded must split: %+v", split)
	}
}

// TestMonitorIgnoresFillingMinute reproduces the isp-eu alert of 2026-10-27 21:56:17
// ("degraded -> operational (response 117ms)"): the newest minute held only the slow
// cross-region vantage's first row (117ms), the fast home vantages' rows had not
// landed yet. The monitor's headline must come from a settled minute, not 117.
func TestMonitorIgnoresFillingMinute(t *testing.T) {
	t0 := int64(1_700_000_000)
	t0 -= t0 % 60 // start of the minute still filling
	data := map[string]map[string]map[int64]Minute{"isp-eu": {"us": {}, "eu": {}}}
	for i := int64(1); i <= 10; i++ {
		m := t0 - i*60
		data["isp-eu"]["us"][m] = Minute{T: m, ResponseMsAvg: 135, SuccessPct: 100, Samples: 3}
		data["isp-eu"]["eu"][m] = Minute{T: m, ResponseMsAvg: 65, SuccessPct: 100, Samples: 3}
	}
	data["isp-eu"]["eu"][t0-60] = Minute{T: t0 - 60, ResponseMsAvg: 46, SuccessPct: 100, Samples: 3}
	data["isp-eu"]["us"][t0] = Minute{T: t0, ResponseMsAvg: 117, SuccessPct: 100, Samples: 1} // only row so far

	// /api/status keeps reading the newest minute (unchanged behaviour).
	if got := statuses(data, time.Unix(t0+17, 0), 10, testSLO, false)[0]; got.ResponseMsAvg != 117 {
		t.Fatalf("complete=false: got %vms, want 117 (unchanged)", got.ResponseMsAvg)
	}
	// Monitor reads the newest minute closed >= 30s ago, via the fast home vantage:
	// 17s into the filling minute -> t0-120 (65ms); 5s after t0 closed -> t0-60 (46ms).
	for _, c := range []struct {
		now    int64
		status string
		ms     float64
	}{{t0 + 17, "degraded", 65}, {t0 + 65, "operational", 46}} {
		got := statuses(data, time.Unix(c.now, 0), 10, testSLO, true)[0]
		if got.ResponseMsAvg != c.ms || got.Vantage != "eu" || got.Status != c.status {
			t.Fatalf("now=t0+%ds: got %s %vms via %q, want %s %vms via eu", c.now-t0, got.Status, got.ResponseMsAvg, got.Vantage, c.status, c.ms)
		}
	}
}

// Overview trim (trimRollup). The grid mirrors RollupPackagesTrimAlts: 90 blocks +
// DegradedForMin (5) warm-up + 1 race minute = 96 fetched minutes.
const trimBlocks = 90

var trimFetched = trimBlocks + testSLO.withDefaults().DegradedForMin + 1

// trimGrid returns an n-minute grid whose newest minute starts at a fixed aligned
// time, and a `now` 10 s into that minute (so the newest data minute is fresh).
func trimGrid(n int) ([]int64, int64) {
	const end = int64(1_700_000_040) // minute-aligned
	return Grid(time.Unix(end, 0), n), end + 10
}

// series fills byMinute for the grid indices where keep(i) is true.
func series(grid []int64, avg float64, keep func(i int) bool) map[int64]Minute {
	m := map[int64]Minute{}
	for i, t := range grid {
		if keep(i) {
			m[t] = Minute{T: t, ResponseMsAvg: avg, SuccessPct: 100, Samples: 10}
		}
	}
	return m
}

func all(int) bool { return true }

// TestTrimRollupDegradedRunStartsBeforeWindow: a degraded run that began before the
// displayed window reads degraded from the first displayed bar, because the warm-up
// minutes are classified before the trim. Control: with only one extra minute (the
// old 91-minute fetch) the first displayed bars read operational.
func TestTrimRollupDegradedRunStartsBeforeWindow(t *testing.T) {
	grid, now := trimGrid(trimFetched)
	pr := rollupPackageSeries("pkg", map[string]map[int64]Minute{"us": series(grid, 80, all)}, grid, now, testSLO)
	got := trimRollup(pr, trimBlocks, now, testSLO)
	if len(got.Bars) != trimBlocks {
		t.Fatalf("len: got %d want %d", len(got.Bars), trimBlocks)
	}
	for i, b := range got.Bars {
		if b.Status != "degraded" {
			t.Fatalf("bar %d: got %s want degraded", i, b.Status)
		}
	}
	for i, b := range got.Vantages[0].Bars {
		if b.Status != "degraded" {
			t.Fatalf("vantage bar %d: got %s want degraded", i, b.Status)
		}
	}
	if got.Status != "degraded" || got.UptimePct != 50 {
		t.Fatalf("current: got %s %v%%, want degraded 50%%", got.Status, got.UptimePct)
	}

	// Control: 91 fetched, 90 shown. Kept indices 1..90 carry runs 2,3,4,5,...: the
	// first DegradedForMin-2 displayed bars are operational run-up.
	grid, now = trimGrid(trimBlocks + 1)
	pr = rollupPackageSeries("pkg", map[string]map[int64]Minute{"us": series(grid, 80, all)}, grid, now, testSLO)
	old := trimRollup(pr, trimBlocks, now, testSLO)
	for i := 0; i < testSLO.DegradedForMin-2; i++ {
		if old.Bars[i].Status != "operational" {
			t.Fatalf("control bar %d: got %s want operational", i, old.Bars[i].Status)
		}
	}
	if old.Bars[testSLO.DegradedForMin-2].Status != "degraded" {
		t.Fatalf("control bar %d: got %s want degraded", testSLO.DegradedForMin-2, old.Bars[testSLO.DegradedForMin-2].Status)
	}
}

// TestTrimRollupNewestMinuteEmpty: the newest minute has no data (race), so the
// newest block is dropped and the rest from the oldest end: kept = indices 5..94.
func TestTrimRollupNewestMinuteEmpty(t *testing.T) {
	grid, now := trimGrid(trimFetched)
	n := len(grid)
	data := series(grid, 10, func(i int) bool { return i < n-1 })
	pr := rollupPackageSeries("pkg", map[string]map[int64]Minute{"us": data}, grid, now, testSLO)
	got := trimRollup(pr, trimBlocks, now, testSLO)
	lo := n - trimBlocks - 1
	if len(got.Bars) != trimBlocks || got.Bars[0].T != grid[lo] || got.Bars[trimBlocks-1].T != grid[n-2] {
		t.Fatalf("kept %d bars %d..%d, want %d bars %d..%d", len(got.Bars), got.Bars[0].T, got.Bars[len(got.Bars)-1].T, trimBlocks, grid[lo], grid[n-2])
	}
	for _, b := range got.Bars {
		if b.T == grid[n-1] {
			t.Fatalf("newest (empty) minute %d must be dropped", b.T)
		}
	}
	if got.Status != "operational" || got.AgeSeconds != now-grid[n-2] {
		t.Fatalf("current: got %s age %d, want operational age %d", got.Status, got.AgeSeconds, now-grid[n-2])
	}
}

// TestTrimRollupNewestMinuteHasData: newest minute kept, all dropped blocks from the
// oldest end: kept = indices 6..95.
func TestTrimRollupNewestMinuteHasData(t *testing.T) {
	grid, now := trimGrid(trimFetched)
	n := len(grid)
	pr := rollupPackageSeries("pkg", map[string]map[int64]Minute{"us": series(grid, 10, all)}, grid, now, testSLO)
	got := trimRollup(pr, trimBlocks, now, testSLO)
	lo := n - trimBlocks
	if len(got.Bars) != trimBlocks || got.Bars[0].T != grid[lo] || got.Bars[trimBlocks-1].T != grid[n-1] {
		t.Fatalf("kept %d bars %d..%d, want %d bars %d..%d", len(got.Bars), got.Bars[0].T, got.Bars[len(got.Bars)-1].T, trimBlocks, grid[lo], grid[n-1])
	}
	if got.AgeSeconds != now-grid[n-1] {
		t.Fatalf("age: got %d want %d", got.AgeSeconds, now-grid[n-1])
	}
}

// TestTrimRollupVantagesTrimmedIdentically: every vantage keeps the same timestamps
// as the package, in the race and the non-race case, and its Current is recomputed
// from its kept bars only.
func TestTrimRollupVantagesTrimmedIdentically(t *testing.T) {
	grid, now := trimGrid(trimFetched)
	n := len(grid)
	warmup := func(i int) bool { return i < n-trimBlocks-1 } // data only in dropped warm-up minutes
	for _, race := range []bool{true, false} {
		a := series(grid, 10, func(i int) bool { return !race || i < n-1 })
		b := series(grid, 10, warmup)
		pr := rollupPackageSeries("pkg", map[string]map[int64]Minute{"a": a, "b": b}, grid, now, testSLO)
		got := trimRollup(pr, trimBlocks, now, testSLO)
		for _, vr := range got.Vantages {
			if len(vr.Bars) != trimBlocks {
				t.Fatalf("race=%v vantage %s: len %d want %d", race, vr.Vantage, len(vr.Bars), trimBlocks)
			}
			for i := range vr.Bars {
				if vr.Bars[i].T != got.Bars[i].T {
					t.Fatalf("race=%v vantage %s bar %d: t %d, package t %d", race, vr.Vantage, i, vr.Bars[i].T, got.Bars[i].T)
				}
			}
			if vr.Current.Vantage != vr.Vantage {
				t.Fatalf("race=%v: Current.Vantage %q want %q", race, vr.Current.Vantage, vr.Vantage)
			}
		}
		// b's only data was in the dropped warm-up minutes: no data left in the window.
		vb := got.Vantages[1]
		if vb.Current.Status != "no_data" || vb.UptimePct != 100 || barsHaveData(vb.Bars) {
			t.Fatalf("race=%v vantage b: got %s %v%%, want no_data 100%% and no data bars", race, vb.Current.Status, vb.UptimePct)
		}
		if va := got.Vantages[0]; va.Current.Status != "operational" {
			t.Fatalf("race=%v vantage a: got %s want operational", race, va.Current.Status)
		}
	}

	// A vantage whose bar count differs from the package's is left as is.
	pr := PackageRollup{Bars: make([]Bar, trimFetched), Vantages: []VantageRollup{{Vantage: "x", Bars: make([]Bar, 3)}}}
	if got := trimRollup(pr, trimBlocks, now, testSLO); len(got.Vantages[0].Bars) != 3 {
		t.Fatalf("mismatched vantage: len %d want 3", len(got.Vantages[0].Bars))
	}
}

// TestTrimRollupNoDataInsideWindowStaysNoData: a gap inside the window stays no_data,
// breaks the degraded run (bars after it are run-up until DegradedForMin consecutive
// over-threshold minutes), and uptime counts only data bars.
func TestTrimRollupNoDataInsideWindowStaysNoData(t *testing.T) {
	grid, now := trimGrid(trimFetched)
	gap := func(i int) bool { return i >= 40 && i <= 42 }
	data := series(grid, 80, func(i int) bool { return !gap(i) })
	pr := rollupPackageSeries("pkg", map[string]map[int64]Minute{"us": data}, grid, now, testSLO)
	got := trimRollup(pr, trimBlocks, now, testSLO)
	lo := len(grid) - trimBlocks
	for k, b := range got.Bars {
		i := k + lo
		want := "degraded"
		switch {
		case gap(i):
			want = "no_data"
		case i > 42 && i < 43+testSLO.DegradedForMin-1:
			want = "operational"
		}
		if b.Status != want {
			t.Fatalf("grid index %d: got %s want %s", i, b.Status, want)
		}
	}
	// 87 data bars: 4 operational, 83 degraded -> (87 - 41.5)/87 = 52.30%.
	if got.UptimePct != 52.3 {
		t.Fatalf("uptime: got %v want 52.3", got.UptimePct)
	}
}

// TestTrimRollupExactBlocksAndShort: n <= blocks and blocks < 1 return the rollup
// unchanged; n == blocks+1 keeps the old rule (drop oldest, or newest when empty).
func TestTrimRollupExactBlocksAndShort(t *testing.T) {
	mk := func(n int, newestEmpty bool) PackageRollup {
		bars := make([]Bar, n)
		for i := range bars {
			bars[i] = Bar{T: int64(i) * 60, Status: "operational", ResponseMsAvg: 10, SuccessPct: 100, Samples: 1}
		}
		if newestEmpty {
			bars[n-1] = Bar{T: int64(n-1) * 60, Status: "no_data"}
		}
		vb := append([]Bar(nil), bars...)
		return PackageRollup{Status: "x", UptimePct: 1, Bars: bars, Vantages: []VantageRollup{{Vantage: "v", Bars: vb}}}
	}
	for _, c := range []struct {
		name   string
		n, blk int
	}{{"exact", 4, 4}, {"short", 3, 4}, {"zero blocks", 4, 0}, {"negative blocks", 4, -1}} {
		pr := mk(c.n, false)
		if got := trimRollup(pr, c.blk, 1000, testSLO); !reflect.DeepEqual(got, pr) {
			t.Fatalf("%s: rollup changed: %+v", c.name, got)
		}
	}
	for _, c := range []struct {
		newestEmpty bool
		firstT      int64
	}{{false, 60}, {true, 0}} {
		got := trimRollup(mk(5, c.newestEmpty), 4, 300, testSLO)
		if len(got.Bars) != 4 || got.Bars[0].T != c.firstT || got.Vantages[0].Bars[0].T != c.firstT {
			t.Fatalf("blocks+1 newestEmpty=%v: got %d bars from %d, want 4 from %d", c.newestEmpty, len(got.Bars), got.Bars[0].T, c.firstT)
		}
	}
}

// TestTrimRollupDoesNotReclassify: kept bars carry the statuses computed over the full
// fetched sequence, element for element.
func TestTrimRollupDoesNotReclassify(t *testing.T) {
	grid, now := trimGrid(trimFetched)
	n := len(grid)
	for _, race := range []bool{true, false} {
		data := map[int64]Minute{}
		for i, tt := range grid {
			if race && i == n-1 {
				continue
			}
			avg, succ := 80.0, 100.0
			if i%17 == 0 {
				avg = 10
			}
			if i%29 == 0 {
				succ = 0
			}
			data[tt] = Minute{T: tt, ResponseMsAvg: avg, SuccessPct: succ, Samples: 5}
		}
		full := rollupPackageSeries("pkg", map[string]map[int64]Minute{"us": data}, grid, now, testSLO)
		want := append([]Bar(nil), full.Bars...)
		got := trimRollup(full, trimBlocks, now, testSLO)
		lo := n - trimBlocks
		if race {
			lo--
		}
		if !reflect.DeepEqual(got.Bars, want[lo:lo+trimBlocks]) {
			t.Fatalf("race=%v: kept bars differ from the untrimmed classification", race)
		}
	}
}
