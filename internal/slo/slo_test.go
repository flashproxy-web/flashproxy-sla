package slo

import (
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
	pr := rollupPackageSeries(bv, grid, now+10, testSLO)
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
	pr = rollupPackageSeries(bv, grid, now+10, testSLO)
	if pr.Status != "down" {
		t.Fatalf("all vantages down -> package must be down; got %s", pr.Status)
	}
	if pr.UptimePct != 0 {
		t.Fatalf("all-down uptime should be 0, got %v", pr.UptimePct)
	}
}

// TestNonHomeVantageNotDegraded: a far vantage measuring a product cross-region (high
// latency but reachable) must NOT be labeled degraded — only the home (lowest-latency)
// vantage's latency feeds the Degraded verdict.
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
	pr := rollupPackageSeries(bv, grid, now+10, testSLO)
	if pr.Status != "operational" {
		t.Fatalf("package should be operational (home=4ms), got %s", pr.Status)
	}
	byVan := map[string]VantageRollup{}
	for _, v := range pr.Vantages {
		byVan[v.Vantage] = v
	}
	if byVan["eu"].Current.Status != "operational" {
		t.Fatalf("far EU vantage (200ms, reachable) must be operational, got %s", byVan["eu"].Current.Status)
	}
	if byVan["eu"].UptimePct != 100 {
		t.Fatalf("far vantage uptime should be 100 (not degraded), got %v", byVan["eu"].UptimePct)
	}
	if byVan["us"].Current.Status != "operational" {
		t.Fatalf("home US vantage should be operational, got %s", byVan["us"].Current.Status)
	}

	// Genuine degradation: home vantage itself slow (>50ms) for the whole window ->
	// home vantage AND package degraded; far vantage still operational.
	for _, tt := range grid {
		bv["us"][tt] = Minute{T: tt, ResponseMsAvg: 80, SuccessPct: 100, Samples: 10}
	}
	pr = rollupPackageSeries(bv, grid, now+10, testSLO)
	if pr.Status != "degraded" {
		t.Fatalf("home vantage 80ms sustained -> package degraded, got %s", pr.Status)
	}
	for _, v := range pr.Vantages {
		if v.Vantage == "us" && v.Current.Status != "degraded" {
			t.Fatalf("home US vantage should be degraded, got %s", v.Current.Status)
		}
		if v.Vantage == "eu" && v.Current.Status != "operational" {
			t.Fatalf("far EU vantage should stay operational, got %s", v.Current.Status)
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
