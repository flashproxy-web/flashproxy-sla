package main

import (
	"context"
	"errors"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/slo"
)

const wkT0 = int64(1763000100) // a whole minute that is not on an even UTC hour

// wkBars returns n consecutive minute bars from start, all with status.
func wkBars(start int64, n int, status string) []slo.Bar {
	bars := make([]slo.Bar, n)
	for i := range bars {
		bars[i] = slo.Bar{T: start + int64(i)*60, Status: status}
	}
	return bars
}

// wkSet sets bars[from:to] to status with the given response time.
func wkSet(bars []slo.Bar, from, to int, status string, ms float64) {
	for i := from; i < to; i++ {
		bars[i].Status, bars[i].ResponseMsAvg = status, ms
	}
}

func blockMinutes(b weeklyBlock) int { return b.Op + b.Deg + b.Down + b.NoData }

func TestBucketBarsEndAligned(t *testing.T) {
	bars := wkBars(wkT0, weeklyWindowMin, "operational")
	blocks := bucketBars(bars, weeklyBlockMin)
	if len(blocks) != 84 {
		t.Fatalf("got %d blocks, want 84", len(blocks))
	}
	total := 0
	for i, b := range blocks {
		if blockMinutes(b) != 120 || (b.End-b.Start)/60 != 120 {
			t.Errorf("block %d: %d minutes, range %d min, want 120", i, blockMinutes(b), (b.End-b.Start)/60)
		}
		if i > 0 && b.Start != blocks[i-1].End {
			t.Errorf("block %d starts at %d, previous ends at %d", i, b.Start, blocks[i-1].End)
		}
		total += blockMinutes(b)
	}
	if total != weeklyWindowMin {
		t.Errorf("total %d minutes, want %d", total, weeklyWindowMin)
	}
	if blocks[0].Start != bars[0].T {
		t.Errorf("block 0 starts at %d, want the first bar %d", blocks[0].Start, bars[0].T)
	}
	if last := blocks[83]; last.Start != bars[weeklyWindowMin-120].T || last.End != bars[weeklyWindowMin-1].T+60 {
		t.Errorf("block 83 = [%d,%d), want the newest 120 minutes", last.Start, last.End)
	}
}

func TestBucketBarsShortOldest(t *testing.T) {
	blocks := bucketBars(wkBars(wkT0, 250, "operational"), 120)
	var got []int
	for _, b := range blocks {
		got = append(got, blockMinutes(b))
		if b.NoData != 0 {
			t.Errorf("phantom no_data minutes: %+v", b)
		}
	}
	if len(got) != 3 || got[0] != 10 || got[1] != 120 || got[2] != 120 {
		t.Errorf("block sizes %v, want [10 120 120]", got)
	}
	if !strings.Contains(blockTitle(blocks[0]), "(10 of 120 minutes)") {
		t.Errorf("short block title %q", blockTitle(blocks[0]))
	}
	for _, bars := range [][]slo.Bar{nil, {}} {
		if b := bucketBars(bars, 120); len(b) != 0 {
			t.Errorf("no bars gave %d blocks", len(b))
		}
	}
	if b := bucketBars(wkBars(wkT0, 5, "operational"), 0); b != nil {
		t.Errorf("size 0 gave %d blocks", len(b))
	}
}

// The race rule ends one package's window a minute earlier. The grid is aligned
// to that package's own newest bar, so it still has 84 full blocks.
func TestBucketBarsRaceShift(t *testing.T) {
	kept := bucketBars(wkBars(wkT0, weeklyWindowMin, "operational"), weeklyBlockMin)
	bars := wkBars(wkT0-60, weeklyWindowMin, "operational")
	shifted := bucketBars(bars, weeklyBlockMin)
	if len(shifted) != 84 {
		t.Fatalf("got %d blocks, want 84", len(shifted))
	}
	for i, b := range shifted {
		if blockMinutes(b) != 120 {
			t.Errorf("block %d has %d minutes", i, blockMinutes(b))
		}
		if b.Start != kept[i].Start-60 {
			t.Errorf("block %d starts at %d, want one minute before %d", i, b.Start, kept[i].Start)
		}
	}
	if got, want := shifted[83].End, bars[len(bars)-1].T+60; got != want {
		t.Errorf("last block ends at %d, want %d", got, want)
	}
}

func TestSegmentCounts(t *testing.T) {
	bars := wkBars(wkT0, 120, "operational")
	wkSet(bars, 0, 3, "down", 0)
	wkSet(bars, 10, 12, "degraded", 80)
	wkSet(bars, 50, 55, "no_data", 0)
	b := bucketBars(bars, 120)[0]
	if b.Down != 3 || b.Deg != 2 || b.NoData != 5 || b.Op != 110 {
		t.Errorf("counts %+v, want Down=3 Deg=2 NoData=5 Op=110", b)
	}
	if blockMinutes(b) != int((b.End-b.Start)/60) {
		t.Errorf("counts sum to %d, range is %d minutes", blockMinutes(b), (b.End-b.Start)/60)
	}
	lone := wkBars(wkT0, 120, "operational")
	lone[77].Status = "no_data"
	if b := bucketBars(lone, 120)[0]; b.NoData != 1 || b.Op != 119 || b.Label != "partial data" {
		t.Errorf("lone empty minute: %+v", b)
	}
}

func TestBlockLabelPrecedence(t *testing.T) {
	for _, c := range []struct {
		down, deg, noData, op int
		want                  string
	}{
		{1, 5, 5, 109, "down"},
		{1, 0, 0, 119, "down"},
		{1, 0, 119, 0, "down"},
		{0, 3, 5, 112, "degraded"},
		{0, 1, 0, 119, "degraded"},
		{0, 0, 1, 119, "partial data"},
		{0, 0, 120, 0, "partial data"}, // never "no data" for a block
		{0, 0, 0, 120, "operational"},
	} {
		if got := blockLabel(c.down, c.deg, c.noData); got != c.want {
			t.Errorf("down=%d deg=%d noData=%d op=%d: %q, want %q", c.down, c.deg, c.noData, c.op, got, c.want)
		}
	}
}

func TestBlockTitle(t *testing.T) {
	b := weeklyBlock{Start: 1762956300, End: 1762963500, Op: 112, Deg: 3, NoData: 5, Label: "partial data",
		Events: []blockEvent{{"degraded", 3}}}
	want := "2025-11-12 14:05-16:05 UTC: partial data - 112 ok, 3 degraded, 5 no data; events: degraded 3 min"
	if got := blockTitle(b); got != want {
		t.Errorf("title\n got %q\nwant %q", got, want)
	}
	empty := weeklyBlock{Start: 1762956300, End: 1762963500, NoData: 120, Label: "partial data"}
	if got := blockTitle(empty); !strings.HasSuffix(got, "partial data - 0 of 120 minutes with data") {
		t.Errorf("empty block title %q", got)
	}
	b.Events = []blockEvent{{"down", 1}, {"degraded", 2}, {"down", 3}, {"degraded", 4}, {"down", 5}}
	if got := blockTitle(b); !strings.HasSuffix(got, "; events: down 1 min, degraded 2 min, down 3 min, +2 more") {
		t.Errorf("many events title %q", got)
	}
}

func TestRowNoDataVsPartial(t *testing.T) {
	// The rollup reports 100 for a package without data; the page must not show it.
	row, evs := buildWeeklyRow(slo.PackageRollup{Package: "isp", UptimePct: 100, Bars: wkBars(wkT0, weeklyWindowMin, "no_data")}, Config{})
	if row.HasData || row.Uptime != "no data" || len(evs) != 0 {
		t.Errorf("no data package: HasData=%v Uptime=%q events=%d", row.HasData, row.Uptime, len(evs))
	}
	if len(row.Blocks) != 84 {
		t.Fatalf("got %d blocks", len(row.Blocks))
	}
	for i, b := range row.Blocks {
		if b.NoData != 120 || b.Label != "partial data" {
			t.Errorf("block %d: %+v", i, b)
		}
	}
	bars := wkBars(wkT0, weeklyWindowMin, "no_data")
	bars[4321].Status = "operational"
	row, _ = buildWeeklyRow(slo.PackageRollup{Package: "isp", UptimePct: 100, Bars: bars}, Config{})
	if !row.HasData || row.Uptime != "100.00%" || row.OpMin != 1 || row.NoDataMin != weeklyWindowMin-1 {
		t.Errorf("one data minute: %+v", row)
	}
	// A rollup without bars (not produced by the rollup, handled anyway).
	row, evs = buildWeeklyRow(slo.PackageRollup{Package: "isp", UptimePct: 100}, Config{})
	if row.HasData || row.Uptime != "no data" || len(row.Blocks) != 0 || len(evs) != 0 {
		t.Errorf("no bars: %+v", row)
	}
}

// The uptime is the rollup's figure, not one derived from the blocks: the bars
// below would give a different value.
func TestUptimePassThrough(t *testing.T) {
	bars := wkBars(wkT0, weeklyWindowMin, "operational")
	wkSet(bars, 100, 400, "down", 0)
	row, _ := buildWeeklyRow(slo.PackageRollup{Package: "isp", UptimePct: 99.5, Bars: bars}, Config{})
	if row.UptimePct != 99.5 || row.Uptime != "99.50%" {
		t.Errorf("UptimePct=%v Uptime=%q, want 99.5 and 99.50%%", row.UptimePct, row.Uptime)
	}
}

func TestOverallUptimePooled(t *testing.T) {
	rows := []weeklyRow{
		{OpMin: 900, DownMin: 50, DegradedMin: 50, NoDataMin: 80}, // 1000 data minutes
		{OpMin: 3000},
		{NoDataMin: 10080}, // no data: not pooled
	}
	// (4000 - 50 - 25) / 4000 = 98.125 -> 98.12 or 98.13 by float formatting
	got, ok := overallUptime(rows)
	if !ok || (got != "98.12%" && got != "98.13%") {
		t.Errorf("pooled = %q ok=%v", got, ok)
	}
	if got, ok := overallUptime(rows[2:]); ok || got != "no data" {
		t.Errorf("no data only: %q ok=%v", got, ok)
	}
	if got, ok := overallUptime(nil); ok || got != "no data" {
		t.Errorf("no rows: %q ok=%v", got, ok)
	}
	if got, _ := overallUptime([]weeklyRow{{OpMin: 150, DegradedMin: 50}}); got != "87.50%" {
		t.Errorf("half weight for degraded: %q, want 87.50%%", got)
	}
}

// wkEventBars holds five incidents: degraded from the first minute, degraded
// across the block 1/2 boundary, down then degraded back to back, down to the end.
func wkEventBars() []slo.Bar {
	bars := wkBars(wkT0, weeklyWindowMin, "operational")
	wkSet(bars, 0, 10, "degraded", 1200)
	bars[4].ResponseMsAvg = 1500
	wkSet(bars, 230, 250, "degraded", 1100)
	wkSet(bars, 5000, 5003, "down", 0)
	wkSet(bars, 5003, 5011, "degraded", 1300.5)
	wkSet(bars, weeklyWindowMin-10, weeklyWindowMin, "down", 0)
	return bars
}

func TestWeeklyEvents(t *testing.T) {
	pkgs := []slo.Package{{Short: "resi", Name: "Residential", TargetMs: 1000}, {Short: "isp"}}
	cfg := Config{Packages: pkgs, SLO: slo.SLO{PackageDegradedMs: slo.PackageTargets(pkgs)}}
	bars := wkEventBars()
	row, evs := buildWeeklyRow(slo.PackageRollup{Package: "resi", UptimePct: 99, Bars: bars}, cfg)
	if row.DownEvents != 2 || row.DegradedEvents != 3 || len(evs) != 5 {
		t.Fatalf("down=%d degraded=%d events=%d, want 2, 3, 5", row.DownEvents, row.DegradedEvents, len(evs))
	}
	// One incident over two blocks: 10 minutes in each.
	for _, i := range []int{1, 2} {
		if e := row.Blocks[i].Events; len(e) != 1 || e[0] != (blockEvent{"degraded", 10}) {
			t.Errorf("block %d events %+v, want degraded 10 min", i, e)
		}
	}
	if e := row.Blocks[41].Events; len(e) != 2 || e[0] != (blockEvent{"down", 3}) || e[1] != (blockEvent{"degraded", 8}) {
		t.Errorf("block 41 events %+v, want down 3 then degraded 8", e)
	}
	if len(row.Blocks[3].Events) != 0 {
		t.Errorf("quiet block has events %+v", row.Blocks[3].Events)
	}

	d := buildWeekly([]slo.PackageRollup{{Package: "resi", UptimePct: 99, Bars: bars}}, cfg)
	want := []weeklyEvent{
		{Severity: "down", Start: "2025-11-20 02:05 UTC", End: "ongoing at data end", Duration: "10m", Worst: "no vantage reachable", Ongoing: true},
		{Severity: "degraded", Start: "2025-11-16 13:38 UTC", End: "2025-11-16 13:46 UTC", Duration: "8m", Worst: "peak 1300.5 ms (limit 1000)"},
		{Severity: "down", Start: "2025-11-16 13:35 UTC", End: "2025-11-16 13:38 UTC", Duration: "3m", Worst: "no vantage reachable"},
		{Severity: "degraded", Start: "2025-11-13 06:05 UTC", End: "2025-11-13 06:25 UTC", Duration: "20m", Worst: "peak 1100 ms (limit 1000)"},
		{Severity: "degraded", Start: "before 2025-11-13 02:15 UTC", End: "2025-11-13 02:25 UTC", Duration: "at least 10m", Worst: "peak 1500 ms (limit 1000)", BeganBefore: true},
	}
	if len(d.Events) != len(want) || d.EventsTotal != 5 || d.EventsMore() != 0 {
		t.Fatalf("%d events, total %d, more %d", len(d.Events), d.EventsTotal, d.EventsMore())
	}
	for i, w := range want {
		g := d.Events[i]
		if g.Name != "Residential" || g.Package != "resi" || g.SeverityLabel != statusLabel[w.Severity] {
			t.Errorf("event %d: name %q package %q label %q", i, g.Name, g.Package, g.SeverityLabel)
		}
		g.Package, g.Name, g.SeverityLabel, g.start = "", "", "", 0
		if g != w {
			t.Errorf("event %d\n got %+v\nwant %+v", i, g, w)
		}
	}
	if d.WindowStart != bars[0].T || d.WindowEnd != bars[len(bars)-1].T+60 {
		t.Errorf("window [%d,%d)", d.WindowStart, d.WindowEnd)
	}

	// Two packages, ten events: the 8 newest, ties ordered by package; the
	// package without its own target uses the global limit.
	d = buildWeekly([]slo.PackageRollup{{Package: "resi", Bars: bars}, {Package: "isp", Bars: bars}}, cfg)
	if len(d.Events) != weeklyMaxEvents || d.EventsTotal != 10 || d.EventsMore() != 2 {
		t.Fatalf("%d events, total %d, more %d, want 8, 10, 2", len(d.Events), d.EventsTotal, d.EventsMore())
	}
	for i, e := range d.Events {
		if wantPkg := []string{"isp", "resi"}[i%2]; e.Package != wantPkg {
			t.Errorf("event %d is %s, want %s", i, e.Package, wantPkg)
		}
		if i > 0 && e.start > d.Events[i-1].start {
			t.Errorf("event %d is newer than event %d", i, i-1)
		}
	}
	if got := d.Events[2].Worst; got != "peak 1300.5 ms (limit 50)" {
		t.Errorf("isp worst %q, want the global limit 50", got)
	}

	d = buildWeekly([]slo.PackageRollup{{Package: "isp", Bars: wkBars(wkT0, weeklyWindowMin, "operational")}}, cfg)
	if len(d.Events) != 0 || d.EventsTotal != 0 || !d.OverallHasData || d.Overall != "100.00%" {
		t.Errorf("quiet week: %d events, overall %q", len(d.Events), d.Overall)
	}
	if d := buildWeekly(nil, cfg); len(d.Rows) != 0 || len(d.Events) != 0 || d.OverallHasData {
		t.Errorf("no packages: %+v", d)
	}
}

func TestWeeklyCacheRefresh(t *testing.T) {
	now := time.Unix(wkT0, 0)
	c := &weeklyCache{now: func() time.Time { return now }}
	ctx := context.Background()
	fetches := 0
	good := func(d *weeklyData) func(context.Context) (*weeklyData, error) {
		return func(context.Context) (*weeklyData, error) { fetches++; return d, nil }
	}
	bad := func(context.Context) (*weeklyData, error) { fetches++; return nil, errors.New("clickhouse down") }

	if v, ok := c.get(); ok || v != nil {
		t.Fatal("value before the first refresh")
	}
	if err := c.refresh(ctx, bad); err == nil || c.fails != 1 {
		t.Fatalf("failed first refresh: err=%v fails=%d", err, c.fails)
	}
	if _, ok := c.get(); ok {
		t.Fatal("a failed refresh stored a value")
	}
	first := &weeklyData{Overall: "first"}
	if err := c.refresh(ctx, good(first)); err != nil {
		t.Fatal(err)
	}
	if v, ok := c.get(); !ok || v != first || !v.FetchedAt.Equal(now) || c.fails != 0 {
		t.Fatalf("after success: ok=%v fetched=%v fails=%d", ok, v.FetchedAt, c.fails)
	}

	// Failures (an error, or no value without an error) keep the last good value.
	now = now.Add(5 * time.Minute)
	for i, f := range []func(context.Context) (*weeklyData, error){bad, good(nil)} {
		if err := c.refresh(ctx, f); err == nil {
			t.Errorf("failure %d returned no error", i)
		}
		if v, _ := c.get(); v != first || !v.FetchedAt.Equal(time.Unix(wkT0, 0)) || c.fails != i+1 {
			t.Errorf("failure %d: value replaced or FetchedAt moved (fails=%d)", i, c.fails)
		}
	}
	second := &weeklyData{Overall: "second"}
	if err := c.refresh(ctx, good(second)); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.get(); v != second || !v.FetchedAt.Equal(now) || c.fails != 0 {
		t.Errorf("recovery: value %q fails=%d", v.Overall, c.fails)
	}

	// get never fetches.
	before := fetches
	for i := 0; i < 100; i++ {
		c.get()
	}
	if fetches != before {
		t.Errorf("get fetched %d times", fetches-before)
	}

	// The fetch gets a deadline no later than weeklyFetchTimeout and is
	// cancelled through its context.
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	err := c.refresh(short, func(ctx context.Context) (*weeklyData, error) {
		if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > weeklyFetchTimeout {
			t.Errorf("fetch deadline %v ok=%v", dl, ok)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("slow fetch: %v, want deadline exceeded", err)
	}
	if v, _ := c.get(); v != second {
		t.Error("a cancelled fetch replaced the value")
	}
	_ = c.refresh(ctx, func(ctx context.Context) (*weeklyData, error) {
		if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > weeklyFetchTimeout {
			t.Errorf("fetch without a caller deadline: %v ok=%v", dl, ok)
		}
		return &weeklyData{}, nil
	})
}

// Refreshes requested while one is running do not start a second fetch.
func TestWeeklyCacheOneFetch(t *testing.T) {
	c := &weeklyCache{now: time.Now}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error)
	fetches := 0
	fetch := func(context.Context) (*weeklyData, error) {
		fetches++ // guarded by c.running; -race reports a second concurrent fetch
		entered <- struct{}{}
		<-release
		return &weeklyData{}, nil
	}
	go func() { done <- c.refresh(context.Background(), fetch) }()
	<-entered
	for i := 0; i < 20; i++ {
		if err := c.refresh(context.Background(), fetch); err != nil {
			t.Errorf("refresh during a running one: %v", err)
		}
	}
	if _, ok := c.get(); ok {
		t.Error("value before the fetch returned")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := c.get(); !ok || fetches != 1 {
		t.Errorf("fetches = %d, want 1", fetches)
	}
}

func TestWeeklyCacheLoop(t *testing.T) {
	c := &weeklyCache{now: time.Now}
	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	runs := 0
	go func() {
		c.loop(ctx, tick, func(context.Context) { runs++; entered <- struct{}{}; <-release })
		close(stopped)
	}()
	for i := 1; i <= 3; i++ {
		tick <- time.Time{}
		<-entered
		select { // the loop is inside run: it takes no tick, so runs cannot overlap
		case tick <- time.Time{}:
			t.Fatal("tick taken while a run was in progress")
		default:
		}
		release <- struct{}{}
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop on cancel")
	}
	if runs != 3 {
		t.Errorf("runs = %d, want 3 (one per tick)", runs)
	}
}

func wkServer(t *testing.T, cfg Config, now func() time.Time) *server {
	t.Helper()
	tmpl, err := template.ParseFiles("../../web/weekly.html.tmpl", "../../web/partials.html.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	cfg.SiteURL = "https://status.example"
	return &server{cfg: cfg, weekly: &weeklyCache{now: now}, weeklyTmpl: tmpl}
}

func wkGet(s *server, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

func TestWeeklyRoute(t *testing.T) {
	// A package named "weekly" does not take the route over.
	cfg := Config{Packages: pkgList("weekly", "", "isp", "")}
	s := wkServer(t, cfg, time.Now)

	w := wkGet(s, "/weekly")
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "15" || w.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("cold: %d Retry-After %q Cache-Control %q", w.Code, w.Header().Get("Retry-After"), w.Header().Get("Cache-Control"))
	}
	if !strings.Contains(w.Body.String(), "warming up") {
		t.Errorf("cold body %q", w.Body.String())
	}

	// A failed first refresh still answers 503.
	_ = s.weekly.refresh(context.Background(), func(context.Context) (*weeklyData, error) { return nil, errors.New("down") })
	if w := wkGet(s, "/weekly"); w.Code != http.StatusServiceUnavailable {
		t.Errorf("after a failed first refresh: %d, want 503", w.Code)
	}

	prs := []slo.PackageRollup{{Package: "weekly", UptimePct: 100, Bars: wkBars(wkT0, weeklyWindowMin, "operational")}}
	if err := s.weekly.refresh(context.Background(), func(context.Context) (*weeklyData, error) { return buildWeekly(prs, cfg), nil }); err != nil {
		t.Fatal(err)
	}
	w = wkGet(s, "/weekly?x=1")
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "public, max-age=3600" {
		t.Errorf("warm: %d Cache-Control %q", w.Code, w.Header().Get("Cache-Control"))
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type %q", ct)
	}
	body := w.Body.String()
	for _, want := range []string{"<title>FlashProxy SLA - Last 7 Days</title>", `href="https://status.example/weekly"`, "No SLA events in the last 7 days.", "</html>"} {
		if !strings.Contains(body, want) {
			t.Errorf("warm body lacks %q", want)
		}
	}
	if w := wkGet(s, "/weekly/"); w.Code != http.StatusNotFound {
		t.Errorf("/weekly/: %d, want 404", w.Code)
	}

	w = wkGet(s, "/sitemap.xml")
	if !strings.Contains(w.Body.String(), "<loc>https://status.example/weekly</loc>") || strings.Contains(w.Body.String(), "%!") {
		t.Errorf("sitemap: %s", w.Body.String())
	}
}

// The weekly bars are placed on device pixels by bars.js + weekly.js: the page
// loads both (bars.js first), still emits the 84 blocks the scripts and the
// CSS grid fallback expect, and both scripts are served.
func TestWeeklyBarScripts(t *testing.T) {
	cfg := Config{WebDir: "../../web"}
	s := wkServer(t, cfg, time.Now)
	prs := []slo.PackageRollup{{Package: "isp", UptimePct: 100, Bars: wkBars(wkT0, weeklyWindowMin, "operational")}}
	if err := s.weekly.refresh(context.Background(), func(context.Context) (*weeklyData, error) { return buildWeekly(prs, cfg), nil }); err != nil {
		t.Fatal(err)
	}
	body := wkGet(s, "/weekly").Body.String()
	bars, weekly := strings.Index(body, `<script src="/bars.js"></script>`), strings.Index(body, `<script src="/weekly.js"></script>`)
	if bars < 0 || weekly < bars {
		t.Errorf("scripts: bars.js at %d, weekly.js at %d, want both, bars.js first", bars, weekly)
	}
	if n := strings.Count(body, `<div class="wk-b"`); n != 84 {
		t.Errorf("%d blocks in the row, want 84", n)
	}
	for _, p := range []string{"/bars.js", "/weekly.js"} {
		if w := wkGet(s, p); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "flashBars") {
			t.Errorf("%s: %d, want 200 with the script", p, w.Code)
		}
	}
}

// A stale value adds the warning line and is served with no-store (degraded
// page); a fresh value keeps the public cache header.
func TestWeeklyStaleFlag(t *testing.T) {
	fetched := time.Unix(wkT0, 0)
	now := fetched
	s := wkServer(t, Config{}, func() time.Time { return now })
	prs := []slo.PackageRollup{{Package: "isp", UptimePct: 100, Bars: wkBars(wkT0-weeklyWindowMin*60, weeklyWindowMin, "operational")}}
	if err := s.weekly.refresh(context.Background(), func(context.Context) (*weeklyData, error) { return buildWeekly(prs, s.cfg), nil }); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		age   time.Duration
		stale bool
	}{{0, false}, {14 * time.Minute, false}, {16 * time.Minute, true}, {48 * time.Hour, true}} {
		now = fetched.Add(c.age)
		w := wkGet(s, "/weekly")
		body := w.Body.String()
		wantCC := "public, max-age=3600"
		if c.stale {
			wantCC = "no-store"
		}
		if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != wantCC {
			t.Errorf("age %v: %d Cache-Control %q", c.age, w.Code, w.Header().Get("Cache-Control"))
		}
		if got := strings.Contains(body, "Refresh is failing; showing the last good data."); got != c.stale {
			t.Errorf("age %v: stale line present = %v, want %v", c.age, got, c.stale)
		}
		if !strings.Contains(body, "Data as of 2025-11-13 02:15 UTC (window ends 2025-11-13 02:15 UTC)") {
			t.Errorf("age %v: data-as-of line missing", c.age)
		}
	}
}

func TestWeeklyTemplate(t *testing.T) {
	pkgs := []slo.Package{{Short: "resi", Name: "Residential <b>", TargetMs: 1000}}
	cfg := Config{Packages: pkgs, SLO: slo.SLO{PackageDegradedMs: slo.PackageTargets(pkgs)}}
	render := func(prs []slo.PackageRollup) string {
		t.Helper()
		s := wkServer(t, cfg, time.Now)
		if err := s.weekly.refresh(context.Background(), func(context.Context) (*weeklyData, error) { return buildWeekly(prs, cfg), nil }); err != nil {
			t.Fatal(err)
		}
		w := wkGet(s, "/weekly")
		if w.Code != http.StatusOK || !strings.HasSuffix(strings.TrimSpace(w.Body.String()), "</html>") {
			t.Fatalf("render: %d, body ends %q", w.Code, w.Body.String()[max(0, w.Body.Len()-80):])
		}
		return w.Body.String()
	}
	count := func(body, sub string) int { return strings.Count(body, sub) }

	// Events, partial data and a package without data.
	partial := wkEventBars()
	wkSet(partial, 600, 605, "no_data", 0) // block 5: 115 ok, 5 empty
	wkSet(partial, 720, 840, "no_data", 0) // block 6: all empty
	body := render([]slo.PackageRollup{
		{Package: "resi", UptimePct: 99.12, Bars: partial},
		{Package: "isp", UptimePct: 100, Bars: wkBars(wkT0, weeklyWindowMin, "no_data")},
	})
	if got := count(body, `class="wk-b"`); got != 2*84 {
		t.Errorf("%d blocks, want %d", got, 2*84)
	}
	// One segment per non-zero count, none for a zero count. resi: down in blocks
	// 41 and 83; degraded in 0, 1, 2, 41; empty minutes in 5 and 6; ok everywhere
	// but block 6. isp: 84 all-empty blocks.
	for class, want := range map[string]int{"s-dn": 2, "s-deg": 4, "s-op": 83, "s-nd": 2 + 84} {
		if got := count(body, `<i class="`+class+`" style="flex-grow:`); got != want {
			t.Errorf("%s segments: %d, want %d", class, got, want)
		}
	}
	for _, want := range []string{
		`<span class="wk-up">99.12%</span>`,
		`<span class="wk-up">no data</span>`,
		"Residential &lt;b&gt;",
		`data-l="partial data"`, `data-l="down"`, `data-l="degraded"`, `data-l="operational"`,
		"partial data - 115 ok, 5 no data",
		"partial data - 0 of 120 minutes with data",
		`style="flex-grow:115"`, `style="flex-grow:5"`,
		"2 down, 3 degraded",
		"peak 1500 ms (limit 1000)", "no vantage reachable", "ongoing at data end", "at least 10m",
		"5 events in the last 7 days",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %q", want)
		}
	}
	// The only scripts are the two that place the bars (TestWeeklyBarScripts).
	if got := count(body, "<script src"); got != 2 {
		t.Errorf("%d scripts loaded, want 2 (bars.js, weekly.js)", got)
	}
	for _, not := range []string{`data-l="no data"`, "100.00%", "ZgotmplZ", "more, see", "No SLA events", "Refresh is failing"} {
		if strings.Contains(body, not) {
			t.Errorf("body contains %q", not)
		}
	}

	// More events than listed.
	body = render([]slo.PackageRollup{{Package: "resi", Bars: wkEventBars()}, {Package: "isp", Bars: wkEventBars()}})
	if !strings.Contains(body, "10 events in the last 7 days") || !strings.Contains(body, "+ 2 more, see") || count(body, `class="inc"`) != 8 {
		t.Errorf("event cap: %d event rows", count(body, `class="inc"`))
	}

	// No package has data.
	body = render([]slo.PackageRollup{{Package: "isp", UptimePct: 100, Bars: wkBars(wkT0, weeklyWindowMin, "no_data")}})
	if !strings.Contains(body, "No data for the last 7 days.") || strings.Contains(body, "No SLA events") || strings.Contains(body, "All products:") {
		t.Error("no-data page: wrong banner or a pooled figure")
	}
	// No packages at all.
	body = render(nil)
	if !strings.Contains(body, "No data for the last 7 days.") || count(body, `class="wk-b"`) != 0 {
		t.Error("empty page: wrong banner or blocks")
	}
}

// Without rollup rows, or with rows that have no bars, there is no window: the
// page shows the no-data banner and no zero timestamp. Packages stay listed.
func TestWeeklyTemplateNoWindow(t *testing.T) {
	cfg := Config{Packages: pkgList("isp", "")}
	for name, prs := range map[string][]slo.PackageRollup{
		"no rows": nil,
		"no bars": {{Package: "isp", UptimePct: 100}},
	} {
		s := wkServer(t, cfg, time.Now)
		if err := s.weekly.refresh(context.Background(), func(context.Context) (*weeklyData, error) { return buildWeekly(prs, cfg), nil }); err != nil {
			t.Fatal(err)
		}
		w := wkGet(s, "/weekly")
		body := w.Body.String()
		if w.Code != http.StatusOK || !strings.HasSuffix(strings.TrimSpace(body), "</html>") {
			t.Fatalf("%s: %d, body ends %q", name, w.Code, body[max(0, len(body)-80):])
		}
		if !strings.Contains(body, "No data for the last 7 days.") || !strings.Contains(body, "Data as of ") {
			t.Errorf("%s: no-data banner or data-as-of line missing", name)
		}
		for _, not := range []string{"1970", "window ends", "No SLA events", "All products:"} {
			if strings.Contains(body, not) {
				t.Errorf("%s: body contains %q", name, not)
			}
		}
		if got := strings.Count(body, `class="wk-row"`); got != len(prs) {
			t.Errorf("%s: %d package rows, want %d (no package is hidden)", name, got, len(prs))
		}
	}
}

// A panic in the fetch (rollup or buildWeekly) is a failed refresh: it does not
// escape, the last good value stays, and the next good refresh publishes.
func TestWeeklyCachePanic(t *testing.T) {
	now := time.Unix(wkT0, 0)
	c := &weeklyCache{now: func() time.Time { return now }}
	ctx := context.Background()
	boom := func(context.Context) (*weeklyData, error) { panic("boom") }
	build := func(context.Context) (*weeklyData, error) {
		var prs []slo.PackageRollup
		return buildWeekly(prs[:1], Config{}), nil // runtime error inside the build path
	}
	refresh := func(f func(context.Context) (*weeklyData, error)) (err error) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped refresh: %v", r)
			}
		}()
		return c.refresh(ctx, f)
	}

	// Before the first success: counted as a failure, no value.
	if err := refresh(boom); err == nil || !strings.Contains(err.Error(), "boom") || c.fails != 1 {
		t.Fatalf("panic before first value: err=%v fails=%d", err, c.fails)
	}
	if _, ok := c.get(); ok {
		t.Fatal("a panicking refresh stored a value")
	}
	first := &weeklyData{Overall: "first"}
	if err := refresh(func(context.Context) (*weeklyData, error) { return first, nil }); err != nil {
		t.Fatal(err)
	}

	// Same state as a failed fetch leaves: value and FetchedAt kept, fails counted.
	now = now.Add(5 * time.Minute)
	for i, f := range []func(context.Context) (*weeklyData, error){boom, build} {
		if err := refresh(f); err == nil {
			t.Errorf("panic %d returned no error", i)
		}
		if v, _ := c.get(); v != first || !v.FetchedAt.Equal(time.Unix(wkT0, 0)) || c.fails != i+1 {
			t.Errorf("panic %d: value replaced or FetchedAt moved (fails=%d)", i, c.fails)
		}
	}

	// The refresh lock was released and a later success publishes.
	second := &weeklyData{Overall: "second"}
	if err := refresh(func(context.Context) (*weeklyData, error) { return second, nil }); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.get(); v != second || !v.FetchedAt.Equal(now) || c.fails != 0 {
		t.Errorf("recovery after panic: value %q fails=%d", v.Overall, c.fails)
	}

	// Cancellation is still reported as itself, not as a panic.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	err := c.refresh(cctx, func(ctx context.Context) (*weeklyData, error) { <-ctx.Done(); return nil, ctx.Err() })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled refresh: %v, want context.Canceled", err)
	}
}

// A server built without the cache, or a cache without a clock, does not panic.
func TestWeeklyZeroServer(t *testing.T) {
	s := wkServer(t, Config{}, nil)
	if err := s.weekly.refresh(context.Background(), func(context.Context) (*weeklyData, error) { return &weeklyData{}, nil }); err != nil {
		t.Fatal(err)
	}
	if w := wkGet(s, "/weekly"); w.Code != http.StatusOK {
		t.Errorf("cache without a clock: %d, want 200", w.Code)
	}
	s.weekly = nil
	if w := wkGet(s, "/weekly"); w.Code != http.StatusServiceUnavailable {
		t.Errorf("server without a cache: %d, want 503", w.Code)
	}
}
