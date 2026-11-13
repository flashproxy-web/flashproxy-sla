package main

// /weekly: the last 7 days of every package as 84 blocks of 2 hours, the 7-day
// uptime and the newest Down/Degraded events.
//
// Why no SQL aggregation: the minute classification is stateful (the consecutive
// degraded run, per-package thresholds, the cross-vantage Down rule, warm-up) and
// lives in internal/slo, the same code the credit engine mirrors. The page takes
// the classified package bars from slo.RollupPackagesTrimAlts and only counts
// them here, so no second copy of the rule exists. Bars and vantage detail are
// dropped before the result is cached.

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/slo"
)

const (
	weeklyWindowMin    = 7 * 24 * 60 // minutes shown per package
	weeklyBlockMin     = 120         // minutes per block; 10080 / 120 = 84 blocks
	weeklyRefresh      = 5 * time.Minute
	weeklyFetchTimeout = 25 * time.Second // below the website profile's 30 s max_execution_time
	weeklyStaleAfter   = 15 * time.Minute // 3 missed refreshes: only adds the warning line
	weeklyCacheControl = "public, max-age=3600"
	weeklyMaxEvents    = 8
	weeklyTimeFmt      = "2006-01-02 15:04 MST"
)

const weeklyJSONLD = `[
{"@context":"https://schema.org","@type":"WebPage","name":"FlashProxy Status - Last 7 Days","url":"https://status.flashproxy.com/weekly","description":"Availability of every FlashProxy product over the last 7 days in 2-hour blocks, with the 7-day uptime and the downtime and degradation events of the week."}
]`

// blockEvent is one incident's share of a block.
type blockEvent struct {
	Type    string // "down" | "degraded"
	Minutes int    // minutes of that incident inside the block
}

// weeklyBlock is up to weeklyBlockMin consecutive minutes of one package.
// Op+Deg+Down+NoData is the number of minutes in the block.
type weeklyBlock struct {
	Start, End            int64 // unix; End is exclusive (last minute + 60)
	Op, Deg, Down, NoData int
	Label                 string
	Title                 string // tooltip
	Events                []blockEvent
}

type weeklyRow struct {
	Package, Name                          string
	HasData                                bool    // false = no data minute in the whole window
	UptimePct                              float64 // slo.PackageRollup.UptimePct, unchanged
	Uptime                                 string  // "99.98%" or "no data"
	OpMin, DegradedMin, DownMin, NoDataMin int
	DownEvents, DegradedEvents             int
	Blocks                                 []weeklyBlock
}

type weeklyEvent struct {
	Package, Name, Severity, SeverityLabel string
	Start, End, Duration, Worst            string
	Ongoing                                bool // reaches the newest kept minute
	BeganBefore                            bool // starts at the first kept minute
	start                                  int64
}

// weeklyData is what the cache holds. It is never changed after it is published.
type weeklyData struct {
	FetchedAt              time.Time // time of the last successful refresh ("data as of")
	WindowStart, WindowEnd int64
	Overall                string // uptime pooled by minute over all packages with data
	OverallHasData         bool
	Rows                   []weeklyRow
	Events                 []weeklyEvent // newest first, at most weeklyMaxEvents
	EventsTotal            int
}

// EventsMore is the number of events not listed.
func (d *weeklyData) EventsMore() int { return d.EventsTotal - len(d.Events) }

// blockLabel names a block by its worst content, first match wins: down,
// degraded, partial data (some or all minutes empty), operational. A block is
// never labelled "no data": that is reserved for a package with no data minute
// in the whole window (weeklyRow.HasData).
func blockLabel(down, deg, noData int) string {
	switch {
	case down > 0:
		return "down"
	case deg > 0:
		return "degraded"
	case noData > 0:
		return "partial data"
	}
	return "operational"
}

// bucketBars counts minute bars into blocks of size minutes. Blocks are aligned
// to the END of bars (the newest kept minute), not to clock hours: the newest
// block is always full and only the oldest can be short. With the 10080 bars the
// rollup returns, all 84 blocks hold exactly 120 minutes. A short oldest block
// just has fewer minutes; minutes before the window are not counted as no data.
func bucketBars(bars []slo.Bar, size int) []weeklyBlock {
	if size < 1 || len(bars) == 0 {
		return nil
	}
	n := (len(bars) + size - 1) / size
	out := make([]weeklyBlock, n)
	for k := range out {
		hi := len(bars) - size*(n-1-k)
		lo := max(hi-size, 0)
		b := weeklyBlock{Start: bars[lo].T, End: bars[hi-1].T + 60}
		for _, m := range bars[lo:hi] {
			switch m.Status {
			case "down":
				b.Down++
			case "degraded":
				b.Deg++
			case "no_data":
				b.NoData++
			default:
				b.Op++
			}
		}
		b.Label = blockLabel(b.Down, b.Deg, b.NoData)
		out[k] = b
	}
	return out
}

// blockTitle is the tooltip, for example
// "2026-11-12 14:05-16:05 UTC: partial data - 112 ok, 3 degraded, 5 no data; events: degraded 3 min".
func blockTitle(b weeklyBlock) string {
	var parts []string
	add := func(n int, what string) {
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, what))
		}
	}
	add(b.Op, "ok")
	add(b.Deg, "degraded")
	add(b.Down, "down")
	add(b.NoData, "no data")
	total := b.Op + b.Deg + b.Down + b.NoData
	if total == b.NoData {
		parts = []string{fmt.Sprintf("0 of %d minutes with data", total)}
	}
	s := fmt.Sprintf("%s-%s UTC: %s - %s", time.Unix(b.Start, 0).UTC().Format("2006-01-02 15:04"),
		time.Unix(b.End, 0).UTC().Format("15:04"), b.Label, strings.Join(parts, ", "))
	if total < weeklyBlockMin {
		s += fmt.Sprintf(" (%d of %d minutes)", total, weeklyBlockMin)
	}
	const shown = 3
	for i, e := range b.Events {
		switch {
		case i == 0:
			s += "; events: "
		case i == shown:
			return s + fmt.Sprintf(", +%d more", len(b.Events)-shown)
		default:
			s += ", "
		}
		s += fmt.Sprintf("%s %d min", e.Type, e.Minutes)
	}
	return s
}

// worstReading describes an incident from the package bars inside it. Degraded:
// the highest minute average (the best-vantage average the rule reads) and the
// package limit. Down: no vantage was reachable, so there is no reading.
func worstReading(bars []slo.Bar, inc slo.Incident, thresholdMs float64) string {
	if inc.Type == "down" {
		return "no vantage reachable"
	}
	var peak float64
	for _, b := range bars {
		if b.T >= inc.Start && b.T < inc.End && b.ResponseMsAvg > peak {
			peak = b.ResponseMsAvg
		}
	}
	return fmt.Sprintf("peak %s ms (limit %s)", fmtMs(peak), fmtMs(thresholdMs))
}

// buildWeeklyRow turns one package rollup into its row and its events. Incidents
// come from the same bars as the blocks (slo.IncidentsFromBars).
func buildWeeklyRow(pr slo.PackageRollup, cfg Config) (weeklyRow, []weeklyEvent) {
	row := weeklyRow{
		Package: pr.Package, Name: cfg.DisplayName(pr.Package),
		UptimePct: pr.UptimePct, Uptime: "no data",
		Blocks: bucketBars(pr.Bars, weeklyBlockMin),
	}
	for _, b := range row.Blocks {
		row.OpMin += b.Op
		row.DegradedMin += b.Deg
		row.DownMin += b.Down
		row.NoDataMin += b.NoData
	}
	// The rollup reports 100 for a package without data; that is never shown.
	if row.HasData = row.OpMin+row.DegradedMin+row.DownMin > 0; row.HasData {
		row.Uptime = fmt.Sprintf("%.2f%%", pr.UptimePct)
	}
	var events []weeklyEvent
	for _, inc := range slo.IncidentsFromBars(pr.Package, pr.Bars) {
		if inc.Type == "down" {
			row.DownEvents++
		} else {
			row.DegradedEvents++
		}
		for i := range row.Blocks {
			b := &row.Blocks[i]
			if m := int((min(inc.End, b.End) - max(inc.Start, b.Start)) / 60); m > 0 {
				b.Events = append(b.Events, blockEvent{Type: inc.Type, Minutes: m})
			}
		}
		ev := weeklyEvent{
			Package: pr.Package, Name: row.Name, Severity: inc.Type, SeverityLabel: statusLabel[inc.Type],
			Start:       time.Unix(inc.Start, 0).UTC().Format(weeklyTimeFmt),
			End:         time.Unix(inc.End, 0).UTC().Format(weeklyTimeFmt),
			Duration:    humanDur(inc.End - inc.Start),
			Worst:       worstReading(pr.Bars, inc, cfg.SLO.DegradedThresholdFor(pr.Package)),
			Ongoing:     inc.End == pr.Bars[len(pr.Bars)-1].T+60,
			BeganBefore: inc.Start == pr.Bars[0].T,
			start:       inc.Start,
		}
		if ev.Ongoing {
			ev.End = "ongoing at data end"
		}
		if ev.BeganBefore {
			ev.Start = "before " + ev.Start
			ev.Duration = "at least " + ev.Duration
		}
		events = append(events, ev)
	}
	for i := range row.Blocks {
		row.Blocks[i].Title = blockTitle(row.Blocks[i])
	}
	return row, events
}

// overallUptime pools the minutes of every package with data, using the same
// formula as the per-package figure. It is a summary, not an SLA figure.
func overallUptime(rows []weeklyRow) (string, bool) {
	var withData, down, deg int
	for _, r := range rows {
		withData += r.OpMin + r.DegradedMin + r.DownMin
		down += r.DownMin
		deg += r.DegradedMin
	}
	if withData == 0 {
		return "no data", false
	}
	return fmt.Sprintf("%.2f%%", (float64(withData)-float64(down)-0.5*float64(deg))/float64(withData)*100), true
}

// buildWeekly collapses the rollups to what the page shows: per-block counts and
// events, the newest events and the pooled uptime. It is pure.
func buildWeekly(prs []slo.PackageRollup, cfg Config) *weeklyData {
	d := &weeklyData{Rows: make([]weeklyRow, 0, len(prs))}
	var events []weeklyEvent
	for _, pr := range prs {
		row, evs := buildWeeklyRow(pr, cfg)
		d.Rows = append(d.Rows, row)
		events = append(events, evs...)
		if n := len(pr.Bars); n > 0 {
			if d.WindowStart == 0 || pr.Bars[0].T < d.WindowStart {
				d.WindowStart = pr.Bars[0].T
			}
			d.WindowEnd = max(d.WindowEnd, pr.Bars[n-1].T+60)
		}
	}
	d.Overall, d.OverallHasData = overallUptime(d.Rows)
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].start != events[j].start {
			return events[i].start > events[j].start
		}
		return events[i].Package < events[j].Package
	})
	d.EventsTotal = len(events)
	d.Events = events[:min(len(events), weeklyMaxEvents)]
	return d
}

// weeklyCache holds the last good weeklyData. Requests only read it (get); the
// background loop is the only caller that queries. A failed refresh leaves the
// stored value as it is, so the page keeps showing the last good data with its
// "data as of" time. Before the first success there is no value and the handler
// answers 503.
type weeklyCache struct {
	now func() time.Time

	running sync.Mutex // held for the length of one refresh
	mu      sync.RWMutex
	val     *weeklyData
	fails   int // consecutive failed refreshes
}

func (c *weeklyCache) get() (*weeklyData, bool) {
	if c == nil { // a server built without the cache answers "warming up"
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.val, c.val != nil
}

// clock is c.now, or time.Now for a cache built without one.
func (c *weeklyCache) clock() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

// safeFetch runs fetch and turns a panic in it (the rollup or buildWeekly) into
// an error. refresh runs on the background goroutine, which the request
// middleware's recover does not cover: without this a panic there would end the
// whole website process.
func safeFetch(ctx context.Context, fetch func(context.Context) (*weeklyData, error)) (d *weeklyData, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("weekly refresh panic: %v\n%s", r, debug.Stack())
			d, err = nil, fmt.Errorf("panic: %v", r)
		}
	}()
	return fetch(ctx)
}

// refresh runs fetch once and stores its result. A call made while another
// refresh is running returns at once without fetching, so one query runs at a
// time whoever calls. A fetch that panics counts as a failed refresh.
func (c *weeklyCache) refresh(ctx context.Context, fetch func(context.Context) (*weeklyData, error)) error {
	if !c.running.TryLock() {
		return nil
	}
	defer c.running.Unlock()
	ctx, cancel := context.WithTimeout(ctx, weeklyFetchTimeout)
	defer cancel()
	d, err := safeFetch(ctx, fetch)
	if err == nil && d == nil {
		err = errors.New("no data returned")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.fails++
		log.Printf("weekly refresh failed (%d): %v", c.fails, err)
		return err
	}
	d.FetchedAt = c.clock()
	c.val, c.fails = d, 0
	return nil
}

// loop calls run on every tick until ctx ends. Runs are sequential: a tick that
// arrives during a run waits (a time.Ticker drops further ones).
func (c *weeklyCache) loop(ctx context.Context, tick <-chan time.Time, run func(context.Context)) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			run(ctx)
		}
	}
}

// refreshWeekly is the one place that queries ClickHouse for /weekly.
func (s *server) refreshWeekly(ctx context.Context) {
	_ = s.weekly.refresh(ctx, func(ctx context.Context) (*weeklyData, error) {
		prs, err := slo.RollupPackagesTrimAlts(ctx, s.ch, s.cfg.ClickHouse.DB, weeklyWindowMin, s.cfg.SLO, s.cfg.Packages)
		if err != nil {
			return nil, err
		}
		return buildWeekly(prs, s.cfg), nil
	})
}

// startWeekly fetches once and then refreshes every weeklyRefresh, in the
// background so the rest of the site does not wait for the first fetch.
func (s *server) startWeekly(ctx context.Context) {
	go func() {
		s.refreshWeekly(ctx)
		t := time.NewTicker(weeklyRefresh)
		defer t.Stop()
		s.weekly.loop(ctx, t.C, s.refreshWeekly)
	}()
}

type weeklyView struct {
	*weeklyData
	SiteURL   string
	AsOf      string
	WindowEnd string // "" when no package has a bar (the template then omits it)
	Stale     bool   // refresh has been failing for longer than weeklyStaleAfter
	JSONLD    template.JS
}

// handleWeekly serves the cached value. It never queries.
func (s *server) handleWeekly(w http.ResponseWriter, r *http.Request) {
	d, ok := s.weekly.get()
	if !ok {
		w.Header().Set("Retry-After", "15")
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "weekly data is warming up", http.StatusServiceUnavailable)
		return
	}
	v := weeklyView{
		weeklyData: d, SiteURL: s.cfg.SiteURL,
		AsOf:   d.FetchedAt.UTC().Format(weeklyTimeFmt),
		Stale:  s.weekly.clock().Sub(d.FetchedAt) > weeklyStaleAfter,
		JSONLD: template.JS(weeklyJSONLD),
	}
	if d.WindowEnd != 0 { // zero = no rollup rows or no bars: no window to name
		v.WindowEnd = time.Unix(d.WindowEnd, 0).UTC().Format(weeklyTimeFmt)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", weeklyCacheControl)
	if err := s.weeklyTmpl.Execute(w, v); err != nil {
		log.Printf("render weekly: %v", err)
	}
}
