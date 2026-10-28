// Package alertgate is the ALERT-ONLY adaptive threshold that sits in front of the
// monitor's Discord post for operational <-> degraded transitions (KAN-247).
//
// It never changes the SLA verdict: events, the ledger, the website and SLA credits
// keep using slo.degraded_avg_ms (the "base"/floor). The gate only decides whether a
// lower-tier Discord message is sent, raises a per-package alert threshold T when a
// package flaps into degraded too often, lowers it again on a recheck timer, and
// emits a once-a-day summary while T is above base.
//
// Inputs are injected (clock + a lookup of the package's recent status_change event
// messages), so the rules are unit-testable without ClickHouse. State is in memory
// only; after a restart T is back at base and no timer runs. A Gate is NOT safe for
// concurrent use (the monitor loop is a single goroutine).
package alertgate

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // embed the tz database so Asia/Jerusalem does not depend on the host
)

// Options are the JSON tunables (all optional; zero = default).
type Options struct {
	WindowMin   int      `json:"window_min"`    // issue-counting window (default 180)
	MinIssues   int      `json:"min_issues"`    // issues needed to raise; also the reference rank (default 5)
	TopN        int      `json:"top_n"`         // readings listed in a summary (default 5)
	RaiseFactor float64  `json:"raise_factor"`  // T = reference x factor (default 1.1)
	LowerRatio  float64  `json:"lower_ratio"`   // lower when reference <= ratio x T (default 0.5)
	RecheckMin  int      `json:"recheck_min"`   // lower-check interval after a change (default 180)
	Timezone    string   `json:"timezone"`      // daily-summary timezone (default Asia/Jerusalem)
	DailySlots  []string `json:"daily_slots"`   // "HH:MM" local times (default ["10:00","16:00"])
	DailyGapMin int      `json:"daily_gap_min"` // last slot: skip the day if a change was posted less than this ago (default 180)
}

// Settings are validated, defaulted Options plus the base (floor) threshold.
type Settings struct {
	Base        int
	Window      time.Duration
	MinIssues   int
	TopN        int
	RaiseFactor float64
	LowerRatio  float64
	Recheck     time.Duration
	Loc         *time.Location
	Slots       []int // minutes after local midnight, ascending
	DailyGap    time.Duration
}

// Build applies defaults and validates. Negative or nonsensical values are errors
// (the caller fails fast), zero means "use the default".
func (o Options) Build(base int) (Settings, error) {
	if base <= 0 {
		return Settings{}, fmt.Errorf("slo.degraded_avg_ms must be > 0 (it is the alert floor)")
	}
	if o.WindowMin < 0 || o.MinIssues < 0 || o.TopN < 0 || o.RecheckMin < 0 || o.DailyGapMin < 0 || o.RaiseFactor < 0 || o.LowerRatio < 0 {
		return Settings{}, fmt.Errorf("negative value")
	}
	s := Settings{
		Base:        base,
		Window:      time.Duration(defInt(o.WindowMin, 180)) * time.Minute,
		MinIssues:   defInt(o.MinIssues, 5),
		TopN:        defInt(o.TopN, 5),
		RaiseFactor: defF(o.RaiseFactor, 1.1),
		LowerRatio:  defF(o.LowerRatio, 0.5),
		Recheck:     time.Duration(defInt(o.RecheckMin, 180)) * time.Minute,
		DailyGap:    time.Duration(defInt(o.DailyGapMin, 180)) * time.Minute,
	}
	if s.RaiseFactor < 1 {
		return Settings{}, fmt.Errorf("raise_factor must be >= 1")
	}
	if s.LowerRatio > 1 {
		return Settings{}, fmt.Errorf("lower_ratio must be in (0,1]")
	}
	tz := o.Timezone
	if tz == "" {
		tz = "Asia/Jerusalem"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Settings{}, fmt.Errorf("timezone %q: %v", tz, err)
	}
	s.Loc = loc
	slots := o.DailySlots
	if len(slots) == 0 {
		slots = []string{"10:00", "16:00"}
	}
	for _, sl := range slots {
		t, err := time.Parse("15:04", sl)
		if err != nil {
			return Settings{}, fmt.Errorf("daily_slots: %q is not HH:MM", sl)
		}
		s.Slots = append(s.Slots, t.Hour()*60+t.Minute())
	}
	sort.Ints(s.Slots)
	return s, nil
}

func defInt(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}

func defF(v, d float64) float64 {
	if v == 0 {
		return d
	}
	return v
}

// MessageFormat is the sla.events status_change message the worker writes (args:
// prev, status, response ms (int), success %). Stored messages are hashed into the
// ledger as written and never rewritten; the text must stay parseable by msgRe.
const MessageFormat = "%s -> %s (response %dms, %.1f%% ok)"

// msgRe matches MessageFormat, and older rows with a fractional response.
var msgRe = regexp.MustCompile(`^(\w+) -> (\w+) \(response (\d+(?:\.\d+)?)ms, [^%]*% ok\)$`)

// ParseMessage extracts (prev, status, response ms) from a status_change message.
// A fractional response is rounded half away from zero.
func ParseMessage(msg string) (prev, status string, ms int, ok bool) {
	m := msgRe.FindStringSubmatch(msg)
	if m == nil {
		return "", "", 0, false
	}
	v, err := strconv.ParseFloat(m[3], 64)
	if err != nil {
		return "", "", 0, false
	}
	return m[1], m[2], int(math.Round(v)), true
}

// Readings returns the response times of the logged issues among msgs (new status
// degraded, previous status not "startup"), sorted descending.
func Readings(msgs []string) []int {
	var out []int
	for _, m := range msgs {
		prev, st, ms, ok := ParseMessage(m)
		if !ok || st != "degraded" || prev == "startup" {
			continue
		}
		out = append(out, ms)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(out)))
	return out
}

// Lister returns the messages of the package's status_change events with ts >= since.
type Lister func(ctx context.Context, pkg string, since time.Time) ([]string, error)

// Post is an adaptive announcement (threshold change or daily summary) to send.
type Post struct {
	Package string
	Text    string
}

type pkgState struct {
	t          int       // alert threshold; 0 = base
	nextCheck  time.Time // zero = no lower-check timer
	suppressed bool      // the current degraded notice was suppressed -> suppress its recovery too
	lastChange time.Time // last raise/lower post; zero = none since T left base
	dailyDay   string    // local date (YYYY-MM-DD) the daily summary was last sent
	skipDay    string    // local date the last slot decided to skip the daily summary

	lastEventAt time.Time // last operational -> degraded; zero = none open
	lastEventMs int       // response ms of that transition
}

// Gate holds the per-package adaptive state.
type Gate struct {
	s    Settings
	list Lister
	now  func() time.Time
	logf func(string, ...any)
	pkgs map[string]*pkgState
}

// New builds a Gate. now and logf may be nil (time.Now / no logging).
func New(s Settings, list Lister, now func() time.Time, logf func(string, ...any)) *Gate {
	if now == nil {
		now = time.Now
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Gate{s: s, list: list, now: now, logf: logf, pkgs: map[string]*pkgState{}}
}

func (g *Gate) st(pkg string) *pkgState {
	p := g.pkgs[pkg]
	if p == nil {
		p = &pkgState{t: g.s.Base}
		g.pkgs[pkg] = p
	}
	return p
}

// Threshold returns the package's current alert threshold.
func (g *Gate) Threshold(pkg string) int { return g.st(pkg).t }

// Transition decides the Discord post for a confirmed status change. It returns
// send=false to suppress the message, and a non-empty extra to send a raise
// announcement (summary) that REPLACES the plain degraded alert. Only operational ->
// degraded and degraded -> operational are gated; everything else returns (true, ""), and a transition to down/no_data also resets the package's adaptive state.
// Call it AFTER the event has been written (the current issue must be in the lookup).
func (g *Gate) Transition(ctx context.Context, pkg, prev, status string, ms float32) (send bool, extra string) {
	p := g.st(pkg)
	switch {
	case prev == "operational" && status == "degraded":
		return g.onDegraded(ctx, pkg, p, ms)
	case prev == "degraded" && status == "operational":
		// While T is raised the package is flapping, so recoveries are noise: drop
		// them all, not just the ones paired with a suppressed degraded alert.
		send = !p.suppressed && p.t <= g.s.Base
		p.suppressed = false
		p.lastEventAt = time.Time{} // episode over
		return send, ""
	default:
		// Any ungated transition sends as today; don't pair with an old suppression.
		// Going to a state other than operational/degraded (down, no_data) is a real
		// outage, not a flap: drop the adaptive state for good (silently).
		if status != "operational" && status != "degraded" {
			g.reset(p)
		}
		p.suppressed = false
		return true, ""
	}
}

func (g *Gate) onDegraded(ctx context.Context, pkg string, p *pkgState, ms float32) (bool, string) {
	p.suppressed = false
	now := g.now()
	p.lastEventAt = now
	p.lastEventMs = int(math.Round(float64(ms)))
	msgs, err := g.list(ctx, pkg, now.Add(-g.s.Window))
	if err != nil {
		g.logf("alertgate %s: list issues failed (%v): failing open, threshold unchanged %dms", pkg, err, p.t)
		return true, ""
	}
	r := Readings(msgs)
	if len(r) >= g.s.MinIssues {
		if nt := int(math.Round(float64(r[g.s.MinIssues-1]) * g.s.RaiseFactor)); nt > p.t {
			old := p.t
			g.change(pkg, p, nt, now, "raise")
			return true, g.summary(pkg, p, old, r)
		}
	}
	if p.t > g.s.Base && ms <= float32(p.t) {
		p.suppressed = true
		g.logf("alertgate %s: degraded alert suppressed (response %dms <= threshold %dms)", pkg, p.lastEventMs, p.t)
		return false, ""
	}
	return true, ""
}

// reset returns a package to base with no timers and no daily-summary state.
func (g *Gate) reset(p *pkgState) {
	*p = pkgState{t: g.s.Base}
}

func (g *Gate) change(pkg string, p *pkgState, nt int, now time.Time, why string) {
	g.logf("alertgate %s: threshold %s %dms -> %dms (base %dms)", pkg, why, p.t, nt, g.s.Base)
	p.t = nt
	if nt > g.s.Base {
		p.lastChange = now
		p.nextCheck = now.Add(g.s.Recheck)
	} else {
		// back at base: the package is green again, so the daily-summary state resets too
		p.lastChange, p.dailyDay, p.skipDay = time.Time{}, "", ""
		p.nextCheck = time.Time{}
	}
}

// Tick runs the lower checks and the daily summary. Call it periodically (the
// monitor's 30 s tick). A lookup error leaves state unchanged; it retries next tick.
func (g *Gate) Tick(ctx context.Context) []Post {
	var out []Post
	now := g.now()
	pkgs := make([]string, 0, len(g.pkgs))
	for k := range g.pkgs {
		pkgs = append(pkgs, k)
	}
	sort.Strings(pkgs)
	for _, pkg := range pkgs {
		p := g.pkgs[pkg]
		if p.t <= g.s.Base {
			continue
		}
		lowerDue := !p.nextCheck.IsZero() && !now.Before(p.nextCheck)
		dailyDue, skip := DailyDue(now, g.s.Loc, g.s.Slots, g.s.DailyGap, p.lastChange, p.dailyDay, p.skipDay)
		if skip {
			p.skipDay = now.In(g.s.Loc).Format("2006-01-02")
		}
		if !lowerDue && !dailyDue {
			continue
		}
		msgs, err := g.list(ctx, pkg, now.Add(-g.s.Window))
		if err != nil {
			g.logf("alertgate %s: list issues failed (%v): retrying next tick", pkg, err)
			continue
		}
		r := Readings(msgs)
		if lowerDue {
			if nt, ok := LowerTarget(g.s, p.t, r); ok {
				old := p.t
				g.change(pkg, p, nt, now, "lower")
				out = append(out, Post{pkg, g.summary(pkg, p, old, r)})
				continue
			}
			p.nextCheck = now.Add(g.s.Recheck)
		}
		if dailyDue {
			p.dailyDay = now.In(g.s.Loc).Format("2006-01-02")
			out = append(out, Post{pkg, g.summary(pkg, p, p.t, r)})
		}
	}
	return out
}

// LowerTarget is the 3 h lower rule. readings must be sorted descending. It returns
// the new threshold and true when T should change.
func LowerTarget(s Settings, t int, readings []int) (int, bool) {
	if len(readings) < s.MinIssues {
		return s.Base, t != s.Base
	}
	ref := readings[s.MinIssues-1]
	if float64(ref) <= s.LowerRatio*float64(t) || ref <= s.Base {
		nt := max(s.Base, int(math.Round(float64(ref)*s.RaiseFactor)))
		if nt < t {
			return nt, true
		}
	}
	return t, false
}

// DailyDue decides the once-a-day summary (called only while T > base). It is due at
// the latest slot reached (local time) unless the day already has a summary or was
// skipped. The first slot waits if a raise/lower was already posted today (that post
// carries the summary; a later slot picks the day up). A later slot is skipped for the
// day if the last change was posted less than gap ago, so the daily post never follows
// right after a change. skip=true means the day is decided with no summary.
func DailyDue(now time.Time, loc *time.Location, slots []int, gap time.Duration, lastChange time.Time, dailyDay, skipDay string) (due, skip bool) {
	l := now.In(loc)
	today := l.Format("2006-01-02")
	if dailyDay == today || skipDay == today {
		return false, false
	}
	mod := l.Hour()*60 + l.Minute()
	i := -1
	for j, s := range slots {
		if mod >= s {
			i = j
		}
	}
	if i < 0 {
		return false, false
	}
	if i == 0 {
		return lastChange.IsZero() || lastChange.In(loc).Format("2006-01-02") != today, false
	}
	if !lastChange.IsZero() && now.Sub(lastChange) < gap {
		return false, i == len(slots)-1
	}
	return true, false
}

// summary renders a raise/lower post (old != p.t) or the daily post (old == p.t).
func (g *Gate) summary(pkg string, p *pkgState, old int, r []int) string {
	var b strings.Builder
	if old != p.t {
		if p.t < old {
			fmt.Fprintf(&b, "📉 **%s** %d→%dms", pkg, old, p.t)
			if !p.lastEventAt.IsZero() {
				fmt.Fprintf(&b, ", degraded (%dms) since %s", p.lastEventMs, p.lastEventAt.In(g.s.Loc).Format("2006-01-02 15:04"))
			}
		} else {
			fmt.Fprintf(&b, "📈 **%s** %d→%dms", pkg, old, p.t)
		}
	} else {
		fmt.Fprintf(&b, "📋 **%s** %dms", pkg, p.t)
	}
	fmt.Fprintf(&b, " · %d in %s", len(r), fmtDur(g.s.Window))
	if len(r) > 0 {
		top := r
		if len(top) > g.s.TopN {
			top = top[:g.s.TopN]
		}
		parts := make([]string, len(top))
		for i, v := range top {
			parts[i] = strconv.Itoa(v)
		}
		fmt.Fprintf(&b, " · top %sms", strings.Join(parts, ", "))
	}
	return b.String()
}

func fmtDur(d time.Duration) string {
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return fmt.Sprintf("%dm", int(d/time.Minute))
}
