package alertgate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// msg renders exactly what cmd/worker fireTransition writes to sla.events.
func msg(prev, st string, ms int) string {
	return fmt.Sprintf(MessageFormat, prev, st, ms, 98.1)
}

type fake struct {
	msgs []string
	err  error
	now  time.Time
}

func (f *fake) list(context.Context, string, time.Time) ([]string, error) { return f.msgs, f.err }

func newGate(t *testing.T, f *fake) *Gate {
	t.Helper()
	s, err := Options{}.Build(50)
	if err != nil {
		t.Fatal(err)
	}
	return New(s, f.list, func() time.Time { return f.now }, nil)
}

func degraded(ms ...int) []string {
	var out []string
	for _, v := range ms {
		out = append(out, msg("operational", "degraded", v))
	}
	return out
}

func TestParseMessageFrozenFormat(t *testing.T) {
	prev, st, ms, ok := ParseMessage("operational -> degraded (response 63ms, 98.1% ok)")
	if !ok || prev != "operational" || st != "degraded" || ms != 63 {
		t.Fatalf("got %q %q %v %v", prev, st, ms, ok)
	}
	if _, st, _, ok := ParseMessage(msg("degraded", "no_data", 0)); !ok || st != "no_data" {
		t.Fatalf("no_data not parsed")
	}
	if _, _, _, ok := ParseMessage("deploy v1.2"); ok {
		t.Fatal("non-status message parsed")
	}
	// Old rows may carry a fractional response: rounded half away from zero.
	for in, want := range map[string]int{"12": 12, "12.5": 13, "12.49": 12, "63.4": 63} {
		m := "operational -> degraded (response " + in + "ms, 98.1% ok)"
		if _, _, ms, ok := ParseMessage(m); !ok || ms != want {
			t.Errorf("ParseMessage(%q) ms=%v ok=%v, want %v", m, ms, ok, want)
		}
	}
}

func TestStartupRowsNotCounted(t *testing.T) {
	r := Readings(append(degraded(60, 70), msg("startup", "degraded", 99), msg("degraded", "operational", 40)))
	if len(r) != 2 || r[0] != 70 {
		t.Fatalf("readings = %v", r)
	}
}

func TestRaiseOnFiveIssues(t *testing.T) {
	f := &fake{now: time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC), msgs: degraded(60, 80, 61, 70, 65, 52)}
	g := newGate(t, f)
	send, extra := g.Transition(context.Background(), "isp_eu", "operational", "degraded", 52)
	if !send || extra == "" {
		t.Fatalf("want raise announcement, got send=%v extra=%q", send, extra)
	}
	if got, want := g.Threshold("isp_eu"), 66; got != want { // 5th highest 60 x 1.1
		t.Fatalf("T = %v, want %v", got, want)
	}
	if !strings.Contains(extra, "50→66ms") || !strings.Contains(extra, "top 80, 70, 65, 61, 60ms") {
		t.Fatalf("summary = %q", extra)
	}
	// Below T now: suppressed, and its recovery too.
	if send, _ := g.Transition(context.Background(), "isp_eu", "operational", "degraded", 58); send {
		t.Fatal("degraded below T should be suppressed")
	}
	if send, _ := g.Transition(context.Background(), "isp_eu", "degraded", "operational", 40); send {
		t.Fatal("recovery of a suppressed alert should be suppressed")
	}
}

// While T is raised, even the recovery of a degraded alert that was sent is dropped.
func TestRecoverySuppressedWhileRaised(t *testing.T) {
	f := &fake{now: time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC), msgs: degraded(60, 80, 61, 70, 65, 52)}
	g := newGate(t, f)
	if send, extra := g.Transition(context.Background(), "isp_eu", "operational", "degraded", 52); !send || extra == "" {
		t.Fatalf("want raise announcement, got send=%v extra=%q", send, extra)
	}
	if send, _ := g.Transition(context.Background(), "isp_eu", "degraded", "operational", 48); send {
		t.Fatal("recovery while T > base should be suppressed")
	}
}

func TestSingleOutlierAndFewIssuesDoNotRaise(t *testing.T) {
	f := &fake{now: time.Now(), msgs: degraded(500, 55, 54, 53)} // 4 issues, one outlier
	g := newGate(t, f)
	if send, extra := g.Transition(context.Background(), "p", "operational", "degraded", 500); !send || extra != "" {
		t.Fatalf("want plain alert, got %v %q", send, extra)
	}
	f.msgs = degraded(500, 55, 54, 53, 51) // 5th highest 51 -> 56.1 -> 56: the outlier does not drive T
	g.Transition(context.Background(), "p", "operational", "degraded", 51)
	if got := g.Threshold("p"); got != 56 {
		t.Fatalf("T = %v", got)
	}
}

func TestLowerCheck(t *testing.T) {
	s, _ := Options{}.Build(50)
	if nt, ok := LowerTarget(s, 100, []int{90, 90, 90, 90}); !ok || nt != 50 {
		t.Fatalf("fewer than 5 -> base, got %v %v", nt, ok)
	}
	if nt, ok := LowerTarget(s, 200, []int{300, 200, 100, 90, 60}); !ok || nt != 66 {
		t.Fatalf("ref <= 50%% of T -> ref*1.1, got %v %v", nt, ok)
	}
	if _, ok := LowerTarget(s, 100, []int{100, 90, 80, 70, 60}); ok {
		t.Fatal("ref > 50% of T should not change")
	}

	// Through the Gate: timer fires 3 h after the change, not before.
	t0 := time.Date(2026, 10, 26, 6, 0, 0, 0, time.UTC) // 08:00/09:00 Jerusalem: before the daily slot
	f := &fake{now: t0, msgs: degraded(60, 60, 60, 60, 60)}
	g := newGate(t, f)
	g.Transition(context.Background(), "p", "operational", "degraded", 60)
	f.msgs = nil
	f.now = t0.Add(179 * time.Minute)
	if posts := g.Tick(context.Background()); len(posts) != 0 {
		t.Fatalf("early tick posted %v", posts)
	}
	f.now = t0.Add(3 * time.Hour)
	posts := g.Tick(context.Background())
	if len(posts) != 1 || g.Threshold("p") != 50 || !strings.Contains(posts[0].Text, "66→50ms") {
		t.Fatalf("posts=%v T=%v", posts, g.Threshold("p"))
	}
}

func TestQueryErrorFailsOpen(t *testing.T) {
	f := &fake{now: time.Now(), msgs: degraded(60, 60, 60, 60, 60)}
	g := newGate(t, f)
	g.Transition(context.Background(), "p", "operational", "degraded", 60) // T = 66
	f.err = errors.New("ch down")
	if send, extra := g.Transition(context.Background(), "p", "operational", "degraded", 55); !send || extra != "" {
		t.Fatalf("query error must fail open, got %v %q", send, extra)
	}
	if g.Threshold("p") != 66 {
		t.Fatalf("T changed on error: %v", g.Threshold("p"))
	}
}

func TestDailyDue(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Jerusalem")
	slots := []int{10 * 60, 16 * 60}
	gap := 3 * time.Hour
	at := func(h, m int) time.Time { return time.Date(2026, 10, 26, h, m, 0, 0, loc) }
	yday := time.Date(2026, 10, 25, 23, 0, 0, 0, loc)
	cases := []struct {
		name       string
		now        time.Time
		lastChange time.Time
		daily      string
		due, skip  bool
	}{
		{"before first slot", at(9, 59), time.Time{}, "", false, false},
		{"10:00 quiet day", at(10, 0), time.Time{}, "", true, false},
		{"10:00 change was yesterday", at(10, 0), yday, "", true, false},
		{"10:00 change at 02:00 today waits", at(10, 0), at(2, 0), "", false, false},
		{"16:00 picks up 09:55 change", at(16, 0), at(9, 55), "", true, false},
		{"16:00 change 2h ago skips the day", at(16, 0), at(14, 0), "", false, true},
		{"16:00 change exactly 3h ago", at(16, 0), at(13, 0), "", true, false},
		{"already sent today", at(16, 0), time.Time{}, "2026-10-26", false, false},
		{"sent yesterday only", at(10, 0), time.Time{}, "2026-10-25", true, false},
	}
	for _, c := range cases {
		due, skip := DailyDue(c.now, loc, slots, gap, c.lastChange, c.daily, "")
		if due != c.due || skip != c.skip {
			t.Errorf("%s: due=%v skip=%v, want %v %v", c.name, due, skip, c.due, c.skip)
		}
	}
	if due, _ := DailyDue(at(20, 0), loc, slots, gap, time.Time{}, "", "2026-10-26"); due {
		t.Error("skipped day must stay skipped")
	}

	// Through the Gate: one summary per day, sent at 16:00 after a morning raise;
	// the state resets when T returns to base.
	f := &fake{now: at(9, 55), msgs: degraded(60, 60, 60, 60, 60, 60)}
	g := newGate(t, f)
	g.Transition(context.Background(), "p", "operational", "degraded", 60) // raise at 09:55
	f.now = at(10, 0)
	if posts := g.Tick(context.Background()); len(posts) != 0 {
		t.Fatalf("10:00 posted after a same-day change: %v", posts)
	}
	f.now = at(16, 0)
	if posts := g.Tick(context.Background()); len(posts) != 1 || !strings.Contains(posts[0].Text, "📋") {
		t.Fatalf("16:00 posts = %v", posts)
	}
	f.now = at(16, 1)
	if posts := g.Tick(context.Background()); len(posts) != 0 {
		t.Fatalf("daily posted twice: %v", posts)
	}
	f.msgs = nil      // green: lower check returns T to base
	f.now = at(19, 1) // recheck was rescheduled to 19:00 at the 16:00 tick
	g.Tick(context.Background())
	if g.Threshold("p") != 50 {
		t.Fatalf("T = %v, want base", g.Threshold("p"))
	}
	if p := g.pkgs["p"]; p.dailyDay != "" || p.skipDay != "" || !p.lastChange.IsZero() {
		t.Fatalf("daily state not reset: %+v", p)
	}
}

// The worker formats events with MessageFormat; pin the text and its round trip.
func TestMessageFormatFrozen(t *testing.T) {
	m := fmt.Sprintf(MessageFormat, "operational", "degraded", 63, 98.1)
	if m != "operational -> degraded (response 63ms, 98.1% ok)" {
		t.Fatalf("message format changed: %q", m)
	}
	if prev, st, ms, ok := ParseMessage(m); !ok || prev != "operational" || st != "degraded" || ms != 63 {
		t.Fatalf("ParseMessage(%q) = %q %q %v %v", m, prev, st, ms, ok)
	}
}

// A transition to down/no_data is an outage, not a flap: adaptive state is dropped.
func TestOutageResetsState(t *testing.T) {
	f := &fake{now: time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC), msgs: degraded(60, 60, 60, 60, 60, 60)}
	g := newGate(t, f)
	g.Transition(context.Background(), "p", "operational", "degraded", 60)
	if g.Threshold("p") != 66 {
		t.Fatalf("T = %v, want 66", g.Threshold("p"))
	}
	if send, extra := g.Transition(context.Background(), "p", "degraded", "down", 0); !send || extra != "" {
		t.Fatalf("down transition = %v %q, want sent as today", send, extra)
	}
	if g.Threshold("p") != 50 {
		t.Fatalf("T = %v after down, want base", g.Threshold("p"))
	}
	if p := g.pkgs["p"]; !p.nextCheck.IsZero() || !p.lastChange.IsZero() || p.dailyDay != "" {
		t.Fatalf("state not cleared: %+v", p)
	}
}

// A lower post names the open degraded episode (response, local start); a recovery
// or an outage closes it.
func TestLowerPostOpenEpisode(t *testing.T) {
	cases := []struct {
		name, tz string
		t0       time.Time
		then     string // "" | "recover" | "down"
		want     string // suffix; "" = none
	}{
		{"open, UTC", "UTC", time.Date(2026, 10, 26, 6, 0, 0, 0, time.UTC), "", ", degraded (60ms) since 2026-10-26 06:00"},
		{"open, Jerusalem crosses midnight", "Asia/Jerusalem", time.Date(2026, 10, 26, 22, 30, 0, 0, time.UTC), "", ", degraded (60ms) since 2026-10-27 00:30"},
		{"open, Los Angeles crosses midnight", "America/Los_Angeles", time.Date(2026, 10, 27, 5, 30, 0, 0, time.UTC), "", ", degraded (60ms) since 2026-10-26 22:30"},
		{"recovered before lowering", "UTC", time.Date(2026, 10, 26, 6, 0, 0, 0, time.UTC), "recover", ""},
		{"down resets", "UTC", time.Date(2026, 10, 26, 6, 0, 0, 0, time.UTC), "down", ""},
	}
	for _, c := range cases {
		s, err := Options{Timezone: c.tz}.Build(50)
		if err != nil {
			t.Fatal(err)
		}
		f := &fake{now: c.t0, msgs: degraded(60, 60, 60, 60, 60)}
		g := New(s, f.list, func() time.Time { return f.now }, nil)
		g.Transition(context.Background(), "p", "operational", "degraded", 60) // T = 66
		switch c.then {
		case "recover":
			f.now = c.t0.Add(time.Hour)
			g.Transition(context.Background(), "p", "degraded", "operational", 40)
		case "down":
			f.now = c.t0.Add(time.Hour)
			g.Transition(context.Background(), "p", "degraded", "down", 0)
		}
		f.msgs = nil
		f.now = c.t0.Add(3 * time.Hour) // before the 10:00 daily slot in every tz above
		posts := g.Tick(context.Background())
		if c.then == "down" {
			// Reset: no lower post at all, and the episode is gone for any later post.
			p := g.pkgs["p"]
			if len(posts) != 0 || !p.lastEventAt.IsZero() || strings.Contains(g.summary("p", p, 66, nil), "degraded (") {
				t.Errorf("%s: posts=%v state=%+v", c.name, posts, p)
			}
			continue
		}
		if len(posts) != 1 || !strings.Contains(posts[0].Text, "📉 **p** 66→50ms") {
			t.Fatalf("%s: posts = %v", c.name, posts)
		}
		if c.want == "" {
			if strings.Contains(posts[0].Text, "degraded (") {
				t.Errorf("%s: unexpected suffix: %q", c.name, posts[0].Text)
			}
		} else if !strings.Contains(posts[0].Text, "66→50ms"+c.want+" · ") {
			t.Errorf("%s: %q lacks %q", c.name, posts[0].Text, c.want)
		}
	}
}

// The gate compares the un-rounded reading: with T = 56, 56.4ms is above T and sent
// (a rounded compare, 56 <= 56, suppressed it). The episode shows the rounded value.
func TestDegradedComparesUnrounded(t *testing.T) {
	cases := []struct {
		ms    float32
		shown int // rounded: the event message and the episode suffix
		send  bool
	}{
		{56.4, 56, true},  // rounded compare suppressed this
		{56.5, 57, true},  // sent either way
		{56, 56, false},   // == T
		{55.6, 56, false}, // suppressed either way
	}
	for _, c := range cases {
		t0 := time.Date(2026, 10, 26, 6, 0, 0, 0, time.UTC) // 08:00/09:00 Jerusalem
		f := &fake{now: t0, msgs: degraded(51, 51, 51, 51, 51)}
		g := newGate(t, f)
		g.Transition(context.Background(), "p", "operational", "degraded", 51) // 51 x 1.1 = 56.1 -> T = 56
		g.Transition(context.Background(), "p", "degraded", "operational", 40)
		if g.Threshold("p") != 56 {
			t.Fatalf("T = %v, want 56", g.Threshold("p"))
		}
		f.msgs = append(f.msgs, msg("operational", "degraded", c.shown)) // this reading's event; 5th highest stays 51
		send, extra := g.Transition(context.Background(), "p", "operational", "degraded", c.ms)
		if send != c.send || extra != "" || g.Threshold("p") != 56 {
			t.Errorf("ms=%v: send=%v extra=%q T=%v, want send=%v, no raise", c.ms, send, extra, g.Threshold("p"), c.send)
		}
		f.msgs = nil
		f.now = t0.Add(3 * time.Hour)
		posts := g.Tick(context.Background())
		if want := fmt.Sprintf("56→50ms, degraded (%dms) since 2026-10-26 08:00 · ", c.shown); len(posts) != 1 || !strings.Contains(posts[0].Text, want) {
			t.Errorf("ms=%v: posts = %v, want %q", c.ms, posts, want)
		}
	}
}
