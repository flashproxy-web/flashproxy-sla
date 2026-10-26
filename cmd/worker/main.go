// Command worker runs the synthetic scenarios from one vantage and writes results
// DIRECTLY to ClickHouse as flashproxy-status-worker (sla_writer role). Run as many
// workers as you like — but each vantage must have EXACTLY ONE worker: the vantage
// id is the integrity-ledger stream key, and a single writer per stream keeps the
// hash chain fork-free (it also prevents double-counted samples). Set
// "monitor": true on exactly one worker to also evaluate SLO status, emit
// alerts/events, and (if a signing key is configured) sign ledger checkpoints.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/alertgate"
	"github.com/flashproxy/flashproxy-status/internal/chstore"
	"github.com/flashproxy/flashproxy-status/internal/ledger"
	"github.com/flashproxy/flashproxy-status/internal/model"
	"github.com/flashproxy/flashproxy-status/internal/probe"
	"github.com/flashproxy/flashproxy-status/internal/slo"
)

type Target struct {
	Package       string `json:"package"`
	ProxyURL      string `json:"proxy_url"`
	ConnectTarget string `json:"connect_target"` // single-target fallback (legacy)
	OriginGet     string `json:"origin_get"`     // GET path for the fallback target
	// ConnectTargets is the variety of endpoints probed each cycle; the BEST result
	// (lowest ttfb among successes; Down only if all fail) is recorded. Empty =>
	// fall back to the single {ConnectTarget, OriginGet}.
	ConnectTargets []probe.Endpoint `json:"connect_targets"`
	IPVersion      uint8            `json:"ip_version"`
	IntervalMS     int              `json:"interval_ms"`
}

// connectEndpoints returns the endpoint set to probe for this target: the configured
// variety list, or the single legacy target as a one-element fallback.
func (t Target) connectEndpoints() []probe.Endpoint {
	if len(t.ConnectTargets) > 0 {
		return t.ConnectTargets
	}
	return []probe.Endpoint{{Target: t.ConnectTarget, Path: t.OriginGet}}
}

type chConn struct {
	URL      string `json:"url"`
	DB       string `json:"db"`
	User     string `json:"user"`
	Password string `json:"password"`
}

// ScenarioCfg holds intervals/sizes for the archetype scenarios (defaults applied
// in loadConfig). The payload scenarios (streaming/large_object/hifreq/long_session)
// run only when Origin is set; scraping/connect always run.
type ScenarioCfg struct {
	StreamingIntervalMS   int `json:"streaming_interval_ms"`
	StreamingBytes        int `json:"streaming_bytes"`
	LargeObjectIntervalMS int `json:"large_object_interval_ms"`
	LargeObjectBytes      int `json:"large_object_bytes"`
	HifreqIntervalMS      int `json:"hifreq_interval_ms"`
	HifreqCount           int `json:"hifreq_count"`
	ScrapingIntervalMS    int `json:"scraping_interval_ms"`
	LongSessionIntervalMS int `json:"long_session_interval_ms"`
	LongSessionHoldMS     int `json:"long_session_hold_ms"`
	NetRTTIntervalMS      int `json:"net_rtt_interval_ms"`
	NetRTTCount           int `json:"net_rtt_count"`
}

type Config struct {
	Vantage            string      `json:"vantage"`
	TimeoutMS          int         `json:"timeout_ms"`
	Monitor            bool        `json:"monitor"`
	Discord            string      `json:"discord_webhook"`
	LedgerSigningKey   string      `json:"ledger_signing_key"`   // Ed25519 seed/key (base64 or hex); enables checkpoint signing
	CheckpointEverySec int         `json:"checkpoint_every_sec"` // checkpoint cadence (default 300)
	MonitorDebounceN   int         `json:"monitor_debounce_n"`   // consecutive evals before a status change is confirmed (default 2)
	ClickHouse         chConn      `json:"clickhouse"`
	SLO                slo.SLO     `json:"slo"`
	Origin             string      `json:"origin"`       // host:port of a self-hosted origin (IPv4 path) for payload scenarios
	OriginIPv6         string      `json:"origin_ipv6"`  // dual-stack/v6 origin for ipv6 packages; empty => they skip payload scenarios
	ScrapeHosts        []string    `json:"scrape_hosts"` // CONNECT targets for the scraping scenario
	Scenarios          ScenarioCfg `json:"scenarios"`
	Targets            []Target    `json:"targets"`

	// AdaptiveAlert enables the alert-only adaptive Discord threshold for
	// operational<->degraded (internal/alertgate). Absent/false = unchanged behaviour.
	AdaptiveAlert       bool              `json:"adaptive_alert"`
	AdaptiveAlertTuning alertgate.Options `json:"adaptive_alert_tuning"` // optional; zero values = defaults
	adaptive            alertgate.Settings
}

// droppedProbes counts probes shed because the ship channel was full (ClickHouse
// slow/down). Shedding keeps probe cadence accurate instead of stalling all
// measurement; the count is logged so the loss is never silent.
var droppedProbes atomic.Uint64

func defInt(v, d int) int {
	if v <= 0 {
		return d
	}
	return v
}

func main() {
	cfgPath := flag.String("config", "config/worker.json", "config path")
	flag.Parse()

	cfg := loadConfig(*cfgPath)
	ch := chstore.New(cfg.ClickHouse.URL, cfg.ClickHouse.DB, cfg.ClickHouse.User, cfg.ClickHouse.Password)
	log.Printf("worker vantage=%q targets=%d ch-user=%q monitor=%v", cfg.Vantage, len(cfg.Targets), cfg.ClickHouse.User, cfg.Monitor)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Resume this vantage's integrity chain from its persisted head.
	chain := resumeChain(ch, cfg.Vantage)

	results := make(chan model.ProbeResult, 1024)

	// Probers: each scenario runs in its own goroutine under ctx; runTarget returns
	// once all its scenarios stop (on shutdown).
	var probers sync.WaitGroup
	for _, t := range cfg.Targets {
		probers.Add(1)
		go func(t Target) { defer probers.Done(); runTarget(ctx, cfg, t, results) }(t)
	}

	flusherDone := make(chan struct{})
	go func() { defer close(flusherDone); flusher(ch, chain, cfg.Vantage, results) }()

	if cfg.Monitor {
		go monitorLoop(ctx, cfg, ch)
		if priv := loadSigningKey(cfg); priv != nil {
			go checkpointLoop(ctx, cfg, ch, priv)
		} else {
			log.Printf("ledger checkpoints DISABLED (no ledger_signing_key configured)")
		}
	}

	<-ctx.Done()
	log.Printf("shutdown: stopping probers and draining...")
	probers.Wait() // no more probes will be emitted
	close(results) // tell the flusher to drain and do its final flush
	<-flusherDone
	if d := droppedProbes.Load(); d > 0 {
		log.Printf("shutdown: %d probes were shed during the run (channel full)", d)
	}
	log.Printf("shutdown complete")
}

func loadConfig(p string) Config {
	b, err := os.ReadFile(p)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	b = []byte(os.ExpandEnv(string(b)))
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if c.TimeoutMS <= 0 {
		c.TimeoutMS = 8000
	}
	s := &c.Scenarios
	s.StreamingIntervalMS = defInt(s.StreamingIntervalMS, 120000)
	s.StreamingBytes = defInt(s.StreamingBytes, 5*1024*1024)
	s.LargeObjectIntervalMS = defInt(s.LargeObjectIntervalMS, 60000)
	s.LargeObjectBytes = defInt(s.LargeObjectBytes, 262144)
	s.HifreqIntervalMS = defInt(s.HifreqIntervalMS, 60000)
	s.HifreqCount = defInt(s.HifreqCount, 10)
	s.ScrapingIntervalMS = defInt(s.ScrapingIntervalMS, 60000)
	s.LongSessionIntervalMS = defInt(s.LongSessionIntervalMS, 180000)
	s.LongSessionHoldMS = defInt(s.LongSessionHoldMS, 20000)
	s.NetRTTIntervalMS = defInt(s.NetRTTIntervalMS, 15000)
	s.NetRTTCount = defInt(s.NetRTTCount, 3)
	c.CheckpointEverySec = defInt(c.CheckpointEverySec, 300)
	c.MonitorDebounceN = defInt(c.MonitorDebounceN, 2)
	if len(c.ScrapeHosts) == 0 {
		// all dual-stack so ipv6-egress packages can reach them too
		c.ScrapeHosts = []string{
			"www.google.com:443", "www.cloudflare.com:443", "www.facebook.com:443",
			"www.wikipedia.org:443", "www.microsoft.com:443",
		}
	}
	validateConfig(c)
	if c.AdaptiveAlert {
		s, err := c.AdaptiveAlertTuning.Build(c.SLO.DegradedAvgMs)
		if err != nil {
			log.Fatalf("config: adaptive_alert_tuning: %v", err)
		}
		c.adaptive = s
	}
	return c
}

// validateConfig fails fast on misconfiguration that would otherwise run silently —
// notably empty ${ENV} expansions (an unset var becomes "") that would connect with
// an empty password or run a target against a credential-less proxy URL.
func validateConfig(c Config) {
	if c.Vantage == "" {
		log.Fatalf("config: vantage is required (it is the integrity-ledger stream key)")
	}
	if c.ClickHouse.URL == "" || c.ClickHouse.User == "" || c.ClickHouse.Password == "" || c.ClickHouse.DB == "" {
		log.Fatalf("config: clickhouse url/db/user/password must all be set (check for unset ${ENV} placeholders)")
	}
	if len(c.Targets) == 0 {
		log.Printf("config: no targets")
	}
	for _, t := range c.Targets {
		if t.Package == "" {
			log.Fatalf("config: a target has no package")
		}
		u, err := url.Parse(t.ProxyURL)
		if err != nil || u.Host == "" {
			log.Fatalf("config: target %q has invalid proxy_url %q (check for an unset ${ENV})", t.Package, redactURL(t.ProxyURL))
		}
	}
}

// resumeChain restores the worker's per-vantage chain head. It guards against seq
// reuse after a crash: if probe_raw already holds rows at a seq beyond the last
// ledger entry (an uncommitted batch lost in a crash), it resumes AFTER those rows
// so the new entries never collide — the orphaned seq(s) surface to verifiers as a
// reported gap (data lost), not as tampering.
func resumeChain(ch *chstore.Client, vantage string) *ledger.Chain {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lseq, lhash, err := ch.LedgerHead(ctx, vantage)
	if err != nil {
		log.Fatalf("resume chain: read ledger head: %v", err)
	}
	pmax, err := ch.ProbeMaxSeq(ctx, vantage)
	if err != nil {
		log.Fatalf("resume chain: read probe max seq: %v", err)
	}
	seq, prev := lseq, lhash
	if pmax > lseq {
		log.Printf("resume %s: probe_raw seq up to %d but ledger head %d — %d uncommitted batch(es) from a prior crash (will show as gaps)", vantage, pmax, lseq, pmax-lseq)
		seq = pmax
		if prev == "" {
			prev = ledger.GenesisHash
		}
	}
	c := ledger.Resume(vantage, seq, prev)
	hs, _ := c.Head()
	log.Printf("resume %s: chain head seq=%d", vantage, hs)
	return c
}

// redactURL strips userinfo from a URL so proxy credentials never reach logs. Uses
// the stdlib Redacted() (handles last-@ / IPv6 / missing userinfo correctly), with a
// hard fallback for anything unparseable.
func redactURL(s string) string {
	if u, err := url.Parse(s); err == nil && u.Scheme != "" {
		return u.Redacted()
	}
	return "***"
}

func runTarget(ctx context.Context, cfg Config, t Target, out chan<- model.ProbeResult) {
	proxy, err := url.Parse(t.ProxyURL)
	if err != nil || proxy.Host == "" {
		log.Printf("[%s] bad proxy_url %q: %v", t.Package, redactURL(t.ProxyURL), err)
		return
	}
	timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
	streamTimeout := 60 * time.Second // big downloads need a generous deadline

	// emit is NON-BLOCKING: if the ship channel is full (ClickHouse slow/down) it
	// sheds the probe and bumps a counter rather than stalling the prober, so probe
	// cadence stays accurate during a store outage instead of going sparse.
	emit := func(rs ...model.ProbeResult) {
		for i := range rs {
			rs[i].Vantage = cfg.Vantage
			rs[i].Package = t.Package
			rs[i].IPVersion = t.IPVersion
			select {
			case out <- rs[i]:
			default:
				droppedProbes.Add(1)
			}
		}
	}
	s := cfg.Scenarios

	var wg sync.WaitGroup
	start := func(intervalMs int, fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); loopCtx(ctx, intervalMs, fn) }()
	}

	// Each scenario runs twice per cycle: through the proxy, and direct (nil proxy)
	// as the no-proxy baseline ("_direct"), so the page can show proxy overhead.
	eps := t.connectEndpoints()
	start(defInt(t.IntervalMS, 20000), func() {
		// Through the proxy, then direct (baseline). Each returns the winning
		// row (scenario 'connect', drives the SLO) plus per-target diagnostic
		// rows (scenario 'connect_probe', for visibility — see ConnectBest).
		best, probes := probe.ConnectBest(proxy, eps, timeout)
		emit(best)
		emit(probes...)
		best, probes = probe.ConnectBest(nil, eps, timeout)
		emit(best)
		emit(probes...)
	})
	start(s.ScrapingIntervalMS, func() {
		emit(probe.Scraping(proxy, cfg.ScrapeHosts, timeout)...)
		emit(probe.Scraping(nil, cfg.ScrapeHosts, timeout)...)
	})
	// TCP-connect RTT to the proxy gateway — raw network floor, runs alongside.
	start(s.NetRTTIntervalMS, func() {
		emit(probe.NetRTT(proxy.Host, s.NetRTTCount, timeout))
	})
	// payload scenarios need a self-hosted origin reachable over the package's IP
	// family. ipv6 packages egress over v6, so they need a dual-stack origin.
	origin := cfg.Origin
	if t.IPVersion == 6 {
		origin = cfg.OriginIPv6
	}
	if origin != "" {
		start(s.StreamingIntervalMS, func() {
			emit(probe.Streaming(proxy, origin, s.StreamingBytes, timeout, streamTimeout))
			emit(probe.Streaming(nil, origin, s.StreamingBytes, timeout, streamTimeout))
		})
		start(s.LargeObjectIntervalMS, func() {
			emit(probe.LargeObject(proxy, origin, s.LargeObjectBytes, timeout))
			emit(probe.LargeObject(nil, origin, s.LargeObjectBytes, timeout))
		})
		start(s.HifreqIntervalMS, func() {
			emit(probe.HifreqSmall(proxy, origin, s.HifreqCount, timeout)...)
			emit(probe.HifreqSmall(nil, origin, s.HifreqCount, timeout)...)
		})
		start(s.LongSessionIntervalMS, func() {
			emit(probe.LongSession(proxy, origin, s.LongSessionHoldMS, timeout))
			emit(probe.LongSession(nil, origin, s.LongSessionHoldMS, timeout))
		})
	}
	wg.Wait()
}

// loopCtx runs fn immediately, then every intervalMs, until ctx is cancelled.
func loopCtx(ctx context.Context, intervalMs int, fn func()) {
	if intervalMs <= 0 {
		return
	}
	tk := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer tk.Stop()
	for {
		fn()
		select {
		case <-tk.C:
		case <-ctx.Done():
			return
		}
	}
}

// flusher batches probe rows, then for each batch: tags rows with (vantage, seq),
// inserts them, appends ONE signed-later ledger entry committing to their hashes,
// and only then advances the chain. On any error it RETAINS the batch and retries
// (inserts are de-duplicated by token, so retries are safe). When the input channel
// closes (shutdown) it drains and does a final, persistent flush so no data is lost.
func flusher(ch *chstore.Client, chain *ledger.Chain, vantage string, in <-chan model.ProbeResult) {
	buf := make([]model.ProbeResult, 0, 256)
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()

	for {
		select {
		case r, ok := <-in:
			if !ok {
				flushUntil(ch, chain, vantage, &buf, 8) // final drain: try hard
				return
			}
			buf = append(buf, r)
			if len(buf) >= 200 {
				flushOnce(ch, chain, vantage, &buf)
			}
		case <-t.C:
			flushOnce(ch, chain, vantage, &buf)
		}
	}
}

// flushOnce attempts a single flush; on failure it keeps the buffer for next time.
func flushOnce(ch *chstore.Client, chain *ledger.Chain, vantage string, buf *[]model.ProbeResult) {
	if len(*buf) == 0 {
		return
	}
	if err := commitProbeBatch(ch, chain, vantage, *buf); err != nil {
		log.Printf("flush %d rows: %v (retained, will retry)", len(*buf), err)
		return
	}
	log.Printf("flushed %d probes (seq %d committed)", len(*buf), seqOf(chain))
	*buf = (*buf)[:0]
}

// flushUntil retries the final flush up to attempts times before giving up (and
// logging the loss). Used on shutdown.
func flushUntil(ch *chstore.Client, chain *ledger.Chain, vantage string, buf *[]model.ProbeResult, attempts int) {
	for i := 0; i < attempts && len(*buf) > 0; i++ {
		if err := commitProbeBatch(ch, chain, vantage, *buf); err != nil {
			log.Printf("final flush attempt %d/%d for %d rows: %v", i+1, attempts, len(*buf), err)
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}
		log.Printf("final flush: %d probes committed (seq %d)", len(*buf), seqOf(chain))
		*buf = (*buf)[:0]
		return
	}
	if len(*buf) > 0 {
		log.Printf("DATA LOSS: gave up flushing %d probes after %d attempts", len(*buf), attempts)
	}
}

func seqOf(chain *ledger.Chain) uint64 { s, _ := chain.Head(); return s }

// commitProbeBatch tags rows with the next seq, inserts them, appends the ledger
// entry, then advances the chain. Insert order is rows-then-ledger with dedup tokens
// so a retry never duplicates and a crash between the two surfaces as a verifiable
// gap (rows without an entry), never as silent corruption.
func commitProbeBatch(ch *chstore.Client, chain *ledger.Chain, vantage string, rows []model.ProbeResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hseq, _ := chain.Head()
	seq := hseq + 1
	canon := make([]string, len(rows))
	var tsFirst, tsLast int64
	for i := range rows {
		rows[i].Stream = vantage
		rows[i].Seq = seq
		canon[i] = rows[i].Canonical()
		ts := rows[i].TS.UTC().Unix()
		if i == 0 || ts < tsFirst {
			tsFirst = ts
		}
		if ts > tsLast {
			tsLast = ts
		}
	}
	e := chain.Build(ledger.KindProbe, canon, tsFirst, tsLast)
	if err := ch.InsertProbes(ctx, rows, fmt.Sprintf("%s:%d", vantage, seq)); err != nil {
		return fmt.Errorf("insert probes: %w", err)
	}
	if err := ch.InsertLedger(ctx, e, fmt.Sprintf("%s:%d:ledger", vantage, seq)); err != nil {
		return fmt.Errorf("insert ledger entry: %w", err)
	}
	chain.Commit(e)
	return nil
}

// monitorLoop evaluates SLO status every 30s and records a status_change event +
// Discord alert on a CONFIRMED transition (debounced to avoid flapping). It alerts
// on the first observation too, so a worker that (re)starts into an already-degraded
// state still pages. Enable on exactly one worker. Events are chained into the
// integrity ledger under stream="events".
func monitorLoop(ctx context.Context, cfg Config, ch *chstore.Client) {
	eventsChain := resumeChain(ch, "events")
	discord := &http.Client{Timeout: 10 * time.Second}
	var gate *alertgate.Gate
	if cfg.AdaptiveAlert {
		gate = alertgate.New(cfg.adaptive, func(ctx context.Context, pkg string, since time.Time) ([]string, error) {
			return ch.StatusChangeMessages(ctx, pkg, since)
		}, nil, log.Printf)
		log.Printf("monitor: adaptive_alert ENABLED (base %.0fms, window %s, min issues %d)", cfg.adaptive.Base, cfg.adaptive.Window, cfg.adaptive.MinIssues)
	}

	type pkgState struct {
		confirmed string
		cand      string
		n         int
	}
	states := map[string]*pkgState{}

	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rows, err := slo.Fetch(qctx, ch, cfg.ClickHouse.DB, cfg.SLO)
		cancel()
		if err != nil {
			log.Printf("monitor: %v", err)
			continue
		}
		for _, p := range rows {
			st := states[p.Package]
			if st == nil {
				states[p.Package] = &pkgState{confirmed: p.Status}
				if p.Status != "operational" && p.Status != "no_data" {
					fireTransition(ctx, ch, eventsChain, discord, cfg, p, "startup", gate)
				}
				continue
			}
			if p.Status == st.confirmed {
				st.cand, st.n = "", 0
				continue
			}
			if p.Status == st.cand {
				st.n++
			} else {
				st.cand, st.n = p.Status, 1
			}
			if st.n >= cfg.MonitorDebounceN {
				prev := st.confirmed
				st.confirmed, st.cand, st.n = p.Status, "", 0
				fireTransition(ctx, ch, eventsChain, discord, cfg, p, prev, gate)
			}
		}
		if gate != nil {
			gctx, gcancel := context.WithTimeout(ctx, 10*time.Second)
			for _, post := range gate.Tick(gctx) {
				if cfg.Discord != "" {
					postDiscord(discord, cfg.Discord, post.Text)
				}
			}
			gcancel()
		}
	}
}

// fireTransition records a status_change event (chained) and posts to Discord. A
// non-nil gate (adaptive_alert) may suppress or replace the Discord post for
// operational<->degraded only; the event write is unchanged.
func fireTransition(ctx context.Context, ch *chstore.Client, eventsChain *ledger.Chain, discord *http.Client, cfg Config, p slo.Status, prev string, gate *alertgate.Gate) {
	log.Printf("status change %s: %s -> %s (response %.0fms)", p.Package, prev, p.Status, p.ResponseMsAvg)
	msg := fmt.Sprintf(alertgate.MessageFormat, prev, p.Status, p.ResponseMsAvg, p.SuccessPct)
	ev := model.Event{TS: time.Now().UTC(), Type: "status_change", Package: p.Package, Message: msg}
	if err := commitEvent(ctx, ch, eventsChain, ev); err != nil {
		log.Printf("monitor: record event: %v", err)
	}
	send, extra := true, ""
	if gate != nil {
		gctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		send, extra = gate.Transition(gctx, p.Package, prev, p.Status, p.ResponseMsAvg)
		cancel()
	}
	if cfg.Discord != "" && send {
		emoji := map[string]string{"operational": "✅", "degraded": "⚠️", "down": "🔴", "no_data": "⚪"}[p.Status]
		text := fmt.Sprintf("%s **%s**: `%s` → `%s` (response %.0fms)", emoji, p.Package, prev, p.Status, p.ResponseMsAvg)
		if extra != "" {
			text = extra // raise announcement replaces the plain alert (it carries the top readings)
		}
		postDiscord(discord, cfg.Discord, text)
	}
}

// commitEvent appends an event to the events table and its ledger entry, then
// advances the events chain (same retry-safe ordering as probe batches).
func commitEvent(ctx context.Context, ch *chstore.Client, chain *ledger.Chain, ev model.Event) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	hseq, _ := chain.Head()
	seq := hseq + 1
	ev.Stream, ev.Seq = "events", seq
	e := chain.Build(ledger.KindEvent, []string{ev.Canonical()}, ev.TS.Unix(), ev.TS.Unix())
	if err := ch.InsertEvent(cctx, ev); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	if err := ch.InsertLedger(cctx, e, fmt.Sprintf("events:%d:ledger", seq)); err != nil {
		return fmt.Errorf("insert event ledger entry: %w", err)
	}
	chain.Commit(e)
	return nil
}

// checkpointLoop periodically signs the head of every chain (all vantages + events)
// and stores the signed checkpoints publicly.
func checkpointLoop(ctx context.Context, cfg Config, ch *chstore.Client, priv ed25519.PrivateKey) {
	id := ledger.PubKeyID(priv.Public().(ed25519.PublicKey))
	log.Printf("ledger checkpoints ENABLED (pubkey_id=%s, every %ds)", id, cfg.CheckpointEverySec)
	t := time.NewTicker(time.Duration(cfg.CheckpointEverySec) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		heads, err := ch.LedgerHeads(qctx)
		if err != nil {
			cancel()
			log.Printf("checkpoint: read heads: %v", err)
			continue
		}
		now := time.Now().UTC().Unix()
		var n int
		for stream, h := range heads {
			if h.Seq == 0 {
				continue
			}
			cp := ledger.SignCheckpoint(priv, stream, h.Seq, h.EntryHash, now)
			if err := ch.InsertCheckpoint(qctx, cp); err != nil {
				log.Printf("checkpoint %s seq %d: %v", stream, h.Seq, err)
				continue
			}
			n++
		}
		cancel()
		log.Printf("signed %d checkpoint(s)", n)
	}
}

func loadSigningKey(cfg Config) ed25519.PrivateKey {
	if cfg.LedgerSigningKey == "" {
		return nil
	}
	priv, err := ledger.ParsePrivateKey(cfg.LedgerSigningKey)
	if err != nil {
		log.Fatalf("ledger_signing_key: %v", err)
	}
	return priv
}

// postDiscord posts an alert, retrying on 429/5xx with backoff and logging failures.
func postDiscord(client *http.Client, webhook, content string) {
	body, _ := json.Marshal(map[string]string{"content": content})
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, webhook, bytes.NewReader(body))
		if err != nil {
			log.Printf("discord: build request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("discord: attempt %d: %v", attempt, err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		code := resp.StatusCode
		resp.Body.Close()
		if code < 300 {
			return
		}
		if code == 429 || code >= 500 {
			log.Printf("discord: attempt %d got %d, retrying", attempt, code)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		log.Printf("discord: got %d (giving up)", code)
		return
	}
	log.Printf("discord: alert dropped after retries")
}
