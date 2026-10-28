// Command website serves the public status page and JSON API. It is STRICTLY
// READ-ONLY: it connects to ClickHouse as flashproxy-status-website (sla_reader
// role) and never writes. Workers write the data; the website only renders it.
//
// It publishes everything needed to reproduce the page independently: the public
// read-only ClickHouse credentials, the SLO thresholds, and the integrity-ledger
// public key + canonicalization spec — all at /api/meta.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
	"github.com/flashproxy/flashproxy-status/internal/slo"
)

type chConn struct {
	URL      string `json:"url"`
	DB       string `json:"db"`
	User     string `json:"user"`
	Password string `json:"password"`
}

// publicCH is what we advertise at /api/meta — the published read-only key.
type publicCH struct {
	URL  string `json:"url"`
	DB   string `json:"db"`
	User string `json:"user"`
	// Password is the PUBLIC key, intentionally published.
	Password string `json:"password"`
	Note     string `json:"note"`
}

type Config struct {
	Listen           string   `json:"listen"`
	WebDir           string   `json:"web_dir"`
	SiteURL          string   `json:"site_url"` // canonical URL for SEO (no trailing slash)
	TLSCertFile      string   `json:"tls_cert_file"`
	TLSKeyFile       string   `json:"tls_key_file"`
	ClickHouse       chConn   `json:"clickhouse"`
	PublicClickHouse publicCH `json:"public_clickhouse"`
	SLO              slo.SLO  `json:"slo"`
	LedgerPubKey     string   `json:"ledger_pubkey"` // base64 Ed25519 public key, published for verification
}

var pkgRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

type server struct {
	cfg     Config
	ch      *chstore.Client
	tmpl    *template.Template
	slaTmpl *template.Template
	incTmpl *template.Template
	static  http.Handler

	readyMu sync.Mutex
	readyAt time.Time
	readyOK bool
}

func main() {
	cfgPath := flag.String("config", "config/website.json", "config path")
	flag.Parse()

	cfg := loadConfig(*cfgPath)
	s := &server{
		cfg:     cfg,
		ch:      chstore.New(cfg.ClickHouse.URL, cfg.ClickHouse.DB, cfg.ClickHouse.User, cfg.ClickHouse.Password),
		tmpl:    template.Must(template.ParseFiles(filepath.Join(cfg.WebDir, "index.html.tmpl"))),
		slaTmpl: template.Must(template.ParseFiles(filepath.Join(cfg.WebDir, "sla.html.tmpl"))),
		incTmpl: template.Must(template.ParseFiles(filepath.Join(cfg.WebDir, "incidents.html.tmpl"))),
		static:  http.FileServer(http.Dir(cfg.WebDir)),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/series", s.handleSeries)
	mux.HandleFunc("/api/scenarios", s.handleScenarios)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/ledger", s.handleLedger)
	mux.HandleFunc("/api/meta", s.handleMeta)
	mux.HandleFunc("/healthz", s.handleReady) // readiness: checks ClickHouse
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })
	mux.HandleFunc("/robots.txt", s.handleRobots)
	mux.HandleFunc("/sitemap.xml", s.handleSitemap)
	mux.HandleFunc("/llms.txt", s.handleLLMs)
	mux.HandleFunc("/sla", s.handleSLA)
	mux.HandleFunc("/incidents", s.handleIncidents)
	mux.HandleFunc("/", s.handleRoot) // SSR for "/", static for everything else

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           s.middleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second, // > chstore's 15s client timeout
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}

	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		log.Printf("website (read-only, TLS) listening on %s as ch-user=%q", cfg.Listen, cfg.ClickHouse.User)
		log.Fatal(srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile))
	}
	log.Printf("website (read-only) listening on %s as ch-user=%q", cfg.Listen, cfg.ClickHouse.User)
	log.Fatal(srv.ListenAndServe())
}

// middleware adds panic recovery and baseline security headers to every response.
func (s *server) middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.Printf("panic %s %s: %v", r.Method, r.URL.Path, v)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		hd := w.Header()
		hd.Set("X-Content-Type-Options", "nosniff")
		hd.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		if r.TLS != nil {
			hd.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		}
		h.ServeHTTP(w, r)
	})
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
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.SiteURL == "" {
		c.SiteURL = "https://status.flashproxy.com"
	}
	c.SiteURL = strings.TrimRight(c.SiteURL, "/")
	if c.WebDir == "" {
		c.WebDir = "web"
	}
	if c.ClickHouse.URL == "" || c.ClickHouse.User == "" || c.ClickHouse.DB == "" {
		log.Fatalf("config: clickhouse url/db/user must be set (check for unset ${ENV})")
	}
	return c
}

// fail logs the real error server-side and returns a generic message — never leak
// ClickHouse SQL/schema/hostnames to the public.
func (s *server) fail(w http.ResponseWriter, where string, err error) {
	log.Printf("%s: %v", where, err)
	http.Error(w, "upstream query failed", http.StatusBadGateway)
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	rows, err := slo.Fetch(r.Context(), s.ch, s.cfg.ClickHouse.DB, s.cfg.SLO, false)
	if err != nil {
		s.fail(w, "status query", err)
		return
	}
	writeJSON(w, map[string]any{"generated_at": nowRFC(), "packages": rows})
}

// component is one package: headline from its best vantage + the per-vantage detail.
type component struct {
	Package        string        `json:"package"`
	Status         string        `json:"status"`
	DefaultVantage string        `json:"default_vantage"`
	ResponseMsAvg  float64       `json:"response_ms_avg"`
	SuccessPct     float64       `json:"success_pct"`
	Samples        float64       `json:"samples"`
	UptimePct      float64       `json:"uptime_pct"`
	Bars           []slo.Bar     `json:"bars"`
	Vantages       []vantageView `json:"vantages"`
}

type vantageView struct {
	Vantage       string    `json:"vantage"`
	Status        string    `json:"status"`
	ResponseMsAvg float64   `json:"response_ms_avg"`
	SuccessPct    float64   `json:"success_pct"`
	Samples       float64   `json:"samples"`
	AgeSeconds    int64     `json:"age_seconds"`
	UptimePct     float64   `json:"uptime_pct"`
	Bars          []slo.Bar `json:"bars"`
}

func (s *server) handleOverview(w http.ResponseWriter, r *http.Request) {
	const windowMin = 90
	// Cross-vantage rollup: a package is Down only when ALL vantages are down. The
	// bars/status/uptime are the package-level (cross-vantage) values; per-vantage
	// detail is still returned for the UI toggle.
	prs, err := slo.RollupPackages(r.Context(), s.ch, s.cfg.ClickHouse.DB, windowMin, s.cfg.SLO)
	if err != nil {
		s.fail(w, "overview query", err)
		return
	}
	comps := make([]component, 0, len(prs))
	statuses := make([]string, 0, len(prs))
	for _, pr := range prs {
		views := make([]vantageView, len(pr.Vantages))
		for i, vr := range pr.Vantages {
			views[i] = vantageView{
				Vantage: vr.Vantage, Status: vr.Current.Status,
				ResponseMsAvg: vr.Current.ResponseMsAvg, SuccessPct: vr.Current.SuccessPct,
				Samples: vr.Current.Samples, AgeSeconds: vr.Current.AgeSeconds,
				UptimePct: vr.UptimePct, Bars: vr.Bars,
			}
		}
		comps = append(comps, component{
			Package: pr.Package, Status: pr.Status, DefaultVantage: pr.BestVantage,
			ResponseMsAvg: pr.ResponseMsAvg, SuccessPct: pr.SuccessPct,
			Samples: pr.Samples, UptimePct: pr.UptimePct, Bars: pr.Bars, Vantages: views,
		})
		statuses = append(statuses, pr.Status)
	}
	status, label := slo.Overall(statuses)
	writeJSON(w, map[string]any{
		"generated_at":   nowRFC(),
		"window_minutes": windowMin,
		"bucket_seconds": 60,
		"overall":        map[string]string{"status": status, "label": label},
		"components":     comps,
	})
}

func (s *server) handleSeries(w http.ResponseWriter, r *http.Request) {
	pkg := r.URL.Query().Get("package")
	if !pkgRe.MatchString(pkg) {
		http.Error(w, "bad package", http.StatusBadRequest)
		return
	}
	mins := 360
	if v := r.URL.Query().Get("minutes"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 60*24*7 { // cap at 7 days
			mins = n
		}
	}
	vantageFilter := ""
	if v := r.URL.Query().Get("vantage"); v != "" {
		if !pkgRe.MatchString(v) {
			http.Error(w, "bad vantage", http.StatusBadRequest)
			return
		}
		vantageFilter = fmt.Sprintf(" AND vantage = '%s'", v)
	}
	// Response time (round-trip through the proxy to the nearest origin and back =
	// ttfb_ms): via proxy vs the direct no-proxy baseline. Two series keyed by scenario.
	sql := fmt.Sprintf(`SELECT scenario,
  toUInt32(toUnixTimestamp(toStartOfMinute(ts))) AS t,
  round(avg(ttfb_ms), 1) AS value
FROM %s.probe_raw
WHERE scenario IN ('connect', 'connect_direct') AND package = '%s'%s AND ts > now() - INTERVAL %d MINUTE
GROUP BY scenario, t ORDER BY scenario, t`, s.cfg.ClickHouse.DB, pkg, vantageFilter, mins)
	rows, err := s.ch.QueryJSON(r.Context(), sql)
	if err != nil {
		s.fail(w, "series query", err)
		return
	}
	series := map[string][]map[string]any{}
	for _, m := range rows {
		sc := chstore.Str(m, "scenario")
		series[sc] = append(series[sc], map[string]any{"t": chstore.Num(m, "t"), "value": chstore.Num(m, "value")})
	}
	writeJSON(w, map[string]any{"package": pkg, "minutes": mins, "series": series})
}

// handleScenarios returns per-archetype-scenario stats for one package (optionally
// one vantage) over the last 10 minutes.
func (s *server) handleScenarios(w http.ResponseWriter, r *http.Request) {
	pkg := r.URL.Query().Get("package")
	if !pkgRe.MatchString(pkg) {
		http.Error(w, "bad package", http.StatusBadRequest)
		return
	}
	vf := ""
	if v := r.URL.Query().Get("vantage"); v != "" {
		if !pkgRe.MatchString(v) {
			http.Error(w, "bad vantage", http.StatusBadRequest)
			return
		}
		vf = fmt.Sprintf(" AND vantage = '%s'", v)
	}
	sql := fmt.Sprintf(`SELECT scenario,
  toUInt32(count())                      AS samples,
  round(100 * sum(success) / count(), 2) AS success_pct,
  round(avg(connect_ms), 1)              AS connect_ms_avg,
  round(avg(dial_ms), 1)                 AS dial_ms_avg,
  round(quantile(0.5)(connect_ms), 1)    AS connect_ms_median,
  round(quantile(0.5)(ttfb_ms), 1)       AS ttfb_ms_median,
  round(avg(throughput_mbps), 1)         AS throughput_mbps_avg,
  round(quantile(0.5)(total_ms), 1)      AS total_ms_median
FROM %s.probe_raw
WHERE package = '%s'%s AND ts > now() - INTERVAL 10 MINUTE
GROUP BY scenario ORDER BY scenario`, s.cfg.ClickHouse.DB, pkg, vf)
	rows, err := s.ch.QueryJSON(r.Context(), sql)
	if err != nil {
		s.fail(w, "scenarios query", err)
		return
	}
	writeJSON(w, map[string]any{"package": pkg, "scenarios": rows})
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.ch.QueryJSON(r.Context(), fmt.Sprintf(
		`SELECT toUInt32(toUnixTimestamp(ts)) AS t, type, package, message, stream, toUInt64(seq) AS seq FROM %s.events ORDER BY ts DESC LIMIT 100`,
		s.cfg.ClickHouse.DB))
	if err != nil {
		s.fail(w, "events query", err)
		return
	}
	writeJSON(w, map[string]any{"events": rows})
}

// handleLedger exposes the integrity ledger's current state: per-stream chain heads
// and the most recent signed checkpoints, so anyone can spot-check the chain or run
// the full verifier (cmd/verify).
func (s *server) handleLedger(w http.ResponseWriter, r *http.Request) {
	heads, err := s.ch.LedgerHeads(r.Context())
	if err != nil {
		s.fail(w, "ledger heads", err)
		return
	}
	cps, err := s.ch.QueryJSON(r.Context(), fmt.Sprintf(
		`SELECT stream, toUInt64(seq) AS seq, entry_hash, toUInt32(toUnixTimestamp(ts)) AS ts, pubkey_id, signature
		 FROM %s.ledger_checkpoints ORDER BY signed_at DESC LIMIT 50`, s.cfg.ClickHouse.DB))
	if err != nil {
		s.fail(w, "ledger checkpoints", err)
		return
	}
	hs := map[string]map[string]any{}
	for stream, h := range heads {
		hs[stream] = map[string]any{"seq": h.Seq, "entry_hash": h.EntryHash}
	}
	writeJSON(w, map[string]any{
		"heads":          hs,
		"checkpoints":    cps,
		"pubkey":         s.cfg.LedgerPubKey,
		"verify_command": "go run github.com/flashproxy/flashproxy-status/cmd/verify@latest -ch <public-ch-url> -user <public-user> -pass <public-pass>",
	})
}

func (s *server) handleMeta(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"public_clickhouse": s.cfg.PublicClickHouse,
		"slo":               s.cfg.SLO,
		"integrity": map[string]any{
			"pubkey":            s.cfg.LedgerPubKey,
			"ledger_table":      "sla.ledger",
			"checkpoints_table": "sla.ledger_checkpoints",
			"row_canonical":     "ts_unix|vantage|package|scenario|proto|target|ip_version|success|error_type|dial_ms|connect_ms|ttfb_ms|total_ms|bytes|throughput_mbps(3dp)",
			"batch_hash":        "sha256( sort(canonical rows) joined by '\\n' )",
			"entry_hash":        "sha256( stream|seq|prev_hash|batch_hash|ts_first|ts_last|row_count )",
			"checkpoint_sig":    "ed25519 over 'ckpt|stream|seq|entry_hash|ts'",
			"note":              "Tamper-evidence: any altered/deleted/reordered row breaks the recomputed hashes vs the signed checkpoints. Run cmd/verify to check independently.",
		},
	})
}

// --- SEO / GEO: server-side rendered home + crawler files ---

type idxProduct struct {
	Name        string
	Status      string
	StatusLabel string
	AvgMs       int
	Vantage     string
}

type idxData struct {
	SiteURL       string
	OverallStatus string
	OverallLabel  string
	Updated       string
	Products      []idxProduct
	JSONLD        template.JS
	VantageCount  int    // distinct vantages currently reporting
	VantagesText  string // human list, e.g. "Ashburn, Dallas and Frankfurt"
}

// vantageList collects the distinct vantage labels currently in the data (aws-
// prefix stripped, capitalized, sorted) and a human-readable join, so copy that
// names the vantages stays accurate as the fleet changes — no hardcoded cities.
func vantageList(prs []slo.PackageRollup) (int, string) {
	seen := map[string]bool{}
	var names []string
	for _, pr := range prs {
		for _, v := range pr.Vantages {
			n := strings.TrimPrefix(v.Vantage, "aws-")
			if n == "" || seen[n] {
				continue
			}
			seen[n] = true
			if r := []rune(n); len(r) > 0 { // capitalize first letter
				r[0] = unicode.ToUpper(r[0])
				n = string(r)
			}
			names = append(names, n)
		}
	}
	sort.Strings(names)
	switch len(names) {
	case 0:
		return 0, "multiple regions"
	case 1:
		return 1, names[0]
	case 2:
		return 2, names[0] + " and " + names[1]
	default:
		return len(names), strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
	}
}

const jsonLD = `[
{"@context":"https://schema.org","@type":"Organization","name":"FlashProxy","url":"https://flashproxy.com","description":"High-performance HTTP and SOCKS5 proxies with Shared ISP, Datacenter, IPv6 Datacenter, and IPv6 Residential IP options."},
{"@context":"https://schema.org","@type":"WebSite","name":"FlashProxy Status","url":"https://status.flashproxy.com"},
{"@context":"https://schema.org","@type":"FAQPage","mainEntity":[
{"@type":"Question","name":"Is FlashProxy operational right now?","acceptedAnswer":{"@type":"Answer","text":"FlashProxy's live operational status, uptime, and latency are published on status.flashproxy.com and updated continuously from US and EU vantage points."}},
{"@type":"Question","name":"What proxy products does FlashProxy offer?","acceptedAnswer":{"@type":"Answer","text":"FlashProxy offers Shared ISP proxies (USA and EU), Datacenter proxies, IPv6 Datacenter proxies, and IPv6 Residential proxies, over HTTP and SOCKS5."}},
{"@type":"Question","name":"How is FlashProxy uptime and latency measured?","acceptedAnswer":{"@type":"Answer","text":"Synthetic probes run continuously from multiple regions, measuring round-trip response time through the proxy, gateway network round-trip, throughput, success rate, and a direct no-proxy baseline for each product. The methodology is open source and the measurements are integrity-protected by a signed, hash-chained ledger."}}
]}
]`

var statusLabel = map[string]string{
	"operational": "Operational", "degraded": "Degraded",
	"down": "Down", "no_data": "No data",
}

const slaJSONLD = `[
{"@context":"https://schema.org","@type":"Organization","name":"FlashProxy","url":"https://flashproxy.com"},
{"@context":"https://schema.org","@type":"WebPage","name":"FlashProxy Service Level Agreement","url":"https://status.flashproxy.com/sla","description":"FlashProxy's 100% uptime guarantee with automatic, proportional compensation, independently verifiable via open-source monitoring and a public, integrity-protected metrics database."},
{"@context":"https://schema.org","@type":"FAQPage","mainEntity":[
{"@type":"Question","name":"Does FlashProxy offer an uptime SLA?","acceptedAnswer":{"@type":"Answer","text":"Yes. FlashProxy guarantees 100% availability on Datacenter, IPv6 Datacenter, Shared ISP USA, IPv6 Residential, and Shared ISP EU, with automatic, proportional compensation for any qualifying downtime or degradation."}},
{"@type":"Question","name":"How does FlashProxy compensate for downtime?","acceptedAnswer":{"@type":"Answer","text":"Per-GB plans receive automatic account credits scaled to monthly availability. Time-based Unlimited plans receive an automatic SLA credit based on total plan cost. Degradation (best-vantage average round-trip response time above 50ms for 5 consecutive minutes) counts at half weight."}},
{"@type":"Question","name":"Is FlashProxy's SLA independently verifiable?","acceptedAnswer":{"@type":"Answer","text":"Yes. The monitoring system is fully open source, and the ClickHouse metrics database that powers status.flashproxy.com is publicly readable and integrity-protected by a signed, hash-chained ledger, so anyone can reproduce every availability figure and confirm the data has not been tampered with."}}
]}
]`

// buildIndex computes a current snapshot (best vantage per product) for SSR.
func (s *server) buildIndex(r *http.Request) idxData {
	d := idxData{SiteURL: s.cfg.SiteURL, OverallStatus: "no_data", OverallLabel: "Awaiting Data", Updated: "Updated " + time.Now().UTC().Format("2006-01-02 15:04 MST"), JSONLD: template.JS(jsonLD)}
	prs, err := slo.RollupPackages(r.Context(), s.ch, s.cfg.ClickHouse.DB, 15, s.cfg.SLO)
	if err != nil {
		log.Printf("buildIndex: %v", err)
		return d
	}
	statuses := []string{}
	for _, pr := range prs {
		d.Products = append(d.Products, idxProduct{
			Name: pr.Package, Status: pr.Status, StatusLabel: statusLabel[pr.Status],
			AvgMs: int(pr.ResponseMsAvg + 0.5), Vantage: strings.TrimPrefix(pr.BestVantage, "aws-"),
		})
		statuses = append(statuses, pr.Status)
	}
	if len(statuses) > 0 {
		d.OverallStatus, d.OverallLabel = slo.Overall(statuses)
	}
	d.VantageCount, d.VantagesText = vantageList(prs)
	return d
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		noCache(s.static).ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=30")
	if err := s.tmpl.Execute(w, s.buildIndex(r)); err != nil {
		log.Printf("render index: %v", err)
	}
}

func (s *server) handleSLA(w http.ResponseWriter, r *http.Request) {
	d := s.buildIndex(r)
	d.JSONLD = template.JS(slaJSONLD)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	if err := s.slaTmpl.Execute(w, d); err != nil {
		log.Printf("render sla: %v", err)
	}
}

// --- Past incidents (human-facing downtime/degradation history) ---

type incidentView struct {
	Package       string
	Severity      string // "down" | "degraded"
	SeverityLabel string
	Start         string // "2006-01-02 15:04 UTC"
	End           string // formatted, or "ongoing"
	Duration      string
	Ongoing       bool
	// Optional operator-authored official statement (from sla.incident_statements,
	// published via the internal dashboard). Empty when none is attached.
	Statement   string
	StatementBy string
	StatementAt string // publish date, "2006-01-02"
}

// statementRow is one published, de-duplicated editorial statement.
type statementRow struct {
	pkg             string
	start, end, pub int64
	body, author    string
}

type incidentsData struct {
	SiteURL    string
	Updated    string
	Incidents  []incidentView
	Total      int
	WindowDays int
	JSONLD     template.JS
}

const incidentsJSONLD = `[
{"@context":"https://schema.org","@type":"WebPage","name":"FlashProxy Status — Past Incidents","url":"https://status.flashproxy.com/incidents","description":"History of FlashProxy proxy downtime and degradation incidents, detected automatically by an open-source prober and committed to a public, signed, hash-chained integrity ledger."}
]`

func humanDur(sec int64) string {
	if sec < 60 {
		if sec < 1 {
			sec = 1
		}
		return fmt.Sprintf("%ds", sec)
	}
	m := sec / 60
	if m < 60 {
		return fmt.Sprintf("%dm", m)
	}
	h := m / 60
	m %= 60
	if h < 24 {
		if m > 0 {
			return fmt.Sprintf("%dh %dm", h, m)
		}
		return fmt.Sprintf("%dh", h)
	}
	dd := h / 24
	h %= 24
	if h > 0 {
		return fmt.Sprintf("%dd %dh", dd, h)
	}
	return fmt.Sprintf("%dd", dd)
}

// incidentsWindowMin bounds how far back /incidents enumerates incidents. They are
// computed from probe_raw via slo.RollupIncidents — the SAME cross-vantage rollup,
// SQL, and thresholds the credit engine (flash-staff-dash) uses — so a given outage
// yields an identical (type, window) in both, rather than the monitor's coarser,
// debounced event log.
const incidentsWindowMin = 30 * 24 * 60 // 30 days

// buildIncidents enumerates Down/Degraded incidents over the window from probe_raw
// (the published cross-vantage SLA contract), newest first, and attaches any
// operator-authored official statement.
func (s *server) buildIncidents(r *http.Request) incidentsData {
	d := incidentsData{
		SiteURL:    s.cfg.SiteURL,
		Updated:    "Updated " + time.Now().UTC().Format("2006-01-02 15:04 MST"),
		WindowDays: incidentsWindowMin / (24 * 60),
		JSONLD:     template.JS(incidentsJSONLD),
	}
	incs, err := slo.RollupIncidents(r.Context(), s.ch, s.cfg.ClickHouse.DB, incidentsWindowMin, s.cfg.SLO)
	if err != nil {
		log.Printf("buildIncidents: %v", err)
		return d
	}
	sort.Slice(incs, func(i, j int) bool { return incs[i].Start > incs[j].Start })
	nowMin := time.Now().UTC().Unix()
	nowMin -= nowMin % 60
	stmts := s.loadStatements(r.Context())
	for _, in := range incs {
		ongoing := in.End >= nowMin // last impacted minute is the current minute
		endStr := time.Unix(in.End, 0).UTC().Format("2006-01-02 15:04 MST")
		dur := in.End - in.Start
		if ongoing {
			endStr = "ongoing"
			dur = nowMin + 60 - in.Start
		}
		v := incidentView{
			Package:       in.Package,
			Severity:      in.Type,
			SeverityLabel: statusLabel[in.Type],
			Start:         time.Unix(in.Start, 0).UTC().Format("2006-01-02 15:04 MST"),
			End:           endStr,
			Duration:      humanDur(dur),
			Ongoing:       ongoing,
		}
		if st := bestStatement(stmts, in.Package, in.Start, in.End); st != nil {
			v.Statement = st.body
			v.StatementBy = st.author
			if st.pub > 0 {
				v.StatementAt = time.Unix(st.pub, 0).UTC().Format("2006-01-02")
			}
		}
		d.Incidents = append(d.Incidents, v)
	}
	d.Total = len(d.Incidents)
	return d
}

// loadStatements reads published, de-duplicated editorial statements. It is
// best-effort: if the table is absent (pre-migration) or the query errors, it logs
// and returns nil so the page still renders incidents without statements.
func (s *server) loadStatements(ctx context.Context) []statementRow {
	rows, err := s.ch.QueryJSON(ctx, fmt.Sprintf(
		`SELECT package,
		        toUInt32(toUnixTimestamp(argMax(period_start, updated_at))) AS pstart,
		        toUInt32(toUnixTimestamp(argMax(period_end, updated_at)))   AS pend,
		        argMax(body, updated_at)   AS body,
		        argMax(author, updated_at) AS author,
		        toUInt32(toUnixTimestamp(argMax(published_at, updated_at))) AS pub
		 FROM %s.incident_statements
		 GROUP BY dedupe_key, package
		 HAVING body != ''`,
		s.cfg.ClickHouse.DB))
	if err != nil {
		log.Printf("incident statements (non-fatal): %v", err)
		return nil
	}
	out := make([]statementRow, 0, len(rows))
	for _, m := range rows {
		out = append(out, statementRow{
			pkg:    chstore.Str(m, "package"),
			start:  int64(chstore.Num(m, "pstart")),
			end:    int64(chstore.Num(m, "pend")),
			body:   chstore.Str(m, "body"),
			author: chstore.Str(m, "author"),
			pub:    int64(chstore.Num(m, "pub")),
		})
	}
	return out
}

// statementMatchTolerance: the dashboard and the monitor detect the same outage
// independently, so their incident windows differ by seconds-to-minutes at the
// edges (and can be adjacent rather than overlapping). Attach a statement to the
// nearest incident of the same package within this gap. Incidents for one package
// are normally spaced far apart, so this won't cross-link distinct incidents.
const statementMatchTolerance = int64(30 * 60)

// bestStatement returns the statement for pkg closest in time to the incident
// [start,end] (gap 0 when they overlap), within statementMatchTolerance; ties broken
// by most-recently published. Returns nil if none is close enough.
func bestStatement(stmts []statementRow, pkg string, start, end int64) *statementRow {
	var best *statementRow
	bestGap := int64(1) << 62
	for i := range stmts {
		st := &stmts[i]
		if st.pkg != pkg {
			continue
		}
		gap := int64(0) // distance between the two intervals; 0 if they overlap
		if st.start > end {
			gap = st.start - end
		} else if start > st.end {
			gap = start - st.end
		}
		if gap > statementMatchTolerance {
			continue
		}
		if gap < bestGap || (gap == bestGap && best != nil && st.pub > best.pub) {
			bestGap, best = gap, st
		}
	}
	return best
}

func (s *server) handleIncidents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=180") // 30-day rollup; history changes slowly
	if err := s.incTmpl.Execute(w, s.buildIncidents(r)); err != nil {
		log.Printf("render incidents: %v", err)
	}
}

// handleReady is a readiness probe: it confirms ClickHouse is reachable (cached for
// a few seconds so a load balancer polling it can't hammer the DB), so a degraded
// instance is pulled out of rotation instead of serving 502s.
func (s *server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.readyMu.Lock()
	fresh := time.Since(s.readyAt) < 5*time.Second
	ok := s.readyOK
	s.readyMu.Unlock()
	if !fresh {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		_, err := s.ch.QueryJSON(ctx, "SELECT 1 AS ok")
		cancel()
		ok = err == nil
		if err != nil {
			log.Printf("readiness: clickhouse unreachable: %v", err)
		}
		s.readyMu.Lock()
		s.readyAt, s.readyOK = time.Now(), ok
		s.readyMu.Unlock()
	}
	if !ok {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprint(w, "ok")
}

func (s *server) handleRobots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, `User-agent: *
Allow: /

# AI / answer engines (explicitly welcomed for GEO)
User-agent: GPTBot
Allow: /
User-agent: OAI-SearchBot
Allow: /
User-agent: ChatGPT-User
Allow: /
User-agent: ClaudeBot
Allow: /
User-agent: PerplexityBot
Allow: /
User-agent: Google-Extended
Allow: /
User-agent: CCBot
Allow: /

Sitemap: %s/sitemap.xml
`, s.cfg.SiteURL)
}

func (s *server) handleSitemap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	today := time.Now().UTC().Format("2006-01-02")
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>%s/</loc><lastmod>%s</lastmod><changefreq>hourly</changefreq><priority>1.0</priority></url>
  <url><loc>%s/sla</loc><lastmod>%s</lastmod><changefreq>monthly</changefreq><priority>0.9</priority></url>
  <url><loc>%s/incidents</loc><lastmod>%s</lastmod><changefreq>hourly</changefreq><priority>0.7</priority></url>
</urlset>
`, s.cfg.SiteURL, today, s.cfg.SiteURL, today, s.cfg.SiteURL, today)
}

func (s *server) handleLLMs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	d := s.buildIndex(r)
	fmt.Fprintf(w, "# FlashProxy Status\n\n")
	fmt.Fprintf(w, "> Live uptime and latency for FlashProxy's proxy network, measured continuously by synthetic probes from %d global vantage points (%s).\n\n", d.VantageCount, d.VantagesText)
	fmt.Fprintf(w, "Current overall status: %s (%s).\n\n", d.OverallLabel, d.Updated)
	fmt.Fprintf(w, "## Products monitored\n\n")
	for _, p := range d.Products {
		fmt.Fprintf(w, "- %s: %s, ~%dms avg connect (best vantage: %s)\n", p.Name, p.StatusLabel, p.AvgMs, p.Vantage)
	}
	fmt.Fprintf(w, "\n## What is measured\n\nPer product, per vantage: average round-trip response time (the time the proxy takes to establish the upstream connection), gateway network round-trip (TCP connect RTT), throughput (streaming and large-object), high-frequency small-payload setup, broad scraping reachability, long-session stability, and a direct (no-proxy) baseline for comparison.\n\n")
	fmt.Fprintf(w, "## Integrity\n\nEvery measurement is committed to a public, append-only, Ed25519-signed hash-chained ledger, so anyone can verify the data has not been tampered with.\n\n")
	fmt.Fprintf(w, "## SLA\n\nFlashProxy backs these products with a 100%% uptime guarantee and automatic, proportional compensation. Full terms: %s/sla\n\n", s.cfg.SiteURL)
	fmt.Fprintf(w, "## Source\n\nThis status page is open source and fully reproducible: %s — and the metrics database is publicly readable.\n", "https://github.com/flashproxy-web/flashproxy-sla")
}

func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(v)
}

func nowRFC() string { return time.Now().UTC().Format(time.RFC3339) }
