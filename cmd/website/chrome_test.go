package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
	"github.com/flashproxy/flashproxy-status/internal/slo"
)

// chromeServer is a server with every page template loaded the way main loads
// them (parsePage) and a stand-in ClickHouse: the package-exists lookup finds a
// row, every other query returns no rows. /weekly gets a minimal cached value.
func chromeServer(t *testing.T) *server {
	t.Helper()
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Query().Get("query"), "SELECT 1 AS ok") {
			io.WriteString(w, `{"data":[{"ok":1}]}`)
			return
		}
		io.WriteString(w, `{"data":[]}`)
	}))
	t.Cleanup(ch.Close)
	const web = "../../web"
	cfg := Config{
		SiteURL: "https://status.example", WebDir: web, ClickHouse: chConn{DB: "sla"},
		Packages: []slo.Package{{Short: "datacenter", Name: "Datacenter"}},
	}
	fetched := time.Date(2026, 11, 13, 17, 17, 42, 0, time.UTC)
	return &server{
		cfg:        cfg,
		ch:         chstore.New(ch.URL, "sla", "u", "p"),
		tmpl:       parsePage(web, "index.html.tmpl"),
		slaTmpl:    parsePage(web, "sla.html.tmpl"),
		incTmpl:    parsePage(web, "incidents.html.tmpl"),
		pkgTmpl:    parsePage(web, "package.html.tmpl"),
		weeklyTmpl: parsePage(web, "weekly.html.tmpl"),
		weekly:     &weeklyCache{now: func() time.Time { return fetched }, val: &weeklyData{FetchedAt: fetched}},
	}
}

var (
	chromeTagRe     = regexp.MustCompile(`<[^>]*>`)
	chromeSpaceRe   = regexp.MustCompile(`\s+`)
	chromeHeaderRe  = regexp.MustCompile(`(?s)<header class="head">.*?</header>`)
	chromeFooterRe  = regexp.MustCompile(`(?s)<footer class="foot">.*?</footer>`)
	chromeUpdatedRe = regexp.MustCompile(`updated \d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC`)
)

// chromeText is the visible text of an HTML fragment: tags removed, runs of
// white space collapsed.
func chromeText(html string) string {
	return strings.TrimSpace(chromeSpaceRe.ReplaceAllString(chromeTagRe.ReplaceAllString(html, ""), " "))
}

// The header and footer come from web/partials.html.tmpl on every page: same
// footer bytes, site name first, the SLA link bold on the right, "updated" in
// one format, and only the active page's bracket link bold.
func TestPageChrome(t *testing.T) {
	s := chromeServer(t)
	pages := []struct {
		name, path string
		header     string // visible header text, minus the time
		active     string // bracket link shown bold, "" for none
	}{
		{"main", "/", "FlashProxy SLA [ Weekly ] [ Incidents ] 100% uptime SLA updated", ""},
		{"package", "/datacenter", "FlashProxy SLA / Datacenter [ Weekly ] [ Incidents ] 100% uptime SLA updated", ""},
		{"sla", "/sla", "FlashProxy SLA / Service Level Agreement [ Weekly ] [ Incidents ] 100% uptime SLA updated", ""},
		{"incidents", "/incidents", "FlashProxy SLA [ Weekly ] [ Incidents ] 100% uptime SLA updated", "incidents"},
		{"weekly", "/weekly", "FlashProxy SLA [ Weekly ] [ Incidents ] 100% uptime SLA updated", "weekly"},
	}
	const wantFooter = "synthetic probes · 100% Uptime SLA · every value on this page was recorded by this open-source app · source"
	var firstFooter string
	for _, p := range pages {
		t.Run(p.name, func(t *testing.T) {
			w := wkGet(s, p.path)
			if w.Code != http.StatusOK {
				t.Fatalf("GET %s = %d: %s", p.path, w.Code, w.Body.String())
			}
			body := w.Body.String()

			headers := chromeHeaderRe.FindAllString(body, -1)
			footers := chromeFooterRe.FindAllString(body, -1)
			if len(headers) != 1 || len(footers) != 1 {
				t.Fatalf("%d headers, %d footers, want one of each", len(headers), len(footers))
			}
			header, footer := headers[0], footers[0]

			// Footer: the same bytes on every page, with the two links.
			if firstFooter == "" {
				firstFooter = footer
			}
			if footer != firstFooter {
				t.Errorf("footer differs from the first page's:\n%s\nwant\n%s", footer, firstFooter)
			}
			if got := chromeText(footer); got != wantFooter {
				t.Errorf("footer text = %q, want %q", got, wantFooter)
			}
			for _, link := range []string{
				`<a href="/sla" class="plain">100% Uptime SLA</a>`,
				`<a href="https://github.com/flashproxy-web/flashproxy-sla">source</a>`,
			} {
				if !strings.Contains(footer, link) {
					t.Errorf("footer lacks %s", link)
				}
			}
			if n := strings.Count(footer, "<a "); n != 2 {
				t.Errorf("footer has %d links, want 2", n)
			}

			// Header: one line of text, site name first, time last.
			text := chromeText(header)
			if !strings.HasPrefix(text, "FlashProxy SLA") {
				t.Errorf("header text %q does not begin with the site name", text)
			}
			stamp := chromeUpdatedRe.FindString(text)
			if stamp == "" || !strings.HasSuffix(text, stamp) {
				t.Errorf("header text %q does not end with an `updated YYYY-MM-DD HH:MM UTC` time", text)
			}
			if got := strings.TrimSpace(strings.TrimSuffix(text, strings.TrimPrefix(stamp, "updated"))); got != p.header {
				t.Errorf("header text = %q, want %q", got, p.header)
			}
			if p.name == "weekly" && stamp != "updated 2026-11-13 17:17 UTC" {
				t.Errorf("weekly header time = %q, want the cache's FetchedAt", stamp)
			}

			// 100% uptime SLA: always bold, always a link to /sla.
			if !strings.Contains(header, `<a href="/sla" class="plain bold">100% uptime SLA</a>`) {
				t.Errorf("header lacks the bold /sla link: %s", header)
			}
			// Bracket links: only the active page's is bold.
			for _, l := range []struct{ key, label string }{{"weekly", "[ Weekly ]"}, {"incidents", "[ Incidents ]"}} {
				plain := `<a href="/` + l.key + `" class="plain">` + l.label + `</a>`
				bold := `<a href="/` + l.key + `" class="plain bold" aria-current="page">` + l.label + `</a>`
				want, not := plain, bold
				if p.active == l.key {
					want, not = bold, plain
				}
				if !strings.Contains(header, want) || strings.Contains(header, not) {
					t.Errorf("active=%q: header wants %s: %s", p.active, want, header)
				}
			}
			wantBold := 1 // the SLA link
			if p.active != "" {
				wantBold = 2
			}
			if n := strings.Count(header, "bold"); n != wantBold {
				t.Errorf("header has %d bold items for active=%q, want %d: %s", n, p.active, wantBold, header)
			}
			if strings.Contains(body, "flashproxy<span") || strings.Contains(body, "ZgotmplZ") {
				t.Errorf("old brand markup or an escaping failure in the page")
			}

			// The header is the only page chrome: no heading, no second nav
			// row, and the old site name nowhere.
			for _, gone := range []string{"<h1", `class="subnav"`, "FlashProxy Status"} {
				if strings.Contains(body, gone) {
					t.Errorf("page still contains %q", gone)
				}
			}
			if p.name == "incidents" && !strings.Contains(body, `<a href="/api/events">events API</a>`) {
				t.Errorf("incidents help text lacks the events API link")
			}
		})
	}
}

// No page keeps its own header or footer markup: each calls the shared
// definitions once.
func TestPagesUsePartials(t *testing.T) {
	for _, f := range []string{"index", "package", "sla", "incidents", "weekly"} {
		b, err := os.ReadFile("../../web/" + f + ".html.tmpl")
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		for _, call := range []string{`{{template "header" .}}`, `{{template "footer" .}}`} {
			if n := strings.Count(src, call); n != 1 {
				t.Errorf("%s: %d x %s, want 1", f, n, call)
			}
		}
		for _, own := range []string{"<header", "<footer"} {
			if strings.Contains(src, own) {
				t.Errorf("%s: has its own %s markup", f, own)
			}
		}
	}
}

// The site is named "FlashProxy SLA" in llms.txt too.
func TestLLMsSiteName(t *testing.T) {
	w := wkGet(chromeServer(t), "/llms.txt")
	if body := w.Body.String(); w.Code != http.StatusOK || !strings.HasPrefix(body, "# FlashProxy SLA\n") || strings.Contains(body, "FlashProxy Status") {
		t.Errorf("GET /llms.txt = %d: %s", w.Code, body)
	}
}

// The partial file is a template source, never served.
func TestPartialsNotServed(t *testing.T) {
	s := chromeServer(t)
	if w := wkGet(s, "/partials.html.tmpl"); w.Code != http.StatusNotFound {
		t.Errorf("GET /partials.html.tmpl = %d, want 404", w.Code)
	}
}
