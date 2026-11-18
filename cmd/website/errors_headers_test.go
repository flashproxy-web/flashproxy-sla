package main

import (
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// chStub swaps s.ch for a ClickHouse stand-in served by h.
func chStub(t *testing.T, s *server, h http.HandlerFunc) {
	t.Helper()
	ch := httptest.NewServer(h)
	t.Cleanup(ch.Close)
	s.ch = chstore.New(ch.URL, "sla", "u", "p")
}

// chFailAll answers every query with a ClickHouse-style 500.
func chFailAll(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusInternalServerError)
	io.WriteString(w, "Code: 241. DB::Exception: secret detail")
}

func serve(s *server, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.middleware(s.routes()).ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

// checkErrorHeaders asserts an error response: the code, Cache-Control exactly
// no-store, no max-age, no ClickHouse text, no cookie or Vary.
func checkErrorHeaders(t *testing.T, path string, w *httptest.ResponseRecorder, code int) {
	t.Helper()
	if w.Code != code {
		t.Errorf("%s: code = %d, want %d", path, w.Code, code)
	}
	cc := w.Header().Get("Cache-Control")
	if cc != "no-store" {
		t.Errorf("%s: Cache-Control = %q, want no-store", path, cc)
	}
	if strings.Contains(cc, "max-age") || strings.Contains(cc, "public") {
		t.Errorf("%s: cacheable error: %q", path, cc)
	}
	if strings.Contains(w.Body.String(), "DB::Exception") {
		t.Errorf("%s: ClickHouse error text leaked: %q", path, w.Body.String())
	}
	checkNoCookieVary(t, w)
}

// Every query failing: every ClickHouse-backed route answers 502 (/healthz 503)
// with no-store, never a degraded 200 or a cacheable error.
func TestErrorCacheHeadersQueryFailure(t *testing.T) {
	s := chromeServer(t)
	chStub(t, s, chFailAll)
	for _, path := range []string{
		"/", "/sla", "/llms.txt",
		"/api/status", "/api/overview", "/api/events", "/api/ledger",
		"/api/series?package=datacenter", "/api/scenarios?package=datacenter",
		"/datacenter", "/incidents",
	} {
		checkErrorHeaders(t, path, serve(s, path), http.StatusBadGateway)
	}
	checkErrorHeaders(t, "/healthz", serve(s, "/healthz"), http.StatusServiceUnavailable)
}

func TestErrorCacheHeadersBadRequest(t *testing.T) {
	s := chromeServer(t)
	for _, path := range []string{
		"/api/series", "/api/series?package=datacenter&vantage=a'b",
		"/api/scenarios", "/api/scenarios?package=datacenter&vantage=a'b",
	} {
		checkErrorHeaders(t, path, serve(s, path), http.StatusBadRequest)
	}
}

// 404s that are not an unknown package (no lookup) stay exactly no-store.
func TestErrorCacheHeadersNotFound(t *testing.T) {
	s := chromeServer(t)
	chStub(t, s, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"data":[]}`) })
	for _, path := range []string{"/nosuchthing.tmpl", "/.hidden", "/a/b"} {
		w := serve(s, path)
		checkErrorHeaders(t, path, w, http.StatusNotFound)
		if got := w.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s: Cache-Control = %q, want exactly no-store", path, got)
		}
	}
}

// An unknown package (lookup finds no rows) is a cacheable negative answer:
// 404, Cache-Control exactly "public, max-age=60", same body as notFound.
func TestUnknownPackageNegativeCache(t *testing.T) {
	s := chromeServer(t)
	chStub(t, s, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"data":[]}`) })
	w := serve(s, "/unknownpkg")
	if w.Code != http.StatusNotFound {
		t.Errorf("code = %d, want 404", w.Code)
	}
	cc := w.Header().Get("Cache-Control")
	if cc != "public, max-age=60" {
		t.Errorf("Cache-Control = %q, want public, max-age=60", cc)
	}
	if strings.Contains(cc, "no-store") {
		t.Errorf("Cache-Control %q contains no-store", cc)
	}
	if got := w.Body.String(); got != "404 page not found\n" {
		t.Errorf("body = %q, want 404 page not found", got)
	}
	checkNoCookieVary(t, w)
}

// A failing lookup on a package path is a 502 with exactly no-store, never the
// negative-cache header.
func TestUnknownPackageLookupErrorNoStore(t *testing.T) {
	s := chromeServer(t)
	chStub(t, s, chFailAll)
	w := serve(s, "/unknownpkg")
	checkErrorHeaders(t, "/unknownpkg", w, http.StatusBadGateway)
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want exactly no-store", got)
	}
}

// Success keeps its cache headers; an empty but successful rollup still renders.
func TestSuccessCacheHeaders(t *testing.T) {
	s := chromeServer(t) // every rollup query returns no rows
	for _, c := range []struct{ path, cc string }{
		{"/", "public, max-age=30"},
		{"/sla", "public, max-age=300"},
		{"/datacenter", "no-cache"}, // no chips: revalidate
		{"/weekly", "public, max-age=3600"},
		{"/incidents", "public, max-age=60, s-maxage=60"},
		{"/llms.txt", ""},
	} {
		w := serve(s, c.path)
		if w.Code != http.StatusOK {
			t.Errorf("%s: code = %d, want 200: %s", c.path, w.Code, w.Body.String())
		}
		if got := w.Header().Get("Cache-Control"); got != c.cc {
			t.Errorf("%s: Cache-Control = %q, want %q", c.path, got, c.cc)
		}
		checkNoCookieVary(t, w)
	}
}

// A template that writes some bytes and then fails: 502 no-store, no partial page.
func TestErrorCacheHeadersRenderError(t *testing.T) {
	bad := func() *template.Template {
		return template.Must(template.New("x").Parse(`partial {{.NoSuchField}}`))
	}
	s := chromeServer(t) // packageExists finds datacenter
	s.tmpl, s.slaTmpl, s.pkgTmpl, s.weeklyTmpl = bad(), bad(), bad(), bad()
	for _, path := range []string{"/", "/sla", "/datacenter", "/weekly"} {
		w := serve(s, path)
		checkErrorHeaders(t, path, w, http.StatusBadGateway)
		if strings.Contains(w.Body.String(), "partial") {
			t.Errorf("%s: partial page written on render error: %q", path, w.Body.String())
		}
	}
}

// The package lookup succeeds but the chip rollup fails: the best-effort page
// still renders (200), but it is never stored.
func TestPackageRollupFailureNoStore(t *testing.T) {
	s := chromeServer(t)
	chStub(t, s, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Query().Get("query"), "SELECT 1 AS ok") {
			io.WriteString(w, `{"data":[{"ok":1}]}`)
			return
		}
		chFailAll(w, r)
	})
	w := serve(s, "/datacenter")
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	if !strings.Contains(w.Body.String(), "</html>") {
		t.Errorf("page not complete")
	}
	checkNoCookieVary(t, w)
}

func TestWeeklyCacheHeaders(t *testing.T) {
	t.Run("cold", func(t *testing.T) {
		s := chromeServer(t)
		s.weekly = &weeklyCache{}
		w := serve(s, "/weekly")
		checkErrorHeaders(t, "/weekly", w, http.StatusServiceUnavailable)
		if w.Header().Get("Retry-After") != "15" {
			t.Errorf("Retry-After = %q, want 15", w.Header().Get("Retry-After"))
		}
	})
	for _, c := range []struct {
		name string
		age  time.Duration
		cc   string
	}{{"fresh", 0, "public, max-age=3600"}, {"stale", 16 * time.Minute, "no-store"}} {
		t.Run(c.name, func(t *testing.T) {
			s := chromeServer(t)
			fetched := s.weekly.val.FetchedAt
			s.weekly.now = func() time.Time { return fetched.Add(c.age) }
			w := serve(s, "/weekly")
			if w.Code != http.StatusOK {
				t.Errorf("code = %d, want 200", w.Code)
			}
			if got := w.Header().Get("Cache-Control"); got != c.cc {
				t.Errorf("Cache-Control = %q, want %q", got, c.cc)
			}
			checkNoCookieVary(t, w)
		})
	}
}

// A panic after the handler set a success header: 500 with no-store.
func TestPanicNoStore(t *testing.T) {
	s := chromeServer(t)
	h := s.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=30")
		panic("boom")
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	checkErrorHeaders(t, "panic", w, http.StatusInternalServerError)
}

// A static file missing from web_dir: ServeFile's 404 still carries no-store.
func TestStaticFileMissingNoStore(t *testing.T) {
	s := chromeServer(t)
	s.cfg.WebDir = t.TempDir() // empty: app.js is absent
	for _, path := range []string{"/app.js", "/style.css"} {
		checkErrorHeaders(t, path, serve(s, path), http.StatusNotFound)
	}
}

// A static file that exists keeps its success header.
func TestStaticFileSuccessCacheHeaders(t *testing.T) {
	s := chromeServer(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app.js"), []byte("// js\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.cfg.WebDir = dir
	w := serve(s, "/app.js")
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", w.Code)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-cache, no-store, must-revalidate" {
		t.Errorf("Cache-Control = %q, want no-cache, no-store, must-revalidate", got)
	}
	checkNoCookieVary(t, w)
}
