package main

import (
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// /incidents is cached only by the CDN: a rendered page carries
// incidentsCacheControl, a failure carries no-store and never the success value.
func TestIncidentsCacheHeaders(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		s := chromeServer(t)
		w := httptest.NewRecorder()
		s.middleware(s.routes()).ServeHTTP(w, httptest.NewRequest("GET", "/incidents", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("GET /incidents = %d: %s", w.Code, w.Body.String())
		}
		if got := w.Header().Get("Cache-Control"); got != "public, max-age=60, s-maxage=60" {
			t.Errorf("Cache-Control = %q, want %q", got, "public, max-age=60, s-maxage=60")
		}
		checkNoCookieVary(t, w)
	})

	t.Run("query error", func(t *testing.T) {
		ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, "Code: 241. DB::Exception: secret detail")
		}))
		t.Cleanup(ch.Close)
		s := chromeServer(t)
		s.ch = chstore.New(ch.URL, "sla", "u", "p")
		w := httptest.NewRecorder()
		s.middleware(s.routes()).ServeHTTP(w, httptest.NewRequest("GET", "/incidents", nil))
		if w.Code != http.StatusBadGateway {
			t.Errorf("code = %d, want 502", w.Code)
		}
		if got := w.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
		if strings.Contains(w.Body.String(), "DB::Exception") {
			t.Errorf("ClickHouse error text leaked: %q", w.Body.String())
		}
		checkNoCookieVary(t, w)
	})

	t.Run("render error", func(t *testing.T) {
		s := chromeServer(t)
		// Writes some bytes, then fails on a field incidentsData does not have.
		s.incTmpl = template.Must(template.New("incidents").Parse(`partial {{.NoSuchField}}`))
		w := httptest.NewRecorder()
		s.middleware(s.routes()).ServeHTTP(w, httptest.NewRequest("GET", "/incidents", nil))
		if w.Code != http.StatusBadGateway {
			t.Errorf("code = %d, want 502", w.Code)
		}
		if got := w.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
		if strings.Contains(w.Body.String(), "partial") {
			t.Errorf("partial page written on render error: %q", w.Body.String())
		}
	})
}

func checkNoCookieVary(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if v := w.Header().Values("Set-Cookie"); len(v) != 0 {
		t.Errorf("Set-Cookie present: %q", v)
	}
	if v := w.Header().Values("Vary"); len(v) != 0 {
		t.Errorf("Vary present: %q", v)
	}
}
