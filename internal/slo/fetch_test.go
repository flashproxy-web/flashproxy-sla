package slo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// A malformed verdict input must fail the fetch, not read as 0 (= down) or NaN.
func TestFetchMinutesFailsOnBadValue(t *testing.T) {
	good := `{"package":"p","vantage":"v","t":1793182800,"response_ms_avg":12.5,"success_pct":100,"samples":3}`
	fetch := func(row string) (map[string]map[string]map[int64]Minute, error) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"data":[` + row + `]}`))
		}))
		defer srv.Close()
		return fetchMinutes(context.Background(), chstore.New(srv.URL, "sla", "u", "p"), "sla", 15)
	}
	got, err := fetch(good)
	if m := got["p"]["v"][1793182800]; err != nil || m.ResponseMsAvg != 12.5 || m.SuccessPct != 100 || m.Samples != 3 {
		t.Fatalf("valid row: %+v, %v", m, err)
	}
	for _, kv := range [][2]string{
		{`"success_pct":100`, `"success_pct":null`},
		{`"success_pct":100`, `"success_pct":"x"`},
		{`"response_ms_avg":12.5`, `"response_ms_avg":"nan"`},
		{`"response_ms_avg":12.5,`, ``},
		{`"t":1793182800`, `"t":1.5`},
		{`"samples":3`, `"samples":-1`},
	} {
		if _, err := fetch(strings.Replace(good, kv[0], kv[1], 1)); err == nil {
			t.Errorf("%s -> %s: no error", kv[0], kv[1])
		}
	}
}

// Values are decoded straight into the destination types (Minute.T int64,
// Samples uint64): text that does not fit uint32 must still decode exactly, and
// the published JSON of a count is a plain integer.
func TestFetchMinutesDestinationTypes(t *testing.T) {
	row := `{"package":"p","vantage":"v","t":"5000000000","response_ms_avg":12.5,"success_pct":99.25,"samples":"4294967296"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":[` + row + `]}`))
	}))
	defer srv.Close()
	got, err := fetchMinutes(context.Background(), chstore.New(srv.URL, "sla", "u", "p"), "sla", 15)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got["p"]["v"][5000000000]
	if !ok || m.T != 5000000000 || m.Samples != 4294967296 {
		t.Fatalf("minute = %+v (found %v)", m, ok)
	}
	b, _ := json.Marshal(Bar{T: m.T, Status: "operational", ResponseMsAvg: m.ResponseMsAvg, SuccessPct: m.SuccessPct, Samples: m.Samples})
	if !strings.Contains(string(b), `"samples":4294967296`) {
		t.Fatalf("json: %s", b)
	}
}
