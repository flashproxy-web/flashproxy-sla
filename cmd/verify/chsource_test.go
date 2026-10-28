package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// fakeCH answers every query with one FORMAT JSON result holding row.
func fakeCH(t *testing.T, row map[string]any, extra string) *chSource {
	t.Helper()
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"meta":[],"data":[` + string(b) + `],"rows":1` + extra + `}`))
	}))
	t.Cleanup(srv.Close)
	return &chSource{ch: chstore.New(srv.URL, "sla", "u", "p"), db: "sla"}
}

// with returns base with key k set to the raw JSON v ("" deletes k).
func with(base map[string]any, k, v string) map[string]any {
	out := map[string]any{}
	for kk, vv := range base {
		out[kk] = vv
	}
	if v == "" {
		delete(out, k)
	} else {
		out[k] = json.RawMessage(v)
	}
	return out
}

// TestChSourceFailsClosed pins that a malformed, out-of-range or missing integer
// column makes every chSource read return an error instead of a zero value.
func TestChSourceFailsClosed(t *testing.T) {
	ctx := context.Background()
	ledgerRow := map[string]any{"stream": "s", "seq": 1, "kind": "probe", "ts_first": 1, "ts_last": 2,
		"row_count": 3, "batch_hash": "b", "prev_hash": "p", "entry_hash": "e"}
	eventRow := map[string]any{"ts": 1, "type": "x", "package": "p", "message": "m", "seq": "1"}
	probeRow := map[string]any{"ts": 1, "vantage": "v", "package": "p", "scenario": "s", "proto": "tcp",
		"target": "t", "ip_version": 4, "success": 1, "error_type": "", "dial_ms": 1, "connect_ms": 2,
		"ttfb_ms": 3, "total_ms": 4, "bytes": "10", "throughput_mbps": 1.5, "seq": "1"}
	cpRow := map[string]any{"stream": "s", "seq": "1", "entry_hash": "e", "ts": 1, "pubkey_id": "k", "signature": "sig"}

	reads := []struct {
		name string
		row  map[string]any
		read func(*chSource) (int, error)
		bad  [][2]string // column, raw JSON value ("" = missing)
	}{
		{"ledgerPage", ledgerRow, func(s *chSource) (int, error) {
			r, err := s.ledgerPage(ctx, "s", 1, 10)
			return len(r), err
		}, [][2]string{{"ts_last", `"x"`}, {"seq", `1.5`}, {"row_count", `4294967296`}, {"ts_first", `null`}, {"seq", ""}}},
		{"rowPage events", eventRow, func(s *chSource) (int, error) {
			r, err := s.rowPage(ctx, "events", 1, 1, 10)
			return len(r), err
		}, [][2]string{{"ts", `"x"`}, {"seq", `1.5`}, {"seq", ""}}},
		{"rowPage probes", probeRow, func(s *chSource) (int, error) {
			r, err := s.rowPage(ctx, "v", 1, 1, 10)
			return len(r), err
		}, [][2]string{{"success", `300`}, {"ip_version", `-1`}, {"seq", `1.5`}, {"bytes", `"x"`},
			{"dial_ms", `4294967296`}, {"throughput_mbps", `null`}, {"ts", ""}}},
		{"checkpoints", cpRow, func(s *chSource) (int, error) {
			r, err := s.checkpoints(ctx)
			return len(r), err
		}, [][2]string{{"ts", `"x"`}, {"seq", `1.5`}, {"seq", ""}}},
	}
	for _, rd := range reads {
		if n, err := rd.read(fakeCH(t, rd.row, "")); err != nil || n != 1 {
			t.Fatalf("%s: valid row: %d rows, err %v", rd.name, n, err)
		}
		for _, b := range rd.bad {
			if n, err := rd.read(fakeCH(t, with(rd.row, b[0], b[1]), "")); err == nil {
				t.Errorf("%s: %s=%q: no error (%d rows)", rd.name, b[0], b[1], n)
			}
		}
	}
}

// A ClickHouse error reported in-band after a partial result must fail the read,
// not end the pager as a short final page.
func TestLedgerExceptionFailsRun(t *testing.T) {
	src := fakeCH(t, map[string]any{"stream": "s", "seq": 1, "kind": "probe", "ts_first": 1, "ts_last": 2,
		"row_count": 3, "batch_hash": "b", "prev_hash": "p", "entry_hash": "e"},
		`,"exception":"Code: 241. DB::Exception: Memory limit exceeded"`)
	v := &verifier{src: src, chunkRows: 1000}
	if es, err := v.ledgerEntries(context.Background(), "s"); err == nil {
		t.Fatalf("exception ignored: pager returned %d entries and no error", len(es))
	}
}
