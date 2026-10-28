package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
	"github.com/flashproxy/flashproxy-status/internal/ledger"
)

// lateSource models a batch and its checkpoint committed while verify runs: the
// ledger read sees seqs 1..5 only, but a checkpoint for seq 6 exists by the time
// checkpoints() is called after any ledger read.
type lateSource struct {
	*fakeSource
	late      ledger.Checkpoint
	readLedge bool
}

func (s *lateSource) ledgerPage(ctx context.Context, stream string, from uint64, limit int) ([]ledger.Entry, error) {
	s.readLedge = true
	return s.fakeSource.ledgerPage(ctx, stream, from, limit)
}

func (s *lateSource) checkpoints(ctx context.Context) ([]ledger.Checkpoint, error) {
	cps, _ := s.fakeSource.checkpoints(ctx)
	if s.readLedge {
		cps = append(cps, s.late)
	}
	return cps, nil
}

// Checkpoints anchor the chain: the run must not pass while it read less than the
// signed heads say exists.
func TestCheckpointsAnchorTheLedger(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	cases := []struct {
		name   string
		src    func() source
		wantOK bool
		want   string
	}{
		{"short tail: ledger read stops below the signed head", func() source {
			f := fixture(t, priv, 1, 1, 1, 1, 1)
			f.entries["s1"] = f.entries["s1"][:3]
			return f
		}, false, "checkpoints s1: 1 attest seq 5..5, not in the ledger read"},
		{"checkpoint for a stream the ledger does not list", func() source {
			f := fixture(t, priv, 1, 1)
			f.cps = append(f.cps, ledger.SignCheckpoint(priv, "s2", 1, strings.Repeat("0", 64), fixtureBase.Unix()))
			return f
		}, false, "checkpoints s2: 1 attest seq 1..1, not in the ledger read"},
		{"checkpoint below the head (ledger grew after signing)", func() source {
			f := fixture(t, priv, 1, 1, 1)
			f.cps[0] = ledger.SignCheckpoint(priv, "s1", 2, f.entries["s1"][1].EntryHash, fixtureBase.Unix())
			return f
		}, true, "1 consistent with chain"},
		{"batch + checkpoint written mid-run are outside the run", func() source {
			f := fixture(t, priv, 1, 1, 1, 1, 1)
			return &lateSource{fakeSource: f, late: ledger.SignCheckpoint(priv, "s1", 6, strings.Repeat("0", 64), fixtureBase.Unix())}
		}, true, "1 consistent with chain"},
		{"failed entry at the checkpoint is counted once", func() source {
			f := fixture(t, priv, 1, 1)
			f.rows["s1"][len(f.rows["s1"])-1].Canon += "x"
			return f
		}, false, "1 problem(s) detected"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runVerify(tc.src(), pub, 2)
			if ok := got.err == ""; ok != tc.wantOK {
				t.Fatalf("ok = %v (err %q), want %v\n%s", ok, got.err, tc.wantOK, got.out)
			}
			if !strings.Contains(got.out+got.err, tc.want) {
				t.Fatalf("output lacks %q:\n%s%s", tc.want, got.out, got.err)
			}
		})
	}
}

// A ledger answer whose "data" is missing or null decodes to zero rows without an
// error; a stream that streams() listed must still fail the run.
func TestLedgerDataMissingFails(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := fixture(t, priv, 1, 1)
	cp, _ := json.Marshal([]any{map[string]any{"stream": "s1", "seq": f.cps[0].Seq, "entry_hash": f.cps[0].EntryHash,
		"ts": f.cps[0].TS, "pubkey_id": f.cps[0].PubKeyID, "signature": f.cps[0].Signature}})
	for _, tc := range []struct{ name, ledgerBody, cps string }{
		{"data null, checkpoint present", `{"meta":[],"data":null,"rows":0}`, string(cp)},
		{"data missing, checkpoint present", `{"meta":[],"rows":0}`, string(cp)},
		{"data null, no checkpoints", `{"meta":[],"data":null,"rows":0}`, `[]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query().Get("query")
				switch {
				case strings.Contains(q, "DISTINCT stream"):
					w.Write([]byte(`{"data":[{"stream":"s1"}]}`))
				case strings.Contains(q, "ledger_checkpoints"):
					w.Write([]byte(`{"data":` + tc.cps + `}`))
				case strings.Contains(q, ".ledger "):
					w.Write([]byte(tc.ledgerBody))
				default:
					w.Write([]byte(`{"data":[]}`))
				}
			}))
			defer srv.Close()
			got := runVerify(&chSource{ch: chstore.New(srv.URL, "sla", "u", "p"), db: "sla"}, pub, 10)
			if got.err == "" {
				t.Fatalf("run passed with an empty ledger read:\n%s", got.out)
			}
		})
	}
}

// The head is re-signed every tick, so a short stream can carry thousands of
// checkpoints: the run reports them as one line and one problem per stream.
func TestUnreadCheckpointsSummarised(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := fixture(t, priv, 1, 1, 1, 1, 1)
	f.entries["s1"] = f.entries["s1"][:2]
	e := f.entries["s1"]
	for i := 0; i < 1500; i++ {
		f.cps = append(f.cps, f.cps[0])
	}
	f.cps = append(f.cps, ledger.SignCheckpoint(priv, "s1", 2, e[1].EntryHash, fixtureBase.Unix()))
	got := runVerify(f, pub, 2)
	if got.err == "" || got.problems != 1 {
		t.Fatalf("want a failed run with 1 problem, got %+v", got.err)
	}
	if n := strings.Count(got.out, "not in the ledger read"); n != 1 || !strings.Contains(got.out, "checkpoints s1: 1501 attest seq 5..5") {
		t.Fatalf("%d summary line(s):\n%.2000s", n, got.out)
	}
}

// cpSource serves the ledger from a fixture but checkpoints from a ClickHouse
// answer, so an empty "data" goes through the real decoder.
type cpSource struct {
	*fakeSource
	ch *chSource
}

func (s cpSource) checkpoints(ctx context.Context) ([]ledger.Checkpoint, error) {
	return s.ch.checkpoints(ctx)
}

// With -pubkey, a checkpoint read that comes back empty must not switch the
// anchor off; without -pubkey (unsigned deployments have none) it is allowed.
func TestEmptyCheckpointReadWithPubkeyFails(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	for _, body := range []string{`{"meta":[],"data":null,"rows":0}`, `{"meta":[],"rows":0}`, `{"data":[]}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
		src := cpSource{fixture(t, priv, 1, 1), &chSource{ch: chstore.New(srv.URL, "sla", "u", "p"), db: "sla"}}
		if got := runVerify(src, pub, 2); !strings.Contains(got.err, "no checkpoints read") {
			t.Errorf("%s with -pubkey: err %q\n%s", body, got.err, got.out)
		}
		if got := runVerify(src, nil, 2); got.err != "" {
			t.Errorf("%s without -pubkey: %q", body, got.err)
		}
		srv.Close()
	}
	// A brand-new deployment (no streams yet) fails as before, not with this error.
	if got := runVerify(&fakeSource{entries: map[string][]ledger.Entry{}}, pub, 2); got.err != "no ledger streams found" {
		t.Fatalf("empty db: %q", got.err)
	}
}
