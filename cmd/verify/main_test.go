package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/ledger"
	"github.com/flashproxy/flashproxy-status/internal/model"
)

// ---- pager ----

// tiesPage implements the fetchFunc contract over an in-memory seq-sorted slice:
// seq >= from, ORDER BY seq, LIMIT limit WITH TIES.
func tiesPage(all []seqRow, from uint64, limit int) []seqRow {
	var out []seqRow
	for _, r := range all {
		if r.Seq < from {
			continue
		}
		if len(out) >= limit && r.Seq != out[limit-1].Seq {
			break
		}
		out = append(out, r)
	}
	return out
}

// rowsOfSizes makes rows for seq 1..len(sizes), sizes[i] rows at seq i+1.
func rowsOfSizes(sizes ...int) []seqRow {
	var out []seqRow
	for i, n := range sizes {
		for j := 0; j < n; j++ {
			out = append(out, seqRow{Seq: uint64(i + 1), Canon: fmt.Sprintf("r%d.%d", i+1, j)})
		}
	}
	return out
}

func TestPagerWholeBatches(t *testing.T) {
	cases := []struct {
		name  string
		sizes []int
		limit int
		want  [][]uint64 // distinct seqs per page
	}{
		{"boundary exactly at batch boundary", []int{2, 2, 2}, 2, [][]uint64{{1}, {2}, {3}}},
		{"two batches fill a page exactly", []int{2, 2, 2, 2}, 4, [][]uint64{{1, 2}, {3, 4}}},
		{"batch spanning the boundary is not split", []int{2, 3, 1}, 3, [][]uint64{{1, 2}, {3}}},
		{"last chunk partial", []int{1, 1, 1, 1, 1}, 2, [][]uint64{{1, 2}, {3, 4}, {5}}},
		{"one seq bigger than the limit comes back whole", []int{1, 5, 1}, 2, [][]uint64{{1, 2}, {3}}},
		{"everything in one page", []int{3, 1, 2}, 1000, [][]uint64{{1, 2, 3}}},
		{"empty stream", nil, 3, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			all := rowsOfSizes(tc.sizes...)
			p := newPager(func(_ context.Context, from uint64, limit int) ([]seqRow, error) {
				return tiesPage(all, from, limit), nil
			}, func(r seqRow) uint64 { return r.Seq }, tc.limit)
			var got [][]uint64
			total := 0
			for {
				page, err := p.next(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(page) == 0 {
					break
				}
				total += len(page)
				var seqs []uint64
				for _, r := range page {
					if len(seqs) == 0 || seqs[len(seqs)-1] != r.Seq {
						seqs = append(seqs, r.Seq)
					}
				}
				got = append(got, seqs)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("pages = %v, want %v", got, tc.want)
			}
			if total != len(all) {
				t.Fatalf("read %d rows, want %d", total, len(all))
			}
		})
	}
}

func TestPagerRejectsInconsistentPages(t *testing.T) {
	boom := errors.New("connection reset")
	cases := []struct {
		name  string
		page  []seqRow
		err   error
		limit int
		want  string
	}{
		{"read error", nil, boom, 2, "connection reset"},
		{"not seq-ordered", []seqRow{{Seq: 2}, {Seq: 1}}, nil, 5, "not seq-ordered"},
		{"more rows than LIMIT WITH TIES allows", []seqRow{{Seq: 1}, {Seq: 1}, {Seq: 2}}, nil, 2, "not the tied seq"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPager(func(context.Context, uint64, int) ([]seqRow, error) { return tc.page, tc.err },
				func(r seqRow) uint64 { return r.Seq }, tc.limit)
			if _, err := p.next(context.Background()); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
	// A page that goes back below the requested from is rejected too.
	calls := 0
	p := newPager(func(_ context.Context, from uint64, _ int) ([]seqRow, error) {
		calls++
		if calls == 1 {
			return []seqRow{{Seq: 1}, {Seq: 2}}, nil
		}
		return []seqRow{{Seq: 2}}, nil // from is 3 here
	}, func(r seqRow) uint64 { return r.Seq }, 2)
	if _, err := p.next(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.next(context.Background()); err == nil {
		t.Fatal("page below from accepted")
	}
}

// ---- verifier over a fake source ----

type fakeSource struct {
	entries map[string][]ledger.Entry
	rows    map[string][]seqRow // sorted by seq
	cps     []ledger.Checkpoint
	calls   int // rowPage calls
	failAt  int // rowPage call number that errors (0 = never)
	ignHead bool
}

func (f *fakeSource) streams(context.Context) ([]string, error) {
	var out []string
	for s := range f.entries {
		out = append(out, s)
	}
	sort.Strings(out)
	return out, nil
}

func (f *fakeSource) ledgerPage(_ context.Context, stream string, from uint64, limit int) ([]ledger.Entry, error) {
	var out []ledger.Entry
	for _, e := range f.entries[stream] {
		if e.Seq < from {
			continue
		}
		if len(out) >= limit && e.Seq != out[limit-1].Seq {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

func (f *fakeSource) rowPage(_ context.Context, stream string, from, head uint64, limit int) ([]seqRow, error) {
	f.calls++
	if f.calls == f.failAt {
		return nil, errors.New("unexpected EOF")
	}
	var in []seqRow
	for _, r := range f.rows[stream] {
		if r.Seq <= head || f.ignHead {
			in = append(in, r)
		}
	}
	return tiesPage(in, from, limit), nil
}

func (f *fakeSource) checkpoints(context.Context) ([]ledger.Checkpoint, error) { return f.cps, nil }

// fixtureBase is taken once per process: fixtures built at different moments of one
// test run (e.g. per chunk size) must be byte-identical, or their chain hashes differ.
var fixtureBase = time.Now().Add(-time.Hour).Truncate(time.Second).UTC()

// fixture builds one probe stream "s1" with the given batch sizes (seq 1..n), its
// ledger, and a signed checkpoint at the head. Timestamps are recent, so missing
// rows count as missing (not TTL-expired).
func fixture(t *testing.T, priv ed25519.PrivateKey, sizes ...int) *fakeSource {
	t.Helper()
	base := fixtureBase
	c := ledger.NewChain("s1")
	f := &fakeSource{entries: map[string][]ledger.Entry{}, rows: map[string][]seqRow{}}
	for b, n := range sizes {
		var canon []string
		for i := 0; i < n; i++ {
			r := model.ProbeResult{
				TS: base.Add(time.Duration(b) * time.Minute), Vantage: "s1", Package: "isp",
				Scenario: "connect", Proto: "http", Target: "x:443", IPVersion: 4, Success: 1,
				ConnectMS: uint32(10 + i), TotalMS: uint32(b), ThroughputMbps: 1.5,
			}
			canon = append(canon, r.Canonical())
		}
		ts := base.Add(time.Duration(b) * time.Minute).Unix()
		e := c.Append(ledger.KindProbe, canon, ts, ts)
		f.entries["s1"] = append(f.entries["s1"], e)
		for _, s := range canon {
			f.rows["s1"] = append(f.rows["s1"], seqRow{Seq: e.Seq, Canon: s})
		}
	}
	seq, head := c.Head()
	f.cps = []ledger.Checkpoint{ledger.SignCheckpoint(priv, "s1", seq, head, base.Unix())}
	return f
}

// verdict is everything a run reports: the error, the problem count, and the output.
type verdict struct {
	err      string
	problems int
	out      string
}

func runVerify(src source, pub ed25519.PublicKey, chunk int) verdict {
	var buf bytes.Buffer
	v := &verifier{src: src, chunkRows: chunk, out: &buf, pub: pub}
	err := v.run(context.Background())
	vd := verdict{problems: v.problems, out: buf.String()}
	if err != nil {
		vd.err = err.Error()
	}
	return vd
}

func rowIndex(f *fakeSource, seq uint64) int {
	for i, r := range f.rows["s1"] {
		if r.Seq == seq {
			return i
		}
	}
	panic("no row at seq")
}

func TestVerifyChunked(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	// Batch sizes chosen so chunk sizes 2/3/4 land boundaries both on and inside batches.
	sizes := []int{2, 3, 1, 4, 2, 2}
	cases := []struct {
		name   string
		mutate func(f *fakeSource)
		wantOK bool
		want   string // substring of the output
	}{
		{"clean", func(*fakeSource) {}, true, "6 entries"},
		{"tampered row in the last chunk", func(f *fakeSource) {
			f.rows["s1"][len(f.rows["s1"])-1].Canon += "x"
		}, false, "FAIL seq 6: stream s1 seq 6: batch_hash mismatch"},
		{"tampered row in a middle chunk", func(f *fakeSource) {
			f.rows["s1"][rowIndex(f, 4)].Canon += "x"
		}, false, "FAIL seq 4"},
		{"missing row at a chunk boundary", func(f *fakeSource) {
			i := rowIndex(f, 3)
			f.rows["s1"] = append(f.rows["s1"][:i], f.rows["s1"][i+1:]...)
		}, false, "FAIL seq 3"},
		{"row moved to the next batch across a boundary", func(f *fakeSource) {
			f.rows["s1"][rowIndex(f, 3)].Seq = 4
		}, false, "FAIL seq 3"},
		{"ledger entry deleted at a boundary breaks the chain", func(f *fakeSource) {
			e := f.entries["s1"]
			f.entries["s1"] = append(e[:2:2], e[3:]...)
		}, false, "seq gap: 1 missing between 2 and 4"},
		{"wrong chain link", func(f *fakeSource) {
			f.entries["s1"][4].PrevHash = ledger.GenesisHash
		}, false, "FAIL seq 5: stream s1 seq 5: prev_hash"},
		{"bad checkpoint signature", func(f *fakeSource) {
			sig := []byte(f.cps[0].Signature)
			if sig[0] == 'A' {
				sig[0] = 'B'
			} else {
				sig[0] = 'A'
			}
			f.cps[0].Signature = string(sig)
		}, false, "checkpoint s1 seq 6"},
		{"rows appended past the ledger head are ignored", func(f *fakeSource) {
			f.rows["s1"] = append(f.rows["s1"], seqRow{Seq: 7, Canon: "live"}, seqRow{Seq: 7, Canon: "live2"})
		}, true, "6 entries"},
		{"rows with no ledger entry below the head are ignored", func(f *fakeSource) {
			f.rows["s1"] = append([]seqRow{{Seq: 0, Canon: "legacy"}}, f.rows["s1"]...)
		}, true, "6 entries"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ref verdict
			for i, chunk := range []int{1_000_000, 1, 2, 3, 4, 7} {
				f := fixture(t, priv, sizes...)
				tc.mutate(f)
				sort.SliceStable(f.rows["s1"], func(a, b int) bool { return f.rows["s1"][a].Seq < f.rows["s1"][b].Seq })
				got := runVerify(f, pub, chunk)
				if i == 0 {
					ref = got
					if ok := got.err == ""; ok != tc.wantOK {
						t.Fatalf("ok = %v (err %q), want %v\n%s", ok, got.err, tc.wantOK, got.out)
					}
					if !strings.Contains(got.out, tc.want) {
						t.Fatalf("output lacks %q:\n%s", tc.want, got.out)
					}
					continue
				}
				if got != ref {
					t.Fatalf("chunk-rows=%d verdict differs from one-chunk run:\n got %+v\nwant %+v", chunk, got, ref)
				}
			}
		})
	}
}

func TestVerifyEmptyAndExpired(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	// Stream with entries but no raw rows: recent entries fail, TTL-expired ones pass on linkage.
	f := fixture(t, priv, 2, 2)
	f.rows["s1"] = nil
	if got := runVerify(f, pub, 1); got.err == "" || got.problems != 2 {
		t.Fatalf("missing recent rows not flagged: %+v", got)
	}
	f = fixture(t, priv, 2, 2)
	f.rows["s1"] = nil
	old := time.Now().Add(-500 * 24 * time.Hour).Unix()
	c := ledger.NewChain("s1")
	for i := range f.entries["s1"] {
		e := f.entries["s1"][i]
		f.entries["s1"][i] = c.Append(ledger.KindProbe, make([]string, e.RowCount), old, old)
	}
	seq, head := c.Head()
	f.cps = []ledger.Checkpoint{ledger.SignCheckpoint(priv, "s1", seq, head, old)}
	if got := runVerify(f, pub, 1); got.err != "" {
		t.Fatalf("expired rows should verify linkage only: %+v", got)
	}
	// No ledger streams at all is a failure, as before.
	if got := runVerify(&fakeSource{entries: map[string][]ledger.Entry{}}, pub, 3); got.err != "no ledger streams found" {
		t.Fatalf("empty db: %+v", got)
	}
}

func TestVerifyFailsClosedOnReadErrors(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	cases := []struct {
		name   string
		chunk  int
		mutate func(f *fakeSource)
		want   string
	}{
		{"read error on a later chunk", 1, func(f *fakeSource) { f.failAt = 3 }, "read rows for s1"},
		{"read error on the first chunk", 1, func(f *fakeSource) { f.failAt = 1 }, "unexpected EOF"},
		{"source returns rows past the head", 1000, func(f *fakeSource) {
			f.ignHead = true
			f.rows["s1"] = append(f.rows["s1"], seqRow{Seq: 9, Canon: "x"})
		}, "past the requested head"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture(t, priv, 1, 1, 1, 1, 1)
			tc.mutate(f)
			got := runVerify(f, pub, tc.chunk)
			if got.err == "" || !strings.Contains(got.err, tc.want) {
				t.Fatalf("err = %q, want it to contain %q", got.err, tc.want)
			}
		})
	}
}
