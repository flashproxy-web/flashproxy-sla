package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/ledger"
	"github.com/flashproxy/flashproxy-status/internal/model"
)

// fault scripts one store call.
type fault int

const (
	ok         fault = iota
	failBefore       // error, nothing stored
	failAfter        // stored, then error (client timeout after the server kept it)
)

type probeCall struct {
	token string
	rows  []model.ProbeResult
}

type ledgerCall struct {
	token string
	entry ledger.Entry
}

// fakeStore mimics ClickHouse token dedup: the FIRST insert per token is kept, a later
// one with the same token is dropped and acked, whatever its content.
type fakeStore struct {
	probeFaults, ledgerFaults, eventFaults []fault
	probeCalls                             []probeCall
	ledgerCalls                            []ledgerCall
	eventCalls                             []model.Event
	probes                                 map[string][]model.ProbeResult
	ledger                                 map[string]ledger.Entry
	events                                 map[string]model.Event
}

func newFakeStore() *fakeStore {
	return &fakeStore{probes: map[string][]model.ProbeResult{}, ledger: map[string]ledger.Entry{}, events: map[string]model.Event{}}
}

func pop(fs *[]fault) fault {
	if len(*fs) == 0 {
		return ok
	}
	f := (*fs)[0]
	*fs = (*fs)[1:]
	return f
}

func result(f fault) error {
	if f == ok {
		return nil
	}
	return errors.New("context deadline exceeded")
}

func (s *fakeStore) InsertProbes(_ context.Context, rs []model.ProbeResult, token string) error {
	cp := append([]model.ProbeResult(nil), rs...)
	s.probeCalls = append(s.probeCalls, probeCall{token, cp})
	f := pop(&s.probeFaults)
	if _, seen := s.probes[token]; f != failBefore && !seen {
		s.probes[token] = cp
	}
	return result(f)
}

func (s *fakeStore) InsertLedger(_ context.Context, e ledger.Entry, token string) error {
	s.ledgerCalls = append(s.ledgerCalls, ledgerCall{token, e})
	f := pop(&s.ledgerFaults)
	if _, seen := s.ledger[token]; f != failBefore && !seen {
		s.ledger[token] = e
	}
	return result(f)
}

func (s *fakeStore) InsertEvent(_ context.Context, e model.Event, token string) error {
	s.eventCalls = append(s.eventCalls, e)
	f := pop(&s.eventFaults)
	if _, seen := s.events[token]; f != failBefore && !seen {
		s.events[token] = e
	}
	return result(f)
}

// walkChain verifies the stored data the way cmd/verify does: every stored ledger
// entry against the stored rows at its seq, chained from genesis, and returns the
// number of entries.
func walkChain(t *testing.T, s *fakeStore, stream string) int {
	t.Helper()
	var entries []ledger.Entry
	for _, e := range s.ledger {
		if e.Stream == stream {
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Seq < entries[j].Seq })
	rowsBySeq := map[uint64][]string{}
	for _, rs := range s.probes {
		for _, r := range rs {
			if r.Stream == stream {
				rowsBySeq[r.Seq] = append(rowsBySeq[r.Seq], r.Canonical())
			}
		}
	}
	for _, ev := range s.events {
		if ev.Stream == stream {
			rowsBySeq[ev.Seq] = append(rowsBySeq[ev.Seq], ev.Canonical())
		}
	}
	prev := ledger.GenesisHash
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Fatalf("ledger seq gap: entry %d has seq %d", i, e.Seq)
		}
		if err := ledger.VerifyEntry(e, rowsBySeq[e.Seq], prev); err != nil {
			t.Fatalf("verify: %v", err)
		}
		prev = e.EntryHash
	}
	if len(rowsBySeq) != len(entries) {
		t.Fatalf("rows at %d seqs, %d ledger entries", len(rowsBySeq), len(entries))
	}
	return len(entries)
}

var t0 = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

// probes returns n distinguishable rows; id is carried in TotalMS.
func probes(from, n int) []model.ProbeResult {
	out := make([]model.ProbeResult, n)
	for i := range out {
		id := from + i
		out[i] = model.ProbeResult{
			TS: t0.Add(time.Duration(id) * time.Second), Vantage: "v1", Package: "isp",
			Scenario: "connect", Success: 1, TotalMS: uint32(id),
		}
	}
	return out
}

func testBatcher(s *fakeStore, maxPending int) *batcher {
	b := newBatcher(s, ledger.NewChain("v1"), "v1", maxPending)
	b.sleep = func(time.Duration) {}
	return b
}

func addAll(b *batcher, rs []model.ProbeResult) {
	for _, r := range rs {
		b.add(r)
	}
}

// oldCommit is the pre-KAN-260 commitProbeBatch row/entry computation, kept here to
// pin that the success path produces identical seq, rows and entry.
func oldCommit(chain *ledger.Chain, vantage string, rows []model.ProbeResult) ([]model.ProbeResult, ledger.Entry) {
	rows = append([]model.ProbeResult(nil), rows...)
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
	return rows, chain.Build(ledger.KindProbe, canon, tsFirst, tsLast)
}

func TestFlushSuccessUnchanged(t *testing.T) {
	s := newFakeStore()
	b := testBatcher(s, 0)
	ref := ledger.NewChain("v1")
	for batch := 0; batch < 3; batch++ {
		in := probes(batch*200, 200)
		in[0].TS = t0.Add(time.Hour) // out of order: ts_first/ts_last still min/max
		wantRows, wantEntry := oldCommit(ref, "v1", in)
		ref.Commit(wantEntry)
		addAll(b, in)
		b.flushOnce()
		pc, lc := s.probeCalls[batch], s.ledgerCalls[batch]
		if !reflect.DeepEqual(pc.rows, wantRows) || pc.token != fmt.Sprintf("v1:%d", batch+1) {
			t.Fatalf("batch %d: rows/token differ from old algorithm (token %s)", batch, pc.token)
		}
		if lc.entry != wantEntry || lc.token != fmt.Sprintf("v1:%d:ledger", batch+1) {
			t.Fatalf("batch %d: entry differs: %+v vs %+v", batch, lc.entry, wantEntry)
		}
	}
	if len(s.probeCalls) != 3 || len(s.ledgerCalls) != 3 || seqOf(b.chain) != 3 || b.pending() != 0 {
		t.Fatalf("calls %d/%d, head %d, pending %d", len(s.probeCalls), len(s.ledgerCalls), seqOf(b.chain), b.pending())
	}
	if n := walkChain(t, s, "v1"); n != 3 {
		t.Fatalf("entries %d", n)
	}
}

// The KAN-260 cases: whatever fails, the retry is byte-identical and new probes go
// to the NEXT seq, so the stored data always verifies.
func TestFlushRetryIsSealed(t *testing.T) {
	cases := []struct {
		name                      string
		probeFaults, ledgerFaults []fault
		wantProbeCalls            int // for batch 1 (seq 1) only
		wantLedgerCalls           int
	}{
		{"rows insert fails", []fault{failBefore}, nil, 2, 1},
		{"rows stored then timeout", []fault{failAfter}, nil, 2, 1},
		{"ledger insert fails", nil, []fault{failBefore}, 1, 2},
		{"ledger stored then timeout", nil, []fault{failAfter}, 1, 2},
		{"rows timeout then ledger timeout", []fault{failAfter}, []fault{failAfter}, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newFakeStore()
			s.probeFaults, s.ledgerFaults = tc.probeFaults, tc.ledgerFaults
			b := testBatcher(s, 0)

			addAll(b, probes(0, 200))
			b.flushOnce() // fails
			if b.sealed == nil || seqOf(b.chain) != 0 {
				t.Fatalf("expected sealed seq 1 retained, chain head %d", seqOf(b.chain))
			}
			first := b.sealed.entry
			addAll(b, probes(200, 57)) // arrive while seq 1 is unresolved
			for i := 0; i < 3 && b.pending() > 0; i++ {
				b.flushOnce() // seq 1 retried unchanged, then seq 2 = the 57 new rows
			}
			if b.pending() != 0 || seqOf(b.chain) != 2 {
				t.Fatalf("pending %d head %d", b.pending(), seqOf(b.chain))
			}

			var p1 []probeCall
			for _, c := range s.probeCalls {
				if c.token == "v1:1" {
					p1 = append(p1, c)
				}
			}
			var l1 []ledgerCall
			for _, c := range s.ledgerCalls {
				if c.token == "v1:1:ledger" {
					l1 = append(l1, c)
				}
			}
			if len(p1) != tc.wantProbeCalls || len(l1) != tc.wantLedgerCalls {
				t.Fatalf("seq 1: %d rows inserts, %d ledger inserts; want %d, %d", len(p1), len(l1), tc.wantProbeCalls, tc.wantLedgerCalls)
			}
			for _, c := range p1[1:] {
				if !reflect.DeepEqual(c.rows, p1[0].rows) {
					t.Fatalf("rows retry differs from first attempt (%d vs %d rows)", len(c.rows), len(p1[0].rows))
				}
			}
			for _, c := range l1 {
				if c.entry != first {
					t.Fatalf("ledger retry differs from sealed entry")
				}
			}
			if first.RowCount != 200 {
				t.Fatalf("sealed row_count %d", first.RowCount)
			}
			e2 := s.ledger["v1:2:ledger"]
			if e2.Seq != 2 || e2.RowCount != 57 || e2.PrevHash != first.EntryHash {
				t.Fatalf("next batch: %+v (want seq 2, 57 rows, prev %s)", e2, first.EntryHash)
			}
			if n := walkChain(t, s, "v1"); n != 2 {
				t.Fatalf("entries %d", n)
			}
		})
	}
}

func TestFlushRetriesManyTimesUnchanged(t *testing.T) {
	s := newFakeStore()
	s.probeFaults = []fault{failAfter, failBefore, failAfter, failBefore}
	b := testBatcher(s, 0)
	for i := 0; i < 5; i++ {
		addAll(b, probes(i*100, 100))
		b.flushOnce()
	}
	if seqOf(b.chain) != 2 || b.pending() != 0 {
		t.Fatalf("head %d pending %d", seqOf(b.chain), b.pending())
	}
	if e := s.ledger["v1:1:ledger"]; e.RowCount != 100 {
		t.Fatalf("seq 1 row_count %d, want 100 (sealed at first flush)", e.RowCount)
	}
	if e := s.ledger["v1:2:ledger"]; e.RowCount != 400 {
		t.Fatalf("seq 2 row_count %d, want 400", e.RowCount)
	}
	walkChain(t, s, "v1")
}

func TestPendingCapDropsOldestUnsealed(t *testing.T) {
	s := newFakeStore()
	s.probeFaults = []fault{failBefore, failBefore}
	b := testBatcher(s, 5)
	addAll(b, probes(0, 3))
	b.flushOnce() // seals rows 0..2, fails
	before := overflowProbes.Load()
	addAll(b, probes(3, 8)) // rows 3..10, cap 5 => keep 6..10
	if got := overflowProbes.Load() - before; got != 3 {
		t.Fatalf("overflow counter +%d, want 3", got)
	}
	if b.sealed.rows != 3 || len(b.buf) != 5 || b.buf[0].TotalMS != 6 || b.buf[4].TotalMS != 10 {
		t.Fatalf("sealed %d, buf %d first %d", b.sealed.rows, len(b.buf), b.buf[0].TotalMS)
	}
	b.flushOnce() // fails again
	b.flushOnce()
	if seqOf(b.chain) != 2 {
		t.Fatalf("head %d", seqOf(b.chain))
	}
	if e := s.ledger["v1:1:ledger"]; e.RowCount != 3 {
		t.Fatalf("sealed batch touched: row_count %d", e.RowCount)
	}
	walkChain(t, s, "v1")
}

func TestSealSplitsBacklog(t *testing.T) {
	s := newFakeStore()
	b := testBatcher(s, 0)
	addAll(b, probes(0, maxSealRows+7))
	b.flushOnce()
	if seqOf(b.chain) != 2 || s.ledger["v1:2:ledger"].RowCount != 7 {
		t.Fatalf("head %d, seq 2 rows %d", seqOf(b.chain), s.ledger["v1:2:ledger"].RowCount)
	}
	walkChain(t, s, "v1")
}

func TestShutdownDrain(t *testing.T) {
	s := newFakeStore()
	s.probeFaults = []fault{failAfter, failBefore}
	b := testBatcher(s, 0)
	var slept int
	b.sleep = func(time.Duration) { slept++ }
	addAll(b, probes(0, 50))
	b.flushOnce()            // seq 1 sealed, fails (stored)
	addAll(b, probes(50, 9)) // pending behind it
	b.flushUntil(8)          // attempt 1 fails, attempt 2 commits seq 1 and seq 2
	if b.pending() != 0 || seqOf(b.chain) != 2 || slept != 1 {
		t.Fatalf("pending %d head %d slept %d", b.pending(), seqOf(b.chain), slept)
	}
	if s.ledger["v1:1:ledger"].RowCount != 50 || s.ledger["v1:2:ledger"].RowCount != 9 {
		t.Fatal("drain did not keep the sealed batch intact")
	}
	walkChain(t, s, "v1")

	// gives up after the attempt budget, sealed batch still unchanged
	s2 := newFakeStore()
	s2.probeFaults = []fault{failBefore, failBefore, failBefore}
	b2 := testBatcher(s2, 0)
	addAll(b2, probes(0, 4))
	b2.flushUntil(3)
	if b2.pending() != 4 || len(s2.probeCalls) != 3 || seqOf(b2.chain) != 0 {
		t.Fatalf("pending %d calls %d head %d", b2.pending(), len(s2.probeCalls), seqOf(b2.chain))
	}
}

func TestEventLogSealed(t *testing.T) {
	s := newFakeStore()
	s.ledgerFaults = []fault{failAfter}
	l := &eventLog{store: s, chain: ledger.NewChain("events")}
	ctx := context.Background()
	ev := func(i int) model.Event {
		return model.Event{TS: t0.Add(time.Duration(i) * time.Minute), Type: "status_change", Package: "isp", Message: fmt.Sprint(i)}
	}
	if err := l.add(ctx, ev(1)); err == nil {
		t.Fatal("expected first event to fail")
	}
	sealed := l.sealed.entry
	if err := l.add(ctx, ev(2)); err != nil { // retries event 1 unchanged, then event 2 at seq 2
		t.Fatal(err)
	}
	if len(s.eventCalls) != 2 || s.eventCalls[0].Message != "1" || s.eventCalls[1].Message != "2" {
		t.Fatalf("event inserts: %+v", s.eventCalls)
	}
	if len(s.ledgerCalls) != 3 || s.ledgerCalls[1].entry != sealed {
		t.Fatalf("ledger calls %d, retry identical %v", len(s.ledgerCalls), s.ledgerCalls[1].entry == sealed)
	}
	if e2 := s.ledger["events:2:ledger"]; e2.PrevHash != sealed.EntryHash {
		t.Fatal("events seq 2 does not chain to seq 1")
	}
	if n := walkChain(t, s, "events"); n != 2 {
		t.Fatalf("entries %d", n)
	}
}
