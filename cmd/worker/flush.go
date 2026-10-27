package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/ledger"
	"github.com/flashproxy/flashproxy-status/internal/model"
)

// probeStore is the slice of chstore.Client the flusher writes through (a fake in tests).
type probeStore interface {
	InsertProbes(ctx context.Context, rs []model.ProbeResult, dedupToken string) error
	InsertLedger(ctx context.Context, e ledger.Entry, dedupToken string) error
}

const (
	flushRows         = 200    // size trigger (the other trigger is the 3s tick)
	maxSealRows       = 10000  // most rows sealed into one batch (bounds a post-outage backlog batch)
	defaultMaxPending = 300000 // unsealed rows kept while a sealed batch cannot commit
	flushTimeout      = 15 * time.Second
)

// overflowProbes counts unsealed rows dropped because the pending buffer hit its cap
// while a sealed batch could not commit (store down for a long time).
var overflowProbes atomic.Uint64

// sealedBatch is one ledger batch whose content is FROZEN at seal time: its seq, rows,
// ledger entry and dedup tokens are identical on every attempt until it commits.
//
// Why (KAN-260): ClickHouse drops an insert whose dedup token it has already seen, and
// acks it OK even when the content differs. A flush that timed out on the client after
// the server stored it, then retried at the same seq with MORE rows, left a ledger
// entry that did not match the stored rows (row_count/batch_hash mismatch) or an
// in-memory chain that advanced on an entry the server never kept (chain break).
// Re-sending byte-identical content with the same token is always harmless.
type sealedBatch struct {
	seq         uint64
	entry       ledger.Entry
	rows        int
	what        string                          // "probes" / "event", for error text
	insertRows  func(ctx context.Context) error // re-sends the frozen rows with the frozen token
	ledgerToken string
	rowsDone    bool // rows insert acked: not re-sent on retry
	ledgerDone  bool // ledger insert acked
	attempts    int
}

// send tries the steps not yet acked (rows, then the ledger entry) and advances the
// chain only once both are done. Safe to call again after any error.
func (s *sealedBatch) send(ctx context.Context, insertLedger func(context.Context, ledger.Entry, string) error, chain *ledger.Chain) error {
	s.attempts++
	if !s.rowsDone {
		if err := s.insertRows(ctx); err != nil {
			return fmt.Errorf("insert %s: %w", s.what, err)
		}
		s.rowsDone = true
	}
	if !s.ledgerDone {
		if err := insertLedger(ctx, s.entry, s.ledgerToken); err != nil {
			return fmt.Errorf("insert %s ledger entry: %w", s.what, err)
		}
		s.ledgerDone = true
	}
	chain.Commit(s.entry)
	return nil
}

// batcher holds the flusher's state: at most one sealed batch in flight plus the
// unsealed buffer that becomes the next batch(es) once it commits. Not concurrent-safe
// (the flusher goroutine owns it).
type batcher struct {
	store      probeStore
	chain      *ledger.Chain
	vantage    string
	maxPending int
	timeout    time.Duration
	sleep      func(time.Duration)

	sealed *sealedBatch
	buf    []model.ProbeResult
}

func newBatcher(store probeStore, chain *ledger.Chain, vantage string, maxPending int) *batcher {
	return &batcher{
		store: store, chain: chain, vantage: vantage,
		maxPending: defInt(maxPending, defaultMaxPending),
		timeout:    flushTimeout,
		sleep:      time.Sleep,
	}
}

// add buffers one probe. Over the cap it drops the OLDEST unsealed rows (never the
// sealed batch) and says so loudly.
func (b *batcher) add(r model.ProbeResult) {
	b.buf = append(b.buf, r)
	if over := len(b.buf) - b.maxPending; over > 0 {
		b.buf = b.buf[over:]
		n := overflowProbes.Add(uint64(over))
		if n == uint64(over) || n%10000 < uint64(over) {
			log.Printf("PENDING BUFFER FULL: dropped %d oldest unsealed probe(s) (cap %d, %d dropped since start; sealed seq %d still not committed)", over, b.maxPending, n, b.sealedSeq())
		}
	}
}

func (b *batcher) sealedSeq() uint64 {
	if b.sealed == nil {
		return 0
	}
	return b.sealed.seq
}

// pending is the number of rows not yet committed (sealed + unsealed).
func (b *batcher) pending() int {
	n := len(b.buf)
	if b.sealed != nil {
		n += b.sealed.rows
	}
	return n
}

// seal freezes up to maxSealRows buffered rows into the next batch at chain head+1.
// Rows, canonical form, ts bounds and the entry are computed exactly as before; only
// now they are computed ONCE.
func (b *batcher) seal() {
	n := min(len(b.buf), maxSealRows)
	rows := make([]model.ProbeResult, n)
	copy(rows, b.buf[:n])
	b.buf = b.buf[n:]

	hseq, _ := b.chain.Head()
	seq := hseq + 1
	canon := make([]string, n)
	var tsFirst, tsLast int64
	for i := range rows {
		rows[i].Stream = b.vantage
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
	rowsToken := fmt.Sprintf("%s:%d", b.vantage, seq)
	b.sealed = &sealedBatch{
		seq:   seq,
		entry: b.chain.Build(ledger.KindProbe, canon, tsFirst, tsLast),
		rows:  n,
		what:  "probes",
		insertRows: func(ctx context.Context) error {
			return b.store.InsertProbes(ctx, rows, rowsToken)
		},
		ledgerToken: fmt.Sprintf("%s:%d:ledger", b.vantage, seq),
	}
}

// commitSealed makes one attempt at the sealed batch. Insert order is rows then
// ledger, so a crash between the two surfaces as a verifiable gap (rows without an
// entry), never as silent corruption.
func (b *batcher) commitSealed() error {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	s := b.sealed
	if err := s.send(ctx, b.store.InsertLedger, b.chain); err != nil {
		return err
	}
	if s.attempts > 1 {
		log.Printf("flushed %d probes (seq %d committed after %d attempts)", s.rows, s.seq, s.attempts)
	} else {
		log.Printf("flushed %d probes (seq %d committed)", s.rows, s.seq)
	}
	b.sealed = nil
	return nil
}

// flush retries the sealed batch (if any), then seals and commits the buffer, until
// everything is committed or an attempt fails. A failed sealed batch is kept as-is.
func (b *batcher) flush() error {
	for b.sealed != nil || len(b.buf) > 0 {
		if b.sealed == nil {
			b.seal()
		}
		if err := b.commitSealed(); err != nil {
			return err
		}
	}
	return nil
}

// flushOnce is one flush pass for the size/tick triggers; on failure the sealed batch
// is retained unchanged for the next pass.
func (b *batcher) flushOnce() {
	if err := b.flush(); err != nil {
		log.Printf("flush seq %d (%d rows sealed, %d pending): %v (retained, will retry)", b.sealedSeq(), b.sealed.rows, len(b.buf), err)
	}
}

// flushUntil is the shutdown drain: it retries the sealed batch and then the rest of
// the buffer up to attempts times before giving up (and logging the loss).
func (b *batcher) flushUntil(attempts int) {
	for i := 0; i < attempts && b.pending() > 0; i++ {
		err := b.flush()
		if err == nil {
			log.Printf("final flush: done (seq %d committed)", seqOf(b.chain))
			return
		}
		log.Printf("final flush attempt %d/%d (seq %d, %d rows pending): %v", i+1, attempts, b.sealedSeq(), b.pending(), err)
		b.sleep(time.Duration(i+1) * time.Second)
	}
	if n := b.pending(); n > 0 {
		log.Printf("DATA LOSS: gave up flushing %d probes after %d attempts", n, attempts)
	}
}

// flusher batches probe rows and commits each batch (rows + one ledger entry), then
// advances the chain. A batch is SEALED when its flush starts (see sealedBatch) and
// retried unchanged until it commits; probes arriving meanwhile wait in the buffer
// for the next seq. When the input channel closes (shutdown) it drains and does a
// final, persistent flush so no data is lost.
func flusher(store probeStore, chain *ledger.Chain, vantage string, maxPending int, in <-chan model.ProbeResult) {
	b := newBatcher(store, chain, vantage, maxPending)
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()

	for {
		select {
		case r, ok := <-in:
			if !ok {
				b.flushUntil(8) // final drain: try hard
				return
			}
			b.add(r)
			if len(b.buf) >= flushRows {
				b.flushOnce()
			}
		case <-t.C:
			b.flushOnce()
		}
	}
}

func seqOf(chain *ledger.Chain) uint64 { s, _ := chain.Head(); return s }

// eventStore is the slice of chstore.Client the events stream writes through.
type eventStore interface {
	InsertEvent(ctx context.Context, e model.Event, dedupToken string) error
	InsertLedger(ctx context.Context, e ledger.Entry, dedupToken string) error
}

const maxQueuedEvents = 1000

// eventLog appends events to the "events" stream with the same sealed-batch rule as
// probes: an event that failed to commit keeps its seq and content and is retried
// before any later event gets a seq (previously the next event was rebuilt at the
// SAME seq with different content). Not concurrent-safe (monitorLoop owns it).
type eventLog struct {
	store  eventStore
	chain  *ledger.Chain
	sealed *sealedBatch
	queue  []model.Event
}

// add queues ev and tries to commit everything queued, oldest first.
func (l *eventLog) add(ctx context.Context, ev model.Event) error {
	l.queue = append(l.queue, ev)
	if over := len(l.queue) - maxQueuedEvents; over > 0 {
		l.queue = l.queue[over:]
		log.Printf("monitor: event queue full, dropped %d oldest unsealed event(s)", over)
	}
	return l.flush(ctx)
}

// flush retries the sealed event, then seals and commits queued ones in order.
func (l *eventLog) flush(ctx context.Context) error {
	for l.sealed != nil || len(l.queue) > 0 {
		if l.sealed == nil {
			ev := l.queue[0]
			l.queue = l.queue[1:]
			hseq, _ := l.chain.Head()
			seq := hseq + 1
			ev.Stream, ev.Seq = "events", seq
			token := fmt.Sprintf("events:%d", seq)
			l.sealed = &sealedBatch{
				seq:   seq,
				entry: l.chain.Build(ledger.KindEvent, []string{ev.Canonical()}, ev.TS.Unix(), ev.TS.Unix()),
				rows:  1,
				what:  "event",
				insertRows: func(ctx context.Context) error {
					return l.store.InsertEvent(ctx, ev, token)
				},
				ledgerToken: fmt.Sprintf("events:%d:ledger", seq),
			}
		}
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := l.sealed.send(cctx, l.store.InsertLedger, l.chain)
		cancel()
		if err != nil {
			return fmt.Errorf("events seq %d (%d more queued): %w", l.sealed.seq, len(l.queue), err)
		}
		l.sealed = nil
	}
	return nil
}
