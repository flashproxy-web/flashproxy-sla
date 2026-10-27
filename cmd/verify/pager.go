package main

import (
	"context"
	"fmt"
	"math"
)

// fetchFunc returns one page of seq-ordered items with seq >= from: at most limit
// items, plus every further item that shares the seq of the limit-th one
// (ClickHouse `ORDER BY seq LIMIT n WITH TIES`). A batch's rows share one seq, so a
// page never ends in the middle of a batch, and one seq with more rows than limit
// comes back whole in a single (oversized) page instead of being truncated.
type fetchFunc[T any] func(ctx context.Context, from uint64, limit int) ([]T, error)

// pager walks a seq-ordered source in whole-seq pages and checks that every page
// is consistent with the contract above. Any violation is an error: the verifier
// fails closed rather than verify against a partial or reordered read.
type pager[T any] struct {
	fetch fetchFunc[T]
	seqOf func(T) uint64
	limit int
	from  uint64
	done  bool
}

func newPager[T any](fetch fetchFunc[T], seqOf func(T) uint64, limit int) *pager[T] {
	return &pager[T]{fetch: fetch, seqOf: seqOf, limit: limit}
}

// next returns the next page, or nil once the source is exhausted.
func (p *pager[T]) next(ctx context.Context) ([]T, error) {
	if p.done {
		return nil, nil
	}
	page, err := p.fetch(ctx, p.from, p.limit)
	if err != nil {
		return nil, fmt.Errorf("read page from seq %d: %w", p.from, err)
	}
	if len(page) == 0 {
		p.done = true
		return nil, nil
	}
	prev := p.from
	for i, it := range page {
		s := p.seqOf(it)
		if s < prev {
			return nil, fmt.Errorf("page from seq %d is not seq-ordered (seq %d after %d at row %d)", p.from, s, prev, i)
		}
		if i >= p.limit && s != p.seqOf(page[p.limit-1]) {
			return nil, fmt.Errorf("page from seq %d has %d rows (limit %d) but row %d has seq %d, not the tied seq %d",
				p.from, len(page), p.limit, i, s, p.seqOf(page[p.limit-1]))
		}
		prev = s
	}
	last := p.seqOf(page[len(page)-1])
	// Fewer than limit rows means nothing at or below the query's bound remains.
	if len(page) < p.limit || last == math.MaxUint64 {
		p.done = true
	}
	p.from = last + 1
	return page, nil
}

// seqRow is one canonical row tagged with its batch seq.
type seqRow struct {
	Seq   uint64
	Canon string
}

// rowCursor serves a stream's canonical rows batch by batch for ledger entries
// visited in ascending seq order, holding only the current page in memory.
type rowCursor struct {
	p      *pager[seqRow]
	buf    map[uint64][]string
	bufMax uint64
	have   bool
}

func newRowCursor(fetch fetchFunc[seqRow], limit int) *rowCursor {
	return &rowCursor{p: newPager(fetch, func(r seqRow) uint64 { return r.Seq }, limit)}
}

// rowsFor returns the rows stored under seq (ok=false if none). seq must not
// decrease between calls. Pages that lie wholly below seq hold rows with no ledger
// entry; they are skipped, exactly as the single-query verifier ignored them.
func (c *rowCursor) rowsFor(ctx context.Context, seq uint64) ([]string, bool, error) {
	for !(c.have && seq <= c.bufMax) && !c.p.done {
		page, err := c.p.next(ctx)
		if err != nil {
			return nil, false, err
		}
		if len(page) == 0 {
			break
		}
		c.buf = make(map[uint64][]string)
		for _, r := range page {
			c.buf[r.Seq] = append(c.buf[r.Seq], r.Canon)
		}
		c.bufMax = page[len(page)-1].Seq
		c.have = true
	}
	r, ok := c.buf[seq]
	return r, ok, nil
}
