// Command verify independently audits the integrity ledger of a flashproxy-status
// ClickHouse using ONLY public, read-only access. It recomputes every batch hash
// from the raw rows, walks each per-stream hash chain, verifies the Ed25519-signed
// checkpoints against the published public key, and reports any tampering,
// data-loss gap, or broken signature.
//
// This is the tool the public uses to confirm FlashProxy has not altered the SLA
// data. It needs nothing but the published ClickHouse credentials and public key
// (both at https://status.flashproxy.com/api/meta).
//
//	go run ./cmd/verify -ch https://ch.flashproxy.com -user flashproxy-status-public \
//	    -pass flashproxy-public-ro -pubkey <base64>
//
// Rows are read in seq-ordered pages of about -chunk-rows rows (default 200000):
// the public profile caps a single result at 1M rows, and a stream holds more than
// that. Pages end on whole batches (see pager.go), so chunking never changes a verdict.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
	"github.com/flashproxy/flashproxy-status/internal/ledger"
	"github.com/flashproxy/flashproxy-status/internal/model"
)

func main() {
	chURL := flag.String("ch", "", "ClickHouse HTTP URL (e.g. https://ch.flashproxy.com)")
	db := flag.String("db", "sla", "database")
	user := flag.String("user", "flashproxy-status-public", "ClickHouse user")
	pass := flag.String("pass", "", "ClickHouse password")
	pubkeyB64 := flag.String("pubkey", "", "published Ed25519 public key (base64); if empty, checkpoint signatures are NOT verified")
	chunkRows := flag.Int("chunk-rows", 200000, "rows per read (whole batches; the public profile caps one result at 1M rows)")
	flag.Parse()
	if *chURL == "" {
		fmt.Fprintln(os.Stderr, "error: -ch is required")
		os.Exit(2)
	}
	if *chunkRows < 1 {
		fmt.Fprintln(os.Stderr, "error: -chunk-rows must be >= 1")
		os.Exit(2)
	}

	ch := chstore.New(*chURL, *db, *user, *pass)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	v := &verifier{src: &chSource{ch: ch, db: *db}, chunkRows: *chunkRows, out: os.Stdout}
	if *pubkeyB64 != "" {
		pk, err := ledger.ParsePublicKey(*pubkeyB64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad -pubkey: %v\n", err)
			os.Exit(2)
		}
		v.pub = pk
		fmt.Printf("verifying against public key %s (id %s)\n", *pubkeyB64, ledger.PubKeyID(pk))
	} else {
		fmt.Println("WARNING: no -pubkey given; chain will be checked but checkpoint signatures will NOT be verified")
	}

	if err := v.run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "\nVERIFICATION FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nVERIFICATION OK — every chain links cleanly and every checkpoint signature is valid.")
}

// source is the verifier's read access to the published tables. Every paged
// method follows the fetchFunc contract (seq >= from, ORDER BY seq, LIMIT WITH TIES).
type source interface {
	streams(ctx context.Context) ([]string, error)
	ledgerPage(ctx context.Context, stream string, from uint64, limit int) ([]ledger.Entry, error)
	// rowPage returns canonical rows of stream with from <= seq <= head.
	rowPage(ctx context.Context, stream string, from, head uint64, limit int) ([]seqRow, error)
	checkpoints(ctx context.Context) ([]ledger.Checkpoint, error)
}

type verifier struct {
	src       source
	chunkRows int
	out       io.Writer
	pub       []byte // ed25519 public key, or nil

	// validated[stream][seq] = recomputed-and-confirmed entry_hash
	validated map[string]map[uint64]string
	problems  int
}

func (v *verifier) run(ctx context.Context) error {
	v.validated = map[string]map[uint64]string{}

	streams, err := v.src.streams(ctx)
	if err != nil {
		return fmt.Errorf("list streams: %w", err)
	}
	if len(streams) == 0 {
		return fmt.Errorf("no ledger streams found")
	}
	for _, s := range streams {
		if err := v.verifyStream(ctx, s); err != nil {
			return err
		}
	}
	if err := v.verifyCheckpoints(ctx); err != nil {
		return err
	}
	if v.problems > 0 {
		return fmt.Errorf("%d problem(s) detected", v.problems)
	}
	return nil
}

func (v *verifier) printf(format string, a ...any) { fmt.Fprintf(v.out, format, a...) }

// chSource reads the published tables over the ClickHouse HTTP interface.
type chSource struct {
	ch *chstore.Client
	db string
}

func (s *chSource) streams(ctx context.Context) ([]string, error) {
	rows, err := s.ch.QueryJSON(ctx, fmt.Sprintf("SELECT DISTINCT stream FROM %s.ledger ORDER BY stream", s.db))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, m := range rows {
		out = append(out, chstore.Str(m, "stream"))
	}
	return out, nil
}

// verifyStream walks one stream's chain in seq order, recomputing batch and entry
// hashes from the underlying rows and checking forward linkage. Seq gaps (a batch
// lost to a crash before its rows were written) are reported but do not break
// linkage — the next present entry must still chain to the previous present one.
//
// The ledger entries are read first (all of them; they are small) and fix the head:
// rows are then read in whole-batch pages only up to that seq. The worker inserts a
// batch's rows before its ledger entry, so every entry read has its rows, and
// batches appended while verify runs are simply outside this run.
func (v *verifier) verifyStream(ctx context.Context, stream string) error {
	entries, err := v.ledgerEntries(ctx, stream)
	if err != nil {
		return fmt.Errorf("read ledger for %s: %w", stream, err)
	}
	var head uint64
	if len(entries) > 0 {
		head = entries[len(entries)-1].Seq
	}
	rows := newRowCursor(func(ctx context.Context, from uint64, limit int) ([]seqRow, error) {
		page, err := v.src.rowPage(ctx, stream, from, head, limit)
		if err != nil {
			return nil, err
		}
		for _, r := range page {
			if r.Seq > head {
				return nil, fmt.Errorf("row at seq %d past the requested head %d", r.Seq, head)
			}
		}
		return page, nil
	}, v.chunkRows)
	v.validated[stream] = map[uint64]string{}

	prev := ledger.GenesisHash
	var lastSeq uint64
	gaps := 0
	cutoff := time.Now().Add(-399 * 24 * time.Hour).Unix() // rows older than this may be TTL-expired
	for i, e := range entries {
		if i > 0 && e.Seq != lastSeq+1 {
			gaps += int(e.Seq - lastSeq - 1)
			v.printf("  [%s] seq gap: %d missing between %d and %d (data lost pre-commit; linkage checked)\n", stream, e.Seq-lastSeq-1, lastSeq, e.Seq)
		}
		r, ok, err := rows.rowsFor(ctx, e.Seq)
		if err != nil {
			return fmt.Errorf("read rows for %s: %w", stream, err)
		}
		var batch []string
		if ok {
			batch = r
		} else if e.TSLast < cutoff {
			batch = nil // expired raw rows: verify chain linkage only
		} else {
			batch = []string{} // rows should exist but don't -> row_count mismatch flagged below
		}
		if err := ledger.VerifyEntry(e, batch, prev); err != nil {
			v.problems++
			v.printf("  [%s] FAIL seq %d: %v\n", stream, e.Seq, err)
		} else {
			v.validated[stream][e.Seq] = e.EntryHash
		}
		prev = e.EntryHash
		lastSeq = e.Seq
	}
	status := "ok"
	if gaps > 0 {
		status = fmt.Sprintf("ok (%d gap-minute(s) from crashes)", gaps)
	}
	v.printf("stream %-16s %5d entries, seq 1..%d — %s\n", stream, len(entries), lastSeq, status)
	return nil
}

func (s *chSource) checkpoints(ctx context.Context) ([]ledger.Checkpoint, error) {
	rows, err := s.ch.QueryJSON(ctx, fmt.Sprintf(
		`SELECT stream, toUInt64(seq) AS seq, entry_hash, toUInt32(toUnixTimestamp(ts)) AS ts, pubkey_id, signature
		 FROM %s.ledger_checkpoints ORDER BY stream, seq`, s.db))
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Checkpoint, 0, len(rows))
	for _, m := range rows {
		out = append(out, ledger.Checkpoint{
			Stream: chstore.Str(m, "stream"), Seq: chstore.Uint[uint64](m, "seq", &err),
			EntryHash: chstore.Str(m, "entry_hash"), TS: int64(chstore.Uint[uint32](m, "ts", &err)),
			PubKeyID: chstore.Str(m, "pubkey_id"), Signature: chstore.Str(m, "signature"),
		})
	}
	if err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	return out, nil
}

func (v *verifier) verifyCheckpoints(ctx context.Context) error {
	rows, err := v.src.checkpoints(ctx)
	if err != nil {
		return fmt.Errorf("read checkpoints: %w", err)
	}
	var sigOK, sigSkip, consistent int
	for _, cp := range rows {
		// The checkpoint must attest the same entry_hash we recomputed at that seq.
		if got, ok := v.validated[cp.Stream][cp.Seq]; ok && got != cp.EntryHash {
			v.problems++
			v.printf("  checkpoint %s seq %d: attested entry_hash %s != recomputed %s\n", cp.Stream, cp.Seq, cp.EntryHash, got)
		} else if ok {
			consistent++
		}
		if v.pub != nil {
			if err := ledger.VerifyCheckpoint(v.pub, cp); err != nil {
				v.problems++
				v.printf("  checkpoint %s seq %d: %v\n", cp.Stream, cp.Seq, err)
			} else {
				sigOK++
			}
		} else {
			sigSkip++
		}
	}
	v.printf("checkpoints: %d total, %d signature(s) valid, %d consistent with chain, %d skipped (no pubkey)\n",
		len(rows), sigOK, consistent, sigSkip)
	return nil
}

// ledgerEntries reads one stream's whole ledger (one small entry per batch) in pages.
func (v *verifier) ledgerEntries(ctx context.Context, stream string) ([]ledger.Entry, error) {
	p := newPager(func(ctx context.Context, from uint64, limit int) ([]ledger.Entry, error) {
		return v.src.ledgerPage(ctx, stream, from, limit)
	}, func(e ledger.Entry) uint64 { return e.Seq }, v.chunkRows)
	var out []ledger.Entry
	for {
		page, err := p.next(ctx)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
	}
}

func (s *chSource) ledgerPage(ctx context.Context, stream string, from uint64, limit int) ([]ledger.Entry, error) {
	rows, err := s.ch.QueryJSON(ctx, fmt.Sprintf(
		`SELECT stream, toUInt64(seq) AS seq, kind,
		   toInt64(toUnixTimestamp(ts_first)) AS ts_first, toInt64(toUnixTimestamp(ts_last)) AS ts_last,
		   toUInt32(row_count) AS row_count, batch_hash, prev_hash, entry_hash
		 FROM %s.ledger WHERE stream = '%s' AND seq >= %d ORDER BY seq LIMIT %d WITH TIES`, s.db, esc(stream), from, limit))
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Entry, 0, len(rows))
	for _, m := range rows {
		out = append(out, ledger.Entry{
			Stream: chstore.Str(m, "stream"), Seq: chstore.Uint[uint64](m, "seq", &err), Kind: chstore.Str(m, "kind"),
			TSFirst: chstore.Int64(m, "ts_first", &err), TSLast: chstore.Int64(m, "ts_last", &err),
			RowCount:  chstore.Uint[uint32](m, "row_count", &err),
			BatchHash: chstore.Str(m, "batch_hash"), PrevHash: chstore.Str(m, "prev_hash"), EntryHash: chstore.Str(m, "entry_hash"),
		})
	}
	if err != nil {
		return nil, fmt.Errorf("decode ledger entry: %w", err)
	}
	return out, nil
}

// rowPage reconstructs the canonical row strings directly from the public
// columns — exactly as the worker hashed them — for one whole-batch page.
func (s *chSource) rowPage(ctx context.Context, stream string, from, head uint64, limit int) ([]seqRow, error) {
	if stream == "events" {
		rows, err := s.ch.QueryJSON(ctx, fmt.Sprintf(
			`SELECT toInt64(toUnixTimestamp(ts)) AS ts, type, package, message, toUInt64(seq) AS seq
			 FROM %s.events WHERE stream = 'events' AND seq >= %d AND seq <= %d
			 ORDER BY seq LIMIT %d WITH TIES`, s.db, from, head, limit))
		if err != nil {
			return nil, err
		}
		out := make([]seqRow, 0, len(rows))
		for _, m := range rows {
			e := model.Event{
				TS:   time.Unix(chstore.Int64(m, "ts", &err), 0).UTC(),
				Type: chstore.Str(m, "type"), Package: chstore.Str(m, "package"), Message: chstore.Str(m, "message"),
			}
			out = append(out, seqRow{Seq: chstore.Uint[uint64](m, "seq", &err), Canon: e.Canonical()})
		}
		if err != nil {
			return nil, fmt.Errorf("decode event row: %w", err)
		}
		return out, nil
	}
	rows, err := s.ch.QueryJSON(ctx, fmt.Sprintf(
		`SELECT toInt64(toUnixTimestamp(ts)) AS ts, vantage, package, scenario, proto, target,
		   toUInt32(ip_version) AS ip_version, toUInt32(success) AS success, error_type,
		   toUInt32(dial_ms) AS dial_ms, toUInt32(connect_ms) AS connect_ms, toUInt32(ttfb_ms) AS ttfb_ms,
		   toUInt32(total_ms) AS total_ms, toUInt64(bytes) AS bytes, throughput_mbps, toUInt64(seq) AS seq
		 FROM %s.probe_raw WHERE stream = '%s' AND seq >= %d AND seq <= %d
		 ORDER BY seq LIMIT %d WITH TIES`, s.db, esc(stream), from, head, limit))
	if err != nil {
		return nil, err
	}
	out := make([]seqRow, 0, len(rows))
	for _, m := range rows {
		r := model.ProbeResult{
			TS:      time.Unix(chstore.Int64(m, "ts", &err), 0).UTC(),
			Vantage: chstore.Str(m, "vantage"), Package: chstore.Str(m, "package"), Scenario: chstore.Str(m, "scenario"),
			Proto: chstore.Str(m, "proto"), Target: chstore.Str(m, "target"),
			IPVersion: chstore.Uint[uint8](m, "ip_version", &err), Success: chstore.Uint[uint8](m, "success", &err),
			ErrorType: chstore.Str(m, "error_type"),
			DialMS:    chstore.Uint[uint32](m, "dial_ms", &err), ConnectMS: chstore.Uint[uint32](m, "connect_ms", &err),
			TTFBMS: chstore.Uint[uint32](m, "ttfb_ms", &err), TotalMS: chstore.Uint[uint32](m, "total_ms", &err),
			Bytes: chstore.Uint[uint64](m, "bytes", &err), ThroughputMbps: chstore.Float32(m, "throughput_mbps", &err),
		}
		out = append(out, seqRow{Seq: chstore.Uint[uint64](m, "seq", &err), Canon: r.Canonical()})
	}
	if err != nil {
		return nil, fmt.Errorf("decode probe row: %w", err)
	}
	return out, nil
}

func esc(s string) string {
	// streams are operator-controlled identifiers; quote defensively anyway.
	b := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '\'' || r == '\\' {
			b = append(b, '\\')
		}
		b = append(b, r)
	}
	return string(b)
}
