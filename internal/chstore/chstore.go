// Package chstore is a tiny stdlib-only ClickHouse client over the HTTP interface.
// It is the reference implementation of the storage backend; the same surface can be
// implemented for Postgres or SQLite so open-source adopters can swap stores.
//
// Timezone correctness relies on the ClickHouse SERVER being configured UTC
// (<timezone>UTC</timezone>; see deploy/clickhouse-deploy-prompt.md), NOT on a
// per-query session_timezone override — reader users are readonly=1, which rejects
// any client-set setting (Code 164), so the client must not send settings on reads.
package chstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/flashproxy/flashproxy-status/internal/ledger"
	"github.com/flashproxy/flashproxy-status/internal/model"
)

const chTime = "2006-01-02 15:04:05"

// maxResponseBytes bounds how much of a ClickHouse response we will buffer, so a
// huge or runaway result can't OOM the process. Result sets here are small.
const maxResponseBytes = 256 << 20 // 256 MiB

type Client struct {
	base string
	db   string
	user string
	pass string
	hc   *http.Client
}

func New(base, db, user, pass string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		db:   db,
		user: user,
		pass: pass,
		hc:   &http.Client{Timeout: 15 * time.Second},
	}
}

// exec runs a statement. extra carries additional URL settings (e.g. read caps).
func (c *Client) exec(ctx context.Context, query string, body io.Reader, extra url.Values) ([]byte, error) {
	v := url.Values{}
	v.Set("query", query)
	for k, vals := range extra {
		for _, val := range vals {
			v.Add(k, val)
		}
	}
	u := c.base + "/?" + v.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.pass)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if readErr != nil {
		return nil, fmt.Errorf("read clickhouse response: %w", readErr)
	}
	return b, nil
}

// dedupExtra returns URL settings that make a retried insert idempotent: ClickHouse
// drops a re-inserted block with the same token (requires the table's
// non_replicated_deduplication_window setting). Empty token => no dedup.
func dedupExtra(token string) url.Values {
	v := url.Values{}
	if token != "" {
		v.Set("insert_deduplication_token", token)
	}
	return v
}

// InsertProbes appends probe rows via JSONEachRow. Throughput is stored quantized so
// it matches the value the integrity ledger committed to. dedupToken makes retries
// idempotent (pass "<stream>:<seq>").
func (c *Client) InsertProbes(ctx context.Context, rs []model.ProbeResult, dedupToken string) error {
	if len(rs) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rs {
		if err := enc.Encode(map[string]any{
			"ts":              r.TS.UTC().Format(chTime),
			"vantage":         r.Vantage,
			"package":         r.Package,
			"scenario":        r.Scenario,
			"proto":           r.Proto,
			"target":          r.Target,
			"ip_version":      r.IPVersion,
			"success":         r.Success,
			"error_type":      r.ErrorType,
			"dial_ms":         r.DialMS,
			"connect_ms":      r.ConnectMS,
			"ttfb_ms":         r.TTFBMS,
			"total_ms":        r.TotalMS,
			"bytes":           r.Bytes,
			"throughput_mbps": model.QuantizeMbps(r.ThroughputMbps),
			"stream":          r.Stream,
			"seq":             r.Seq,
		}); err != nil {
			return fmt.Errorf("encode probe row: %w", err)
		}
	}
	_, err := c.exec(ctx, fmt.Sprintf("INSERT INTO %s.probe_raw FORMAT JSONEachRow", c.db), &buf, dedupExtra(dedupToken))
	return err
}

// InsertEvent records a deploy/maintenance/status-change marker. dedupToken
// ("events:<seq>") only takes effect if sla.events has non_replicated_deduplication_window.
func (c *Client) InsertEvent(ctx context.Context, e model.Event, dedupToken string) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]any{
		"ts":      e.TS.UTC().Format(chTime),
		"type":    e.Type,
		"package": e.Package,
		"message": e.Message,
		"stream":  e.Stream,
		"seq":     e.Seq,
	}); err != nil {
		return err
	}
	_, err := c.exec(ctx, fmt.Sprintf("INSERT INTO %s.events FORMAT JSONEachRow", c.db), &buf, dedupExtra(dedupToken))
	return err
}

// InsertLedger appends one integrity-ledger entry. dedupToken makes retries
// idempotent (pass "<stream>:<seq>:ledger").
func (c *Client) InsertLedger(ctx context.Context, e ledger.Entry, dedupToken string) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]any{
		"stream":     e.Stream,
		"seq":        e.Seq,
		"kind":       e.Kind,
		"ts_first":   time.Unix(e.TSFirst, 0).UTC().Format(chTime),
		"ts_last":    time.Unix(e.TSLast, 0).UTC().Format(chTime),
		"row_count":  e.RowCount,
		"batch_hash": e.BatchHash,
		"prev_hash":  e.PrevHash,
		"entry_hash": e.EntryHash,
	}); err != nil {
		return err
	}
	_, err := c.exec(ctx, fmt.Sprintf("INSERT INTO %s.ledger FORMAT JSONEachRow", c.db), &buf, dedupExtra(dedupToken))
	return err
}

// InsertCheckpoint appends one signed checkpoint.
func (c *Client) InsertCheckpoint(ctx context.Context, cp ledger.Checkpoint) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]any{
		"stream":     cp.Stream,
		"seq":        cp.Seq,
		"entry_hash": cp.EntryHash,
		"ts":         time.Unix(cp.TS, 0).UTC().Format(chTime),
		"pubkey_id":  cp.PubKeyID,
		"signature":  cp.Signature,
	}); err != nil {
		return err
	}
	_, err := c.exec(ctx, fmt.Sprintf("INSERT INTO %s.ledger_checkpoints FORMAT JSONEachRow", c.db), &buf, nil)
	return err
}

// LedgerHead returns the last (seq, entry_hash) for a stream so a restarted worker
// can resume its chain. Returns (0, "") when the stream has no entries yet.
func (c *Client) LedgerHead(ctx context.Context, stream string) (uint64, string, error) {
	rows, err := c.QueryJSON(ctx, fmt.Sprintf(
		"SELECT seq, entry_hash FROM %s.ledger WHERE stream = '%s' ORDER BY seq DESC LIMIT 1",
		c.db, escapeLit(stream)))
	if err != nil {
		return 0, "", err
	}
	if len(rows) == 0 {
		return 0, "", nil
	}
	seq := Uint[uint64](rows[0], "seq", &err)
	return seq, Str(rows[0], "entry_hash"), err
}

// ProbeMaxSeq returns the highest seq already present in probe_raw for a stream, so
// a restarted worker never reuses a seq that a pre-crash insert may have written
// without its ledger entry. Returns 0 if none.
func (c *Client) ProbeMaxSeq(ctx context.Context, stream string) (uint64, error) {
	rows, err := c.QueryJSON(ctx, fmt.Sprintf(
		"SELECT toUInt64(max(seq)) AS s FROM %s.probe_raw WHERE stream = '%s'",
		c.db, escapeLit(stream)))
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	s := Uint[uint64](rows[0], "s", &err)
	return s, err
}

// LedgerHeads returns the current head (seq, entry_hash) of every stream — used by
// the checkpoint signer to attest all chains, including other workers' vantages.
func (c *Client) LedgerHeads(ctx context.Context) (map[string]struct {
	Seq       uint64
	EntryHash string
}, error) {
	// Alias the aggregate to head_seq, NOT seq: aliasing it `seq` shadows the column,
	// so ClickHouse resolves the `seq` inside argMax(entry_hash, seq) to the alias
	// (an aggregate) and rejects it as max() nested in argMax() (Code 184).
	rows, err := c.QueryJSON(ctx, fmt.Sprintf(
		"SELECT stream, toUInt64(max(seq)) AS head_seq, argMax(entry_hash, seq) AS entry_hash FROM %s.ledger GROUP BY stream",
		c.db))
	if err != nil {
		return nil, err
	}
	out := map[string]struct {
		Seq       uint64
		EntryHash string
	}{}
	for _, m := range rows {
		out[Str(m, "stream")] = struct {
			Seq       uint64
			EntryHash string
		}{Uint[uint64](m, "head_seq", &err), Str(m, "entry_hash")}
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// StatusChangeMessages returns the messages of a package's status_change events
// with ts >= since (adaptive alert gate input; the writer role can SELECT).
func (c *Client) StatusChangeMessages(ctx context.Context, pkg string, since time.Time) ([]string, error) {
	rows, err := c.QueryJSON(ctx, fmt.Sprintf(
		"SELECT message FROM %s.events WHERE type = 'status_change' AND package = '%s' AND ts >= toDateTime(%d, 'UTC') ORDER BY ts",
		c.db, escapeLit(pkg), since.UTC().Unix()))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, m := range rows {
		out = append(out, Str(m, "message"))
	}
	return out, nil
}

// LastStatusMessages returns, per package, the message of its newest status_change
// event (monitor restart reconciliation; the writer role can SELECT).
func (c *Client) LastStatusMessages(ctx context.Context) (map[string]string, error) {
	rows, err := c.QueryJSON(ctx, fmt.Sprintf(
		"SELECT package, argMax(message, (ts, seq)) AS message FROM %s.events WHERE type = 'status_change' GROUP BY package",
		c.db))
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, m := range rows {
		out[Str(m, "package")] = Str(m, "message")
	}
	return out, nil
}

// QueryJSON runs a SELECT and returns the rows of the ClickHouse JSON format.
// Numbers are kept as json.Number (never float64), so integer columns decode
// exactly via Uint/Int64 ((U)Int64 may also arrive as JSON strings).
// No client-set settings are sent: reader users are readonly=1, so query caps and
// cancel_http_readonly_queries_on_client_close are configured server-side on the
// reader profiles instead (see schema/roles.sql).
func (c *Client) QueryJSON(ctx context.Context, sql string) ([]map[string]any, error) {
	b, err := c.exec(ctx, sql+" FORMAT JSON", nil, nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Data      []map[string]any `json:"data"`
		Exception string           `json:"exception"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode clickhouse json: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF { // json.Unmarshal rejected trailing data too
		return nil, fmt.Errorf("decode clickhouse json: trailing data")
	}
	// An error raised after the response started is written in-band next to a
	// truncated "data"; returning those rows would read as a short (final) page.
	if e := out.Exception; e != "" {
		if len(e) > 300 {
			e = e[:300] + "..."
		}
		return nil, fmt.Errorf("clickhouse exception in result: %q", e)
	}
	return out.Data, nil
}

// escapeLit escapes a string for use inside a single-quoted ClickHouse literal.
// Callers should still validate identifiers; this is defense-in-depth.
func escapeLit(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`)
}

// Num extracts a float from a ClickHouse JSON value (number, or numeric string).
// Lenient (malformed -> 0): only for display values that never feed a verdict,
// alert or credit; those use Float64. Integer columns use Uint/Int64.
func Num(m map[string]any, k string) float64 {
	switch v := m[k].(type) {
	case json.Number:
		x, _ := v.Float64()
		return x
	case string:
		x, _ := strconv.ParseFloat(v, 64)
		return x
	}
	return 0
}

type unsigned interface {
	uint8 | uint32 | uint64
}

// Uint decodes integer column k exactly into T: the value must be a base-10
// integer (JSON number or, for UInt64, JSON string) that fits T. On failure it
// returns 0 and stores the error in *errp unless *errp already holds one, so a
// caller can decode a whole row and check a single error.
func Uint[T unsigned](m map[string]any, k string, errp *error) T {
	s, err := numText(m, k)
	if err == nil {
		x, perr := strconv.ParseUint(s, 10, 64)
		if v := T(x); perr == nil && uint64(v) == x {
			return v
		}
		err = fmt.Errorf("column %q: %.64q is not a %T", k, s, T(0))
	}
	keepFirst(errp, err)
	return 0
}

// Int64 is Uint for an Int64 column.
func Int64(m map[string]any, k string, errp *error) int64 {
	s, err := numText(m, k)
	if err == nil {
		x, perr := strconv.ParseInt(s, 10, 64)
		if perr == nil {
			return x
		}
		err = fmt.Errorf("column %q: %.64q is not an int64", k, s)
	}
	keepFirst(errp, err)
	return 0
}

// Float32 decodes a Float32 column, rounding the text straight to float32 (no
// float64 detour). Error handling as Uint.
func Float32(m map[string]any, k string, errp *error) float32 {
	return float32(parseFloat(m, k, 32, errp))
}

// Float64 decodes a Float64 column strictly, for values that feed a verdict.
func Float64(m map[string]any, k string, errp *error) float64 {
	return parseFloat(m, k, 64, errp)
}

// parseFloat accepts only finite decimal numbers: strconv.ParseFloat also takes
// inf/nan, hex and underscore forms, and returns an error on overflow.
func parseFloat(m map[string]any, k string, bitSize int, errp *error) float64 {
	s, err := numText(m, k)
	if err == nil {
		x, perr := strconv.ParseFloat(s, bitSize)
		if perr == nil && strings.TrimLeft(s, "0123456789+-.eE") == "" {
			return x
		}
		err = fmt.Errorf("column %q: %.64q is not a float%d", k, s, bitSize)
	}
	keepFirst(errp, err)
	return 0
}

// numText returns the literal text of a numeric column.
func numText(m map[string]any, k string) (string, error) {
	switch v := m[k].(type) {
	case json.Number:
		return string(v), nil
	case string:
		return v, nil
	case nil:
		if _, ok := m[k]; !ok {
			return "", fmt.Errorf("column %q missing", k)
		}
		return "", fmt.Errorf("column %q is null", k)
	default:
		return "", fmt.Errorf("column %q: unexpected %T", k, v)
	}
}

func keepFirst(errp *error, err error) {
	if *errp == nil {
		*errp = err
	}
}

// Str extracts a string from a ClickHouse JSON value.
func Str(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}
