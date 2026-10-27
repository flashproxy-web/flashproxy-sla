-- flashproxy-status schema. Loaded automatically by the ClickHouse container on
-- first boot (mounted into /docker-entrypoint-initdb.d). This is the app's OWN,
-- separate ClickHouse — nothing here references any production data source.
--
-- Timezone: every DateTime column is pinned to UTC so the stored value is
-- unambiguous regardless of the server's local timezone, and probe `ts > now()`
-- window comparisons stay correct even if the host TZ is ever non-UTC.

CREATE DATABASE IF NOT EXISTS sla;

-- Raw probe results: one row per probe attempt, the system of record. 400d TTL so
-- a full year of monthly SLA accounting can always be reproduced from raw data.
--
-- stream + seq tie each row to the append-only integrity ledger (see sla.ledger):
-- a worker assigns its rows a per-vantage `stream` (= its vantage id) and a
-- monotonic batch `seq`, and publishes one ledger entry per (stream, seq) whose
-- batch_hash commits to the canonical serialization of exactly these rows.
CREATE TABLE IF NOT EXISTS sla.probe_raw
(
    ts              DateTime('UTC'),
    vantage         LowCardinality(String),
    package         LowCardinality(String),
    scenario        LowCardinality(String),
    proto           LowCardinality(String),
    target          String,
    ip_version      UInt8,
    success         UInt8,
    error_type      LowCardinality(String),
    dial_ms         UInt32,
    connect_ms      UInt32,
    ttfb_ms         UInt32,
    total_ms        UInt32,
    bytes           UInt64,
    throughput_mbps Float32,
    stream          LowCardinality(String) DEFAULT '',
    seq             UInt64 DEFAULT 0
)
ENGINE = MergeTree
-- vantage is in the sort key so per-vantage queries (the public chart path) prune
-- by it instead of scanning every vantage in the (package, scenario, ts) range.
ORDER BY (package, scenario, vantage, ts)
TTL ts + INTERVAL 400 DAY
-- de-dup retried inserts: a re-sent batch with the same insert_deduplication_token
-- (the worker uses "<vantage>:<seq>") is dropped, so a flush retry after a transient
-- error cannot create duplicate rows.
SETTINGS non_replicated_deduplication_window = 1000;

-- Self-recorded markers: deploys, maintenance windows, status changes. Also chained
-- into the integrity ledger under stream='events'. 400d TTL (was unbounded).
CREATE TABLE IF NOT EXISTS sla.events
(
    ts      DateTime('UTC') DEFAULT now(),
    type    LowCardinality(String),
    package LowCardinality(String),
    message String,
    stream  LowCardinality(String) DEFAULT 'events',
    seq     UInt64 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY ts
TTL ts + INTERVAL 400 DAY
SETTINGS non_replicated_deduplication_window = 1000;

-- ===========================================================================
-- Operator-authored incident statements (editorial — NOT a measurement).
-- ===========================================================================
-- Official, human-written explanations of an incident, published by staff via the
-- internal dashboard (flash-staff-dash → sla_events.official_comment). Public-
-- readable so the status page can show "what happened" beside an auto-detected
-- incident. Deliberately OUTSIDE the integrity ledger: the ledger attests
-- measurements; these are editorial commentary. Keyed by dedupe_key and matched to
-- the page's incidents by (package + time overlap). ReplacingMergeTree(updated_at):
-- re-saving a statement inserts a newer row that supersedes the old one (read with
-- argMax/FINAL); an emptied body retracts it.
CREATE TABLE IF NOT EXISTS sla.incident_statements
(
    dedupe_key   String,                    -- stable id from the dashboard: incident:<pkg>:<type>:<start>
    package      LowCardinality(String),
    event_type   LowCardinality(String),    -- 'down' | 'degraded'
    period_start DateTime('UTC'),
    period_end   DateTime('UTC'),
    body         String,                    -- the official statement (empty = retracted)
    author       String,
    published_at DateTime('UTC'),
    updated_at   DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY dedupe_key
TTL period_start + INTERVAL 400 DAY;

-- ===========================================================================
-- Append-only, hash-chained integrity ledger (tamper-evidence).
-- ===========================================================================
-- Each entry commits to a batch of rows (probe_raw rows for a flush, or one event)
-- via batch_hash = sha256(canonical rows), and to the previous entry via prev_hash,
-- forming a per-stream hash chain. entry_hash chains forward, so any retroactive
-- edit/delete/reorder of a covered row breaks batch_hash, and any tampering with a
-- ledger entry breaks every downstream entry_hash. seq is gap-free per stream, so a
-- deleted batch is detectable as a missing seq.
--
-- This proves tamper-EVIDENCE, not tamper-PROOFING: ClickHouse remains mutable by
-- the operator. Integrity rests on (a) this chain, (b) Ed25519-signed checkpoints
-- in sla.ledger_checkpoints, and (c) — when configured — an external anchor of the
-- signed head. The ledger is tiny, so it carries NO TTL: chain/seal integrity stays
-- verifiable indefinitely even after the underlying raw rows expire at 400 days
-- (after which the batch CONTENT can no longer be re-derived, but the chain can).
CREATE TABLE IF NOT EXISTS sla.ledger
(
    stream      LowCardinality(String),
    seq         UInt64,
    kind        LowCardinality(String),   -- 'probe' | 'event'
    ts_first    DateTime('UTC'),          -- earliest row ts in the batch
    ts_last     DateTime('UTC'),          -- latest row ts in the batch
    row_count   UInt32,
    batch_hash  FixedString(64),          -- lowercase hex sha256 of the canonical rows
    prev_hash   FixedString(64),          -- entry_hash of (stream, seq-1); genesis = 64×'0'
    entry_hash  FixedString(64),          -- sha256(stream|seq|prev_hash|batch_hash|ts_first|ts_last|row_count)
    recorded_at DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (stream, seq)
SETTINGS non_replicated_deduplication_window = 1000;

-- Periodic Ed25519-signed checkpoints of each stream's chain head. The signature
-- covers a canonical message over (stream, seq, entry_hash, ts); the public key is
-- published (repo + /api/meta) so anyone can verify. Storing the head here lets a
-- verifier confirm the chain it recomputed matches a value FlashProxy signed at a
-- point in time. (External anchoring of these checkpoints can be added later with
-- no schema change.)
CREATE TABLE IF NOT EXISTS sla.ledger_checkpoints
(
    stream     LowCardinality(String),
    seq        UInt64,                    -- head seq this checkpoint attests
    entry_hash FixedString(64),           -- head entry_hash at this checkpoint
    ts         DateTime('UTC'),           -- checkpoint time (part of the signed message)
    pubkey_id  LowCardinality(String),    -- short id of the signing key (first 8 hex of the pubkey)
    signature  String,                    -- base64 Ed25519 signature of the canonical message
    signed_at  DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (stream, seq);

-- Optional 1-minute rollup (query optimization + durable history). Phase 1 queries
-- hit probe_raw directly — synthetic volume is tiny — but this is here for when
-- history grows. TTL is longer than probe_raw so a downsampled record outlives raw.
CREATE TABLE IF NOT EXISTS sla.probe_1m
(
    bucket              DateTime('UTC'),
    vantage             LowCardinality(String),
    package             LowCardinality(String),
    scenario            LowCardinality(String),
    -- SimpleAggregateFunction(sum,…), NOT plain UInt64: this is an AggregatingMergeTree,
    -- and ClickHouse 26.x rejects a bare aggregatable column here (Code 36 — on merge it
    -- would collapse rows and keep an arbitrary value). sum is the correct reducer, and the
    -- MV below inserts raw count()/sum(success), which SimpleAggregateFunction consumes directly.
    attempts            SimpleAggregateFunction(sum, UInt64),
    successes           SimpleAggregateFunction(sum, UInt64),
    connect_ms_state    AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), UInt32),
    connect_ms_avg_st   AggregateFunction(avg, UInt32),
    ttfb_ms_state       AggregateFunction(quantilesTDigest(0.5, 0.95), UInt32),
    throughput_avg_st   AggregateFunction(avg, Float32)
)
ENGINE = AggregatingMergeTree
ORDER BY (package, scenario, vantage, bucket)
TTL bucket + INTERVAL 400 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS sla.probe_1m_mv TO sla.probe_1m AS
SELECT
    toStartOfMinute(ts) AS bucket,
    vantage, package, scenario,
    count()       AS attempts,
    sum(success)  AS successes,
    quantilesTDigestState(0.5, 0.95, 0.99)(connect_ms) AS connect_ms_state,
    avgState(connect_ms)                               AS connect_ms_avg_st,
    quantilesTDigestState(0.5, 0.95)(ttfb_ms)          AS ttfb_ms_state,
    avgState(throughput_mbps)                          AS throughput_avg_st
FROM sla.probe_raw
GROUP BY bucket, vantage, package, scenario;
