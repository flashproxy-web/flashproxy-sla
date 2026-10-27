-- Migration 003 — insert de-duplication for sla.events on the LIVE database (KAN-260).
--
-- Run as sla_admin ON THE CLICKHOUSE HOST (loopback). ADDITIVE and ONLINE: a table
-- setting change only (metadata), no rewrite, no downtime, existing rows untouched.
--
--   clickhouse-client --user sla_admin --password '<pw>' --multiquery \
--     < deploy/migrations/003-events-dedup-window.sql
-- (or send one statement per HTTP request to the loopback admin endpoint)
--
-- Why: the worker now seals an event at first attempt and retries it with the same
-- insert_deduplication_token ('events:<seq>'). probe_raw and ledger already have a
-- dedup window (migration 001), but sla.events did not, so the token did nothing:
-- an insert that was stored but timed out client-side was stored AGAIN on retry, and
-- verify reports it as a row_count mismatch at that seq. With the window, the retry
-- is dropped and the stored row matches the ledger entry.
--
-- Order: safe before OR after deploying the new worker (old workers send no token
-- for events, so nothing changes for them). Run it BEFORE the worker deploy so the
-- new worker is protected from its first insert.
--
-- Rollback (restores the old behaviour, drops no data):
--   ALTER TABLE sla.events RESET SETTING non_replicated_deduplication_window;

ALTER TABLE sla.events MODIFY SETTING non_replicated_deduplication_window = 1000;

-- Verify (expect the setting in the output):
--   SELECT create_table_query FROM system.tables WHERE database = 'sla' AND name = 'events';
