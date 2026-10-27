# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`flashproxy-status` is an open-source, independently-reproducible SLA monitor for proxy
services (the instrument behind FlashProxy's public SLA). Synthetic probes simulate real
proxy-user workloads, run each **through the proxy and direct (no-proxy baseline)**, and
write results to a ClickHouse that is itself **publicly readable** — so every number on
the status page can be reproduced by anyone. See `README.md` for the product framing and
the public DB credentials.

Note the naming split: the Go module is `github.com/flashproxy/flashproxy-status`, but the
published source URL referenced in code/docs is `github.com/thealonlevi/flashproxy-sla`.

## Build, test, run

```bash
make build                  # builds bin/{origin,worker,website,verify,keygen} (CGO_ENABLED=0, -trimpath)
make origin|worker|website|verify|keygen   # one binary
make vet                    # go vet ./...
make test                   # go test ./...  (ledger, slo, probe have tests)
go test ./internal/slo -run TestEvaluator  # single test
go run ./cmd/keygen         # generate the Ed25519 ledger-signing keypair

make up                     # docker compose: own ClickHouse + origin + website (no real proxy)
make demo                   # adds a worker; needs ISP_PROXY_URL='http://user:pass@host:port'
```

Dependency-free Go 1.25 (stdlib only — no `go.sum`, builds offline; the integrity ledger
uses `crypto/sha256` + `crypto/ed25519` from the stdlib, so the no-deps property holds).
ClickHouse is reached over its HTTP interface, never a driver. No lint config beyond `go vet`.

Tests do **not** need a live proxy or a live ClickHouse — `internal/ledger` and
`internal/slo` are pure logic (tamper-detection and the SLA verdict/uptime math), and
`internal/probe` exercises the probe logic directly. The worker measures network RTT with a
stdlib TCP dial (`internal/probe/ping.go` → `NetRTT`, scenario `net_rtt`); there is no
`ping`/ICMP shell-out, so it runs unprivileged in the distroless image.

## Architecture: a one-directional, role-split pipeline

```
origin ──▶ [proxy under test] ──▶ worker ──▶ ClickHouse ──▶ website ──▶ status page + /sla
           (+ direct baseline)   (writer)   (sla.probe_raw)  (reader)
```

Three binaries, strictly separated by what they touch:

- **`cmd/worker`** — the only component that touches a real proxy. Runs every scenario from
  one vantage, **twice per cycle** (through the proxy, and direct with `proxy==nil` → the
  `_direct` baseline), writes rows as the **writer** role, and appends one **integrity-ledger
  entry per flush batch**. Each scenario runs in its own ctx-aware goroutine (`runTarget`);
  `emit` is **non-blocking** (sheds + counts probes if the channel is full, so cadence stays
  accurate during a store outage). The `flusher` (`cmd/worker/flush.go`) **seals** a batch
  when its flush starts (size 200 or 3s tick): seq, rows, ledger entry and dedup tokens are
  computed once, then rows and ledger entry are inserted (in that order) and only then is the
  chain advanced. A failed sealed batch is **retried byte-identical** (an insert step that was
  already acked is not re-sent) until it commits; probes arriving meanwhile wait in a buffer
  for the next seq (sealed in chunks of <= 10000 rows). **Never rebuild or grow a batch at a
  seq that was already sent (KAN-260):** ClickHouse drops a re-insert with a seen token and
  acks it OK even if the content differs, so a client timeout after the server stored attempt
  1 used to leave row_count/batch_hash mismatches and chain breaks. The unsealed buffer is
  capped (`max_pending_rows`, default 300000; oldest unsealed rows dropped with a
  `PENDING BUFFER FULL` log line). It **drains on SIGTERM** (sealed batch first, then the
  buffer, 8 attempts) so deploys/outages don't lose data. Status-change events use the same
  rule (`eventLog`: a failed event keeps its seq and is retried each monitor tick before any
  newer event); note `sla.events` has no dedup window, so a timed-out-but-stored event insert
  is stored twice on retry until that table gets `non_replicated_deduplication_window`. **Exactly one worker per vantage** (vantage = ledger stream key → single-writer keeps
  the chain fork-free). `"monitor": true` on one worker runs `monitorLoop` (SLO eval every
  30s, debounced status-change events + Discord) and, if `ledger_signing_key` is set, the
  `checkpointLoop` that signs all chains' heads.
  **Restart reconciliation (KAN-254):** the monitor's state is in memory, so on the first
  observation of each package it reads the package's last `status_change` row from `sla.events`
  (`LastStatusMessages` + `seedConfirmed`) and continues from that status; a mismatch goes
  through the normal debounce and writes an ordinary `from -> to` row (e.g. `degraded ->
  operational` closing an episode that recovered while the worker was down). Same status = no
  row, no alert. No `startup` rows are written any more (old ones still parse). With no rows
  at all, a degraded/down package is treated as coming from `operational`.
  **`"adaptive_alert": true`** (default off; `internal/alertgate`) adds an *alert-only*
  per-package Discord threshold T for `operational <-> degraded` only: after the event is
  written, `fireTransition` asks the gate, which reads the package's `status_change` rows of
  the last 3 h from `sla.events` (parsing the frozen message `"<prev> -> degraded (response
  Nms, X% ok)"`, `startup` rows excluded). With >= 5 issues, T rises to 5th-highest x 1.1 and
  a one-line raise announcement (old→new, count, top readings) replaces the alert; below T alerts are suppressed, and while T > base every `degraded -> operational` recovery is suppressed too. A 3 h recheck from each change lowers T; while T > base a once-a-day summary
  goes out per package (10:00; 16:00 instead if a raise/lower was already posted that day and the last one is >= 3 h old, else skipped; Asia/Jerusalem). The daily state resets when T returns to base. A transition to `down`/`no_data` also resets the package's adaptive state. Issue counting is deliberately broader than the gate: every `* -> degraded` row (except from `startup`) counts, but only `operational <-> degraded` is gated. If the event write fails (only logged), the lookup misses the current issue, so that one decision counts one short; this fails safe. State is in memory (restart = base, no startup post; the next degraded event with >= 5 issues still in the window raises T again and announces it);
  a query error fails open. Events, ledger, website and SLA still use `slo.degraded_avg_ms`.
  Tunables: `adaptive_alert_tuning` (see `config/worker.example.json`).
- **`cmd/website`** — strictly **read-only** (reader role; never writes). Hardened
  `http.Server` (timeouts, panic recovery, security headers, TLS 1.2 min); server-renders
  `/`, `/sla`, and `/incidents` (downtime & degradation history) from Go `html/template`
  files loaded at startup from the `web_dir` config path (`web/*.html.tmpl` — **not** embedded,
  so the `web/` dir must ship alongside the binary); serves the JSON API; `/healthz` is a real
  ClickHouse readiness check, `/livez` is liveness; publishes the public CH key + SLO thresholds
  + ledger public key at `/api/meta`; `/api/ledger` exposes chain heads + checkpoints. Generic
  errors only (never echoes ClickHouse error text to clients). `/` falls through to a static
  server otherwise. The `/incidents` page enumerates incidents from the **same cross-vantage
  rollup as the credit engine** and attaches optional operator-authored official statements
  (see incident statements below); it degrades gracefully when the statements table is absent.
- **`cmd/origin`** — deterministic upstream (`/connect`, `/bytes/{n}`, `/small`, `/hold`) so
  payload metrics are pure SLA signal with no third-party variance. Bind dual-stack (`:8080`
  serves v4+v6) so IPv6 packages exercise v6 egress. `/bytes/{n}` above `-max-bytes` (default
  5 MiB) is not an error: it serves exactly the cap plus `X-Bytes-Capped: requested=<n>; served=<cap>`. The headline `connect` probe is
  **best-of-N** (`probe.ConnectBest`) over a **provider-diverse pool** (~10 connectivity-check
  endpoints across 8 providers — mozilla/google/microsoft/apple/cloudflare/ubuntu/debian/gnome).
  Each cycle it **stratifies**: the `origin`-group (or ungrouped) endpoints are ALWAYS probed
  (availability floor, so no *hard* external dependency and no all-blocked draw), plus one random
  endpoint from each of `GroupsPerCycle`=5 random provider groups. It records the BEST as
  scenario `connect` (lowest `ttfb` among successes; Down only if EVERY sampled target fails) —
  this isolates the proxy from target-side noise — and also emits every sampled target as a
  diagnostic `connect_probe` row. **The SLO reads only `connect`**, so `connect_probe` never
  changes a verdict. Any 2xx counts as success (the `generate_204` endpoints; a 200-only check
  once silently failed gstatic). The payload scenarios (throughput etc.) use only our origin.
  The pool is per-target config `connect_targets: [{target, path, group}]` (`path: ""` ⇒ the
  tunnel itself is the signal, no GET); the singular `connect_target`/`origin_get` is the
  **legacy single-target fallback**, still honored. `docs/` holds forward-looking plans (e.g.
  multi-provider payload origins) — not current behavior. Whichever origin a package points at should be the
  one **co-located with that package's PROXY**, not the vantage's own origin — response time
  then isolates the proxy hop (vantage→proxy + proxy→co-located origin ≈ vantage→proxy).
- **`cmd/verify`** — standalone public auditor: recomputes every batch/entry hash and checks
  every checkpoint signature using only public read access. It reads in pages of whole
  batches (`-chunk-rows`, default 200000; `ORDER BY seq LIMIT n WITH TIES`, so a page never
  splits a batch), because the public profile's `max_result_rows` = 1M is below one stream's
  `probe_raw` size; don't raise that cap, keep paging. Each stream's ledger is read first and
  fixes the head; rows are read only up to it, so batches written during a run are skipped,
  not failed. Any read error or inconsistent page fails the run. **`cmd/keygen`** mints the keypair.

Shared `internal/` packages:

- **`internal/probe`** — the scenarios. `openTunnel` is the core: with a proxy it does an
  HTTP `CONNECT` tunnel (`connect_ms` = proxy upstream establishment); with `proxy==nil` it
  does a direct TCP dial (the baseline). Connect and body phases have separate timeouts;
  throughput is measured over the transfer window (first→last byte).
- **`internal/slo`** — implements the **published SLA contract exactly, CROSS-VANTAGE**: a
  package is **Down only when ALL vantages are down that minute** (a single-vantage failure is
  NOT Down — it isolates one network path, not the proxy); **Degraded** = best-vantage
  **average round-trip response time (`ttfb_ms`)** > `degraded_avg_ms` (50) for `degraded_for_min`
  (5) **consecutive** minutes; Availability% = (withData − Down − ½·Degraded)/withData. The SLA
  *latency* metric is the response time (`MinutesSQL` selects `avg(ttfb_ms)`), NOT connect-ms;
  availability/Down is still based on connect `success`. The per-vantage latency-Degraded verdict
  applies only to a product's **home (lowest-latency) vantage** — a far vantage measuring it
  cross-region (high geographic latency but reachable) is availability-only, never degraded.
  `RollupPackages()` is the authoritative cross-vantage implementation — the banner, the monitor's
  `Fetch`, and the uptime/credit accounting all derive from it; `rollupSeries` is the per-vantage
  building block it reduces. `RollupIncidents()`/`IncidentsFromBars()` derive the `/incidents`
  history from those **same rollup bars**, so a listed incident matches what the credit engine
  bills. Thresholds published at `/api/meta` so the verdict is reproducible.
- **`internal/chstore`** — stdlib-only ClickHouse HTTP client. **Sends NO client-set settings on
  reads** — reader users are `readonly=1`, which rejects any setting with `Code 164: Cannot
  modify '…' in readonly mode` and 502s the whole site (this caused an outage). Query caps and
  cancel-on-disconnect live server-side on the reader *profiles*; timezone correctness relies on
  the **server** being UTC, not a per-query `session_timezone`. Bounds response size, returns
  errors (no silent truncation). `Num`/`Str`/**`NumU64`**: 64-bit ints arrive as JSON **strings**
  — use `NumU64` for UInt64, never `Num`. Inserts (writer role, not readonly) take a dedup token.
- **`internal/model`** — `ProbeResult` + `Event` and their **frozen `Canonical()`**
  serialization (the byte-exact form the ledger hashes and `cmd/verify` reproduces). Changing
  field order/format breaks verification of all historical data.
- **`internal/ledger`** — the hash chain (`Chain.Build`/`Commit`, `BatchHash`, `EntryHash`,
  `VerifyEntry`) and Ed25519 checkpoint sign/verify. Pure, stdlib-only, fully unit-tested
  (incl. tamper detection).

## ClickHouse roles (the trust boundary)

`schema/clickhouse.sql` creates `sla.probe_raw` + `sla.events` (both UTC, **400-day TTL**,
carrying `stream`/`seq`), the append-only **`sla.ledger`** + **`sla.ledger_checkpoints`**, and
the `probe_1m` rollup. probe_raw/ledger set `non_replicated_deduplication_window` so retried
inserts are idempotent. `schema/roles.sql` creates `sla_reader` (SELECT) and `sla_writer`
(SELECT+INSERT); the **public** user is published (resource-capped on every axis) so anyone
can reproduce the page, the **website** user renders, the **worker**
user writes. The read/write split is the mechanism that makes the data trustworthy — keep it.

Passwords are `${ENV}` placeholders in the SQL; substitute at bootstrap. The Docker demo
shortcuts this with a single `sla` admin user — production uses the three scoped roles.

**Incident statements are editorial, NOT ledger data.** `sla.incident_statements`
(`ReplacingMergeTree`, 400-day TTL — migration `002`) holds operator-authored official
statements attached to incidents. It is deliberately **outside the integrity ledger** (empty
`body` = retracted). The internal dashboard **`flash-staff-dash`** publishes statements through
a fourth, narrow role — `sla_statements_writer` / user `flashproxy-status-statements` — that can
only SELECT/INSERT this one table, so it can **never forge measurements** the way the broad
`sla_writer` (probe_raw/events/ledger) could. `sla_reader` already covers `sla.*`, so the public
and website users read statements with no grant change.

## Conventions that matter

- **Config is JSON with `${ENV}` expansion** (`os.ExpandEnv` on the file bytes), so proxy
  credentials and DB passwords never live in the file. `config/*.docker.json` are the
  committed demo configs; `config/*.example.json` document the full shape. Both binaries
  **validate required fields and fail fast** (an unset `${ENV}` expands to `""`, which would
  otherwise silently mean an empty password) — keep that validation when adding fields.
- **Never log proxy credentials.** `worker` runs `redactURL` (`url.Redacted()`) on any proxy
  URL before logging. Preserve this in any new log line that includes a proxy URL.
- **Scenario naming:** the direct baseline of scenario `X` is stored as `X_direct` (via
  `scn()`). Queries and the UI rely on this suffix to pair proxy vs baseline.
- **The integrity ledger constrains the write path.** Rows carry `(stream=vantage, seq)`; the
  flusher inserts rows then the ledger entry (dedup-token idempotent) and only then advances
  the chain (`Chain.Build` → write → `Commit`). One writer per vantage. `model.Canonical()`
  is **frozen** — changing field order/format invalidates every historical hash.
- **Timezone is UTC end-to-end, pinned on the SERVER (not per-query).** Columns are
  `DateTime('UTC')` and ClickHouse runs `<timezone>UTC</timezone>`. The client must NOT send
  `session_timezone` (or any setting) on reads — reader users are `readonly=1` and reject it.
- **Numeric helpers:** keep most SQL outputs <64-bit (`toUInt32`, `round`) so they render as
  JSON numbers; for genuine UInt64 (`seq`, `bytes`) use `chstore.NumU64`, never `Num` (float
  precision loss past 2^53).
- **Probe volume is a cost line, not just a knob.** Every payload byte is billed AWS egress —
  the `streaming` scenario alone (proxy + `_direct`, every vantage) once ran at its 5 MB /120s
  default and was ~90% of a data-transfer bill that was ~83% of total AWS spend. Prod pins
  `"scenarios": {"streaming_bytes": 524288, "streaming_interval_ms": 600000}` in
  `deploy/terraform/modules/node/user-data.sh.tftpl` (512 KB /600s — still past TCP slow-start,
  so Mbps stays meaningful). **`config/worker.example.json` still documents the expensive
  defaults** — don't copy it to a metered host verbatim, and price any new payload scenario or
  cadence change (bytes × 2 for the baseline × vantages × cycles/day) before shipping it.
- **No new dependencies.** The dependency-free property is load-bearing (offline, reproducible
  builds; cloud-init builds from source). The ledger crypto is stdlib (`crypto/ed25519`,
  `crypto/sha256`) precisely to preserve this.

## Deploy

- **`deploy/terraform/`** — three AWS vantages: Ashburn (`us-east-1`), Frankfurt
  (`eu-central-1`), and Dallas (the `us-east-1-dfw-2` **Local Zone**, so it lives in the
  `us-east-1` parent region/provider, needs its zone-group opted in, and must use
  `m6g.medium` — dfw-2a offers no t-family sizes). Each is a module instance of
  `modules/node` with its own VPC CIDR;
  a dual-stack VPC, `t4g.small` instances that **build the binaries from source in cloud-init**
  (no registry), worker + origin everywhere and website in Ashburn on `:443` with a Cloudflare
  Origin Certificate. Instances enforce IMDSv2 (`metadata_options`), verify the Go toolchain
  SHA-256, and build from a pinnable `git_ref` (use an immutable commit SHA). Secrets come via
  env / a gitignored `*.tfvars`; `terraform.tfstate*`, `*.tfvars`, and `.terraform/` are
  gitignored and must never be committed (state would leak resource attributes).
  **State lives in S3, not on any one box** — versioned + SSE-S3 + TLS-only, with S3-native
  locking (`use_lockfile`, needs Terraform >= 1.10 — hence the bumped `required_version`).
  `backend.tf` is a **partial** backend config: the actual bucket/key/region live in
  `backend.hcl`, which is **gitignored** (this repo is public, and the state location is
  not something to publish) — copy `backend.hcl.example` and run
  `terraform init -backend-config=backend.hcl`. The bucket is bootstrapped **out-of-band**
  (it can't be managed by the state it stores); `backend.tf` carries the `aws s3api`
  commands to recreate it. Credentials come from the ambient AWS identity, never the repo.
- **Everything needed to operate the fleet lives in S3 — use `./tf.sh`, not bare `terraform`.**
  Alongside the state, `<prefix>/secrets/` holds `terraform.tfvars` and the break-glass SSH
  private key. `tf.sh` streams them in at run time (`-var-file=<(aws s3 cp … -)`), so a fresh
  machine needs only `git clone` + an AWS identity: `./tf.sh init`, `./tf.sh plan`,
  `./tf.sh apply`, `./tf.sh ssh <ashburn|frankfurt|dallas>`. There is **no `terraform.tfvars`
  in the working dir** — bare `terraform plan` will fail on missing variables, by design.
  **Never** read these secrets with a Terraform `data` source (`aws_secretsmanager_secret_version`,
  `aws_ssm_parameter`): data-source results are persisted to state in PLAINTEXT. State today
  contains zero secrets — the provider stores `user_data` as a SHA1 hash, not the rendered
  script — and passing secrets as `-var-file` keeps it that way.
  After changing a secret, re-upload it or the S3 copy silently goes stale:
  `aws s3 cp terraform.tfvars s3://<bucket>/<prefix>/secrets/terraform.tfvars --sse AES256`.
- **`deploy/CLICKHOUSE-SETUP.md`** — the full human tutorial for standing up the datastore on
  a new box: install, config (incl. **system-log TTLs + `keep_free_space_bytes`**, whose absence
  caused the 2026-08-14 outage), schema, migrations, roles, Cloudflare tunnel, a security
  verification checklist, and day-2 ops (disk alerting, backups, truncation rules).
  Start here; the prompt file below is the same deployment addressed to an agent.
- **`deploy/clickhouse-deploy-prompt.md`** + **`deploy/bootstrap-roles.sh`** — runbook + script
  for the self-hosted ClickHouse: loopback bind, UTC timezone, `select_from_system_db_requires_grant`
  + query masking (so the published reader can't read `system.*`), the schema incl. the ledger,
  the scoped roles, exposed only via a Cloudflare tunnel at `ch.flashproxy.com`.

### Operating the live fleet (hard-won)

- **Instances build from source on boot (~5 min on `t4g.small`)** — slow and fragile (one bad
  boot-script line aborts cloud-init silently and the unit never starts). `terraform apply
  -replace=module.<region>.aws_instance.this` rebuilds a whole instance; the EIP re-attaches so
  the public IP is stable, but that vantage is down during the rebuild. Apply a **saved reviewed
  plan** (`terraform plan -replace … -out`, then `apply <plan>`) — blind `-auto-approve` on prod
  is blocked. **Baking an AMI** removes the slow/fragile boot — the recurring real fix.
- **A bare `terraform apply` will rebuild the ENTIRE fleet — check the plan every time.**
  `modules/node` picks its AMI with `data "aws_ami" "ubuntu" { most_recent = true }`, so as
  soon as Canonical publishes a new Ubuntu 24.04 image the `ami` attribute changes and that
  **forces replacement** of all three instances at once (all vantages down together, each
  needing the fragile ~5 min cloud-init build). As of 2026-07-27 a plan already shows
  `3 to add, 3 to change, 3 to destroy` for exactly this reason — it is AMI drift, not a
  config change. Roll instances **one at a time** with `-replace=module.<region>.aws_instance.this`,
  and to defuse it permanently either pin the AMI id in a variable or add
  `lifecycle { ignore_changes = [ami] }` to the instance.
- **Faster than a replace: in-place binary redeploy.** Break-glass SSH key is
  `~/.ssh/flashproxy-sla` (matches tfvars `ssh_public_key`, comment `flashproxy-sla-breakglass`).
  On the box: `cd /opt/flashproxy/src && git fetch && git checkout <sha> && CGO_ENABLED=0
  /usr/local/go/bin/go build -trimpath -o /opt/flashproxy/<bin> ./cmd/<bin> && systemctl restart
  flashproxy-<unit>`. Only that unit blips. (Replacing an instance changes its SSH host key —
  clear the stale `known_hosts` entry; same EIP.)
- **Config lives in cloud-init, so a config-only change needs a hot-patch too.** The unit configs
  are heredoc'd into `/opt/flashproxy/config/*.json` by `user-data.sh.tftpl`, which runs **only on
  first boot** — editing the template changes new/replaced instances, not the running fleet. To
  land it now, SSH in, edit `/opt/flashproxy/config/worker.json`, `systemctl restart
  flashproxy-worker`, and commit the template change so a rebuild doesn't silently revert it.
- **Migrate ClickHouse BEFORE deploying new binaries.** Admin is loopback-only on the CH box, so
  migrations (`deploy/migrations/*.sql`, additive/online ALTERs) run THERE, not from a dev box.
  The new worker reads `sla.ledger` / writes `stream`,`seq` on startup → it **crash-loops** on an
  un-migrated schema. (Old code is forward-compatible with the migrated schema, so migrate first
  is always safe.) Note: existing MergeTree sort keys can't be ALTERed (e.g. adding `vantage`),
  so the live table keeps its old `ORDER BY` — that's only a pruning optimization.
- **The ClickHouse box fills up, and `system.*` is what fills it.** On 2026-08-14 the page
  went dark for hours: all three workers healthy, every insert rejected with
  `Code: 243 … NOT_ENOUGH_SPACE`. Probe data was only ~2 months old (nowhere near the 400-day
  TTL) — the space went to un-TTL'd ClickHouse **log tables**, which grow with query volume,
  not data volume. Fixes live in `deploy/CLICKHOUSE-SETUP.md` §4.4. Two things to know when
  it happens: the workers **self-heal** once space is freed (they retain the sealed batch and retry
  it on every flush trigger — restarting them *discards* the sealed batch and the buffer), and the built-in monitor **cannot alert**,
  because it writes its events to the same full database. Disk alerting must live outside
  this stack.
- **Truncating / resetting data:** STOP the workers → `TRUNCATE` → START the workers. The chain
  head (next `seq` + prev `entry_hash`) lives in worker memory, so truncating under a running
  worker leaves the new ledger starting mid-chain and `cmd/verify` reports a break.
  **`resumeChain` derives the head from `MAX(seq)` in `probe_raw`, NOT from the ledger**
  (`cmd/worker/main.go` — it guards against seq reuse after a crash). Consequences when you
  reset (verified the hard way during the 2026-08-14 hostname cutover):
  - Truncating **`sla.ledger` only** (keeping `probe_raw`, e.g. to preserve recovered rows) does
    NOT reset seq to 1. The worker resumes at `probe_raw`'s max seq and re-roots the chain there
    with `prev = GenesisHash`. That still **passes `cmd/verify`** — `verifyStream` roots every
    chain at Genesis against its *first present* entry and does not require seq to start at 1 —
    the chain just carries a cosmetically high starting seq. The pre-reset rows below the new
    head become uncovered-by-ledger (honest data-loss gap), which is fine.
  - To genuinely reset to seq 1 you must truncate **both** `probe_raw` and `ledger` (you lose the
    data). Only the `events` stream resets to 1 on a ledger-only truncate, because its
    `ProbeMaxSeq` reads `probe_raw WHERE stream='events'`, which is always empty.
  - **Also truncate `sla.ledger_checkpoints`.** Old checkpoints attest heads of the pre-reset
    chain; the `checkpointLoop` re-signs current heads within `checkpoint_every_sec`. Leaving
    them is a latent false-positive: a stream that re-seeds low (like `events` → seq 1) will over
    time re-reach the old checkpoint seqs with *different* entry_hashes, and `cmd/verify` will
    then flag tampering. (High-seq `aws-*` streams don't collide, but the stale checkpoints still
    read as orphaned.)

## Sibling repos

This repo is one of several under `~/dev/` (see `/root/CLAUDE.md` for the workspace map).
It is independent of `flash-riptide-isp` / `riptide-autodeploy` — it *monitors* a proxy
endpoint as an external input, and shares no code with them.
