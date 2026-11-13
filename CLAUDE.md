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
published source URL referenced in code/docs is `github.com/flashproxy-web/flashproxy-sla`.

## Build, test, run

```bash
make build                  # builds bin/{origin,worker,website,verify,keygen} (CGO_ENABLED=0, -trimpath)
make origin|worker|website|verify|keygen   # one binary
make vet                    # go vet ./...
make test                   # go test ./...  (no tests: cmd/website, cmd/keygen, internal/model)
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
  for the next seq (sealed in chunks of <= 10000 rows). Flushes run in their own goroutine
  (`flushAsync`), so a slow insert does not stop intake: `flushMu` (TryLock) allows one flush in
  flight and owns the sealed batch (a trigger that finds it taken does nothing); `mu` guards
  only the unsealed buffer. Code that runs outside the flush goroutine must not read `sealed`
  (use `sealedSeqN`); the shutdown drain (`flushUntil`) takes `flushMu`. Run worker tests with
  `-race`. **Never rebuild or grow a batch at a
  seq that was already sent (KAN-260):** ClickHouse drops a re-insert with a seen token and
  acks it OK even if the content differs, so a client timeout after the server stored attempt
  1 used to leave row_count/batch_hash mismatches and chain breaks. The unsealed buffer is
  capped (`max_pending_rows`, default 300000; oldest unsealed rows dropped with a
  `PENDING BUFFER FULL` log line). It **drains on SIGTERM** (sealed batch first, then the
  buffer, 8 attempts) so deploys/outages don't lose data. Status-change events use the same
  rule (`eventLog`: a failed event keeps its seq and is retried each monitor tick before any
  newer event); the event insert's dedup token (`events:<seq>`) only works once `sla.events`
  has `non_replicated_deduplication_window` (migration `003`; without it a timed-out-but-stored
  event insert is stored twice on retry). **Exactly one worker per vantage** (vantage = ledger stream key → single-writer keeps
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
  the last 3 h from `sla.events` (parsing the message `"<prev> -> degraded (response
  Nms, X% ok)"`, `startup` rows excluded; N is an integer in new rows, old rows may hold
  decimals and parse rounded; stored messages are hashed as written and never rewritten).
  All gate ms values are `int`; the one exception is the incoming reading, passed as an
  unrounded `float32` so the `<= T` suppression check is not decided by rounding. With >= 5 issues, T rises to 5th-highest x 1.1 and
  a one-line raise announcement (old→new, count, top readings) replaces the alert; below T alerts are suppressed, and while T > base every `degraded -> operational` recovery is suppressed too. A 3 h recheck from each change lowers T (if the package is still in an open degraded episode, the 📉 post appends `, degraded (<ms>ms) since <date time>`; the episode start and its first reading are kept per package in memory, set on `operational -> degraded` and cleared on recovery or reset); while T > base a once-a-day summary
  goes out per package (10:00; 16:00 instead if a raise/lower was already posted that day and the last one is >= 3 h old, else skipped; Asia/Jerusalem). The daily state resets when T returns to base. A transition to `down`/`no_data` also resets the package's adaptive state. Issue counting is deliberately broader than the gate: every `* -> degraded` row (except from `startup`) counts, but only `operational <-> degraded` is gated. If the event write fails (only logged), the lookup misses the current issue, so that one decision counts one short; this fails safe. State is in memory (restart = base, no startup post; the next degraded event with >= 5 issues still in the window raises T again and announces it);
  a query error fails open. Events, ledger, website and SLA still use the package's Degraded threshold (`slo.degraded_avg_ms`, or its `target_ms` in the top-level `packages` list when set).
  Tunables: `adaptive_alert_tuning` (see `config/worker.example.json`).
- **`cmd/website`** — strictly **read-only** (reader role; never writes). Hardened
  `http.Server` (timeouts, panic recovery, security headers, TLS 1.2 min); server-renders
  `/`, `/<package>`, `/sla`, and `/incidents` (downtime & degradation history) from Go `html/template`
  files loaded at startup from the `web_dir` config path (`web/*.html.tmpl` — **not** embedded,
  so the `web/` dir must ship alongside the binary; all four templates, including
  `package.html.tmpl`, are parsed with `template.Must`, so the binary does not start without them);
  serves the JSON API; `/healthz` is a real
  ClickHouse readiness check, `/livez` is liveness; publishes the public CH key + SLO thresholds
  + ledger public key at `/api/meta`; `/api/ledger` exposes chain heads + checkpoints. Generic
  errors only (never echoes ClickHouse error text to clients). The `/incidents` page enumerates incidents from the **same cross-vantage
  rollup as the credit engine** and attaches optional operator-authored official statements
  (see incident statements below); it degrades gracefully when the statements table is absent.
  See "Public site" below for routing, caching and the page behaviour.
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
  not failed. Any read error or inconsistent page fails the run. Checkpoints are read
  **before** the ledgers and anchor them: every checkpoint must name an entry this run read
  and verified (the ledger has no TTL), so a ledger read short below the newest checkpoint
  fails the run, as does a listed stream with no entries; with `-pubkey`, an empty checkpoint
  read fails too. **`cmd/keygen`** mints the keypair.

Shared `internal/` packages:

- **`internal/probe`** — the scenarios. `openTunnel` is the core: with a proxy it does an
  HTTP `CONNECT` tunnel (`connect_ms` = proxy upstream establishment); with `proxy==nil` it
  does a direct TCP dial (the baseline). Connect and body phases have separate timeouts;
  throughput is measured over the transfer window (first→last byte).
- **`internal/slo`** — implements the **published SLA contract exactly, CROSS-VANTAGE**: a
  package is **Down only when ALL vantages are down that minute** (a single-vantage failure is
  NOT Down — it isolates one network path, not the proxy); **Degraded** = best-vantage
  **average round-trip response time (`ttfb_ms`)** > the package's Degraded threshold (`degraded_avg_ms`, 50, unless the package's `target_ms` in the `packages` list overrides it) for `degraded_for_min`
  (5) **consecutive** minutes; Availability% = (withData − Down − ½·Degraded)/withData. The SLA
  *latency* metric is the response time (`MinutesSQL` selects `avg(ttfb_ms)`), NOT connect-ms;
  availability/Down is still based on connect `success`. The **package** verdict, bars, uptime,
  incidents and alerts are best-vantage (home) based: the latency-Degraded verdict reads only the
  lowest-latency available vantage each minute. The **per-vantage detail** (`Vantages` in the
  rollup, shown by the card's vantage filter) is separate and does not change any of that: each
  vantage classifies its OWN minutes with its own `Evaluator` — down when its connect success is
  below `down_success_pct`, degraded when its own response time stays above the package threshold
  for `degraded_for_min` consecutive minutes, otherwise operational (so a far vantage measuring a
  product cross-region can read degraded there while the package stays operational).
  `RollupPackages()` is the authoritative cross-vantage implementation — the banner, the monitor's
  `Fetch`, and the uptime/credit accounting all derive from it; `rollupSeries` runs only on the
  synthetic best-vantage series; the per-vantage detail uses one `Evaluator` per vantage. `RollupIncidents()`/`IncidentsFromBars()` derive the `/incidents`
  history from those **same rollup bars**, so a listed incident matches what the credit engine
  bills. Thresholds published at `/api/meta` so the verdict is reproducible.
- **`internal/chstore`** — stdlib-only ClickHouse HTTP client. **Sends NO client-set settings on
  reads** — reader users are `readonly=1`, which rejects any setting with `Code 164: Cannot
  modify '…' in readonly mode` and 502s the whole site (this caused an outage). Query caps and
  cancel-on-disconnect live server-side on the reader *profiles*; timezone correctness relies on
  the **server** being UTC, not a per-query `session_timezone`. Bounds response size, returns
  errors (no silent truncation). `QueryJSON` decodes numbers as `json.Number` (never float64);
  integer columns are read with the exact, range-checked `Uint[T]`/`Int64` (JSON number, or
  JSON string for 64-bit), strict `Float64`/`Float32` for floats, lenient `Num` only for display floats.
  Inserts (writer role, not readonly) take a dedup token.
- **`internal/model`** — `ProbeResult` + `Event` and their **frozen `Canonical()`**
  serialization (the byte-exact form the ledger hashes and `cmd/verify` reproduces). Changing
  field order/format breaks verification of all historical data.
- **`internal/ledger`** — the hash chain (`Chain.Build`/`Commit`, `BatchHash`, `EntryHash`,
  `VerifyEntry`) and Ed25519 checkpoint sign/verify. Pure, stdlib-only, fully unit-tested
  (incl. tamper detection).

## Public site (`cmd/website`, `web/`)

Phase 1 (now): the site is served internally only (`127.0.0.1:8080`). Phase 2: put it behind
Cloudflare. **Phase 2 is not set up**; the cache headers below exist for it, and Phase 1 has no CDN in front.

- **`/`** is the status page: legend at the top, one card per product with a status square, the
  display name (`name` from the `packages` list, else the short key) as a stretched link to `/<package>`,
  vantage chips, the status pill, uptime, and the footer text
  `marked degraded above N ms` (N = that package's effective Degraded threshold). Row order: name,
  spacer, chips, status pill, so the pill is at the right and the chips sit left of it (the server-rendered first version has no chips; `app.js` adds them). Status labels
  are padded from the left to 11 characters so they right-align; chip milliseconds are right-aligned in
  4 characters plus `ms` using non-breaking spaces (values above 9999 are shown as they are). Chips never navigate;
  they drive the vantage filter below. The server renders a first version; `app.js`
  replaces it from `/api/overview`. The card code (`initCards`) is shared with the package page.
- **Overview window (`handleOverview`, `slo.RollupPackagesTrimAlts`, `trimRollup`).** The overview fetches
  blocks + DegradedForMin + 1 minutes (96 by default), classifies all of them (every Evaluator, package and
  per-vantage, runs over the full fetch, so the DegradedForMin warm-up minutes let a degraded run that began
  before the window show from the first displayed block) and then keeps the last 90 blocks of each package.
  If the NEWEST minute has no data (package bars), the newest block is dropped too (race), the rest from the
  oldest end. The same blocks are dropped from the package bars and from every
  vantage's bars. Status, age and uptime of the package and of each vantage are recomputed from the 90
  kept blocks (statuses are not re-classified). Only the overview bars use this: `/api/series`,
  `/api/scenarios`, `/incidents` (30 days), the SSR index (15 min) and the worker's Fetch are unchanged.
  **Owner rule:** the trim is for a RACE, not for data problems. The newest minute is dropped only because
  its data has not been filled yet. Every other empty minute is a real problem and is shown as no data,
  never hidden: a second empty minute at the end, empty minutes earlier in the window, a vantage that was
  down, lost connectivity, or no vantage running. The card hides no empty minute on the client (removed in
  e872db4). Goal: the graphs show real data; any miscalculation or race must be ruled out, not papered over.
- **Bar layout (`app.js` `barLayout` / `layoutBars`, `web/style.css`).** The card sizes its bars for
  the series length (90). Blocks are never stretched. The block width `b` and gap `g` are whole DEVICE
  pixels (devicePixelRatio aware) chosen from the bars container's current width: the gap is between
  10% and 25% of the pitch `b + g` (target about 20%), and the largest `b` that fits is used. Each block is
  placed absolutely at an exact device-pixel offset (`left`, `width` in CSS px = device px / ratio), and the
  row is centred with an even offset on each side (computed for the full series). Sizes are recalculated on a resize (ResizeObserver on the card list plus `resize`) or when the
  display scaling changes (`matchMedia` resolution query), not on the 15 s refresh (the layout is keyed on width, n, ratio and blocks
  drawn). Below about 180 px (90 x 1 px + 89 x 1 px gap) the blocks fall back to 1 px and the row scrolls
  sideways, newest block in view.
- **Vantage filter (cards on `/`).** By default a card's bars are the PACKAGE bars and the vantage
  chips are dimmed. Clicking a chip shows that vantage's bars; clicking the active chip clears the
  filter; clicking another chip switches. With a vantage selected each bar is split vertically with no
  gap: upper half = package status, lower half = that vantage's status (a thin border marks vantage
  data). The vantage status comes from the per-vantage verdict described under `internal/slo`. The
  package bars, the SLA verdict, uptime, incidents and alerts still follow the best-vantage (home)
  rule; the filter is display only. The choice is kept per card across the 15 s refresh.
- **Display names.** The website display name comes from the `name` field of the `packages` list (card,
  package page title and heading, incident chips and rows). Routes, `data-pkg`, `?package=` and
  thresholds use the short key.
- **`/<package>`** is a server-rendered detail page: title, canonical and og tags come from the
  package name; the intro text states the full rule (average response time above N ms for 5 consecutive
  minutes on the best vantage). Below it sits the same product card as `/` (`#pkg-card`, filled by
  `initCards(pkgCard, pkg)` from `/api/overview` for this product, refreshed every 15 s), followed by a legend
  (`ul.keys`) explaining the status squares, bar colours, vantage chips and the filter,
  the split bar, the degraded rule, "down" and the best-vantage star. The first server-rendered version
  contains only the name, the degraded threshold and the legend; the status square, status pill, chips,
  bars and uptime appear once `app.js` loads. On this page the card's name is plain text (not a link) and
  a click on the bars does nothing. Then the response-time chart (proxy vs direct) with HTML axis
  labels, chart vantage chips and scenarios (last 10 min, proxy vs direct), unchanged; links to `/`
  and to `/incidents?package=<name>`.
- **Routing (`handleRoot`).** Exact routes win: `/sla`, `/incidents`, `/api/*`, `/healthz`, `/livez`,
  `/robots.txt`, `/sitemap.xml`, `/llms.txt`. Only `/app.js`, `/style.css`, `/favicon.svg` are served
  as files (`no-cache, no-store, must-revalidate`); templates are never served. Any other single
  path segment matching `pkgRe` (`^[a-zA-Z0-9_.-]{1,64}$`) gets one primary-key-style lookup in
  `probe_raw` (last 7 days): found = page; not found = 404 with `Cache-Control: public, max-age=3600`;
  lookup error = 502 with no cache header. Two-segment paths, trailing slashes, names starting with `.`
  and names ending in `.tmpl` are 404 (no lookup). **Old-key redirect:** a request for `/<alt>`
  (an alt of the `packages` list that is not also a short key) returns `302` to `/<short>` with
  `Cache-Control: no-cache`, before any lookup. Website only.
- **Cache headers.** `/api/overview` `public, max-age=14`; `/api/series` and `/api/scenarios`
  `public, max-age=29` (success only; errors carry no max-age); package page `public, max-age=300` when its
  vantage chips loaded, else `no-cache`; other JSON endpoints `no-cache`. Also set in code: `/` `max-age=30`,
  `/sla` 300, `/incidents` 180. No cookies, no `Vary`. These are meant for Phase 2.
- **Timers (`web/app.js`).** The index refreshes `/api/overview` every 15 s; the detail page refreshes
  series and scenarios every 30 s. Intent: only while the tab is visible. Actual: both `setInterval`s keep
  ticking while the tab is hidden and the tick skips the request; a `visibilitychange` to visible reloads at once.
- **`/api/overview`** components carry `degraded_ms` (the effective threshold; 50 when unset).
- **`/incidents`**: one toggle chip per package; dimmed = not in the filter, highlighted = in it, several
  may be on, none on = all rows. Filtering is client-side on each row's `data-pkg`. `?package=` is read
  by the JS and used only if it matches a chip; the server ignores it. Chips are sorted down first, then
  degraded (by whether the package has such an incident in the listed window), then the rest, by name.

### Per-package configuration (`packages`)

- One list, `packages`, at the TOP LEVEL of `website.json` (and its example file) holds everything
  per product for the website. The worker reads `slo.package_degraded_ms` instead (a map keyed by
  probe name, old and new names both allowed); a worker config with a `packages` key refuses to
  start. The old `package_names` and `package_alts` maps are removed. Each entry:
  - `short`: the real key, used internally (routes, ledger, rollup, `data-pkg`, thresholds, alerts).
  - `alt`: the old live key, or `null`; its rows are merged into `short` during the cutover overlap.
  - `name`: display text; absent = the short key. Present but empty is a config error.
  - `target_ms`: Degraded threshold in whole ms, minimum 1; absent or invalid (below 1, not whole)
    falls back to `slo.degraded_avg_ms` (50 by default); absent is silent, only a nonzero invalid value logs.
  - `covered`: carried as data only; no verdict or page uses it.
- `target_ms` also sets the Discord adaptive gate's base per package (`alertgate` `PackageBase`, built
  in `cmd/worker` config load). Events, ledger, SLA credits and incidents use the same value through the
  same rollup (`SLO.ForPackage`); the card/page text uses `SLO.DegradedThresholdFor`.
- **Alt merge (website only, `internal/slo` `MergeAlts`).** Per product, vantage and minute the
  short-key row wins; otherwise the alt-key row is used; never both. Identity pairs (`alt == short`,
  e.g. `mobile` in the examples; `isp` has `alt: null`) are no-ops. Chained aliases, an alt used by two entries, alts
  failing `pkgRe` and empty names stop the website and the worker at startup (`slo.CheckPackages`, called by both binaries; an empty name fails at parse in `Package.UnmarshalJSON`).
  The worker does not merge yet: its thresholds key on the short key only, so rows still stored under
  an alt key keep the global threshold until the worker moves to the real keys (KAN-412, linked to KAN-403).
- The example configs list nine products. Examples set `target_ms` 50 for the first five, 1000 for
  `residential` and `residential-lite`, 2000 for `mobile` and `mobile_usa`.
- `config/worker.example.json` has example probe entries (`targets`) for residential, residential-lite,
  mobile and mobile_usa (`${RESI_PROXY_URL}`, `${LITE_PROXY_URL}`, `${MOBILE_PROXY_URL}`,
  `${MOBILE_US_PROXY_URL}`; names taken from the zest env template). They are not in the live worker config.
- The credit port in `flash-staff-dash` does not yet read the per-package thresholds, so **do not set
  `target_ms` overrides in a live config until it does.**

### Known gaps

- No browser test has been run for the layout or the JavaScript; the card layout (chip wrap, split
  bar, status alignment) has not been checked in a browser at 500 px.
- The worker does not merge alt keys yet (KAN-412), see Per-package configuration.
- The bar layout (device-pixel placement, centring and the moire fix) and the package-page card have not
  been checked in a browser.
- The overview trim is per package: each package decides on its own newest minute, so one package may
  drop its newest block while another keeps it.
- The live `/opt/flashproxy/sla` `website.json` and `worker.json` still need the `packages` list; they
  must be restarted and checked at deploy.
- Phase 2 (Cloudflare caching) is not set up.
- The threshold text for the four new products is accurate only once their probes exist and their
  thresholds are set in the live configs.

## ClickHouse roles (the trust boundary)

`schema/clickhouse.sql` creates `sla.probe_raw` + `sla.events` (both UTC, **400-day TTL**,
carrying `stream`/`seq`), the append-only **`sla.ledger`** + **`sla.ledger_checkpoints`**, and
the `probe_1m` rollup. probe_raw/ledger/events set `non_replicated_deduplication_window` so retried
inserts are idempotent (events: migration `003`). `schema/roles.sql` creates `sla_reader` (SELECT) and `sla_writer`
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
- **Numeric helpers:** read every integer column with `chstore.Uint[T]`/`Int64` sized to
  the column (they error on missing/malformed/out-of-range, first error kept in `*errp`);
  `cmd/verify` must return that error (fail closed). Floats that feed a verdict, alert or
  credit (slo `response_ms_avg`/`success_pct`) use strict `Float64`/`Float32`. Lenient `Num`
  (malformed -> 0) is only for pure display values (website `/api/series`); never narrow
  `Num` into an integer type.
- **Probe volume is a cost line, not just a knob.** Every payload byte is billed egress on a
  metered host — on the former AWS fleet the `streaming` scenario alone (proxy + `_direct`,
  every vantage) once ran at its 5 MB /120s default and was ~90% of a data-transfer bill that
  was ~83% of total AWS spend. Prod pins `"scenarios": {"streaming_bytes": 524288,
  "streaming_interval_ms": 600000}` in `/opt/flashproxy/sla/worker.json` (512 KB /600s — still
  past TCP slow-start, so Mbps stays meaningful). **`config/worker.example.json` still
  documents the expensive defaults** — don't copy it to a metered host verbatim, and price any
  new payload scenario or cadence change (bytes × 2 for the baseline × vantages × cycles/day)
  before shipping it.
- **No new dependencies.** The dependency-free property is load-bearing (offline, reproducible
  builds). The ledger crypto is stdlib (`crypto/ed25519`,
  `crypto/sha256`) precisely to preserve this.

## Deploy

- **Current deployment: everything runs on the ClickHouse box from `/opt/flashproxy/sla`**,
  under systemd: `sla-worker` (monitor worker), `sla-website` (`127.0.0.1:8080`; needs `web/`
  next to the binary) and, on probe nodes, `sla-origin` (port 980). The AWS fleet (Ashburn,
  Frankfurt, Dallas) and `deploy/terraform/` were removed on 2026-11-02 (commit `88738a7`);
  nothing runs on AWS. Live configs are `/opt/flashproxy/sla/worker.json` and `website.json`;
  secrets are in root-only env files there (`.env`, `worker.env`, `website.env`, `ledger.env`)
  — never print, copy or commit them, and never put secrets in git.
- **Manual deploy:** `make build` in the repo, copy the binaries and `web/` to
  `/opt/flashproxy/sla`, restart the unit. Website checklist (do not run unasked):
  1. Build the website binary (`make website`).
  2. Copy the whole `web/` directory, including `package.html.tmpl`, next to the binary.
  3. Restart `sla-website` (it exits at startup if any template is missing).
  4. Smoke test: `/`, `/<a real package>`, `/<unknown name>` (expect 404), `/app.js`, `/style.css`,
     `/favicon.svg`, `/sla`, `/api/overview` (check `degraded_ms` is present), `/healthz`,
     `/index.html.tmpl` (expect 404); then a browser check of `/` and a package page at 500 px wide. **Config changes are hot-patched on the box**
  (edit the live json, restart the unit), then the same change is made in whichever config
  repo is canonical (config-as-code is moving to `/root/dev/config`, zest) so a rebuild
  doesn't silently revert it.
- **`deploy/CLICKHOUSE-SETUP.md`** — the full human tutorial for standing up the datastore on
  a new box: install, config (incl. **system-log TTLs + `keep_free_space_bytes`**, whose absence
  caused the 2026-08-14 outage), schema, migrations, roles, Cloudflare tunnel, a security
  verification checklist, and day-2 ops (disk alerting, backups, truncation rules).
  Start here; the prompt file below is the same deployment addressed to an agent.
- **`deploy/clickhouse-deploy-prompt.md`** + **`deploy/bootstrap-roles.sh`** — runbook + script
  for the self-hosted ClickHouse: loopback bind, UTC timezone, `select_from_system_db_requires_grant`
  + query masking (so the published reader can't read `system.*`), the schema incl. the ledger,
  the scoped roles, exposed only via a Cloudflare tunnel at `ch.flashproxy.com`.

### Operating the live deployment (hard-won)

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
    with `prev = GenesisHash`. That still **passes `cmd/verify` provided `sla.ledger_checkpoints`
    is truncated too** (see below) — `verifyStream` roots every
    chain at Genesis against its *first present* entry and does not require seq to start at 1 —
    the chain just carries a cosmetically high starting seq. The pre-reset rows below the new
    head become uncovered-by-ledger (honest data-loss gap), which is fine.
  - To genuinely reset to seq 1 you must truncate **both** `probe_raw` and `ledger` (you lose the
    data). Only the `events` stream resets to 1 on a ledger-only truncate, because its
    `ProbeMaxSeq` reads `probe_raw WHERE stream='events'`, which is always empty.
  - **Also truncate `sla.ledger_checkpoints`.** Old checkpoints attest heads of the pre-reset
    chain; the `checkpointLoop` re-signs current heads within `checkpoint_every_sec`. Leftover
    checkpoints after a ledger reset make `cmd/verify` FAIL immediately: every checkpoint must
    name an entry in the current ledger (checkpoints anchor the chain), and one at a seq the
    new chain re-reaches with a different entry_hash is reported as tampering.

## Sibling repos

This repo is one of several under `~/dev/` (see `/root/CLAUDE.md` for the workspace map).
It is independent of `flash-riptide-isp` / `riptide-autodeploy` — it *monitors* a proxy
endpoint as an external input, and shares no code with them.
