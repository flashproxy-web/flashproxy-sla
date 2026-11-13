# flashproxy-status

Open-source, fully reproducible **SLA monitoring for proxy services**. It actively
simulates the payload archetypes of real proxy users, runs each workload **both
through the proxy and directly (no-proxy baseline)**, and measures latency,
throughput, and availability — so degradation is caught before customers complain,
and the proxy's overhead over the raw network is explicit.

**Live:** <https://status.flashproxy.com> · **SLA:** <https://status.flashproxy.com/sla>
· built by **[FlashProxy](https://flashproxy.com)**.

It is a **closed, fully reproducible system**: every number on the public page was
recorded by this app itself, into its *own* ClickHouse. Nothing is read from any
internal or production data source — and that metrics database is **publicly
readable**, so anyone can independently reproduce every figure (this is what makes
the FlashProxy SLA verifiable). Clone it, point it at any proxy, and you get the
same dashboard.

```
 origin ──▶ [proxy under test] ──▶ worker ──▶ own ClickHouse ──▶ website ──▶ status page + /sla
            (+ direct baseline)   (probes,     (system of        (read-only,   (SSR, crawlable,
                                   writer role)  record)           sla_reader)   independently
                                                                                 verifiable)
```

The only thing crossing into the system from the outside is the **proxy endpoint +
credentials** you put in config — an input, not a data source. Read and write are
split by ClickHouse role, so you can run **N workers** (e.g. one VM per region) all
writing to one ClickHouse, with read-only websites on top.

## Live deployment

| | |
|---|---|
| **Status page** | <https://status.flashproxy.com> |
| **SLA** | <https://status.flashproxy.com/sla> — 100% uptime guarantee, automatically compensated, independently verifiable |
| **Public metrics DB** (read-only) | `https://ch.flashproxy.com` · user `flashproxy-status-public` · password `flashproxy-public-ro` · database `sla`. **Not published yet** (checked 2026-11-02: `ch.flashproxy.com` returned HTTP 530, `status.flashproxy.com` returned HTTP 404; the tunnel ingress for both is commented out) |
| **Company** | <https://flashproxy.com> |

The production deployment runs on the ClickHouse box itself, from `/opt/flashproxy/sla`,
under systemd: `sla-worker` (the monitor worker), `sla-website` (the status page, on
`127.0.0.1:8080`, which needs `web/` next to the binary) and, on probe nodes, `sla-origin`
(port 980). Live configs are `worker.json` and `website.json` in that directory; secrets
live in root-only env files next to them and are never committed. The earlier AWS fleet
(Ashburn, Frankfurt, Dallas) and its Terraform were removed on 2026-11-02. The ClickHouse
host is exposed only through a Cloudflare tunnel (`ch.flashproxy.com`).

## What it measures

Per product, per vantage, every workload runs **twice** — through the proxy and
**direct** (no-proxy baseline, suffixed `_direct`) — so the proxy's overhead is
visible. One row per attempt lands in `sla.probe_raw`.

| Scenario | Simulates | Headline metric |
|---|---|---|
| `connect` | upstream connect + round-trip to origin | **round-trip response time (ttfb)** — the SLA latency metric |
| `net_rtt` | raw gateway network RTT (TCP connect) | network RTT ms |
| `streaming` | heavy streaming / buffering | sustained throughput (Mbps, measured over the transfer window) |
| `large_object` | large-object download | TTFB / throughput |
| `hifreq_small` | account / credential checkers | connect-ms distribution + setup success |
| `scraping` | broad web scraping (many hosts) | connect-ms spread |
| `long_session` | long-maintained / persistent sessions | hold stability |

The headline `connect` probe targets the project's **own deterministic origin**, not a
third-party site, so the SLA number isn't polluted by some external host's availability
(third-party reachability is measured separately by `scraping`). The page auto-selects
the **best vantage** per product (lowest average response time) as the default; on the
status page a vantage chip splits each card's bars (package above, that vantage's own status below); the latency chart overlays **response time via proxy vs direct** so the
network floor and the proxy overhead are side by side. `net_rtt` is a stdlib TCP-connect
timing (no ICMP/raw sockets), so it works unprivileged in the distroless image.

## The SLA verdict (how status & uptime are computed)

`internal/slo` implements the published SLA contract exactly, so the page and the
contract can't drift:

- **Available** — the `connect` scenario succeeds.
- **Down** — a minute that is below the success threshold (`down_success_pct`, default 90) **from every vantage simultaneously**. A single-vantage failure is **not** Down (it isolates one network path, not the proxy).
- **Degraded** — Available, but the **best vantage's average round-trip response time exceeds the product's Degraded threshold (`slo.degraded_avg_ms`, 50 ms by default, unless the package's `target_ms` in the `packages` list overrides it) for 5 consecutive minutes** (and stays Degraded until it recovers). The run-up minutes before the trigger are not yet Degraded.
- The package verdict, uptime and incidents follow the best vantage. The per-vantage view on the status page is display only: each vantage classifies its **own** minutes (down below `down_success_pct`; degraded when its own response time stays above the package threshold for 5 consecutive minutes; otherwise operational).
- **Availability%** = (minutes with data − Down − ½·Degraded) / minutes with data × 100.

The exact thresholds are published at `/api/meta`, so the verdict is reproducible from
data — not just from the source.

## Architecture

| Component | What it does |
|---|---|
| `cmd/origin` | Deterministic dual-stack upstream (`/connect`, `/bytes/{n}`, `/small`, `/hold`). Pure SLA signal, no third-party variance. `/bytes/{n}` above `-max-bytes` (default 5 MiB) serves the cap and sets `X-Bytes-Capped: requested=<n>; served=<cap>`. |
| `cmd/worker` | Runs every scenario from one vantage (via proxy + direct), writes results **directly** to ClickHouse as the `sla_writer` role, and appends an integrity-ledger entry per batch. The only component that touches a real proxy. **Exactly one worker per vantage** (the vantage is the ledger stream key). Set `"monitor": true` on one to evaluate SLO, fire Discord alerts, and sign ledger checkpoints. Drains on shutdown and retries a failed batch unchanged (sealed: same seq, rows and ledger entry) on transient errors, so deploys/outages don't drop SLA data. |
| `cmd/website` | **Read-only.** Server-renders the status page and `/sla`, serves the JSON API, and publishes the public read-only key + SLO thresholds + ledger public key at `/api/meta` — all as the `sla_reader` role; never writes. Hardened HTTP server (timeouts, recovery, security headers) with a real ClickHouse readiness check. |
| `cmd/verify` | **Standalone public verifier.** Recomputes every batch/entry hash and checks every Ed25519 checkpoint signature using only public read access — the tool anyone runs to confirm the data is untampered. |
| `cmd/keygen` | Generates the Ed25519 checkpoint-signing keypair. |
| `internal/{probe,slo,chstore,model,ledger}` | Scenario probes, the SLA-contract SLO evaluation, a stdlib-only ClickHouse HTTP client, shared types + their frozen canonical serialization, and the hash-chain/signing primitives. |
| `web/` | Framework-free, console-style, server-rendered status + SLA pages. |

## Integrity: tamper-evident measurements

Because FlashProxy controls the database, the metrics alone can't prove they weren't
edited after the fact. So every batch of measurements is committed to an **append-only,
hash-chained ledger** (`sla.ledger`), and the chain heads are periodically
**Ed25519-signed** into `sla.ledger_checkpoints`. Any retroactive edit, deletion, or
reorder of a measurement breaks the recomputed hashes against the signed checkpoints.

Verify it yourself against the public database — nothing but the published key needed:

```bash
go run ./cmd/verify -ch https://ch.flashproxy.com \
  -user flashproxy-status-public -pass flashproxy-public-ro \
  -pubkey "$(curl -s https://status.flashproxy.com/api/meta | jq -r .integrity.pubkey)"
```

It walks every per-vantage chain, recomputes each batch hash from the raw rows, and
checks every checkpoint signature, reporting any break, gap, or bad signature. It reads
in seq-ordered pages of whole batches (`-chunk-rows`, default 200000), because the public
user caps one query result at 1M rows and a stream holds more than that; the chunk size
never changes the verdict. This is **tamper-evidence**, not a claim that the measurements are honest at generation time —
that rests on the open-source code, multiple independent vantages, and live publication.
(Checkpoints are signed and stored in the public DB; an external anchor — OpenTimestamps
/ a transparency log — can be added with no schema change to also defeat key-holder
rewrites.)

Dependency-free Go (standard library only) — builds offline, trivially reproducible.
ClickHouse is accessed over its HTTP interface. Binaries are built manually with
`make build` and copied to `/opt/flashproxy/sla` (no container registry required).

## ClickHouse roles

`schema/clickhouse.sql` creates the tables; `schema/roles.sql` creates two roles and
three users (concurrency-capped via `max_concurrent_queries_for_user`):

| User | Role | Cap | Use |
|---|---|---|---|
| `flashproxy-status-public` | `sla_reader` | 50 | **published** — anyone can query the raw data, verify the ledger, and reproduce the page |
| `flashproxy-status-website` | `sla_reader` | 50 | the website renders with this |
| `flashproxy-status-worker` | `sla_writer` | 200 | workers push probe results + ledger entries |

The public profile is bounded on every axis (rows, bytes, memory per-query and
per-user, execution time, concurrency) so the published credential can't be used for a
resource-exhaustion query. Because it's internet-facing, the deploy also sets
`select_from_system_db_requires_grant` so it can't read `system.*` (query logs /
password hashes) — see `deploy/clickhouse-deploy-prompt.md`.

> SQL-created users need a writeable access storage (`<local_directory>` under
> `user_directories`) or ClickHouse errors with "no writeable access storage".

## Pages & API

- **`/`** — server-rendered status page (crawlable; Organization + WebSite + FAQ JSON-LD). One card per product: display name (links to `/<package>`), vantage chips, status pill at the right; the footer reads "marked degraded above N ms". The card shows 90 one-minute blocks (the overview fetches blocks + DegradedForMin + 1 minutes (96 by default), classifies them all, then keeps the last 90 blocks; the newest block is dropped too if its minute has no data yet (a race); every other empty minute is shown as no data, never hidden); the blocks are never stretched: their width and gap are whole device pixels chosen from the card width, and the row is centred. By default the bars are the package bars and the chips are dimmed; clicking a chip shows that vantage's bars (each bar split: package above, vantage below), clicking it again clears the filter.
- **`/<package>`** — server-rendered detail page per product: the same product card as `/` (live, refreshed every 15 s; its name is not a link), a legend explaining the squares, bars, chips, split bar, degraded rule, "down" and the best-vantage star, then the chart, vantage chips and last-10-min scenarios. Unknown names (no probe rows in the last 7 days) return 404. An old key (`alt` of a product) redirects (302) to its short key.
- **`/incidents`** — downtime and degradation history; client-side package filter chips.
- **`/sla`** — server-rendered Service Level Agreement (crawlable; FAQ JSON-LD).
- **`/api/overview`** — per-product, per-vantage status + 90-block uptime (blocks + DegradedForMin + 1 minutes fetched (96 by default), all classified, then the last 90 blocks kept per package), with the best vantage marked; each component carries `degraded_ms` (its effective Degraded threshold).
- **`/api/series?package=&vantage=&minutes=`** — network-RTT and proxy-connect time series.
- **`/api/scenarios?package=&vantage=`** — per-scenario stats (proxy vs `_direct`).
- **`/api/ledger`** — integrity-ledger chain heads + recent signed checkpoints + verify command.
- **`/api/status`**, **`/api/meta`** (public CH key, SLO thresholds, ledger public key + hashing spec), **`/api/events`**, **`/healthz`** (readiness — checks ClickHouse), **`/livez`** (liveness).
- **`/robots.txt`**, **`/sitemap.xml`**, **`/llms.txt`** — SEO + GEO (AI answer engines welcomed).

## Quick start

```bash
# ClickHouse + origin + website (no real proxy needed):
docker compose up --build
# open http://localhost:8080  (renders; shows "no data" until a worker runs)

# Add a worker against your own proxy to see live numbers:
ISP_PROXY_URL='http://USER:PASS@HOST:30' docker compose --profile demo up --build
```

## Configuration

JSON with `${ENV}` expansion so secrets never live in the file.

- `config/website.example.json` — listen addr, ClickHouse (reader) creds, the public
  key to publish, optional TLS, `site_url`, SLO thresholds (`slo` block), and the top-level
  `packages` list (see below).
- `config/worker.example.json` — vantage id, ClickHouse (writer) creds, the
  origin(s) for payload scenarios, scrape hosts, scenario intervals, and the list of
  probe `targets` → `proxy_url` (e.g. `${ISP_PROXY_URL}`), target, and IP version, plus the `slo` block
  with `slo.package_degraded_ms` (per-package thresholds keyed by probe name, old and new names both allowed). The example holds probe entries for residential, residential-lite,
  mobile and mobile_usa; they are not in the live worker config.

### `packages` (top level of `website.json`)

One list, one entry per product, read by the website (replaces the removed `package_names` and
`package_alts`). The worker reads `slo.package_degraded_ms` instead (probe names, old and new both
allowed); a worker config with a `packages` key refuses to start:

- `short` — the real key (routes, ledger, rollup, `data-pkg`, thresholds, alerts).
- `alt` — the old live key, or `null`. The website merges alt-key rows into `short` per product,
  vantage and minute: the short-key row wins, else the alt row, never both. `alt == short` is a no-op;
  chains, duplicate alts, invalid alts and empty names stop the website and the worker at startup. The worker does not
  merge yet (KAN-412), so rows still under an alt key keep the global threshold there.
- `name` — display text (cards, package page, incident chips and rows); absent = the short key.
- `target_ms` — Degraded threshold in whole ms, minimum 1; absent or invalid falls back to
  `slo.degraded_avg_ms` (50).
- `covered` — carried as data; no verdict uses it.

## Deploy

- **`deploy/clickhouse-deploy-prompt.md`** — runbook to bring up a self-hosted
  ClickHouse on a bare-metal host: loopback bind + kernel-reserved ports, the three
  roles, exposed only via a Cloudflare tunnel (`ch.flashproxy.com`).

## Transparency & the SLA

This project is the measurement instrument behind FlashProxy's SLA. Because **the code
is open source** and **the metrics database is publicly readable**, every availability
figure — and every automatic SLA credit — can be independently reproduced and
challenged. See the full SLA at <https://status.flashproxy.com/sla>.

## Author

Built and maintained by **Alon Levi**, Co-Founder of **[FlashProxy](https://flashproxy.com)**
— `alon@flashproxy.io`.

## License

MIT — © 2026 Alon Levi / FlashProxy. See [LICENSE](LICENSE).
