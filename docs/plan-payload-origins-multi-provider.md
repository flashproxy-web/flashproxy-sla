> **Historical.** This plan was written for the AWS fleet. AWS and `deploy/terraform/` were
> removed on 2026-11-02 (commit `88738a7`); the text below is kept unchanged as a record.

# Plan: get the payload origins off AWS-only (item 4)

*Companion to the connect-pool work (items 1–3, already implemented). This is the
bigger lift because it needs new infrastructure, not just config + a small code
change.*

## The problem

The **connect** scenario is now resilient: it probes a stratified sample of a
provider-diverse pool of third-party connectivity-check endpoints plus our own
origin, and is Down only if *every* sampled target fails (items 1–3). So AWS
blocking our datacenter egress no longer takes the SLA verdict down.

The **payload** scenarios have no such protection. `hifreq_small`,
`large_object`, `streaming`, and `long_session` each dial a **single hardcoded
origin** (`cmd/worker/main.go`, the `origin`/`origin_ipv6` fields resolved in
`runTarget`), and that origin is provisioned only on **AWS EC2**
(`deploy/terraform/modules/node`). With AWS blocking the datacenter egress
range, these scenarios sit at **5–32% success** and drag down the
`/api/scenarios` panel — even though the SLA verdict (connect-only) reads
operational.

They can't borrow the connect fix: connectivity-check endpoints only answer a
tiny GET. The payload scenarios need our **custom** endpoints — `/bytes/{n}`,
`/small`, `/hold` (`cmd/origin`) — so they *must* hit our own origins. The only
way to make them provider-resilient is to run origins on **more than one
provider** and select a reachable one.

## Constraints that shape the fix

1. **Egress cost is the dominant AWS line item.** `streaming` alone was ~90% of
   a data-transfer bill that was ~83% of AWS spend (see CLAUDE.md). So the
   payload scenarios must **failover**, not best-of-N — probe one origin, fall
   back to another only on failure. Best-of-N (used for the cheap connect probe)
   would multiply every payload byte by N. Do **not** copy `ConnectBest` here.
2. **Origins must be co-located with each package's proxy** (the rule under
   `cmd/origin` / `package_targets`), so response time isolates the proxy hop.
   A failover origin on another provider is a *reachability* fallback, not a
   latency source — when the primary is up, latency must come from the primary.
3. **Per-family reachability.** IPv6-egress packages need a v6-reachable origin;
   the AWS Local Zone (Dallas) has no v6, which is why `ipv6-residential` already
   borrows Ashburn's v6 origin. Any new provider must offer the right family.

## The change

### Infrastructure (the lift)
- Stand up `cmd/origin` on a **second and third non-AWS provider** (e.g. a
  bare-metal / alt-cloud host near each proxy region). They already build from
  source in cloud-init; the origin is a tiny static binary with no state, so
  this is cheap to run — the cost is provisioning + a dual-stack address.
- Prefer providers whose egress is **not** on the same blocked path as AWS, so
  a block on one can't take all origins down at once — the same diversity
  principle as the connect pool.

### Config
- Generalize the single `origin` / `origin_ipv6` into an **ordered list** of
  payload origins per IP family (mirror the shape of `connect_targets`, minus
  the `group`/stratification — failover is ordered, not sampled). First entry is
  the co-located primary; the rest are fallbacks.

### Code (`internal/probe` + `cmd/worker`)
- Add a **failover origin selector** used by the four payload scenarios: try the
  primary; on a connect-phase failure (dial/connect error or non-2xx), advance
  to the next origin. Crucially, **fail over only on the connect phase**, never
  mid-transfer — a body error after bytes started flowing is real signal, not a
  reason to re-download from another origin (and re-pay the egress).
- Record which origin served the row (the existing `Target` field already
  carries this), so `/api/scenarios` shows per-origin health the same way
  `connect_probe` now shows per-target health.
- A short **stickiness / health cache** so we don't re-probe a known-down
  primary every cycle (which would add a failed dial to every payload cycle):
  remember the last working origin for a few minutes, re-probe the primary
  periodically to recover.

### Terraform
- Add the new provider hosts as `modules/node`-style instances (or a lighter
  origin-only module), wire their addresses into the per-package origin list in
  `main.tf`, and keep the co-location rule (`package_targets`) intact.

## Rollout
1. Land the code (failover selector + config shape) — inert until a second
   origin is configured, so it ships safely ahead of the infra.
2. Stand up one alternate-provider origin; add it as the fallback for the
   AWS-blocked datacenter packages first (the ones at 5–32%).
3. Confirm `/api/scenarios` recovers for those packages, then extend to the rest.

## Effort

Bigger than items 1–3: it needs **new hosts on 1–2 additional providers**
(provisioning + cost), a **failover selector** with a health cache in
`internal/probe`, a **config shape change**, and **terraform** for the new
origins. Items 1–3 gave you the connect-path resilience and all the per-target
visibility for config-only + a small code change; this restores the payload
panel and removes the last AWS-only single point of failure.
