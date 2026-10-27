# Setting up the flashproxy-status ClickHouse on a new box

End-to-end tutorial for standing up the datastore behind the status page: install →
configure → schema → roles → tunnel → verify → operate.

Companion documents:

- **`clickhouse-deploy-prompt.md`** — the same deployment expressed as a prompt to hand
  to an agent running on the target box. This file is the human version, and it is the
  more complete of the two (it adds retention, disk headroom, backups, and day-2 ops).
- **`bootstrap-roles.sh`** — the script that creates roles/users without leaking
  passwords into shell history or `query_log`.
- **`../schema/clickhouse.sql`**, **`migrations/*.sql`** — the canonical table
  definitions. Load them; never hand-copy (embedded copies drift).

Everything below assumes Ubuntu/Debian with systemd and root access.

---

## 0. What you are building

One small, dedicated, single-node ClickHouse holding a few hundred rows/minute of
synthetic probe data. It is reached **only** over loopback plus a Cloudflare tunnel —
there is no inbound firewall port and the origin IP stays hidden.

```
workers (3 vantages) ──┐
                       ├──▶ https://ch.flashproxy.com ──▶ cloudflared ──▶ 127.0.0.1:HTTP_PORT
website ───────────────┤                                                   (ClickHouse)
public / auditors ─────┘
```

Four users, separated by what they may touch — this split is the reason the published
numbers are trustworthy, so preserve it:

| User | Role | Can |
|---|---|---|
| `flashproxy-status-worker` | `sla_writer` | SELECT + INSERT on `sla.*` |
| `flashproxy-status-website` | `sla_reader` | SELECT on `sla.*` |
| `flashproxy-status-public` | `sla_reader` | SELECT on `sla.*`, resource-capped, **password published** |
| `flashproxy-status-statements` | `sla_statements_writer` | SELECT + INSERT on `sla.incident_statements` **only** |
| `sla_admin` | — | everything, **loopback only**, never via the tunnel |

### Sizing

Budget from real numbers: the live fleet writes ~2.1M rows/day across three vantages.
Raw probe data compresses well (LowCardinality + MergeTree), but **plan on the system
log tables being larger than your data** unless you cap them — see §4.4.

| | Recommendation |
|---|---|
| Disk | **50 GB minimum**, 100 GB comfortable. Never a `tmpfs` or `/tmp`. |
| RAM | 4 GB works; 8 GB comfortable |
| CPU | 2 cores |

---

## 1. Safety rules when the box is shared

**A previous attempt at this caused a production outage.** ClickHouse was bound to
`0.0.0.0` on a port a co-located proxy was using as an ephemeral egress source port,
and then the wrong PID — the proxy — was killed while chasing the conflict. If other
services run on this box, requirement #1 is not disturbing them.

1. **Bind to `127.0.0.1` only.** Never `0.0.0.0`, never a public IP.
2. **Pick two random high ports** (HTTP + native TCP) in 20000–60000 and **reserve them
   from ephemeral allocation** so nothing else can grab them as a source port (§3).
3. **Never kill a process found by port** without confirming it is yours:
   `ps -o cmd= -p <pid>` must contain your ClickHouse config path. If it shows anything
   else, stop and ask a human.
4. **Data dir on a disk with room** — check `df -h` first. Not `/tmp`: systemd-tmpfiles
   reaps files older than 30 days and will silently eat a running database.
5. **Stop/start via systemd only.** ClickHouse runs under a watchdog that respawns it;
   `kill` fights the watchdog.
6. **If a distro ClickHouse already exists here, run yours as a separate instance** —
   own config file, data dir, ports, and unit name. Do not touch theirs.

---

## 2. Install

Use the official repo (packages include `clickhouse-client`, useful later):

```bash
apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
  | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" \
  > /etc/apt/sources.list.d/clickhouse.list
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-server clickhouse-client
```

The package starts its own `clickhouse-server.service` on the default ports. If this box
is dedicated you may reuse it, but this guide runs a **separate, explicitly configured
instance**, so disable the packaged one:

```bash
systemctl disable --now clickhouse-server
```

---

## 3. Choose and reserve ports

```bash
HTTP_PORT=28123      # pick your own random values in 20000-60000
NATIVE_PORT=29000

ss -ltnp | grep -E ":($HTTP_PORT|$NATIVE_PORT)\b"   # MUST be empty

cat >/etc/sysctl.d/99-clickhouse-reserved.conf <<EOF
net.ipv4.ip_local_reserved_ports = $HTTP_PORT,$NATIVE_PORT
EOF
sysctl --system
sysctl net.ipv4.ip_local_reserved_ports    # confirm both appear
```

Reserving matters: without it the kernel may hand one of these to another process as an
ephemeral source port while ClickHouse is down, and startup then fails confusingly.

---

## 4. Configuration

```bash
mkdir -p /etc/clickhouse-flashproxy /var/lib/clickhouse-flashproxy /var/log/clickhouse-flashproxy
chown -R clickhouse:clickhouse /var/lib/clickhouse-flashproxy /var/log/clickhouse-flashproxy
```

### 4.1 `/etc/clickhouse-flashproxy/config.xml`

Substitute your two ports. Every block below is load-bearing; §4.4 explains the ones
that are easy to skip and expensive to omit.

```xml
<clickhouse>
    <logger>
        <level>information</level>
        <log>/var/log/clickhouse-flashproxy/server.log</log>
        <errorlog>/var/log/clickhouse-flashproxy/server.err.log</errorlog>
        <!-- Bounded, or the log itself eventually fills the disk. -->
        <size>100M</size>
        <count>3</count>
    </logger>

    <!-- Loopback ONLY. External access is the Cloudflare tunnel, never an open port. -->
    <listen_host>127.0.0.1</listen_host>
    <http_port>__HTTP_PORT__</http_port>
    <tcp_port>__NATIVE_PORT__</tcp_port>

    <path>/var/lib/clickhouse-flashproxy/</path>
    <tmp_path>/var/lib/clickhouse-flashproxy/tmp/</tmp_path>
    <user_files_path>/var/lib/clickhouse-flashproxy/user_files/</user_files_path>

    <!-- The app writes naive-UTC timestamps and compares them to now(). The client is
         forbidden from sending session_timezone (readonly=1 users reject settings), so
         UTC correctness depends entirely on the SERVER being UTC. -->
    <timezone>UTC</timezone>

    <!-- Writable access storage so SQL-created users survive restarts. -->
    <user_directories>
        <users_xml>
            <path>/etc/clickhouse-flashproxy/users.xml</path>
        </users_xml>
        <local_directory>
            <path>/var/lib/clickhouse-flashproxy/access/</path>
        </local_directory>
    </user_directories>

    <!-- readonly=1 does NOT block SELECTs from `system`. Without these, the PUBLISHED
         public user could read system.query_log (other users' queries, including
         CREATE USER ... BY '<plaintext>') and system.users (password hashes). -->
    <access_control_improvements>
        <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
        <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    </access_control_improvements>

    <!-- Defense in depth for the bootstrap step: redact passwords from the query log. -->
    <query_masking_rules>
        <rule>
            <regexp>IDENTIFIED\s+WITH\s+\w+\s+BY\s+'[^']+'</regexp>
            <replace>IDENTIFIED ... BY '[HIDDEN]'</replace>
        </rule>
    </query_masking_rules>

    <!-- Refuse to consume the last 10 GiB. ClickHouse fails inserts instead of filling
         the filesystem and taking down anything else on the box. -->
    <storage_configuration>
        <disks>
            <default>
                <keep_free_space_bytes>10737418240</keep_free_space_bytes>
            </default>
        </disks>
    </storage_configuration>

    <!-- ===== System log retention. DO NOT SKIP — see 4.4. ===== -->
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <ttl>event_date + INTERVAL 14 DAY DELETE</ttl>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>
    <part_log>
        <database>system</database>
        <table>part_log</table>
        <ttl>event_date + INTERVAL 14 DAY DELETE</ttl>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </part_log>

    <!-- Not useful at this data volume; each one is a table that grows forever. -->
    <trace_log remove="1"/>
    <query_thread_log remove="1"/>
    <query_views_log remove="1"/>
    <metric_log remove="1"/>
    <asynchronous_metric_log remove="1"/>
    <session_log remove="1"/>
    <text_log remove="1"/>

    <max_server_memory_usage_to_ram_ratio>0.5</max_server_memory_usage_to_ram_ratio>
    <mark_cache_size>536870912</mark_cache_size>
</clickhouse>
```

### 4.2 `/etc/clickhouse-flashproxy/users.xml`

Because `user_directories` points at this file, **only the users defined here exist** —
there is no packaged `default` user to disable. Define exactly one admin, loopback-only.

Generate a 32+ character password and its hash:

```bash
ADMIN_PW="$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40)"
echo "$ADMIN_PW"                          # store in your password manager NOW
printf '%s' "$ADMIN_PW" | sha256sum       # take the hex, no trailing dash
```

```xml
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage>4000000000</max_memory_usage>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>

    <quotas>
        <default><interval><duration>3600</duration></interval></default>
    </quotas>

    <users>
        <sla_admin>
            <password_sha256_hex>__PASTE_THE_SHA256_HEX__</password_sha256_hex>
            <access_management>1</access_management>
            <!-- Loopback only: the admin must never be reachable through the tunnel. -->
            <networks>
                <ip>127.0.0.1</ip>
                <ip>::1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </sla_admin>
    </users>
</clickhouse>
```

```bash
chown -R clickhouse:clickhouse /etc/clickhouse-flashproxy
chmod 640 /etc/clickhouse-flashproxy/users.xml
```

### 4.3 systemd unit

`/etc/systemd/system/clickhouse-flashproxy.service`:

```ini
[Unit]
Description=ClickHouse (flashproxy-status)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=clickhouse
Group=clickhouse
ExecStart=/usr/bin/clickhouse-server --config-file=/etc/clickhouse-flashproxy/config.xml
Restart=always
RestartSec=5
LimitNOFILE=500000
LimitCORE=0

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now clickhouse-flashproxy
systemctl status clickhouse-flashproxy --no-pager
```

Smoke test over loopback:

```bash
curl -sS "http://127.0.0.1:${HTTP_PORT}/" \
  -H "X-ClickHouse-User: sla_admin" -H "X-ClickHouse-Key: ${ADMIN_PW}" \
  -d 'SELECT version(), timezone()'
```

`timezone()` **must** return `UTC`. If it does not, fix `<timezone>` and restart before
loading any data — every window comparison on the status page depends on it.

### 4.4 Why the retention block matters

The status page went dark on **2026-08-14** because this box ran out of disk. All three
workers were healthy; every insert was rejected with:

```
Code: 243. DB::Exception: Cannot reserve 1.81 MiB, not enough space:
While executing WaitForAsyncInsert. (NOT_ENOUGH_SPACE)
```

Actual probe data was only ~2 months old and nowhere near the 400-day TTL. On a box
this size the space goes to **`system.*` log tables**, which have no TTL by default and
grow with query volume rather than data volume — and the workers query constantly. The
`<ttl>` and `remove="1"` entries above are the fix; `keep_free_space_bytes` is the
backstop that keeps a full disk from taking out co-located services too.

---

## 5. Load the schema

From a checkout of this repo, on the ClickHouse host:

```bash
clickhouse-client --host 127.0.0.1 --port "$NATIVE_PORT" \
  --user sla_admin --password "$ADMIN_PW" \
  --multiquery < schema/clickhouse.sql
```

No `clickhouse-client`? The HTTP interface runs one statement per request, so split on
`;` — that is exactly what `bootstrap-roles.sh` does, and you can reuse its loop.

This creates `sla.probe_raw`, `sla.events`, `sla.incident_statements`, the append-only
`sla.ledger` + `sla.ledger_checkpoints`, and the `probe_1m` rollup with its materialized
view. All UTC, 400-day TTL except the ledger (deliberately unbounded — it is tiny, and
chain integrity must stay verifiable after raw rows expire).

Verify:

```bash
clickhouse-client --host 127.0.0.1 --port "$NATIVE_PORT" --user sla_admin --password "$ADMIN_PW" \
  --query "SELECT name FROM system.tables WHERE database='sla' ORDER BY name"
```

Expect: `events`, `incident_statements`, `ledger`, `ledger_checkpoints`, `probe_1m`,
`probe_1m_mv`, `probe_raw`.

### Migrations

`schema/clickhouse.sql` is current, so a **fresh** install needs no migrations. They
exist for upgrading a database created by an older version:

| Migration | Adds | Notes |
|---|---|---|
| `001-add-integrity-ledger.sql` | ledger tables, `stream`/`seq`, UTC/TTL | Additive, online |
| `002-add-incident-statements.sql` | `sla.incident_statements` + narrow writer | Additive, online |
| `003-events-dedup-window.sql` | insert dedup window on `sla.events` | Setting only, online; run before deploying the sealed-batch worker |

**Always migrate before deploying new binaries.** A current worker reads `sla.ledger` and
writes `stream`/`seq` at startup, so it crash-loops against an un-migrated schema. Old
binaries are forward-compatible with the migrated schema, so migrating first is always
the safe order.

---

## 6. Create roles and users

Never type these passwords on a command line — `bootstrap-roles.sh` reads them from the
environment, disables history, and sends one statement per request:

```bash
export CH_ADMIN_URL="http://127.0.0.1:${HTTP_PORT}"
export CH_ADMIN_USER=sla_admin
export CH_ADMIN_PASS="$ADMIN_PW"

export SLA_WORKER_PASSWORD="$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40)"
export SLA_WEBSITE_PASSWORD="$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40)"
export SLA_STATEMENTS_PASSWORD="$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40)"
# The public one is MEANT to be published — use a readable token, not a random string.
export SLA_PUBLIC_PASSWORD="flashproxy-public-ro"

./deploy/bootstrap-roles.sh
```

Record worker/website/statements passwords in your password manager immediately — they
are only recoverable by resetting the user. In this project they belong in
`deploy/terraform/terraform.tfvars`, which lives in S3 (see the Deploy section of
`CLAUDE.md`).

---

## 7. Expose it through Cloudflare

The tunnel is the only thing that makes ClickHouse reachable: no inbound ports, TLS
terminated by Cloudflare, origin IP hidden.

```bash
# install cloudflared per Cloudflare's docs, then:
cloudflared tunnel login
cloudflared tunnel create flashproxy-ch          # note the tunnel UUID
cloudflared tunnel route dns flashproxy-ch ch.flashproxy.com
```

`/etc/cloudflared/config.yml` — **hostname-scoped**, with a catch-all that refuses
everything else. A bare `- service: http://...` catch-all will happily serve your
database to any hostname pointed at the tunnel:

```yaml
tunnel: <TUNNEL_UUID>
credentials-file: /root/.cloudflared/<TUNNEL_UUID>.json

ingress:
  - hostname: ch.flashproxy.com
    service: http://127.0.0.1:__HTTP_PORT__
  - service: http_status:404
```

```bash
cloudflared service install
systemctl enable --now cloudflared
systemctl status cloudflared --no-pager
```

---

## 8. Verification — do this before announcing the endpoint

Run from **another machine**. Every item is security-relevant; a failure here means
stop and fix, not proceed.

```bash
CH=https://ch.flashproxy.com
PUB='-u flashproxy-status-public:flashproxy-public-ro'

# 1. reachable
curl -sS $PUB "$CH/?query=SELECT%201"                       # -> 1

# 2. public user CANNOT write
curl -sS $PUB "$CH/" --data-binary "INSERT INTO sla.events (type,message) VALUES ('x','x')"
#    -> must FAIL (readonly)

# 3. public user CANNOT read system tables
curl -sS $PUB "$CH/?query=SELECT%20count()%20FROM%20system.query_log"   # -> must FAIL
curl -sS $PUB "$CH/?query=SELECT%20*%20FROM%20system.users"             # -> must FAIL

# 4. admin is NOT reachable through the tunnel
curl -sS -u "sla_admin:$ADMIN_PW" "$CH/?query=SELECT%201"    # -> must FAIL

# 5. worker user CAN write
curl -sS -u "flashproxy-status-worker:$SLA_WORKER_PASSWORD" "$CH/" \
  --data-binary "INSERT INTO sla.events (type,message) VALUES ('deploy','ch bootstrap')"

# 6. ledger present, server is UTC
curl -sS $PUB "$CH/?query=SELECT%20count()%20FROM%20sla.ledger"
curl -sS $PUB "$CH/?query=SELECT%20timezone()"               # -> UTC
```

If #3 succeeds, `select_from_system_db_requires_grant` is not in effect — do not expose
the tunnel until it is.

---

## 9. Point the fleet at it

Workers and the website take `clickhouse: {url, db, user, password}` in their JSON
config, with `${ENV}` expansion so passwords never sit in the file. In this deployment
those configs are rendered by cloud-init from
`deploy/terraform/modules/node/user-data.sh.tftpl`, fed by `ch_url`,
`ch_worker_password`, and `ch_website_password` in `terraform.tfvars`.

Because that template runs **only on first boot**, changing the CH endpoint on a running
fleet needs both an edit to the template (so rebuilds keep it) and a hot-patch on each
box:

```bash
./tf.sh ssh ashburn     # then, per box:
sudo vi /opt/flashproxy/config/worker.json
sudo systemctl restart flashproxy-worker
```

Confirm data is landing:

```sql
SELECT vantage, count(), max(ts) FROM sla.probe_raw
WHERE ts > now() - INTERVAL 10 MINUTE GROUP BY vantage;
```

---

## 10. Day-2 operations

### Watch the disk — the failure this project actually hits

```bash
cat >/usr/local/bin/ch-disk-check <<'EOF'
#!/usr/bin/env bash
# Alert when the ClickHouse filesystem crosses 80% used.
set -euo pipefail
THRESH=80
WEBHOOK="${DISCORD_WEBHOOK:-}"
use=$(df --output=pcent /var/lib/clickhouse-flashproxy | tail -1 | tr -dc '0-9')
[ "$use" -lt "$THRESH" ] && exit 0
msg="ClickHouse disk ${use}% used on $(hostname) - inserts start failing at 100%"
logger -t ch-disk-check "$msg"
if [ -n "$WEBHOOK" ]; then
  payload=$(python3 -c 'import json,sys; print(json.dumps({"content": sys.argv[1]}))' "$msg")
  curl -sS -H 'Content-Type: application/json' --data-binary "$payload" "$WEBHOOK" >/dev/null || true
fi
EOF
chmod +x /usr/local/bin/ch-disk-check
echo '*/15 * * * * root DISCORD_WEBHOOK=... /usr/local/bin/ch-disk-check' \
  >/etc/cron.d/ch-disk-check
```

This alert must live **outside** the status-page stack. The built-in monitor writes its
events to this same ClickHouse, so when the database is full the alerting path is full
too — on 2026-08-14 the monitor never fired.

### Where the space went

```bash
df -h /var/lib/clickhouse-flashproxy
du -xh --max-depth=1 /var/lib/clickhouse-flashproxy | sort -h | tail
```

```sql
SELECT database, table, formatReadableSize(sum(bytes_on_disk)) AS size, sum(rows)
FROM system.parts WHERE active
GROUP BY database, table ORDER BY sum(bytes_on_disk) DESC LIMIT 20;
```

If `system.*` dominates, the §4.4 retention config is missing or was never applied.
Reclaim immediately — these are logs, not data:

```sql
TRUNCATE TABLE IF EXISTS system.query_log;
TRUNCATE TABLE IF EXISTS system.part_log;
TRUNCATE TABLE IF EXISTS system.trace_log;
TRUNCATE TABLE IF EXISTS system.metric_log;
TRUNCATE TABLE IF EXISTS system.asynchronous_metric_log;
TRUNCATE TABLE IF EXISTS system.text_log;
```

Then add the retention block and restart.

**Recovering from a full disk:** free the space and stop. Workers retain their pending
batch and retry roughly twice a second, so they refill on their own within a minute.
**Do not restart the workers** — that discards the retained batch. **Do not truncate
`sla.probe_raw`** to make room; see below.

### Truncating or resetting probe data

Only ever: **stop the workers → `TRUNCATE` → start the workers.** Each worker holds its
chain head (next `seq`, previous `entry_hash`) in memory, so truncating underneath a
running worker leaves the ledger starting mid-chain and `cmd/verify` reports a break. A
restart re-runs `resumeChain`, sees an empty ledger, and re-seeds from seq 1.

### Backups

The ledger makes this data worth keeping — the chain can only be verified against rows
that still exist. Native backup to a local directory:

```xml
<!-- config.xml -->
<backups>
    <allowed_path>/var/backups/clickhouse</allowed_path>
</backups>
```

```sql
BACKUP DATABASE sla TO Disk('backups', 'sla-{timestamp}.zip');
-- restore:
RESTORE DATABASE sla FROM Disk('backups', 'sla-....zip');
```

Ship the resulting archive off-box (e.g. `aws s3 cp … --sse AES256`) on a cron. A backup
that only exists on the box that dies is not a backup.

### Upgrades

```bash
systemctl stop clickhouse-flashproxy
apt-get update && apt-get install -y clickhouse-server clickhouse-client
systemctl start clickhouse-flashproxy
```

Single node, so this is a brief write outage. Workers retain and retry; the website
serves whatever is in its 90-minute window and recovers on its own.

---

## 11. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Code: 243 … NOT_ENOUGH_SPACE` on insert | Disk full — usually `system.*` logs | §10; add the §4.4 retention block |
| `Code: 164 … Cannot modify '<setting>' in readonly mode`, site 502s | Client sent a setting on a read; reader users are `readonly=1` | Send **no** client settings on reads; caps belong on the server-side profile |
| `Code: 516 … Authentication failed` | Wrong password, or the user lives in an access store that was wiped | Check `user_directories.local_directory` persists across restarts |
| Status page shows "Awaiting Data" but returns 200 | No rows in the last 90 min — writes are failing upstream | Check worker logs; the page is fine, the pipeline is not |
| Worker crash-loops at startup | Schema predates the ledger | Run `migrations/001` **then** restart |
| Timestamps off by hours | Server not UTC | `<timezone>UTC</timezone>`, restart; verify `SELECT timezone()` |
| Public user can read `system.query_log` | `select_from_system_db_requires_grant` not applied | Fix §4.1 and restart **before** exposing the tunnel |
| ClickHouse won't start after reboot, port in use | Port taken as an ephemeral source port | Confirm the reservation in §3 (`sysctl net.ipv4.ip_local_reserved_ports`) |

---

## 12. Quick reference

```bash
systemctl status clickhouse-flashproxy
journalctl -u clickhouse-flashproxy -n 100 --no-pager
tail -f /var/log/clickhouse-flashproxy/server.err.log

clickhouse-client --host 127.0.0.1 --port "$NATIVE_PORT" --user sla_admin --password "$ADMIN_PW"
```

```sql
-- is data landing?
SELECT vantage, count(), max(ts) FROM sla.probe_raw WHERE ts > now() - INTERVAL 10 MINUTE GROUP BY vantage;
-- ledger heads
SELECT stream, max(seq) FROM sla.ledger GROUP BY stream;
-- table sizes
SELECT table, formatReadableSize(sum(bytes_on_disk)) FROM system.parts
WHERE active AND database='sla' GROUP BY table ORDER BY 2 DESC;
```
