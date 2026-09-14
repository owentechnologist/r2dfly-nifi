# Tutorial: Copy all keys from Redis to Dragonfly with NiFi

> **This is now automatic.** `./deploy-to-nifi.sh` imports the exact flow this
> tutorial builds — as `r2dfly.json` — into a fresh container via the NiFi CLI
> (`pg-import`), with the two connection-pool controller services left
> deliberately unconfigured. After running it you only need to set each pool's
> **Connection String** property, enable those two services, and start the
> flow — skip straight to step 4. The walkthrough below is for understanding
> what got built, changing it, or regenerating `r2dfly.json` after a processor
> change (build it here, then `nifi pg-export -pgid <id> -o r2dfly.json`).

This walks through starting the local NiFi test container, wiring up the controller
services the migration processors need, and building a minimal flow that does a
one-time full copy of every key from a source Redis to a target Dragonfly instance.

It only covers the **full-copy scan phase** (`RedisScanReader` → `RedisTypeDeserializer`
→ `RedisBatchWriter`, plus `RedisScanReader`'s `unknown` relationship → `ModuleTypeHandler`
for ReJSON-RL/TopK keys) — not the live keyspace-notification phase described in
[`NIFI_REDIS_MIGRATION_SPEC.md`](../artifacts/NIFI_REDIS_MIGRATION_SPEC.md).

`RedisScanReader`'s **Key Type Filter** property controls which types get scanned at all
(default: `string,hash,list,set,zset,stream,ReJSON-RL,TopK-TYPE`); anything not in that list
is skipped entirely, not routed anywhere. Non-core types you *do* include (currently
`ReJSON-RL` and `TopK-TYPE`) go out the `unknown` relationship instead of to
`RedisTypeDeserializer`, since `RedisTypeDeserializer`/`RedisBatchWriter` only know the six
core Redis types. `ModuleTypeHandler` reads and writes ReJSON-RL keys directly via
`JSON.GET`/`JSON.SET`, and TopK keys via `TOPK.INFO`/`TOPK.LIST WITHCOUNT`/`TOPK.RESERVE`/
`TOPK.ADD` (an approximate reconstruction - membership is preserved, per-item counts are
not, since it replays each surviving member once rather than replaying its recorded count),
bypassing the core pipeline entirely. Other module types NiFi's `TYPE` command reports
(`CMSk-TYPE`, `MBbloom--`, etc.) fall through `ModuleTypeHandler` to its `failure`
relationship by default - only ReJSON-RL and TopK are implemented that way (spec section
8.3). See **Dragonfly-to-Dragonfly Optimizations** below for a way to migrate those too.

If **both** source and target are Dragonfly, `ModuleTypeHandler`'s **Dragonfly-to-Dragonfly
Optimizations** property switches ReJSON-RL, TopK-TYPE, `MBbloom--` (Bloom filter), and
`CMSk-TYPE` (Count-Min Sketch) keys to a `DUMP`/`RESTORE` fast path instead - a byte-for-byte
copy of the key's internal serialization, roughly half the round trips of a type-specific
read-then-rebuild, and (for TopK) exact rather than approximate, since nothing gets replayed
from scratch. ReJSON-RL and TopK-TYPE fall back to their normal reconstruction above for any
individual key where `DUMP`/`RESTORE` itself fails; Bloom and CMS have no other
reconstruction path in this processor, so they're only migrated at all when this is enabled.
Verified directly between two real Dragonfly instances for all four types - **not safe**
against a real Redis source/target, or mismatched Dragonfly versions, since `DUMP` payloads
aren't a portable format.

`RedisScanReader` also has optional **Prefix Deny List**/**Prefix Only List** properties
(comma-separated key prefixes) for narrowing a migration by key name rather than type - a
key is skipped if it starts with any Prefix Deny List entry, or (if Prefix Only List is
non-empty) if it *doesn't* start with any Prefix Only List entry. Deny is checked first.
`run-r2dfly.sh` exposes these as `--prefix-deny-list`/`--prefix-only-list`.

By default, `run-r2dfly.sh` also migrates search indexes, via two more processors already in
the canvas (`SearchIndexExporter`/`SearchIndexRehydrator`, started/stopped by `run-r2dfly.sh`
itself, not part of the main scan/write pipeline): `SearchIndexExporter` runs `FT._LIST`/
`FT.INFO` against the source and caches a normalized definition of each index (in the same
Cursor State Cache `RedisScanReader` uses); `SearchIndexRehydrator` reads that back and
rebuilds each index with `FT.CREATE` on the target, before the main flow even starts (the
index just starts out empty/partially populated and gets kept current by the search module's
own normal indexing as matching documents are written during the migration). Covers
`TEXT`/`NUMERIC`/`TAG`/`GEO`/`VECTOR` fields, including `ON JSON` indexes and `AS` aliases; a
`VECTOR` field is skipped only if `FT.INFO` doesn't report a reconstructable algorithm/
data_type/dim/distance_metric for it, with a warning. Pass `--ignore-search-indexes` to skip
search index migration entirely.

## Prerequisites

- A source Redis (or Valkey/Redis Stack) instance reachable from the NiFi container.
- A target Dragonfly instance reachable from the NiFi container.
- Docker or Podman — the only thing you need on the host. `deploy-to-nifi.sh` builds the NAR
  with a containerized Maven+JDK21 and never needs those installed locally.

This project is laid out as `scripts/` (every `*.sh` file, including `deploy-to-nifi.sh`),
`docs/` (this file), and `artifacts/` (the Maven project, `r2dfly.json`, and everything else
needed to build/run the flow but not run directly). Every command below is run from inside
`scripts/`.

If you don't already have test instances running, the simplest option is two
plaintext containers on the same Docker/Podman network as NiFi:

```bash
docker network create redis-migration-test   # once

docker run -d --name source-redis --network redis-migration-test -p 6380:6379 redis:latest
docker run -d --name target-dragonfly --network redis-migration-test -p 6379:6379 \
  docker.dragonflydb.io/dragonflydb/dragonfly
```

(Swap `docker` for `podman` if that's your runtime.) Load a few keys into the source
so you have something to migrate:

```bash
docker exec source-redis redis-cli MSET foo bar baz qux
docker exec source-redis redis-cli HSET user:1 name Ada role admin
```

## 1. Start NiFi and deploy the NAR

From the `scripts/` directory:

```bash
./deploy-to-nifi.sh
```

This builds `nifi-redis-migration-nar`, starts (or reuses) a `nifi-redis-migration`
container, and drops the NAR into NiFi's auto-load directory. When it finishes it
prints the NiFi URL and, on first run only, a generated single-user login:

```
==> NiFi is up: https://localhost:8443/nifi
==> single-user login credentials (only shown on first container creation):
Generated Username [xxxxxxxx-xxxx-...]
Generated Password [xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx]
```

Copy those down — NiFi prints them only once. `deploy-to-nifi.sh` also saves them
inside the container, at `/tmp/r2dfly-nifi-credentials.env`, so `simple-migration.sh`,
`simple-troubleshoot.sh`, and `reset-r2dfly-flow.sh` recover them automatically on
every later run, even once the log line above is gone for good.

If you're locked out anyway — this container predates that feature, or its saved
copy was somehow lost too — run `./update_nifi_credentials.sh` from `scripts/`. It
resets the login to a new value, restarts the container, and saves the new
credentials in that same place, so the other scripts pick them up automatically.
Don't run `nifi.sh set-single-user-credentials` by hand instead: it changes the
login but never updates the saved copy, so every other script here stays locked
out.

> **Redeploying after a code change:** NiFi's NAR auto-loader only loads a NAR
> whose `groupId:artifactId:version` it hasn't seen before. This project pins
> `1.0.0-SNAPSHOT`, so re-running `./deploy-to-nifi.sh` after editing processor
> code copies the new NAR in but NiFi silently keeps running the old one. Force
> a reload with `<docker|podman> restart nifi-redis-migration` after any
> rebuild where the version didn't change.

Open `https://localhost:8443/nifi/` in a browser (the trailing slash matters —
NiFi redirects to it anyway, but going there directly avoids an extra hop;
`https://127.0.0.1:8443/nifi/` also works). The browser will likely say the
connection **"isn't private"** or **"isn't secure"** — that's expected, since
NiFi is serving a self-signed certificate. Click through it (e.g. "Advanced" →
"Proceed to localhost (unsafe)" in Chrome, or "Advanced" → "Accept the Risk and
Continue" in Firefox) to reach the login page, then log in with those
credentials.

> **Connecting from a different machine than the one running NiFi** (e.g. you SSH'd into a
> remote migration host and are browsing from your own laptop): `https://localhost:8443/nifi/`
> won't work, and pointing your browser at the host's own hostname/IP directly (e.g.
> `https://ip-172-31-36-179:8443/nifi/`) fails differently, with an **"Invalid SNI"** error.
> That's not a Host-header/proxy check (`NIFI_WEB_PROXY_HOST`, already set for you) - it's
> Jetty validating the TLS SNI name against the self-signed certificate's own SAN list, which
> only contains `localhost`, `127.0.0.1`, and the **NiFi container's own internal hostname**
> (a short id like `80334a51b1b2`, auto-generated by the container runtime - run
> `<docker|podman> exec nifi-redis-migration hostname` to see it). The fix is to make your
> *own* machine resolve that internal hostname to the remote host's IP, not to change anything
> on the NiFi side: add a line to your local `/etc/hosts`
> (`C:\Windows\System32\drivers\etc\hosts` on Windows) mapping that hostname to the remote
> host's IP address, e.g.:
> ```
> 54.158.214.127  80334a51b1b2
> ```
> then browse to `https://80334a51b1b2:8443/nifi/` - the SNI now matches a name the
> certificate actually covers. `simple-migration.sh` prints this exact hostname/URL, along
> with your login, right after starting NiFi - and pauses there until you press Enter, so
> you have time to open the browser and log in before the rest of the script runs.

**If you started your Redis/Dragonfly containers on a Docker network** (as above),
also attach the NiFi container to it so it can resolve their hostnames:

```bash
docker network connect redis-migration-test nifi-redis-migration
```

You can then reach them from inside NiFi as `source-redis:6379` and
`target-dragonfly:6379`. If instead you're pointing at services on your host
machine, use `host.docker.internal` (Docker) or the host's LAN IP (Podman) rather
than `localhost`, since `localhost` inside the NiFi container refers to the
container itself.

> `simple-migration.sh` does this same check for you automatically, for both the source and
> target connection strings, by testing the actual connection from inside the NiFi container
> rather than guessing. It tries `localhost`/`127.0.0.1` first, then the runtime's own gateway
> alias (`host.containers.internal` for Podman, `host.docker.internal` for Docker), then your
> own machine's real IP address - that last fallback matters on a rootless Podman host using
> the default `slirp4netns` network, where the gateway alias can resolve in the container's
> `/etc/hosts` but still fail to connect. Building the flow by hand, as in this tutorial, still
> needs the manual check above.

## 2. Add the controller services

Controller services are configured at the canvas level, not per-processor. Right
click an empty area of the canvas → **Configure** → **Controller Services** tab →
**+** to add each of the following.

### 2a. Redis Connection Pool (source)

Type to add: **`StandardRedisConnectionPoolService`**
(`io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService`)

| Property | Set to |
|---|---|
| Connection Mode | `STANDALONE` |
| Connection String | `redis://source-redis:6379` |
| Require TLS | **`false`** |
| Connection Timeout (ms) | `5000` (default) |
| Command Timeout (ms) | `10000` (default) |
| Max Pool Size | `8` (default) |
| Database Index | `0` (default) |

> **Require TLS defaults to `true`**, and the service will fail validation unless
> you either set it to `false` or also configure an **SSL Context Service**. For a
> plaintext local test container, set it to `false` — leave **SSL Context Service**
> empty in that case.

Name it something like `Source Redis Pool`.

### 2b. Dragonfly Connection Pool (target)

Same steps, but this is a **separate controller service type** (kept distinct
from the Redis one so source/target credentials stay independent), even though
the properties are identical:

Type to add: **`StandardDragonflyConnectionPoolService`**
(`io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService`)

| Property | Set to |
|---|---|
| Connection Mode | `STANDALONE` |
| Connection String | `redis://target-dragonfly:6379` |
| Require TLS | `false` |
| (rest) | defaults |

Name it `Target Dragonfly Pool`.

### 2c. Cursor State Cache

`RedisScanReader` checkpoints its SCAN cursor and partition claims through a
NiFi `DistributedMapCacheClient`, so it needs one enabled even for a single-node,
single-partition test run. The simplest option is NiFi's built-in in-process
cache server/client pair — add **both** of these:

Type to add: **`MapCacheServer`** (`org.apache.nifi.distributed.cache.server.map.MapCacheServer`)

| Property | Set to |
|---|---|
| Port | `4557` (default) |
| Maximum Cache Entries | `10000` (default) |
| (rest) | defaults |

Type to add: **`MapCacheClientService`** (`org.apache.nifi.distributed.cache.client.MapCacheClientService`)

| Property | Set to |
|---|---|
| Server Hostname | `localhost` |
| Server Port | `4557` (must match the server's Port) |
| Communications Timeout | `30 secs` (default) |

Name the client `Cursor State Cache`.

### 2d. Enable them all

Back in the Controller Services list, click the lightning-bolt icon and enable,
in this order: `MapCacheServer` → `MapCacheClientService` → the two connection
pools. (The server must be enabled before the client that connects to it.)

## 3. Build the flow

Drag four processors onto the canvas from the Add Processor dialog (the search
box matches on class name, e.g. type `RedisScanReader`):

1. **RedisScanReader**
2. **RedisTypeDeserializer**
3. **RedisBatchWriter**
4. **ModuleTypeHandler** (reads/writes ReJSON-RL keys directly - see the intro note above)

Connect them: `RedisScanReader` → `RedisTypeDeserializer` → `RedisBatchWriter`.
When you draw the first connection, NiFi asks which relationships to include —
for a first pass, select the six core-type relationships (`string`, `hash`,
`list`, `set`, `zset`, `stream`) so every core type flows into the deserializer.
For `RedisTypeDeserializer` → `RedisBatchWriter`, connect its `success`
relationship. Separately, connect `RedisScanReader`'s `unknown` relationship to
**ModuleTypeHandler** - its `Source Connection Pool`/`Target Connection Pool`
properties reuse the same two connection pool controller services from step 2;
auto-terminate its `success` and `failure` relationships for a minimal flow.

> **Every relationship must be connected or auto-terminated.** NiFi won't let a
> processor validate — and won't let you start it — if any of its relationships
> is left with neither a downstream connection nor auto-terminate checked. For
> this minimal flow, auto-terminate every relationship you're *not* wiring
> above: on `RedisScanReader`, just `failure` (`unknown` is wired to
> `ModuleTypeHandler`); on `ModuleTypeHandler`, `success` and `failure`; on
> `RedisTypeDeserializer`, `key_missing`, `module_type`, and `failure`. Each
> processor's **Relationships** tab (double-click the processor → Relationships)
> has an auto-terminate checkbox per relationship. See "Handling failures"
> below for the alternative — routing these somewhere durable instead of
> discarding them.

### 3a. Configure RedisScanReader

| Property | Set to |
|---|---|
| Redis Connection Pool | `Source Redis Pool` |
| Migration ID | any string, e.g. `test-run-1` |
| Cursor State Cache | `Cursor State Cache` |
| Partition Count | `1` |
| Scan Count | `200` (default) |
| Key Pattern | `*` (default) |
| Key Type Filter | `string,hash,list,set,zset,stream` (default) |
| Prefix Deny List | empty (default - no prefix excluded) |
| Prefix Only List | empty (default - all prefixes included) |
| Emit TTL | `true` (default) |

Leave **Concurrent Tasks** (Scheduling tab) at `1` — it must equal Partition Count. See
"Tuning parallelism and container resources" below for when and how to raise both together.

### 3b. Configure RedisTypeDeserializer

| Property | Set to |
|---|---|
| Redis Connection Pool | `Source Redis Pool` |
| (rest) | defaults are fine for a small test dataset |

### 3c. Configure RedisBatchWriter

| Property | Set to |
|---|---|
| Dragonfly Connection Pool | `Target Dragonfly Pool` |
| Batch Size | `50` (default; fine for small tests) |
| Batch Timeout (ms) | `1000` (default) |
| TTL Strategy | `PRESERVE` (default) |
| Key Prefix Separator | `:` (default) |
| Conflict Strategy | `OVERWRITE` (default — replaces existing target keys) |
| Write Confirmation | `true` (default — don't disable this) |

`RedisBatchWriter` has no downstream processor in this minimal flow, so open its
**Relationships** tab and check **auto-terminate** for all four relationships —
`success`, `skipped`, `failure`, and `retry` — or it won't validate (see the
note in step 3 above; "Handling failures" below covers routing these somewhere
durable instead).

`run-r2dfly.sh` exposes RedisBatchWriter's batch properties as
`--batch-size`/`--batch-timeout-ms` — raise these for a remote target where a real
network round trip per pipeline flush (rather than a near-zero-latency local one) leaves
throughput on the table. `--batch-size` is the default for any core type
(string/hash/list/set/zset/stream) without its own override — `--batch-size-string`,
`--batch-size-hash`, `--batch-size-list`, `--batch-size-set`, `--batch-size-zset`,
`--batch-size-stream` each independently override it for that one type, e.g. to shrink batches
for a type with unusually large values without also shrinking every other type's.

### 3d. Configure ModuleTypeHandler

| Property | Set to |
|---|---|
| Source Connection Pool | `Source Redis Pool` |
| Target Connection Pool | `Target Dragonfly Pool` |
| Batch Size | `50` (default; fine for small tests) |
| Batch Timeout (ms) | `10000` (default — higher than RedisBatchWriter's, since each ReJSON-RL/TopK item needs its own `JSON.GET`/`TOPK.INFO` round trip before the write half of the batch) |
| Dragonfly-to-Dragonfly Optimizations | `false` (default) — set `true` only when both source and target are Dragonfly |

`run-r2dfly.sh` exposes these as `--module-batch-size`/`--module-batch-timeout-ms`/`--dfly-to-dfly`.
Like RedisBatchWriter, `--module-batch-size` is the default for any category
(json/topk/bloom-cms) without its own override — `--batch-size-json`, `--batch-size-topk`,
`--batch-size-bloom-cms` each independently override it for that one category.

## 4. Run it

Select all four processors (drag a box around them) and click the **▶ Start**
button in the Operate palette. `RedisScanReader` runs once per invocation of its
`onTrigger` — with default scheduling it fires continuously, so it will drain
the whole source keyspace in a few seconds for a small test dataset and then sit
idle (each subsequent trigger finds its partition already marked complete and
yields).

Watch the connection queues between processors — the counts climb as
`RedisScanReader` emits FlowFiles, then drain as `RedisTypeDeserializer` and
`RedisBatchWriter` process them. When both queues settle at 0, the copy is done.

Verify on the target (swap `docker` for `podman` if that's your runtime):

```bash
docker exec target-dragonfly redis-cli KEYS '*'
docker exec target-dragonfly redis-cli HGETALL user:1
```

## 5. Re-running against the same source

`RedisScanReader` tracks partition-claim and cursor state in the Cursor State
Cache, keyed by **Migration ID**. Running the flow again with the *same*
Migration ID will find the partition already marked complete and do nothing.
To re-scan, either stop/restart `MapCacheServer` (clears its in-memory state) or
change **Migration ID** to a new value.

## 6. Tuning parallelism and container resources

The flow above runs single-threaded (`Concurrent Tasks: 1` everywhere, `Partition Count: 1`)
with no limit on the NiFi container's CPU/memory. That's fine for the handful of test keys
used above, but leaves throughput on the table for a real migration. Two independent knobs,
plus the container's own resources:

### Scan/read parallelism (`RedisScanReader` / `RedisTypeDeserializer` / `ModuleTypeHandler`)

Raise this when the source keyspace is large enough that a single-threaded scan is the
bottleneck (watch the queue *before* `RedisTypeDeserializer`/`ModuleTypeHandler` growing while
`RedisBatchWriter`'s stays shallow - that's the scan side falling behind, not the write side).

1. Double-click **RedisScanReader** → **Properties** tab → set **Partition Count** to `N`.
2. Same processor → **Scheduling** tab → set **Concurrent Tasks** to the *same* `N`. NiFi
   validates these must match - each of the `N` concurrent tasks claims one partition (via a
   hash-slot range assignment, since Redis `SCAN` has no native range-partitioning; see
   `NIFI_REDIS_MIGRATION_SPEC.md` section 4.1.1) and every partition needs a task running it.
3. Double-click **RedisTypeDeserializer** → **Scheduling** tab → set **Concurrent Tasks** to
   the same `N` (it's downstream of the same partitioned queue, so it needs matching
   throughput to avoid becoming the new bottleneck).
4. Double-click **ModuleTypeHandler** → **Scheduling** tab → same `N`, for the same reason on
   the `unknown`-relationship side (ReJSON-RL/TopK keys).

Keep `N` at or below the NiFi container's CPU count (see below) and the source Redis's own
headroom - each task issues its own `SCAN` calls, so raising `N` past what the source can
serve just adds contention without more real throughput. `NIFI_REDIS_MIGRATION_SPEC.md`
section 7.1 has a sizing formula if you want to compute a target `N` from key count and CPU
cores rather than guessing.

### Write parallelism (`RedisBatchWriter`)

Independent of scan parallelism - raise this when the *write* side is the bottleneck instead
(the queue feeding `RedisBatchWriter` stays deep while its own processing keeps up, or a
remote/slower target can't keep pace with a single writer's pipeline). Double-click
**RedisBatchWriter** → **Scheduling** tab → set **Concurrent Tasks** to `W`. There's no
Partition Count to keep in sync here - each writer task just pulls whatever's queued. Don't
raise `W` past the target Dragonfly's core count (section 7.2) - Dragonfly's thread-per-core
design already spreads writes across its own shards by key hash, so more NiFi writers beyond
that just adds contention on the target.

### Container CPU/memory

Both knobs above only help if the NiFi container itself has the CPU/memory to run that many
concurrent tasks. Set limits when creating the container (`docker`/`podman run --cpus`/
`--memory`, or the `NIFI_CPUS`/`NIFI_MEMORY` env vars `deploy-to-nifi.sh` reads) - these are
creation-time only, so changing them means removing the existing container (or using a
different name) and letting it get recreated. A memory limit also grows NiFi's own JVM heap
to roughly 75% of it (via the `NIFI_JVM_HEAP_MAX` env var the `apache/nifi` image reads) so
the extra RAM is actually usable - override `NIFI_JVM_HEAP_MAX` yourself if 75% isn't right
for your case.

### The easy way

All of the above is what `run-r2dfly.sh --parallelism N --writer-concurrency N` and
`simple-migration.sh`'s interactive resource/parallelism prompt (or its `--cpus`/`--memory`/
`--parallelism`/`--writer-concurrency` flags) do for you - they set Partition Count and
Concurrent Tasks together on all three scan-side processors, set RedisBatchWriter's Concurrent
Tasks independently, and pass the container resource settings through to `deploy-to-nifi.sh`.
Building the flow by hand as in this tutorial is for understanding what those flags actually
change, or for tuning a flow you built manually rather than imported from `r2dfly.json`.

## Handling failures (optional, but recommended beyond a quick test)

The spec's design principle is "no fire-and-forget" — every relationship should
land somewhere durable rather than being left unconnected (an unconnected
relationship with data flowing to it will build up and eventually apply
back-pressure, which is NiFi's way of forcing you to deal with it). For a real
test, connect:

- `RedisScanReader`'s `failure` relationship
- `ModuleTypeHandler`'s `failure` relationship (module types it doesn't implement,
  e.g. `MBbloom--`/`CMSk-TYPE` without Dragonfly-to-Dragonfly Optimizations enabled, or a
  reconstruction that errored)
- `RedisTypeDeserializer`'s `key_missing`, `module_type`, and `failure`
- `RedisBatchWriter`'s `skipped`, `failure`, and `retry` (loop `retry` back to
  `RedisBatchWriter`'s own input)

to a `LogAttribute` processor (or a `PutFile` dead-letter sink) so nothing is
silently dropped.

## Troubleshooting

- **A controller service won't enable / shows invalid**: click its warning icon —
  it'll usually say either "Require TLS is true but no SSL Context Service is
  configured" (set Require TLS to `false` for plaintext test instances) or point
  at a required property you haven't set yet.
- **RedisScanReader emits nothing**: check `Migration ID` — if you're re-running
  after a prior successful pass with the same ID, the partition is already
  marked complete. Use a new Migration ID or restart `MapCacheServer`.
- **Connection refused from inside NiFi**: `localhost` inside the NiFi container
  means the container itself, not your host machine or other containers. Put
  NiFi on the same Docker/Podman network as your Redis/Dragonfly containers and
  use their container names as hostnames (see step 1).
- **`simple-migration.sh` can't connect to the source or target**: it retries a few times
  with backoff before giving up, and prints the real `redis-cli` error on the final failure -
  read that line, it usually names the actual problem (wrong password, unreachable host, wrong
  port) directly, rather than just "could not connect".
- **Locked out of NiFi's login**: see "Copy those down" in step 1 - `simple-migration.sh` and
  the other scripts here recover saved credentials automatically, and `./update_nifi_credentials.sh`
  resets them if even that saved copy is gone.
