# Tutorial: Copy all keys from Redis to Dragonfly with NiFi

> **You do not need to build this by hand.** `./deploy-to-nifi.sh` imports a ready-made
> flow, `artifacts/r2dfly.json`, into the container with the NiFi CLI (`pg-import`). The
> imported flow holds everything this tutorial builds, plus the search-index processors,
> a `RedisDeadLetterWriter` that records every failed key, and a few more connections (see
> "Handling failures" below). Its two connection pools have no **Connection String**. Set
> it on each pool, enable those two services, and start the flow: skip to step 4.
> `simple-migration.sh` and `run-r2dfly.sh` do all of that for you (see
> [`quickstart.md`](quickstart.md)). Read the walkthrough to understand what the flow does,
> to change it, or to regenerate `r2dfly.json` after a processor change (build it here, then
> `nifi pg-export -pgid <id> -o r2dfly.json`).

This tutorial starts the local NiFi test container, adds the controller services the
migration processors need, and builds a minimal flow that copies every key once from a
source Redis to a target Dragonfly instance.

It covers only the **one-time scan**: `RedisScanReader` → `RedisTypeDeserializer` →
`RedisBatchWriter`, plus `RedisScanReader`'s `unknown` relationship → `ModuleTypeHandler`
for module keys. For other ways to run a migration, see [`quickstart.md`](quickstart.md):

- `continuous-migration.sh` adds a Live Phase that keeps applying source changes. It follows
  keyspace notifications by default, or the replication stream (PSYNC) with
  `--live-source replication`. `MigrationReconciler` repairs drift, and `cutover.sh` tells
  you when it is safe to switch.
- `headless-migration.sh` runs a one-time copy on NiFi's Stateless engine, with no NiFi
  server or UI.
- `simple-migration.sh` reads from RDB/AOF files and writes JSON-lines or RESP files
  (`--source-rdb-file`, `--source-aof`, `--source-file`, `--target-file`).
- `import-shake.sh` translates a RedisShake config into an r2dfly one.

`RedisScanReader`'s **Key Type Filter** property sets which types it scans. The
processor's default is `string,hash,list,set,zset,stream`. It skips a key of any other type
and counts it in the `Scan Keys Skipped by Type Filter (<type>)` counter; the key is not
routed anywhere. `run-r2dfly.sh` and `simple-migration.sh` set a wider filter by default:
every core type and every module type, so each key is copied or dead-lettered with a reason
(see `--key-types` in `./run-r2dfly.sh --help`).

Module types such as `ReJSON-RL` and `TopK-TYPE` go out the `unknown` relationship to
`ModuleTypeHandler`, because `RedisTypeDeserializer` and `RedisBatchWriter` handle only the
six core types. `ModuleTypeHandler` writes module keys to the target itself:

- **ReJSON-RL**: read with `JSON.GET`, written with `JSON.SET`.
- **TopK**: read with `TOPK.INFO` and `TOPK.LIST WITHCOUNT`, rebuilt with `TOPK.RESERVE` and
  `TOPK.INCRBY` with each item's recorded count. Dragonfly's TopK can still count a colliding
  item differently from RedisBloom's; see [`verified-behavior.md`](verified-behavior.md).
- **Bloom (`MBbloom--`), Cuckoo (`MBbloomCF`), Count-Min Sketch (`CMSk-TYPE`) and unknown
  module types**: copied with `DUMP`/`RESTORE`. This works between Dragonfly instances.
  Dragonfly rejects these payloads from Redis, so those keys are not copied.
- **Time series, T-Digest, graph and vector set keys**: not copied. Dragonfly does not support
  them.

A key that is not copied goes to the `unsupported` relationship with a
`redis.incompatible.reason` attribute. While `unsupported` has no connection, these keys go
to `failure` instead.

If **both** source and target are Dragonfly, set **Dragonfly-to-Dragonfly Optimizations** to
`true` on `ModuleTypeHandler` (and on `RedisTypeDeserializer`). `ModuleTypeHandler` then copies
ReJSON-RL and TopK keys with `DUMP`/`RESTORE` too: a byte-for-byte copy with about half the
round trips. If `DUMP` or `RESTORE` fails for a key, it falls back to the rebuild above.
`DUMP` payloads are not a portable format, so this is **not safe** with a Redis source or
target, or between Dragonfly versions that differ.

`RedisScanReader` also has optional **Prefix Deny List**/**Prefix Only List** properties
(comma-separated key prefixes) for narrowing a migration by key name rather than type - a
key is skipped if it starts with any Prefix Deny List entry, or (if Prefix Only List is
non-empty) if it *doesn't* start with any Prefix Only List entry. Deny is checked first.
`run-r2dfly.sh` exposes these as `--prefix-deny-list`/`--prefix-only-list`.

By default, `run-r2dfly.sh` also migrates search indexes. It runs two processors from the
imported flow once each, before it starts the main flow. They are not connected to the
scan/write pipeline:

1. `SearchIndexExporter` runs `FT._LIST` and `FT.INFO` on the source and saves each index's
   definition in the Cursor State Cache.
2. `SearchIndexRehydrator` reads those definitions and creates each index on the target with
   `FT.CREATE`. The index starts empty and the target's search module fills it as the
   migration writes matching keys.

It handles `TEXT`, `NUMERIC`, `TAG`, `GEO` and `VECTOR` fields, including `ON JSON` indexes
and `AS` aliases. It skips a `VECTOR` field, with a warning, when `FT.INFO` does not report
its algorithm, data type, dimension and distance metric. Pass `--ignore-search-indexes` to
skip search indexes. An index the target already has is kept unless you pass
`--index-overwrite true`, which drops it (`FT.DROPINDEX`) and creates it again from the
source's definition. Either way the conflict is logged as an error, because the two
definitions may differ.

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
container, and copies the NAR into NiFi's auto-load directory (`nar_extensions/`). It also
turns off NiFi's provenance recording (pass `--no-disable-provenance` to keep it; see
`quickstart.md`) and imports `r2dfly.json` if the container has no "R2Dfly Migration"
process group yet. To build a flow by hand as below, work beside that group or delete it
first with `./reset-r2dfly-flow.sh`. When it finishes, it prints the NiFi URL and, on first
run only, a generated single-user login:

```
==> NiFi is up: https://localhost:8443/nifi
==> single-user login credentials (only shown on first container creation):
Generated Username [xxxxxxxx-xxxx-...]
Generated Password [xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx]
```

Copy those down — NiFi prints them only once. `deploy-to-nifi.sh` also saves them
inside the container, at `/tmp/r2dfly-nifi-credentials.env`. `simple-migration.sh`,
`continuous-migration.sh`, `simple-troubleshoot.sh`, `diagnose-r2dfly.sh`,
`reset-r2dfly-flow.sh` and `cutover.sh` read them from there on every later run, even after
the log lines are gone. `run-r2dfly.sh` does not: pass it `--nifi-user`/`--nifi-password`
or `--nifi-token`.

If you're locked out anyway — this container predates that feature, or its saved
copy was somehow lost too — run `./update_nifi_credentials.sh` from `scripts/`. It
resets the login to a new value, restarts the container, and saves the new
credentials in that same place, so the other scripts pick them up automatically.
Don't run `nifi.sh set-single-user-credentials` by hand instead: it changes the
login but never updates the saved copy, so every other script here stays locked
out.

> **Redeploying after a code change:** NiFi's NAR auto-loader only loads a NAR
> whose `groupId:artifactId:version` it hasn't seen before. This project pins
> `1.0.0-SNAPSHOT`, so a rebuilt NAR always has the same coordinate as the one
> already running. `deploy-to-nifi.sh` handles this for you: it hashes the
> built NAR, and if it's different from the one already deployed to a
> container that's already running, it copies the new NAR in and restarts the
> container automatically so NiFi actually reloads it. You don't need to
> restart it by hand.

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
> `RedisTypeDeserializer`, `key_missing`, `module_type`, and `failure`.
> `ModuleTypeHandler`'s `unsupported` and `RedisBatchWriter`'s `restore_rejected` are
> auto-terminated by default; while they have no connection, their keys go to `failure`. Each
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
| RESTORE Probe Target | empty (default) — or `Target Dragonfly Pool`, as the imported flow sets it |
| (rest) | defaults are fine for a small test dataset |

With **RESTORE Probe Target** empty, every key is read and written type by type. When it
names the target pool, the processor tests `DUMP`/`RESTORE` against the target once per type
and encoding. It then copies keys of each type and encoding that passed with `DUMP`/`RESTORE`.
If the target rejects a payload anyway, `RedisBatchWriter` sends the key out
`restore_rejected`; connect that back to `RedisTypeDeserializer` and it reads the key type by
type. `run-r2dfly.sh --no-restore-fast-path` turns this off.

### 3c. Configure RedisBatchWriter

| Property | Set to |
|---|---|
| Dragonfly Connection Pool | `Target Dragonfly Pool` |
| Batch Size | `50` (default; fine for small tests) |
| Batch Timeout (ms) | `30000` (default) |
| TTL Strategy | `PRESERVE` (default) |
| Key Prefix Separator | `:` (default) |
| Conflict Strategy | `OVERWRITE` (default — replaces existing target keys) |
| Write Confirmation | `true` (default — don't disable this) |

`RedisBatchWriter` has no downstream processor in this minimal flow, so open its
**Relationships** tab and check **auto-terminate** for `success`, `skipped`, `failure`,
and `retry`, or it won't validate. Its fifth relationship, `restore_rejected`, is
auto-terminated by default. See the note in step 3 above; "Handling failures" below
covers routing these somewhere durable instead.

`RedisBatchWriter` (and `ModuleTypeHandler`) also have **Key Prefix Rename**, **Key Prefix**,
and **TTL Reset Offset (ms)** properties. `TTL Strategy` takes `PRESERVE`, `STRIP` or `RESET`.
`run-r2dfly.sh` sets these with `--rename-prefix`, `--ttl-strategy` and
`--ttl-reset-offset-ms`, and `--transform-script` adds a Groovy hook in front of the writer.
See "Transforms" in `quickstart.md`.

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
| Batch Timeout (ms) | `10000` (default — applies to one batch's reads, and again to its writes) |
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
change **Migration ID** to a new value. `run-r2dfly.sh` and `simple-migration.sh` use a new
Migration ID on every run unless you pass `--migration-id`.

## 6. Tuning parallelism and container resources

The flow above runs single-threaded (`Concurrent Tasks: 1` everywhere, `Partition Count: 1`)
with no limit on the NiFi container's CPU/memory. That's fine for the handful of test keys
used above, but leaves throughput on the table for a real migration. Two independent knobs,
plus the container's own resources:

### Scan/read parallelism (`RedisScanReader` / `RedisTypeDeserializer` / `ModuleTypeHandler`)

Raise this when the read side is the bottleneck: the queue in front of
`RedisTypeDeserializer`/`ModuleTypeHandler` grows while the queue in front of
`RedisBatchWriter` stays shallow.

1. Double-click **RedisScanReader** → **Properties** tab → set **Partition Count** to `N`.
2. Same processor → **Scheduling** tab → set **Concurrent Tasks** to the *same* `N`. The
   Partition Count property requires the two to match.
3. Double-click **RedisTypeDeserializer** → **Scheduling** tab → set **Concurrent Tasks** to
   `N`. This is where most of the read work happens: it fetches each key's value.
4. Double-click **ModuleTypeHandler** → **Scheduling** tab → same `N`, for module keys.

How the scan splits depends on the source:

- **Standalone or Sentinel source**: partition 0 scans the whole keyspace once, whatever
  Partition Count says. More scans of one keyspace would only add source load. The extra
  read throughput comes from `RedisTypeDeserializer`'s Concurrent Tasks.
- **Cluster source**: the masters are dealt out among the partitions, and each master is
  scanned once, on its own node.

Keep `N` at or below the NiFi container's CPU count (see below) and the source's headroom.
`NIFI_REDIS_MIGRATION_SPEC.md` section 7.1 has a sizing formula that computes `N` from key
count and CPU cores.

### Write parallelism (`RedisBatchWriter`)

Independent of scan parallelism - raise this when the *write* side is the bottleneck instead
(the queue feeding `RedisBatchWriter` stays deep while its own processing keeps up, or a
remote/slower target can't keep pace with a single writer's pipeline). Double-click
**RedisBatchWriter** → **Scheduling** tab → set **Concurrent Tasks** to `W`. There's no
Partition Count to keep in sync here - each writer task just pulls whatever's queued. Don't
raise `W` past the target Dragonfly's core count (spec section 7.2) - Dragonfly's thread-per-core
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
`--parallelism`/`--writer-concurrency` flags) do for you. `--parallelism N` sets
RedisScanReader's Partition Count and the Concurrent Tasks of RedisScanReader,
RedisTypeDeserializer and ModuleTypeHandler. `--writer-concurrency W` sets RedisBatchWriter's
Concurrent Tasks. `simple-migration.sh` passes `--cpus`/`--memory` to `deploy-to-nifi.sh` as
`NIFI_CPUS`/`NIFI_MEMORY`.
Building the flow by hand as in this tutorial is for understanding what those flags actually
change, or for tuning a flow you built manually rather than imported from `r2dfly.json`.

## Handling failures (optional, but recommended beyond a quick test)

The spec's design principle is "no fire-and-forget": every failed key should land somewhere
durable instead of being auto-terminated. The project's sink for this is
**`RedisDeadLetterWriter`**. It writes one JSON record per failed key, with the reason, to its
**Output Directory**, raises an ERROR bulletin, and adds to the `migration.failures.total`
counter. The imported `r2dfly.json` wires it like this:

- `RedisScanReader` `failure` → `RedisDeadLetterWriter`
- `RedisTypeDeserializer` `failure` → `RedisDeadLetterWriter`;
  `module_type` → `ModuleTypeHandler`; `key_missing` auto-terminated (the key expired
  between scan and read)
- `RedisBatchWriter` `failure` → `RedisDeadLetterWriter`; `retry` → back to
  `RedisBatchWriter`; `restore_rejected` → `RedisTypeDeserializer`; `success` and `skipped`
  auto-terminated
- `ModuleTypeHandler` `failure` and `unsupported` → `RedisDeadLetterWriter`; `success`
  auto-terminated
- `RedisDeadLetterWriter` `failure` → back to itself; `success` auto-terminated

The imported flow's Output Directory is `./r2dfly-dead-letter`, which is
`/opt/nifi/nifi-current/r2dfly-dead-letter` inside the NiFi container. `simple-migration.sh`
and `continuous-migration.sh` print how many records a run added there.

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
- **The flow runs but the target stays short, or stops growing**: see
  [`debugging-help.md`](debugging-help.md). Check the dead-letter directory first: every key
  that failed, or that the target cannot hold, has a record there with its reason. Keys left
  out by the Key Type Filter or a prefix list, and keys that expired mid-copy, have none.
