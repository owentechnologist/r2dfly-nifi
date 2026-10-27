# Verified behavior

r2dfly's design rests on facts about Dragonfly, the Redis and Valkey sources, NiFi and the client
library. Each fact on this page was tested rather than assumed, in October 2026. Each entry says
what was tested, on which versions, what happened, and which test keeps checking it. Where a
fact is checked by the code at run time rather than by a test, the entry says so.

The target is Dragonfly v2.0.2 throughout. "Typed path" means r2dfly reads a key's value with
type-specific commands and writes it back the same way. "RESTORE path" means it copies the key's
`DUMP` payload.

## RESTORE of source DUMP payloads into Dragonfly

| Source | Result |
|---|---|
| Redis 6.2 | Every core type and encoding restores. Streams gain entries-added and max-deleted-entry-id fields, which Redis 6.2 does not have. |
| Redis 7.2, Redis 7.4 | Every core type and encoding restores, streams included with their groups, consumers and pending entries. |
| Redis 7.4 hash with field TTLs (`listpackex` encoding) | Rejected: "Bad data format". |
| Valkey 7.2, Valkey 8.1 | Every core type and encoding restores. |
| Redis 8.10, Valkey 9.1 | Every payload rejected: "DUMP payload version or checksum are wrong". |

Redis 7.0 was not tested. Valkey reports `redis_version:7.2.4` whatever its real version is, so
r2dfly never decides by version number. Instead `RestoreProbe` restores the first key of each
type and encoding to a scratch key, compares the two sides, and uses the RESTORE path only where
that passed. Everything else takes the typed path. Guarded by `RestoreFastPathIT`.

## Hash field TTLs on Dragonfly

- Work: `HEXPIRE` (seconds), `HTTL`, `HPEXPIRETIME`, `FIELDTTL`, `FIELDEXPIRE`, `HSETEX`.
- Not supported: `HPEXPIRE` (with or without options), `HPTTL`, `HEXPIRETIME`, `HPERSIST`.
- A hash with field TTLs cannot be copied with RESTORE from Redis 7.4 (see above).

So the typed path reads field TTLs from the source with `HPTTL` and writes them with `HEXPIRE`,
at one-second precision. A Dragonfly source has no `HPTTL`, so its field TTLs survive only on the
RESTORE path. Guarded by `StreamHashFidelityIT` (Redis 8.10 and Valkey 9.1 sources).

## Stream consumer state on Dragonfly

- Work: `XGROUP CREATECONSUMER`; `XCLAIM` with `FORCE`, `TIME`, `RETRYCOUNT` and `JUSTID`;
  `XGROUP SETID ... ENTRIESREAD`; `XSETID <id>`.
- `XCLAIM ... IDLE n` is wrong. The entry reads back as idle since 1970. r2dfly uses `TIME`
  instead, computed on the target's own clock.
- `XGROUP CREATE ... ENTRIESREAD n` is accepted and ignored. r2dfly sets it with `XGROUP SETID`.
- `XSETID ... ENTRIESADDED ... MAXDELETEDID` is rejected. On the typed path, entries-added and
  max-deleted-entry-id therefore count only what the target received.
- `TIME` replies with integers, which Lettuce's typed `time()` cannot parse. r2dfly sends it raw.
- A pending entry whose stream entry was deleted cannot be claimed, because `XCLAIM` ignores
  missing ids. Consumer idle and seen times cannot be set.

Guarded by `StreamHashFidelityIT`, which compares `XINFO STREAM`, `XINFO GROUPS`,
`XINFO CONSUMERS` and `XPENDING` on both sides.

## Module types on Dragonfly

Tested with Redis Stack 7.4 and Redis 8.10 sources.

- `RESTORE` accepts only `ReJSON-RL` payloads from Redis Stack 7.4, and nothing from Redis 8.10.
  TopK, Bloom, Cuckoo, Count-Min Sketch, time series and T-Digest payloads fail with
  "Bad data format".
- Dragonfly has `BF.SCANDUMP` and `BF.LOADCHUNK`, but `BF.LOADCHUNK` rejects the chunks
  RedisBloom produces ("invalid bloom dump payload").
- Dragonfly has no `CF.SCANDUMP`, `CF.LOADCHUNK`, `TS.*`, `TDIGEST.*`, `GRAPH.*` or `VADD`.

So JSON and TopK keys are rebuilt from their values. Bloom, Cuckoo and Count-Min Sketch keys are
tried with RESTORE, which works between Dragonfly instances, and otherwise dead-lettered with the
reason. Time series, T-Digest, graph and vector set keys are dead-lettered without an attempt.
`util/ModuleCopy` holds this table. Guarded by `ModuleTypesIT`.

## Keyspace notifications from Redis 8 module commands

Redis 8 publishes no keyspace notification for `TOPK.*`, `BF.*`, `CF.*` or `CMS.*`. `JSON.*` and
`TS.*` do publish. A live phase driven by keyspace notifications can never see those changes, so
they need `--live-source replication` or a reconcile pass. Guarded by `LivePhaseIT`, which fails
if Redis starts announcing them.

## The replication protocol

- `com.moilioncircle:redis-replicator` 3.11.0, the latest version, was rejected. It cannot read
  Redis 8's RDB version 15, silently drops module commands, and hardcodes `PSYNC`. r2dfly uses its
  own replica client, `util/ReplicationStream`, which skips the RDB without parsing it. So the
  RDB versions of Valkey 8 and 9 do not matter to it.
- `REPLCONF rdb-filter-only ""`, which asks the master to leave the data out of the RDB, works on
  Redis 7.4, Redis 8.10 and Valkey 9.1. Redis 6.2 refuses it and sends the full RDB, which is then
  skipped.
- A partial resync (`+CONTINUE`) does not repeat the `SELECT` that preceded the offset, so the
  selected database is saved with the offset.
- The renamed `PSYNC` command that AWS ElastiCache provides on request (RedisShake's `aws_psync`)
  is not supported.

Guarded by `ReplicationReaderIT` (Redis 6.2, 7.4, 8.10 and Valkey 9.1).

## Redis Cluster topology changes

Tested on a Redis 7.4 cluster of three masters and three replicas.

- After `CLUSTER FAILOVER`, the promoted replica reports its old master's replication id as
  `master_replid2`. `PSYNC <old id> <offset+1>` to it answers `+CONTINUE` and streams every write
  after that offset. So a failover never forces a full resync of the shard.
- `MIGRATE` reaches the source shard's replication stream as `DEL`, and the destination's as
  `RESTORE-ASKING`. `COMMAND GETKEYS` knows `RESTORE-ASKING`'s key position.
- Lettuce refuses a redirect to a node missing from its cached view ("This partition is not known
  in the cluster view") unless `validateClusterNodeMembership` is off. r2dfly turns it off.

Guarded by `ClusterTopologyIT`.

## Dragonfly as a change source

Needed for the rollback path, which keeps the old Redis current from Dragonfly after cutover.

- `CONFIG SET notify-keyspace-events` accepts only `Ex` (expired events). `KEA`, `Kx`, `K$` and
  `Kg` are rejected, and the startup flag's help says "Only Ex is supported for now".
- Dragonfly answers `PSYNC` and `SYNC` with "unknown command", and rejects
  `CLIENT TRACKING on BCAST`.

So rollback runs `MigrationReconciler` passes with no change stream (`--live-source reconcile`).
This was a one-time probe with `redis-cli`. No test repeats it.

## Reading and writing files

Tested with a Redis 7.4 source of 607 keys: strings with and without TTLs, a 12,000-field hash
and a 6,000-element list (both split into chunks by the flow), sets, sorted sets, and a stream
with a consumer group and a pending entry.

- A Redis 7.4 RDB file, and a Redis 7.4 multi-part AOF directory (a base file plus an
  incremental file written after `BGREWRITEAOF`), each loaded into a throwaway `redis:7.4` or
  `redis:latest` container and copied to Dragonfly, left the target identical to the source.
- `redis:6-alpine` refuses the Redis 7.4 RDB ("Fatal error loading the DB: Invalid argument")
  and exits, so the image has to be at least as new as the Redis that wrote the file. The script
  stops with that log line.
- Redis 7.4 started with `--appendonly yes` and a RESP command file as `appendonly.aof`, with no
  manifest, loads it as a legacy AOF. So the RESP file written by `KeyRecordFileWriter` is an
  AOF. `redis-cli --pipe` into Redis 7.4 loads it too. Into Dragonfly, `--pipe` garbles the
  command stream (found earlier, while bulk-loading test data), so load a RESP file into
  Dragonfly through `--source-aof` instead.
- Source to JSON-lines file to Dragonfly, and source to RESP file to AOF to Dragonfly, both left
  the target identical to the source, the pending entry and its consumer included.

Guarded by `FileRoundTripIT` (the JSON-lines and RESP round trips); the RDB and AOF loading was
an end-to-end run of `simple-migration.sh` on scratch containers.

## NiFi 2.11

- Processor counters are exported on `/nifi-api/flow/metrics/prometheus` as
  `nifi_processor_counters{processor_name, counter_name}`. Values recorded with
  `ProcessSession.recordGauge` are not exported, so r2dfly publishes current values (lag, pending
  dead letters) as counters adjusted by their change.
- `/nifi-api/counters` lists every counter twice, once per processor and again under
  "All <type>'s". Scripts that sum counters skip the second form.
- `NoOpProvenanceRepository` and `VolatileProvenanceRepository` are bundled in
  `nifi-provenance-repository-nar`. With the default `WriteAheadProvenanceRepository`, several runs
  totalling about 25 million keys filled its Lucene index queue until every session commit in the flow
  blocked, and stayed blocked after the flow stopped. So `deploy-to-nifi.sh` switches to the NoOp
  repository by default.
- NiFi refuses Primary Node Only on a processor that has an incoming connection. `nifi-mock` does
  not check this, so only a run against a real NiFi catches it.

## Client library

`LettuceFutures.awaitAll` (Lettuce 6.3) stops waiting at the first reply that failed, so replies
still in flight behind it look like timeouts. r2dfly waits with `util/Futures.awaitAll`, which
waits for every reply. Seen as `ModuleTypesIT` failing three runs in five before the change.
