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

A stream read while the source keeps adding entries can return entries newer than the
`last-generated-id` that `XINFO STREAM` reported a moment earlier. Dragonfly then refuses the
record's `XSETID` ("The ID specified in XSETID is equal or smaller than the target stream top
item") on every attempt, so the key used to end in the dead-letter directory. r2dfly now raises the
last id to the newest entry it read. Guarded by `ReplicationReaderIT`, whose test pipeline loops
writer retries back the way the flow does instead of dropping them, which had hidden this.

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

`JSON.SET key $` over an existing key drops the key's TTL on Dragonfly v2.0.2, so rebuilding a
JSON key needs no `PERSIST` when the source key has no TTL. Guarded by `ModuleTypeHandlerIT`.

## TopK counts on Dragonfly

Dragonfly's TopK can count an item differently from RedisBloom's when items collide in the
sketch. Tested on Dragonfly Cloud `df-i1.40.8` with `TOPK.RESERVE k 50 2000 7 0.925` and three
items added once each, as `scripts/populate-test-data.sh` does: in about 0.7% of keys (12 of 1,500),
Dragonfly reports one of the three items with count 0 where Redis Stack reports 1. The same happens
writing straight to Dragonfly with `TOPK.ADD` or with `TOPK.INCRBY`, so r2dfly's TopK rebuild
cannot avoid it, and which item gets 0 depends on the order the items are written. The list of
items always matches.

## Keyspace notifications from Redis 8 module commands

Redis 8 publishes no keyspace notification for `TOPK.*`, `BF.*`, `CF.*` or `CMS.*`. `JSON.*` and
`TS.*` do publish. A live phase driven by keyspace notifications can never see those changes, so
they need `--live-source replication` or a reconcile pass. `LivePhaseIT` fails if Redis starts
announcing `TOPK.*`; the `BF.*`, `CF.*` and `CMS.*` results come from a one-time probe that no test
repeats.

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

## Transforms

Tested end to end on scratch containers, a Redis Stack 7.4 source and a Dragonfly target, with
`simple-migration.sh` and `continuous-migration.sh`.

- `--rename-prefix user:=account:` left the target identical to the source under the new names,
  for the copy, the keyspace live phase and the replication live phase (writes and deletes
  alike). The source key `account:9`, which `user:9` would overwrite, was dead-lettered with
  the reason. A search index on `PREFIX 1 user:h:` was rebuilt on Dragonfly with prefix
  `account:h:`.
- A full reconcile pass under the rename and `--ttl-strategy reset` found no differences.
  `reset` left each target TTL 600 seconds above the source's, as asked.
- Copying database 3 into database 5 (`/3` and `/5` on the connection strings) matched under
  both live phases and left database 0 of the target empty. With the keyspace live phase, a
  write to a key in database 0 of the source did not reach the target.
- NiFi 2.11's `ExecuteGroovyScript` names its properties `Script File` and `Failure Strategy`.
  The older `groovyx-script-file` names are taken as dynamic properties without an error, which
  leaves the processor invalid with "At least one parameter required". `run-r2dfly.sh` therefore
  checks that the hook is valid before starting the flow.

- The example hook in the quickstart, run by `continuous-migration.sh --transform-script`,
  upper-cased the `order:` strings and dropped the `tmp:` keys in the copy and in the keyspace
  live phase. A record whose script threw was dead-lettered with the script's reason, and the
  `DB` variable held the source database. A later run without the flag bypassed the hook and
  left the target identical to the source.
- An exception the script does not catch is not handled by `Failure Strategy` set to
  `transfer to failure` when the script takes a batch with `session.get(1000)`: NiFi logs
  "Processing halted", rolls the batch back and retries it without end. So the example catches
  per record. A script that does not compile is refused at validation, and `run-r2dfly.sh`
  stops with Groovy's compiler error.

Guarded by `KeyMappingTest`, and by `MigrationReconcilerIT`, which runs a rename with reset TTLs
through a pass, a clean pass, and a deleted source key.

## Binary keys and values

Tested with key names and values that are not UTF-8 text in every core type (string values, hash
fields and values, list, set and sorted-set members, stream field names and values), seeded as
raw RESP and read back on both sides by one Lua script, so no byte passed through a shell
argument.

- RedisShake's `sync_reader` and `scan_reader` (Redis 7.4 to Dragonfly) left every key name and
  value byte-identical.
- r2dfly, with base64 carried in key records and `redis.key.base64`, left every key name and value
  byte-identical through `simple-migration.sh` from Redis 7.4 (RESTORE where the probe passed)
  and Redis 8.10 (the typed path), and through `continuous-migration.sh` with the keyspace live
  source (Redis 7.4) and the replication live source (Redis 8.10), live writes included. Before
  base64, the typed path wrote U+FFFD in place of the invalid bytes and could not find keys whose
  names were not text.

- Stream consumer group and consumer names are byte strings too. The typed path used to read them
  as text: a group name that was not UTF-8 failed the read, and a consumer name lost its bytes.
  r2dfly now reads them raw (`XPENDING` dispatched without Lettuce's text model) and stores them
  like every other byte string in the record. Dragonfly keeps a group name exactly, zero bytes
  included, but cuts a consumer name at its first zero byte, through `XGROUP CREATECONSUMER` and
  `XREADGROUP` alike (Redis 7.4 keeps it), so two consumers whose names differ only after a zero
  byte become one on Dragonfly.

Guarded by `BinaryDataIT`: the typed copy, the RESTORE path, a JSON-lines file, the replication
live phase, the reconciler, a prefix rename, and stream group and consumer names.

## Writes with no SELECT after a full resync

A Redis 8.10 master that has already selected database 0 for a replica sends its later writes
with no SELECT before them, including to a new replica after its full resync: a `SET` grew
`master_repl_offset` by the 32 bytes of the `SET` alone. A real replica's connection to its master
starts in database 0. The replication reader used to treat the database as unknown until the
first SELECT, so it skipped every write in that state: in one run, the first `DEL` after startup
never reached the target. It now starts a full-resync stream in database 0. Seen on scratch
containers; an integration test with a fresh master did not reproduce it, since that master sent
the SELECT. Redis 8.10.3 also sent `SELECT 0` to a second replica's full resync after a first
replica had seen `SELECT 0`, both with the first replica disconnected and with it still connected,
so the trigger on the scratch containers is still unknown and nothing guards the fix.

## NiFi's Stateless engine

Tested with `nifi-stateless-assembly-2.11.0`, the r2dfly NAR and NiFi's standard NARs, copying
from Redis 7.4 and Redis Stack 7.4 into Dragonfly v2.0.2.

- The one-time copy flow runs unchanged, with its MapCacheServer and MapCacheClientService pair
  inside the flow. Every key arrived, JSON included, at 1,000 and 50,000 keys.
- `nifi-stateless.sh` cannot end a scan at the right time. Without `-c` it triggers once: in
  Stateless, a commit waits until the whole invocation is acknowledged, which happens once the
  first page has drained, so the run stops after 200 keys. With `-c` it triggers forever. So
  `scripts/headless/HeadlessCopy.java` triggers the flow itself until RedisScanReader's
  "Scan Partitions Complete" counter reaches the partition count and nothing is queued.
- A write to a target that refuses connections fails the trigger, so a headless run ends with
  that error instead of retrying.
- Stateless never comes back for a penalized FlowFile. The first failure a processor penalizes
  leaves the trigger waiting forever, and while it waits the JVM's heap fills until it runs out:
  a run copying 1,200,000 keys into a target that answered "ERR Out of memory" hung, then failed
  with OutOfMemoryError, and 20 JSON keys refused by a plain Redis target hung a 520-key run. The
  headless flow therefore sets every processor's penalty to 0 (its failures go to the dead-letter
  writer once; retries use RedisBatchWriter's own delay), and its JVM runs with
  `-XX:+ExitOnOutOfMemoryError`. With both, the 1,200,000-key run into a target capped at 5.82 GiB
  finished in 126 s with exit 0 and 158,044 keys dead-lettered with the target's reason.
- A TLS connection through the added SSL Context Service was checked only as far as a valid flow
  and a refused connection; no TLS handshake has been tested headless.

Checked end to end by running `scripts/headless-migration.sh` against scratch containers; no
automated test covers it.

## RedisShake with module keys, side by side with r2dfly

Tested on 2026-10-28 with RedisShake at commit 014e2f4 (v4.6.2 plus "restore unknown MODULE_2
values"), from a Redis Stack source filled by `scripts/populate-test-data.sh --count 1500 --types
json,string,hash,zset,set,list,stream,topk` (12,000 keys) into a Dragonfly Cloud `df-i1.40.8`
target, renaming keys with RedisShake's documented Lua prefix rewrite. Every key was compared by
type, value and TTL.

- RedisShake writes JSON and TopK keys with `RESTORE`. The target rejects the TopK payload ("ERR Bad
  data format"), and RedisShake stops at that first error reply: `sync_reader` exited 1 with 103 of
  12,000 keys written. r2dfly copied all 12,000, rebuilding TopK from its items.
- With TopK left out (`block_key_prefix`, or `scan_reader`'s `skip_unknown_type`), RedisShake's
  `sync_reader` and `scan_reader` copied the other 10,500 keys exactly, as r2dfly did.
- In the live phase RedisShake does not know `JSON.SET` ("unknown command"), so its Lua function
  gets no key positions and the prefix rewrite does not apply: the command went to the un-renamed
  key, the target answered "ERR no such key", and RedisShake stopped, so the hash and TTL changes
  made after it never arrived. r2dfly's continuous run applied all five live changes.
- With 1,500 Bloom keys added (13,500 keys), RedisShake stopped at the first rejected Bloom
  `RESTORE` once TopK was blocked (49 keys written), and copied the other types exactly once both
  were blocked. Dragonfly `df-i1.40.8` still rejects RedisBloom's dump formats: `RESTORE` gives "ERR
  Bad data format" and `BF.LOADCHUNK` of `BF.SCANDUMP` chunks gives "ERR INVALIDOBJ invalid bloom
  dump payload". A Bloom filter does not keep its items, so no tool can rebuild one. r2dfly
  copied the other 12,000 keys. Its default key types then left Bloom out, so the Bloom keys were
  skipped with only a NiFi counter to show it; they now include every module type, so each Bloom
  key is dead-lettered with "no lossless copy path for MBbloom-- on this Dragonfly version; RESTORE
  was rejected: ERR Bad data format", and the summary reports keys a user's type filter leaves out.

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
