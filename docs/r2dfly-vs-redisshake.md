# r2dfly vs. RedisShake: which one moves your data?

Both tools move data from Redis into Dragonfly. Dragonfly speaks the Redis protocol, so
[RedisShake](https://github.com/tair-opensource/RedisShake) treats it as a normal Redis target.
The two solve different problems, though. This page says what each one is, compares them row by
row, gives measured throughput, and ends with which to choose. To switch an existing RedisShake
setup to r2dfly, see [Migrating from RedisShake](migrating-from-redisshake.md). The facts about
Dragonfly, Redis and NiFi that this comparison relies on are tested in
[Verified behavior](verified-behavior.md).

## What RedisShake is

RedisShake is a single Go binary, maintained by Alibaba Cloud's Tair team, with about 4,400
GitHub stars and an MIT license. You run it with one TOML file, or one Docker command. It reads a
Redis-protocol source (Redis, Valkey, Tair, AWS ElastiCache) in one of four ways: `sync_reader`
connects as a replica and receives the source's replication stream, `scan_reader` scans the
keyspace (optionally followed by keyspace notifications), and `rdb_reader` and `aof_reader` read
files. It writes to a Redis-protocol target or to a file. A Lua hook can filter or rewrite
commands in flight.

RedisShake's documentation states two limits plainly. It has no checkpoint or resume, so a stopped
run starts over. And it assumes a stable topology, so a resharding, a failover or a slot migration
during the sync stops it. Its documentation calls it a tool for one-time migrations.

## What r2dfly is

r2dfly is this project, a set of NiFi processors and the scripts that deploy and drive them. It
reads Redis, Valkey and Dragonfly sources. It copies RDB and AOF files by loading them into a
throwaway Redis, and can write a copy to a JSON-lines or RESP file instead of a target. Its
`RecordToKeyRecord` and `ExecuteSQLIncremental` processors also bring in JDBC databases, Kafka,
files and MongoDB, but you wire those into the flow yourself; no script does it.

- **The copy** scans the source once, by shard on a cluster, and resumes from checkpoints after a
  restart. Each key is copied with `RESTORE` where a probe has shown the target restores that type
  and encoding faithfully, and with type-specific commands otherwise. The typed path keeps stream
  consumer groups, pending entries and hash field TTLs. JSON and TopK keys are rebuilt for
  Dragonfly. A key that cannot be copied, such as a Bloom filter from Redis, gets a dead-letter
  record with its reason. `headless-migration.sh` runs the same copy on NiFi's Stateless engine,
  with no NiFi server, but does not resume after a crash.
- **The live phase** keeps the target current after the copy. Its default source is keyspace
  notifications. `--live-source replication` instead follows each master as a replica (`PSYNC`),
  resumes after a restart with a partial resync, and follows failovers, resharding and new shards.
  Either way it copies each changed key's current value rather than replaying commands.
- **The reconciler** compares source and target, repairs every difference it finds, and runs a full
  pass when the source's key count drops suddenly (a `FLUSHALL` or `FLUSHDB`). By default it
  deletes target keys the source does not have; `--reconcile-spare-target-keys` keeps them.
- **Cutover** has a script that says whether it is safe to switch, waits for the live phase to drain,
  and confirms a clean full pass. After cutover, a reverse sync can keep the old Redis current for
  rollback.
- **`import-shake.sh`** translates a RedisShake `shake.toml` into an r2dfly config.

## Where the two compete

The last column says which tool is ahead on that row.

| | RedisShake | r2dfly | Ahead |
|---|---|---|---|
| Sources | Redis-protocol servers, RDB and AOF files | Redis-protocol servers, RDB and AOF files (loaded through a throwaway Redis), plus JDBC, Kafka, files and MongoDB wired in NiFi | Tie |
| Writing to a file | Command file, AOF or JSON | JSON lines (one key per line, loadable back) or RESP commands (also an AOF); core types only | Tie |
| One-time copy of small keys | 63,700 keys/s for 10M strings | 38,300 keys/s | RedisShake |
| One-time copy of large values | 5.1 s for 10k lists of 1,000 elements | Under 1 s | r2dfly |
| Continuous sync | Replication stream, or scan plus keyspace notifications | Replication stream, keyspace notifications, or reconcile passes | Tie |
| Gaps in continuous sync | None while the topology holds | None with replication; with keyspace notifications, a dropped event waits for the reconciler (a full pass finds every one) | Tie |
| Catching up after a write burst | Replays every command: 1,075 s | Copies each changed key once: 328 s | r2dfly |
| Topology change during the sync | Stops | With `--live-source replication`: a failover continues, resharded keys follow, a new shard gets its own snapshot. The copy reads a slot that was migrating once it settles | r2dfly |
| Restart | Starts over | Resumes from scan checkpoints and a partial resync (a headless copy starts over) | r2dfly |
| A key the target cannot take | Stops at the first error reply (tested with TopK and Bloom keys) | Dead-letter record with the reason; the run continues | r2dfly |
| Module keys into Dragonfly | Written with `RESTORE`; Dragonfly rejects TopK and Bloom payloads | JSON and TopK rebuilt from their values; Bloom, Cuckoo and Count-Min Sketch dead-lettered with the reason | r2dfly |
| Filters | Key prefix, suffix, regex, database, command, command group, slot, Lua hook | Key prefix and real key type, applied to the copy, the live phase and the reconciler alike | RedisShake |
| Binary key names and values | Copied byte for byte (tested) | Copied byte for byte; carried as base64 where they are not UTF-8 text (tested) | Tie |
| Transforms | A Lua hook on each command | Built-in prefix rename, database remap and TTL strategies that the live phase, reconciler and search indexes follow, plus a Groovy hook on each key record | Tie |
| Cutover and rollback | Not built in | Safe-to-switch check, final reconcile, reverse sync | r2dfly |
| Metrics | An HTTP status port | Prometheus, through NiFi | |
| Operating model | One binary, one config file | A NiFi deployment: a JVM, containers, a canvas, a REST API. A one-time copy can also run headless: one script and the `apache/nifi` image, no NiFi server | RedisShake |
| Maturity | Shipping since the redis-port lineage, about 4,400 stars | New. The copy is field-tested; the rest is tested in integration and on scratch deployments | RedisShake |

## Throughput, measured

Both tools copied the same datasets between the same two containers (Redis 7.4 to Dragonfly
2.0.2) on one podman VM with 8 CPUs and 12 GB of memory, in October 2026. Each figure runs from the
first key reaching the target until the last one has; NiFi deployment and container start-up are
left out on both sides. Rerun any row with `scripts/benchmark-vs-redisshake.sh --dataset <name>`.

| Dataset | RedisShake `sync_reader` | RedisShake `scan_reader` | r2dfly |
|---|---|---|---|
| 10 million strings, 64-byte values | 63,700 keys/s (157 s) | 20,900 keys/s (479 s) | 38,300 keys/s (261 s) |
| 1 million hashes, 20 fields of 32 bytes | 77,900 keys/s (13 s) | 21,200 keys/s (47 s) | 37,100 keys/s (27 s) |
| 10,000 lists, 1,000 elements of 100 bytes | 2,000 keys/s (5.1 s) | 7,900 keys/s (1.3 s) | under 1 s |
| Live: 60 s of SET, INCR, HSET, LPUSH on 1M random keys (r2dfly with `--live-source replication`) | target matched 1,075 s after the writes stopped | not tested | target matched 328 s after the writes stopped |

What the numbers say:

- For a one-time copy of many small keys, RedisShake's replication reader is faster, by about
  1.7x on strings and 2.1x on hashes. It parses the source's RDB stream; r2dfly reads key by key, one
  NiFi FlowFile per key.
- r2dfly is 1.8x faster than RedisShake's own scan reader, the comparable way of reading.
- For large values r2dfly took under a second against RedisShake's 5.1 s and 1.3 s, moving each
  key as one RESTORE payload. At this size the half-second polling limits the precision.
- In a live sync r2dfly caught up about 3x sooner. It copies each changed key's current value
  once, where RedisShake replays every command it saw, so its backlog grows with write volume
  rather than with the number of keys changed (4.1 million here).

These figures assume NiFi's provenance recording is off, which is `deploy-to-nifi.sh`'s
default. With provenance on, r2dfly copied the 10 million strings at 20,700 keys/s, and on long
runs the provenance index eventually stopped the flow. More concurrent tasks did not help on this
machine: `--parallelism 8 --writer-concurrency 8` copied the 10 million strings at 12,600 keys/s,
so the default of one task each stays.

The bulk-copy gap was accepted deliberately. Closing it means carrying many keys per FlowFile
through every processor, which would rework the per-key failure reasons, dead letters, RESTORE
fallback and replication ordering that the rest of this comparison relies on. A three-node NiFi
cluster was not measured, and neither was a headless copy.

## The decision

- **A one-time copy of many small keys, as fast as possible:** RedisShake's `sync_reader`. It was
  1.7 to 2.1 times faster on the benchmark machine, and it is one binary with one config file.
  If you want r2dfly's dead-letter records or module-key rebuilds without running a NiFi server,
  `headless-migration.sh` is one script with one TOML file; its speed was not measured.
- **A migration that has to keep running until cutover:** r2dfly. It resumes after a crash, follows
  a cluster through failovers and resharding, repairs what a change stream missed, and tells you
  when it is safe to switch.
- **Data that is not in Redis** (a Postgres table, a Kafka topic, files, MongoDB): r2dfly.
  RedisShake reads only the Redis protocol and its file formats.
- **Fine-grained key selection by suffix, regex or Lua:** RedisShake. r2dfly's filters select by
  prefix and key type. Its Groovy hook can drop keys by any rule, but then the reconciler keeps
  reporting the dropped keys, so the cutover check cannot pass.

If your organization already runs NiFi, r2dfly fits that operating model, with its counters in
Prometheus and its flow on the NiFi canvas. RedisShake is a program you run, not a managed flow.
