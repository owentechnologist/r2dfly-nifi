# r2dfly vs. RedisShake: which one moves your data?

Both tools can move data from Redis into Dragonfly. Dragonfly speaks the Redis protocol, so
[RedisShake](https://github.com/tair-opensource/RedisShake) treats it as a normal Redis
target with no special handling. The two tools solve different problems, though, and picking
the wrong one costs you either a rebuild partway through a migration or months spent
outgrowing a heavier tool you didn't need. This page lays out what each tool actually is,
where they overlap, where they don't, and which one fits your situation.

## What RedisShake is

RedisShake is a single Go binary, maintained by Alibaba Cloud's Tair team, with about 4,400
GitHub stars and an MIT license. You run it with one TOML config file, or one Docker command.
It reads from a Redis-protocol-compatible source (Redis, Valkey, Tair, AWS ElastiCache) using
one of four methods: `sync_reader` (it connects as a replica and receives the source's real
replication stream), `rdb_reader`, `aof_reader`, or `scan_reader`. It writes to a
Redis-protocol-compatible target, or to a file. A Lua hook lets you filter, rewrite, or split
commands in flight, for example expanding one `MSET` into several `SET` calls or dropping a
key prefix.

RedisShake's own documentation states two limits plainly. It has no checkpoint or resume. If
the process stops partway through, it resyncs from the beginning on restart. It also assumes
a stable topology. A cluster resharding, a failover, or a slot migration during the sync
crashes it. RedisShake's docs say outright that this makes it a tool for one-time migrations,
not for long-running continuous sync.

## What r2dfly is

r2dfly is this project, a set of custom NiFi processors, not a single binary. Its Scan Phase
does a one-time copy from a Redis or Valkey source, and it's the most tested part of this
tool. It's verified against real Redis Cluster and DragonflyDB Cloud deployments, with
pre-flight cluster health checks and a Dragonfly-to-Dragonfly `DUMP`/`RESTORE` fast path. Unlike
RedisShake, which replays whatever commands it reads, r2dfly's Scan Phase understands
Dragonfly's own native types: it rebuilds RedisJSON documents, TopK structures with exact
counts, and search indexes, rather than only replaying raw commands.

Its Live Phase adds continuous sync on top of a scan, driven by Redis keyspace notifications.
Unlike RedisShake's `sync_reader`, which streams the source's actual replication protocol,
keyspace notifications are fire-and-forget. A dropped connection loses whatever changed during
the outage, and r2dfly doesn't yet have a reconciliation pass to repair that gap. Treat the
Live Phase as a short cutover bridge, not an unattended service, until that gap closes.

r2dfly's newest piece breaks the Redis-only assumption entirely. A `RecordToKeyRecord`
processor and an `ExecuteSQLIncremental` processor let you pull rows from any JDBC database, a
Kafka topic, a file, or a MongoDB collection, and land them in Dragonfly as a hash or a native
JSON document, shaped for an `FT.CREATE` search index on top. RedisShake has no equivalent.
Every one of its readers speaks the Redis protocol, so a Postgres table or a Kafka topic is
outside what it can read at all.

## Where the two actually compete

| | RedisShake | r2dfly |
|---|---|---|
| Sources it can read | Redis, Valkey, Tair, ElastiCache only | The same, plus any JDBC database, Kafka, files, MongoDB |
| One-time migration | Its main use case | Its most tested capability |
| Continuous sync mechanism | Real replication protocol (`sync_reader`) | Keyspace-notification pub/sub |
| Continuous sync durability | Gap-free while the topology holds | Documented gap: a dropped connection loses events |
| Handles a live topology change | No: it crashes | No: it detects the drift but doesn't repair it |
| Checkpoint/resume on restart | None: full resync from scratch | Per-partition checkpointing on the Scan Phase |
| Type fidelity on the target | Replays recorded commands | Rebuilds Dragonfly-native JSON, TopK, and search indexes |
| Operating model | One binary, one config file | A NiFi deployment: JVM, containers, a canvas, a REST API |
| Maturity | Shipping since the redis-port lineage, ~4,400 stars | New to this project; Scan Phase field-tested, the rest is not |

## The decision

If your job is moving data from a Redis-compatible source into Dragonfly, once, RedisShake is
the leaner choice. You run one binary, write one config file, and you're done. Reach for
r2dfly's Scan Phase instead only when you need the extra type fidelity it has and RedisShake
doesn't: rebuilding native search indexes, exact TopK counts, or a Dragonfly-to-Dragonfly fast
path.

If the source isn't Redis at all, a Postgres table, a Kafka topic, a file, a MongoDB
collection, RedisShake can't help you. None of its readers speak anything but the Redis
protocol. Bringing that other data into Dragonfly as a hash or a JSON document, ready for a
search index, is `ExecuteSQLIncremental` and `RecordToKeyRecord`'s actual reason to exist.

If you need a long-running, gap-free continuous sync between Redis-compatible systems only,
neither tool is there yet. RedisShake's replication-protocol stream is closer to gap-free than
r2dfly's pub/sub-based Live Phase, but RedisShake still crashes on a topology change, and
neither tool self-heals a gap once the connection drops. Budget for that limitation regardless
of which one you pick.

If your organization already runs NiFi for data integration and wants the migration's
provenance, lineage, and lifecycle visible through NiFi's own REST API and canvas, r2dfly fits
that operating model. RedisShake doesn't try to. It's a script you run, not a managed flow.
