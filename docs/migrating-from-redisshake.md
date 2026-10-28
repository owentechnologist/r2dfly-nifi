# Migrating from RedisShake

This guide is for someone who knows RedisShake and wants to run the same migration with r2dfly.
It maps RedisShake's settings to r2dfly's, explains where the two behave differently, and walks
through cutover and rollback. For when to choose which tool, see
[r2dfly vs. RedisShake](r2dfly-vs-redisshake.md).

## Translate your config

`scripts/import-shake.sh` translates a RedisShake 4.x `shake.toml` into an r2dfly TOML file:

```bash
./scripts/import-shake.sh shake.toml -o migrate.toml
```

The first line of `migrate.toml` names the script to run it with, for example
`./continuous-migration.sh --toml-file migrate.toml`. The importer translates what the tables below
map. It lists everything else as a note on the terminal and as a comment at the top of the file:
settings with no r2dfly equivalent, places where r2dfly behaves differently, and keys that are not
RedisShake options at all, which RedisShake ignores without a word. Read the notes before you run
the file. A Lua `function` is translated only when it matches one of RedisShake's documented
examples (a prefix filter, a database filter or move, a command-group filter, a prefix rewrite);
any other function is printed for you to port.

`./scripts/import-shake.sh --self-test` checks the translation against built-in cases.

The tables below are what it does, for reading a translation or translating by hand.

## Pick the equivalent run

| RedisShake | r2dfly |
|---|---|
| `sync_reader`, stopped once the copy is done | `scripts/simple-migration.sh` |
| `sync_reader`, left running as a continuous sync | `scripts/continuous-migration.sh --live-source replication` |
| `scan_reader` with `ksn = false` | `scripts/simple-migration.sh` |
| `scan_reader` with `ksn = true` | `scripts/continuous-migration.sh` (keyspace notifications are its default live source) |
| `rdb_reader` | `scripts/simple-migration.sh --source-rdb-file dump.rdb` |
| `aof_reader` | `scripts/simple-migration.sh --source-aof appendonlydir` (a multi-part AOF directory, or one AOF file) |
| `file_writer` with `type = "json"` | `scripts/simple-migration.sh --target-file backup.jsonl`, loaded back with `--source-file` |
| `file_writer` with `type = "cmd"` or `"aof"` | `scripts/simple-migration.sh --target-file backup.aof --target-file-format resp` |

Both scripts deploy NiFi and the r2dfly flow into a container on first use, then run the
migration. Every flag also has a TOML key, so a whole run can live in one file passed with
`--toml-file`. See [the quickstart](quickstart.md) for the full key reference.

## Map the settings

### Source and target

RedisShake puts connection details in separate keys. r2dfly takes one connection string per side.

| RedisShake | r2dfly |
|---|---|
| `[sync_reader]` or `[scan_reader]` `address` | `--source-connection-string redis://host:port` (`[source] connection-string`) |
| `username`, `password` | In the connection string: `redis://user:password@host:port` |
| `tls = true` | A `rediss://` connection string, or `--source-require-tls` |
| `cluster = true` | `--source-connection-mode cluster`, with any node (or a comma-separated list of nodes) as the connection string |
| A Sentinel-managed source | `--source-connection-mode sentinel` |
| `prefer_replica = true` | `--source-read-from replica`, for one-time copies only: a replica lags its master, so a live phase refuses it |
| `dbs = [n]`, `allow_db = [n]` | One database per run, given at the end of the connection string: `redis://host:port/n` |
| `block_db` | No equivalent: run once per database you want |
| `[redis_writer]` `address`, `username`, `password`, `tls`, `cluster` | The same, as `--target-connection-string`, `--target-require-tls` and `--target-connection-mode` |

### Filters

| RedisShake | r2dfly |
|---|---|
| `allow_key_prefix` | `--prefix-only-list a:,b:` (`[migration] prefix-only-list`) |
| `block_key_prefix` | `--prefix-deny-list a:,b:` (`[migration] prefix-deny-list`) |
| `allow_keys`, `allow_key_suffix`, `allow_key_regex`, and their `block_` forms | No equivalent: r2dfly filters by prefix and type only |
| `allow_command_group`, `allow_command` and their `block_` forms, used to pick data types | `--key-types string,hash,...` and `--disallow-key-types` (see below) |
| `skip_unknown_type` | `--disallow-key-types` with the same types |
| `allow_slot`, `block_slot` | No equivalent |
| `function` (a Lua hook) | `--transform-script` (a Groovy hook), or the built-in transforms below |

RedisShake filters commands, not keys. In its RDB phase it writes every key with `RESTORE`,
whatever the key's type, so a command-group filter does not mean "copy only these key types".
r2dfly's `--key-types` reads each key's real `TYPE`, and applies the same filter in the copy and
in the live phase, through renames into and out of a prefix.

### Transforms

RedisShake does these in its Lua `function`. r2dfly has them built in, so the reconciler and the
live phase follow them too. See [Transforms](quickstart.md#transforms) for the details.

| What the Lua function did | r2dfly |
|---|---|
| Rewrite a key prefix | `--rename-prefix old:=new:` (`[transform] rename-prefix`) |
| Move keys to another database | Give each connection string its database: `redis://source/0` to `redis://target/2` |
| Drop a TTL, or lengthen it | `--ttl-strategy strip`, or `--ttl-strategy reset --ttl-reset-offset-ms N` |
| Skip keys by prefix or type | `--prefix-deny-list`, `--key-types` (see Filters) |
| Change values | `--transform-script`, a Groovy script that gets each key's whole record instead of each command |

### Behavior and tuning

| RedisShake | r2dfly |
|---|---|
| `rdb_restore_command_behavior = "rewrite"` | The default: RedisBatchWriter's `conflict-strategy` is `OVERWRITE` |
| `rdb_restore_command_behavior = "skip"` | `conflict-strategy` `SKIP`, set on RedisBatchWriter in NiFi |
| `rdb_restore_command_behavior = "panic"` | `conflict-strategy` `FAIL`, which dead-letters the one key and carries on rather than stopping the run |
| `target_redis_max_qps` | No write rate limit. `--max-source-ops-per-sec` and `--max-source-latency-ms` slow the scan instead, to protect the source. |
| `pipeline_count_limit` | `--batch-size` and its per-type forms |
| `ncpu` | `--parallelism` and `--writer-concurrency`, but measure first: one task each was fastest on the benchmark machine |
| `empty_db_before_sync` | No equivalent: flush the target yourself if it must start empty |
| `aws_psync` | Not supported. On ElastiCache, use the default keyspace-notification live source, with notifications enabled in the parameter group. |
| `status_port` | `scripts/cutover.sh --status`, and NiFi's Prometheus endpoint (`/nifi-api/flow/metrics/prometheus`) |

## What behaves differently

- **The live phase copies keys, not commands.** RedisShake replays each command it reads.
  r2dfly notes which keys changed and copies each key's current value. A key written a million
  times while the flow is busy is copied once, and a command replayed after a crash cannot apply
  twice. In the benchmark, r2dfly caught up after a write burst in about a third of RedisShake's
  time.
- **A restart resumes.** The copy restarts from its checkpointed cursors. The replication live
  source resumes with a partial resync from its saved offset. RedisShake starts over.
- **A cluster can change underneath it.** With `--live-source replication`, a failover continues
  on the promoted replica, resharded keys are copied as they move, and a new shard gets its own
  snapshot. RedisShake's documentation says a topology change stops it.
- **One bad key does not stop the run.** A key that cannot be copied gets a dead-letter record
  with its reason, and the flow continues. Read the records in the dead-letter directory
  (`/opt/nifi/nifi-current/r2dfly-dead-letter` inside the NiFi container). Each key has one
  record. It holds the latest `reason` and `failed_at`, the earlier reasons under `history`
  (newest first, up to 20), the FlowFile's attributes, and its content in `content_base64`. A
  chunk of a large key that failed on its own has a record of its own.
- **Some module types cannot be copied from Redis.** Dragonfly v2.0.2 rejects Redis's Bloom,
  Cuckoo and Count-Min Sketch payloads, and has no time series, T-Digest, graph or vector sets.
  r2dfly dead-letters those keys with the reason. JSON and TopK are rebuilt from their values.
  See [verified behavior](verified-behavior.md) for exactly what was tested.
- **Bulk copy is slower.** RedisShake's `sync_reader` copied small keys about 1.7 to 2.1 times
  faster on the benchmark machine. See the
  [measured throughput](r2dfly-vs-redisshake.md#throughput-measured).
- **Files are read through a Redis, and written per key.** RedisShake parses RDB and AOF files
  itself. r2dfly loads them into a throwaway Redis container (`--source-file-image`, which must
  be as new as the Redis that wrote the file) and copies from that. Its file writer writes one
  record or one set of rebuilding commands per key, not the source's command stream, so a file
  written by one tool cannot be read by the other. Module keys are not written to files.
- **It runs in NiFi.** There is a JVM and a NiFi canvas to look at. NiFi's provenance recording
  is off by default, because at migration volumes it slows the flow and can stall it.

## Cut over

1. Run `scripts/continuous-migration.sh` with the live source you chose, and let it catch up.
2. Run `scripts/cutover.sh`. It waits until the live phase is less than `--max-lag-seconds`
   behind, then tells you to stop writes.
3. Stop application writes to the source, then run `scripts/cutover.sh --writes-stopped`. It
   waits for the flow to drain, runs full reconcile passes until one finds no difference, and
   prints the switch checklist. Keys that were not copied block it until you review their
   dead-letter records and move them out of the directory.
4. Run `scripts/stop-continuous-migration.sh`. It stops every live processor, the reconciler
   included, which would otherwise copy the old source's values back over the application's new
   writes.
5. Point the application at Dragonfly.

## Roll back

To keep the old Redis current after cutover, so you can switch back, run the migration in the
other direction with reconcile passes as its only change source:

```
scripts/continuous-migration.sh --source-connection-string <dragonfly> \
  --target-connection-string <old redis> --live-source reconcile --reconcile-interval-ms 60000
```

Each full pass copies what changed on Dragonfly, so the old Redis trails by up to one interval.
Dragonfly publishes only expiry notifications and does not serve `PSYNC`, so there is no
continuous change stream in this direction.
