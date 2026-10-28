# Quickstart

## Prerequisites

- A recent Linux distro (RHEL/Fedora, Debian/Ubuntu, etc.) or macOS
- Docker or Podman
- 16GB RAM and 8 CPU cores free for the containers this spins up (NiFi, the Maven build, and
  a small helper "toolbox" image)

Nothing else needs to be installed on the host: the NAR is built with a containerized
Maven+JDK21 (`scripts/deploy-to-nifi.sh`), and `redis-cli`/`python3`/`curl` run from a small
toolbox image (see `artifacts/toolbox/Dockerfile`, built automatically on first use) rather
than requiring any of those on the host. All scripts are plain bash and run unmodified on
RHEL/Fedora, Debian/Ubuntu, and macOS alike.

The project is laid out as `scripts/` (every `*.sh` file), `docs/` (this file plus
`TUTORIAL.md`/`debugging-help.md`), and `artifacts/` (the Maven project, `r2dfly.json`, Lua
helpers, and the toolbox Dockerfile - everything needed to build and run the flow, but nothing
you run directly). All commands below assume you're in `scripts/` (`cd scripts`).

All container images are fully-qualified `docker.io` (Docker Hub) references, so pulling them
never pauses for a registry choice (podman in particular will otherwise prompt interactively
for an unqualified image name if the host has more than one registry configured). Pulling and
building shows a progress bar rather than raw layer-by-layer/dependency-by-dependency output;
pass `--verbose` (or set `VERBOSE=true`) to see the real output instead - useful if a
pull/build actually fails and you want the full log inline rather than only on failure.
Starting NiFi for the first time can take a few minutes (JVM cold start, generating a
self-signed cert, extracting NARs) - the script says so and suggests some light desk yoga
while you wait.

## Run a migration

`simple-migration.sh` runs a snapshot migration: it scans the source once and makes the target
match that scan, then exits. It does not track changes made on the source after the scan
starts - that's `--mode snapshot`, the default and only value. For an ongoing sync that keeps
applying source changes until you stop it, use `continuous-migration.sh`
([below](#run-a-continuous-migration)).

```bash
./simple-migration.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

A `rediss://` connection string automatically enables TLS to that side - no separate flag
needed.

This single command:

1. Validates the source and target are reachable
2. Asks whether to use default container resources/parallelism, or customize them (see
   [Container resources and migration parallelism](#container-resources-and-migration-parallelism)
   below) - skipped if you already passed `--cpus`/`--memory`/`--parallelism`/
   `--writer-concurrency`/`--defaults`, or the shell isn't interactive
3. Builds the processor NAR and starts NiFi (if not already running)
4. Authenticates with NiFi
5. Checks for a pre-existing migration still queued from an earlier run, and if found,
   prompts to stop it and clear its queues before continuing (see below)
6. Configures and starts the migration
7. Prints a summary of keys migrated

## Run a continuous migration

`continuous-migration.sh` runs the same copy as `simple-migration.sh`, then keeps applying the
source's changes to the target until you stop it. By default it follows the source's keyspace
notifications. It takes no file options, and it refuses `--source-read-from replica`, because a
replica lags its master.

```bash
./continuous-migration.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

By default the live phase follows the source's keyspace notifications, so the source must have
them on (`CONFIG SET notify-keyspace-events AE`). The script checks this before it changes
anything and stops with that instruction if the setting is missing. `--live-source replication`
follows the source as a replica instead (`PSYNC`): it needs no notifications, sees module commands
that publish none, resumes after a restart, and follows a cluster through failovers and
resharding. The source must accept `PSYNC` from NiFi, which managed services often block.

The script starts the flow and exits. NiFi keeps running unsupervised and keeps applying source
changes until you stop it:

```bash
./stop-continuous-migration.sh --nifi-container nifi-redis-migration \
  --nifi-user USER --nifi-password PASSWORD
```

With keyspace notifications, a dropped pub/sub connection loses the changes made during the
outage. The stop script prints the `Keyspace Pub/Sub Disconnects` and `Keyspace Pub/Sub Downtime
(ms)` counters so you can see that it happened. MigrationReconciler, which continuous mode runs
alongside the live phase, repairs those gaps: its default SAMPLE passes find part of a gap each
time, and a FULL pass (`--reconcile-mode FULL`) finds all of it. Run
`./continuous-migration.sh --help` for the other limitations.

To switch the application over, run `./cutover.sh`. It says when the live phase has caught up,
and with `--writes-stopped` it confirms the target with a final full reconcile and prints the
steps to switch. [Migrating from RedisShake](migrating-from-redisshake.md#cut-over) walks through
cutover and rollback.

## Options

These options apply to `simple-migration.sh` and `continuous-migration.sh` alike.
`continuous-migration.sh` takes no `--mode` and no file options, and adds `--live-source`,
`--reconcile-mode`, `--reconcile-interval-ms`, `--reconcile-sample-size`, `--keyspace-pattern`,
`--event-types`, `--max-queue-depth`, `--skip-keyspace-check`, `--require-config-check` and
`--reconciliation-signals`; see its `--help`.

```
--source-connection-string S  redis://[:password@]host:port[/db] (or rediss:// for TLS -
                               a rediss:// scheme automatically enables TLS to that side)
--target-connection-string S  same format, for the Dragonfly target
--source-require-tls          force TLS to the source (implied by a rediss:// source string)
--target-require-tls          force TLS to the target (implied by a rediss:// target string)
--nifi-container NAME         container to run NiFi in (default: nifi-redis-migration)
--nifi-user USER              use existing NiFi credentials instead of reading the
--nifi-password PASSWORD      auto-generated ones from the container's logs
--cpus N                      CPU limit for the NiFi container (default: none)
--memory SIZE                 memory limit for the NiFi container, e.g. 4g (default: none)
--parallelism N                concurrent tasks for the scan/read side of the flow, and
                               RedisScanReader's Partition Count (default: 1)
--writer-concurrency N          concurrent tasks for the write side of the flow (default: 1)
--key-types LIST               comma-separated Redis types to scan/migrate (see run-r2dfly.sh
                               --help for the full alias/TopK/dfly-to-dfly interaction notes)
--disallow-key-types LIST       comma-separated Redis types to exclude from whatever
                               --key-types would otherwise include (accepts the same
                               type strings/aliases; see run-r2dfly.sh --help for exactly
                               when this is applied relative to --topk-mode/--dfly-to-dfly)
--prefix-deny-list LIST         comma-separated key prefixes to exclude (see run-r2dfly.sh --help)
--prefix-only-list LIST         comma-separated key prefixes to exclusively migrate (see
                               run-r2dfly.sh --help)
--topk-mode MODE               exact (default) or off (see run-r2dfly.sh --help)
--dfly-to-dfly                 force-enable the DUMP/RESTORE fast path (normally auto-detected)
--no-dfly-to-dfly              force-disable it
--toml-file FILE               load any/all of the above from a TOML config file - see
                               [Using a TOML config file](#using-a-toml-config-file) below
--disable-provenance           turn NiFi's provenance recording off (default: on) - see [Disk space and
                               --disable-provenance](#disk-space-and---disable-provenance) below
--no-disable-provenance        restore this project's own provenance defaults (1GB, full lineage/audit
                               history) instead, if you actually want that and have the disk
--defaults                    skip the resource/parallelism prompt and use the defaults above
--verbose                     show real container image pull/build/download output instead of
                               a progress bar
-y, --yes                     don't prompt before stopping/clearing a pre-existing migration
```

## Transforms

These options change what lands on the target. `simple-migration.sh` and
`continuous-migration.sh` both take them, and each has a key in the `[transform]` TOML section.

```
--rename-prefix RULES        comma-separated old=new key prefix rules, e.g. user:=account:
--ttl-strategy S             preserve (default), strip or reset
--ttl-reset-offset-ms N      milliseconds added to each TTL under --ttl-strategy reset
--transform-script FILE      a Groovy script run on every key record before it is written
```

```toml
[transform]
rename-prefix = "user:=account:,session:=s:"
ttl-strategy = "reset"
ttl-reset-offset-ms = 600000
script = "my-transform.groovy"
```

**Prefix rename.** A key that starts with an old prefix is written under the new one, and the
longest matching old prefix wins. An empty old prefix (`=v2:`) puts a prefix in front of every
key. The rename applies everywhere a key is named: the copy, the live phase's writes and deletes,
the reconciler's comparison, and the `PREFIX` list of rebuilt search indexes. A source key that
already sits under a new prefix would be overwritten by a renamed key, so it is dead-lettered
with that reason instead of copied. The prefix filters (`--prefix-only-list`,
`--prefix-deny-list`) match the source names, before the rename.

**TTL strategy.** `preserve` copies the TTL each key has left. `strip` writes every key without
a TTL. `reset` adds `--ttl-reset-offset-ms` to each remaining TTL, which gives the application
time to settle after cutover. A key without a TTL gets none either way. The reconciler expects
the TTLs the strategy produces, so it does not report them as differences.

**Database remap.** A run copies one database. Give each side's connection string its database,
`redis://source:6379/3` and `redis://target:6379/5`, to copy database 3 into database 5. A
Dragonfly cluster has only database 0.

**Scripted hook.** `--transform-script` places NiFi's `ExecuteGroovyScript` processor, named
"Transform Hook", in front of RedisBatchWriter. Every key record on its way to the target passes
through it: the copy, the live phase, the reconciler's repairs and a file import. Each FlowFile
holds one key record as JSON, the same record `--target-file` writes. The hook turns off the
RESTORE fast path, so every record is JSON rather than a DUMP payload. A run without the flag
bypasses and disables a hook an earlier run placed.

What a RedisShake Lua hook sees, and where it is here:

| RedisShake | r2dfly hook |
|---|---|
| `DB` | `DB`, a script variable holding the source database |
| `KEYS` | `record.key`, one key per record (base64 when `record.key_base64` is true) |
| `TYPE` | `record.type`, the key's real type (`string`, `hash`, `list`, `set`, `zset`, `stream`, or `deleted` for a delete) |
| `CMD`, `ARGV` | None. r2dfly copies keys, not commands: the record holds the key's whole value (`record.value`) and its TTL (`record.ttl_ms`) instead. When `record.base64` is true, the value's byte strings are base64 (see [Binary keys and values](#binary-keys-and-values)). |

A script that drops `tmp:` keys and upper-cases the strings under `order:`:

```groovy
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

session.get(1000).each { flowFile ->
    try {
        def record
        flowFile.read { record = new JsonSlurper().parse(it) }
        if (record.key.startsWith('tmp:')) {
            session.remove(flowFile)
            return
        }
        if (record.type == 'string' && !record.base64 && record.key.startsWith('order:')) {
            record.value = record.value.toUpperCase()
        }
        flowFile.write('UTF-8', JsonOutput.toJson(record))
        REL_SUCCESS << flowFile
    } catch (Exception e) {
        // One bad record goes to the dead-letter directory with this reason; the rest carry on.
        flowFile.'redis.failure.reason' = "transform script: ${e}"
        REL_FAILURE << flowFile
    }
}
```

Things to know before using a hook:

- Use `--rename-prefix` for renames. A rename in a script also has to set the `redis.key`
  attribute, and the live phase's deletes and the reconciler do not pass through the hook.
- Module keys (JSON, TopK, Bloom), search indexes and `--target-file` output do not pass
  through the hook either.
- A large hash or list can arrive in several chunks, each a record with the same key and part
  of the value, so decide per key, not per record.
- The reconciler compares the target with the untransformed source. A hook that changes values
  or drops keys makes every pass find those keys again. Their repairs go through the hook too, so
  the target stays transformed, but no pass is clean and `cutover.sh` never reports SAFE. Decide
  the cutover yourself in that case.
- Catch exceptions per record, as the example does, and send the record to `REL_FAILURE` with a
  `redis.failure.reason`. It then lands in the dead-letter directory with that reason. An
  exception the script lets escape holds back its whole batch, and NiFi retries that batch
  without end.

## Files: RDB and AOF input, backups to a file

`simple-migration.sh` can take a file in place of the source or the target. These options are
for one-time copies only; `continuous-migration.sh` does not take them.

```
--source-rdb-file FILE     copy from an RDB file
--source-aof PATH          copy from an AOF: one file, or a multi-part AOF directory (with a *.manifest)
--source-file-image IMAGE  the image that loads the RDB or AOF (default: redis:latest)
--source-file FILE         copy from a JSON-lines file written with --target-file
--target-file FILE         write the keys to FILE instead of a target (never overwritten)
--target-file-format F     jsonl (default) or resp
```

r2dfly does not parse RDB or AOF files itself. A throwaway Redis container loads the file, the
copy reads from it like any other source, and the container is removed when the script exits.
So filters, dead letters and module handling work as usual. The image must be the Redis version
that wrote the file or newer: an older one cannot load it, and the script stops with the
container's own error. Use a `valkey/valkey` image for a Valkey file and
`redis/redis-stack-server` for one holding module keys.

```bash
./simple-migration.sh --source-rdb-file dump.rdb --target-connection-string redis://localhost:6380
./simple-migration.sh --source-aof appendonlydir --target-connection-string redis://localhost:6380
```

`--target-file` writes each key to a file instead. `jsonl` writes one key per line, in the same
record the flow carries, and `--source-file` loads it back into a target. `resp` writes the
commands that rebuild each key, the same commands a target would receive. A RESP file is also
a single-file AOF, so `--source-aof` loads it, and so does `redis-cli --pipe` into Redis (not
into Dragonfly). Either format works as a backup, or as a dry run that shows what a migration
would write.

```bash
./simple-migration.sh --source-connection-string redis://localhost:6379 --target-file backup.jsonl
./simple-migration.sh --source-file backup.jsonl --target-connection-string redis://localhost:6380
```

Things to know about writing to a file:

- Only string, hash, list, set, sorted set and stream keys are written. Module keys (JSON, TopK,
  Bloom) are skipped, and NiFi counts them under "Scan Keys Skipped by Type Filter".
- TTLs are stored as time remaining, so they count from when the file is loaded, not from when
  it was written. Stream delivery times are absolute.
- A big hash or list that the flow splits into chunks is joined back into one line.
- A key name or value that is not UTF-8 text is written in base64; see
  [Binary keys and values](#binary-keys-and-values).

## Binary keys and values

Redis keys and values are bytes, not text, and r2dfly copies them byte for byte. The flow carries
keys and values as text where it can, so the files, the dead-letter records and the hook stay
readable, and switches to base64 only where the bytes are not valid UTF-8:

- A key record whose key name is not UTF-8 has `"key_base64": true`, and its `key` is base64.
- A key record with any value that is not UTF-8 has `"base64": true`, and every byte string in
  its value is base64: a string's value, hash fields and values, list, set and sorted-set
  members, and stream field names and values. Stream entry ids, scores and TTLs stay as they are.
- A FlowFile whose key name is not UTF-8 carries the exact bytes in `redis.key.base64`;
  `redis.key` then shows the name with replacement characters, for people only.

Stream consumer group and consumer names are carried as UTF-8 text, so a name that is not valid
UTF-8 is not copied exactly.

## Using a TOML config file

Instead of (or alongside) flags, `--toml-file FILE` loads settings from a TOML file. `FILE` is
looked up as given first (a path relative to the current directory, or absolute); if that
doesn't exist, `scripts/config/FILE` is tried next - that's where the sample configs below
live, and the natural place to keep your own.

```bash
./simple-migration.sh --toml-file source-cluster-hefty-box-config.toml
```

Any flag also given on the command line overrides the corresponding TOML setting, regardless
of where `--toml-file` appears among the other flags - so a config file can hold your usual
settings while a flag overrides just one of them for a single run:

```bash
./simple-migration.sh --toml-file source-cluster-hefty-box-config.toml --parallelism 8
```

Every flag has a TOML key, grouped into sections, and most keys are the flag's name without
`--`. The exceptions are the on/off flags, whose key takes `true` or `false`
(`restore-fast-path = false` for `--no-restore-fast-path`), and a few renamed under their section:
`[transform] script` is `--transform-script`, `[live] source` is `--live-source`, and
`[run] migration-id` is `--migration-id`. An unknown key is reported as a warning. You don't
need to set every key - anything left out falls back to the script's own default:

```toml
[source]
connection-string = "redis://source-host:6379"   # --source-connection-string
connection-mode = "standalone"                    # --source-connection-mode
require-tls = false                               # --source-require-tls

# Files instead of a server (simple-migration.sh only); give one source and one target.
# rdb-file = "dump.rdb"                           # --source-rdb-file
# aof = "appendonlydir"                           # --source-aof
# file-image = "redis:latest"                     # --source-file-image
# file = "backup.jsonl"                           # --source-file

[target]
connection-string = "rediss://default:pw@target-host:6385"  # --target-connection-string
# file = "backup.jsonl"                           # --target-file
# file-format = "jsonl"                           # --target-file-format
connection-mode = "standalone"                    # --target-connection-mode
require-tls = false                               # --target-require-tls

[nifi]
container = "nifi-redis-migration"                # --nifi-container
user = ""                                         # --nifi-user
password = ""                                     # --nifi-password

[resources]
cpus = 4                                          # --cpus
memory = "8g"                                     # --memory
parallelism = 2                                   # --parallelism
writer-concurrency = 5                            # --writer-concurrency

[migration]
key-types = "string,hash,ReJSON-RL"                # --key-types
disallow-key-types = ""                            # --disallow-key-types
prefix-deny-list = ""                              # --prefix-deny-list
prefix-only-list = ""                              # --prefix-only-list
topk-mode = "exact"                                # --topk-mode
dfly-to-dfly = true                                # --dfly-to-dfly / --no-dfly-to-dfly

[run]
defaults = true                                    # --defaults
verbose = false                                    # --verbose
yes = true                                         # -y / --yes
migration-id = "r2dfly-orders"                     # --migration-id
disable-provenance = true                          # --disable-provenance / --no-disable-provenance
```

An unrecognized section or key is a warning (likely a typo), not a silent no-op or a hard
error. Several annotated samples ship in `scripts/config/`:

- `source-cluster-hefty-box-config.toml` - a real Redis Cluster source, migrated onto a bigger
  host with parallelism/resources turned up
- `key-filter-example.toml` - only `hash`/`json`/`string` keys, and only under the
  `accounting:shelter:123`, `pet:detail`, and `geo` prefixes
- `disallow-key-types-example.toml` - the subtractive counterpart to `key-filter-example.toml`:
  migrate `--key-types`' own default list except `stream` and Cuckoo Filter keys
- `per-type-batch-size-example.toml` - shrink `--batch-size`/`--module-batch-size` for one or
  two expensive types (large zsets, TopK under `--topk-mode exact`) while leaving the rest at
  the tool's own default
- `simple-config.toml` - a minimal starter config with the common flags stubbed out and
  commented alternatives (e.g. a Dragonfly Cloud `rediss://` target) shown inline

## Disk space and --disable-provenance

NiFi's provenance repository logs a fine-grained lineage event (CREATE, ATTRIBUTES_MODIFIED,
etc.) for every FlowFile that passes through the flow - its disk usage grows with FlowFile
**count**, entirely independent of whether the migration is actually making progress writing to
the target. The default cap (`nifi.provenance.repository.max.storage.size`) is 10GB - larger
than the entire disk on a small host.

This was confirmed directly as the root cause of a real stuck migration: on a 7.6GB host, the
provenance repository's Lucene index grew until the disk filled completely, after which *every*
write path in NiFi - provenance indexing, FlowFile repository checkpointing, and eventually
`RedisBatchWriter`'s own writes to the target - started failing with `IOException: No space
left on device` in a loop. The visible symptom was silent and confusing: the target sat at 0
keys/0 bytes written the whole time, with no clearer error surfaced anywhere in this tool's own
output (only `nifi-app.log` showed the real cause).

A cap alone did not prevent the second way it stalls: the repository still indexes every event
in Lucene, and after about 25 million keys over several runs on NiFi 2.11 its index queue filled until every
session commit in the flow blocked, and stayed blocked after the flow was stopped.

**So provenance is off by default** - `--disable-provenance` is on unless you say otherwise. It
sets `nifi.provenance.repository.implementation` to NiFi's own `NoOpProvenanceRepository`, which
records nothing, restarting the NiFi container to apply it if it isn't already set (a no-op,
safe to pass on every run, once it is). Failures stay visible without it: every failed key has
a dead-letter record with its reason, and processors raise bulletins.

Pass `--no-disable-provenance` instead if you actually want NiFi's fine-grained lineage/audit
history for the migration and the host has the disk for it (this project's own default is a 1GB
cap, well under NiFi's own 10GB shipped default) - this also restarts the container if a
previous default-on run turned it off, restoring the WriteAheadProvenanceRepository with this
project's own defaults.

## Re-running against a container with leftover queued data

If a previous run left FlowFiles queued (e.g. it was interrupted), the next run detects
this, prints what it found, and asks:

```
Any pre-existing migration will be stopped and its queued data cleared. Proceed? [Y/n]
```

Answering `n` aborts without changing anything. Pass `-y`/`--yes` to skip the prompt (e.g.
for non-interactive/scripted runs - a non-interactive shell also proceeds automatically).

## Examples

Local test containers, no TLS:

```bash
./simple-migration.sh \
  --source-connection-string redis://source-redis:6379 \
  --target-connection-string redis://target-dragonfly:6379
```

Cloud target requiring TLS (the `rediss://` scheme takes care of it, no `--target-require-tls`
needed):

```bash
./simple-migration.sh \
  --source-connection-string redis://192.168.1.50:6379 \
  --target-connection-string rediss://default:mypassword@my-instance.dragonflydb.cloud:6385
```

Re-running against a container you already have NiFi credentials for:

```bash
./simple-migration.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string redis://target-host:6379 \
  --nifi-user <username> --nifi-password <password>
```

## Migrating a Redis/Dragonfly Cluster

`--source-connection-mode`/`--target-connection-mode` select `standalone` (default), `sentinel`,
or `cluster` independently per side - either side, or both, can be a real Redis/Valkey/Dragonfly
Cluster. In `cluster` mode, the corresponding `--*-connection-string` is a comma-separated list
of seed nodes instead of a single host:port:

```bash
./simple-migration.sh \
  --source-connection-mode cluster \
  --source-connection-string redis://node1:6379,redis://node2:6379,redis://node3:6379 \
  --target-connection-string redis://target-dragonfly:6379
```

Migrating into a clustered Dragonfly target ("swarm") works the same way - just set
`--target-connection-mode cluster` and pass its seed nodes. **The target cluster's slot layout
must already be assigned before you run this** (via DragonflyDB Cloud, or a manual
`DFLYCLUSTER CONFIG` push) - this tool only verifies that's already true, it never provisions
or bootstraps a cluster's slots itself.

Before configuring or starting anything, each `cluster` side gets a pre-flight check:

- `CLUSTER INFO` must report `cluster_state:ok`.
- Every master's slot ranges must union to full coverage (0-16383) with no gaps or overlaps,
  no node flagged `fail`/`fail?`, and no slot caught mid-migration (an importing/migrating
  marker) - any of these aborts the run with the specific problem named, before anything is
  touched.
- A cluster-aware key count: `DBSIZE` summed across every master node (not just the one seed
  node you passed), printed per-node and as a total, so the pre-migration count is accurate for
  a clustered side instead of reflecting only one shard.

This is a safety gate, not a fix-it tool - if a target cluster isn't fully slot-covered and
healthy, the run aborts and tells you to provision it first.

## Container resources and migration parallelism

The default configuration runs everything single-threaded with no CPU/memory limit on the
NiFi container - fine for a quick test or a small dataset. Change it when:

- **The source keyspace is large** (many millions of keys) and a single-threaded scan is too
  slow. Raise `--parallelism N` to drive N concurrent RedisScanReader/RedisTypeDeserializer/
  ModuleTypeHandler tasks - each scans the full keyspace but only processes the fraction that
  hashes to its own partition, so more of the scan happens in parallel. `N` should stay under
  the NiFi container's CPU count (see `--cpus` below) and the source Redis's own headroom.
- **The target is remote or otherwise slower to write to than to scan from**, and the
  connection queues between RedisScanReader and RedisBatchWriter keep growing. Raise
  `--writer-concurrency N` independently of `--parallelism` - there's no benefit setting it
  higher than the target Dragonfly's core count.
- **The NiFi container itself is CPU- or memory-starved** once you raise parallelism, or you
  want to give it a fixed budget on a shared host. Set `--cpus N` and/or `--memory SIZE`
  (e.g. `--memory 4g`) - these only take effect when the container is first created (not on a
  container `simple-migration.sh`/`deploy-to-nifi.sh` is reusing from an earlier run - remove
  it, or pick a different `--nifi-container` name, to apply new limits). Setting `--memory`
  also grows NiFi's own JVM heap to match (about 75% of the container limit) unless you set
  `NIFI_JVM_HEAP_MAX` yourself - otherwise the container gets more RAM but NiFi's heap stays at
  its small image default and can't use it.

`simple-migration.sh` asks for these interactively the first time (or use `--defaults` /
pass any of the flags directly to skip the prompt, e.g. for scripted runs). Once NiFi is
already running, `run-r2dfly.sh --parallelism N --writer-concurrency N` re-applies new values
to an in-place flow without recreating the container - see `artifacts/NIFI_REDIS_MIGRATION_SPEC.md`
section 7 for the underlying sizing formulas, or `TUTORIAL.md`'s "Tuning parallelism and
container resources" section for how to change the same settings by hand in the NiFi canvas.

## Advanced usage

For anything beyond the default migration - custom key-type filters, TopK modes, batch
tuning, Dragonfly-to-Dragonfly optimizations, prefix filters, search index migration
control, etc. - use `run-r2dfly.sh` directly:

```bash
./run-r2dfly.sh --help
```

## Bringing in other datasources (Postgres, files, Kafka, MongoDB, ...)

Everything above is Redis-as-source. The NAR also ships two processors for bringing
non-Redis data into Dragonfly as a hash or a native JSON document, so search indexes
(`FT.CREATE`) can be built on top of it:

- **`RecordToKeyRecord`** converts any NiFi Record stream - from a DB query, Kafka, files, a
  Mongo document, anything with a matching NiFi Record Reader - into the same envelope
  `RedisBatchWriter` already writes to Dragonfly. `Target Type` picks `hash` (flat, nested
  fields get stringified) or `json` (full nested structure preserved). `Key Format` plus
  per-record RecordPath dynamic properties build the Dragonfly key, e.g. `orders:${id}` with
  a dynamic property `id` set to RecordPath `/id`.
- **`ExecuteSQLIncremental`** runs an arbitrary SQL query against any JDBC source on NiFi's
  own schedule, optionally tracking a cursor across runs so only new rows come back (e.g.
  `SELECT * FROM orders WHERE id > ${cursor} ORDER BY id`). Unlike the bundled
  `QueryDatabaseTableRecord`, this isn't limited to one whole table - any query shape works,
  including joins. Set `Cursor Column` to enable cursor tracking; leave it unset to just
  rerun the same query on a schedule. **The processor persists the last fetched row's value
  for that column, not a computed maximum, so the query itself must `ORDER BY` that column
  ascending** or rows get silently skipped. A numeric cursor also needs a guard against the
  empty string it starts as, e.g. `CAST(COALESCE(NULLIF('${cursor}', ''), '0') AS INT)`.

Wire them as `ExecuteSQLIncremental` (or `ConsumeKafka`/`FetchFile`/`GetMongoRecord`/any other
NiFi source with a Record Reader) → `RecordToKeyRecord` → the existing `RedisBatchWriter`.

**There is no one-command script for this yet** - no `--source-connection-string`-style
wrapper, and `r2dfly.json` doesn't include these processors. Build the flow by hand in the
NiFi canvas or via its REST API today; `deploy-to-nifi.sh` still builds and loads the NAR
containing both processors, so they're available to drag onto the canvas as soon as NiFi is
up.
