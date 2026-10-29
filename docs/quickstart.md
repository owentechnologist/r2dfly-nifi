# Quickstart

## Prerequisites

- A recent Linux distro (RHEL/Fedora, Debian/Ubuntu, etc.) or macOS
- Docker or Podman
- 16GB RAM and 8 CPU cores free for the containers this spins up (NiFi, the Maven build, and
  a small helper "toolbox" image)

You do not need Maven, Java, `redis-cli` or `python3` on the host. `deploy-to-nifi.sh` builds
the NAR in a Maven + JDK 21 container, and `redis-cli`, `python3` and `curl` run from a small
toolbox image (`artifacts/toolbox/Dockerfile`), built on first use. `headless-migration.sh` is
the exception: on its first run it downloads NiFi's Stateless engine with the host's `curl`,
`unzip` and `shasum` (or `sha256sum`). The scripts are plain bash and run unmodified on
RHEL/Fedora, Debian/Ubuntu and macOS.

The project has three directories:

- `scripts/`: every script you run, plus sample TOML configs in `scripts/config/` and the
  headless driver in `scripts/headless/`.
- `docs/`: this file, `TUTORIAL.md`, `debugging-help.md`, the RedisShake guides and the
  verified-behavior notes.
- `artifacts/`: the Maven project, `r2dfly.json`, Lua helpers and the toolbox Dockerfile. The
  scripts use these; you do not run them directly.

All commands below assume you are in `scripts/` (`cd scripts`).

Every container image is a fully qualified `docker.io` reference, so podman never stops to ask
which registry to use. Pulls and builds show a progress bar. Pass `--verbose` (or set
`VERBOSE=true`) to see the full output, for example when a pull or build fails. The first NiFi
start can take a few minutes (JVM cold start, self-signed certificate, NAR extraction).

To build the NAR without touching any container, run `./deploy-to-nifi.sh --build-only`.
`deploy-to-nifi.sh` rejects any argument it does not know.

## Run a migration

`simple-migration.sh` runs a snapshot migration: it scans the source once, makes the target
match that scan, and exits. It does not copy changes made on the source after the scan starts.
`--mode snapshot` is the default and the only value it accepts. For an ongoing sync that keeps
applying source changes until you stop it, use `continuous-migration.sh`
([below](#run-a-continuous-migration)).

```bash
./simple-migration.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

A `rediss://` connection string turns on TLS to that side. You do not need a separate flag.

The script runs eight stages:

1. Checks that the source and target are reachable, and reads each side's product and version.
   When both sides report the same Dragonfly version, it turns on the DUMP/RESTORE fast path
   (`--dfly-to-dfly`).
2. Asks whether to use the default container resources and parallelism, or to set your own (see
   [Container resources and migration parallelism](#container-resources-and-migration-parallelism)).
   It skips the question if you passed `--cpus`, `--memory`, `--parallelism`,
   `--writer-concurrency` or `--defaults`, or if the shell is not interactive.
3. Builds the processor NAR and starts NiFi if it is not running. It prints the NiFi URL and
   login, and in an interactive shell waits for you to press Enter.
4. Rewrites a `localhost` or `127.0.0.1` connection string to an address the NiFi container can
   reach, if the container cannot reach it as given.
5. Logs in to NiFi.
6. Looks for FlowFiles still queued from an earlier run. If it finds any, it asks before it
   stops that run and clears its queues (see
   [Re-running against a container with leftover queued data](#re-running-against-a-container-with-leftover-queued-data)).
7. Configures and starts the copy.
8. Prints a summary: source and target key counts, keys copied this run, dead-letter records
   (keys not copied, each with its reason), and keys that `--key-types` or
   `--disallow-key-types` left out, by type.

If you are moving from RedisShake, `./import-shake.sh shake.toml -o migrate.toml` translates
its config into an r2dfly TOML file. The first comment line names the script to run it with. See
[Migrating from RedisShake](migrating-from-redisshake.md).

## Run a one-time copy without a NiFi server

`headless-migration.sh` runs the same one-time copy on NiFi's Stateless engine, inside the
`apache/nifi` image, with no NiFi server, UI or login. It exits when the copy is complete:

```bash
./headless-migration.sh --toml-file migrate.toml
./headless-migration.sh --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

It takes only `--toml-file`, the two connection strings and `--work-dir`. The TOML file uses the
same keys as `simple-migration.sh`, limited to these:

- `[source]` and `[target]`: `connection-string`, `connection-mode`, `require-tls`
- `[migration]`: `key-types`, `disallow-key-types`, `topk-mode`, `prefix-only-list`,
  `prefix-deny-list`, `batch-size`
- `[transform]`: `rename-prefix`, `ttl-strategy`, `ttl-reset-offset-ms`
- `[run]`: `migration-id`

Any other key is an error that names `simple-migration.sh`, which supports them all. Give the
TOML path as it is; unlike the other scripts, this one does not look in `scripts/config/`.

Things to know about a headless run:

- The first run downloads the Stateless engine (about 40 MB, checksum-verified) into
  `~/.cache/r2dfly`, and every run builds the NAR (`deploy-to-nifi.sh --build-only`).
- Each run keeps its flow, log (`headless.log`) and dead-letter records in
  `./r2dfly-headless-<migration-id>`, or in `--work-dir`.
- It does not copy search indexes, and it scans with one partition.
- `localhost` in a connection string means the container runtime's host (on macOS, its VM).
- `HEADLESS_JVM_OPTS` passes options to the engine's JVM, for example
  `HEADLESS_JVM_OPTS=-Xmx4g`.
- It exits 0 when the copy is complete. On a failure it exits non-zero and prints the last
  lines of the log. If the scan makes no progress and nothing is queued for five minutes, it
  stops with exit 3.
- It does not resume after a crash: run it again, and it copies everything again.

## Run a continuous migration

`continuous-migration.sh` runs the same copy as `simple-migration.sh`, then keeps applying the
source's changes to the target until you stop it. It does not take the file options (RDB, AOF,
`--source-file`, `--target-file`). It refuses `--source-read-from replica`, because a replica
lags its master.

```bash
./continuous-migration.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

`--live-source` picks how the live phase sees changes:

- `keyspace` (default) follows the source's keyspace notifications, so the source must have them
  on (`CONFIG SET notify-keyspace-events AE`). The script checks this before it changes anything
  and stops with that instruction if the setting lacks `A` or `E`. If the source refuses
  `CONFIG GET` (as managed services do), it warns and continues; `--require-config-check` makes
  that an error, and `--skip-keyspace-check` skips the check.
- `replication` follows the source as a replica (`PSYNC`). It needs no notifications, sees module
  commands that publish none, resumes after a restart, and follows a cluster through failovers
  and resharding. The source must accept `PSYNC` from NiFi, which managed services often block.
- `reconcile` has no change stream: each reconcile pass copies what differs. It is the rollback
  path after cutover, with Dragonfly as the source; see `./cutover.sh --help`.

The script starts the flow and exits. NiFi keeps applying source changes until you stop it:

```bash
./stop-continuous-migration.sh
```

It stops only the live-phase processors and MigrationReconciler. It reads the NiFi login from the
container, like the migration scripts. Pass `--nifi-container` if NiFi runs in a container other
than `nifi-redis-migration`, and `--nifi-user` and `--nifi-password` if the container no longer
has the login. Running `simple-migration.sh` against the same NiFi also turns a continuous run's
live phase off, and the next `continuous-migration.sh` turns it back on.

With keyspace notifications, a dropped pub/sub connection loses the changes made during the
outage. The stop script prints the `Keyspace Pub/Sub Disconnects` and `Keyspace Pub/Sub Downtime
(ms)` counters so you can see that it happened. MigrationReconciler, which continuous mode runs
alongside the live phase, repairs those gaps: its default SAMPLE passes find part of a gap each
time, and a FULL pass (`--reconcile-mode FULL`) finds all of it. Run
`./continuous-migration.sh --help` for the other limitations.

The reconciler also deletes every target key whose source key does not exist. That is how a
source `FLUSHALL` reaches the target, and it also deletes data the target held before the
migration. If the target has data of its own, keep it with `--reconcile-spare-target-keys`:
`ALL_KEYS` keeps every such key, `SPARED_PREFIXES` with `--reconcile-spared-prefixes legacy:,cache:`
keeps keys with those prefixes, and `SPARED_TYPES` with `--reconcile-spared-types hash,set` keeps
keys of those types. The counter `Reconcile Target-Only Keys Spared` counts what was kept.

With `--live-source replication`, the reconciler starts only once the replication reader has
connected to every source master, and `continuous-migration.sh` waits up to 300 seconds for that.

To switch the application over, run `./cutover.sh`. It waits until the live phase has caught up,
then tells you to stop application writes. Run it again with `--writes-stopped`: it waits for the
flow to drain, runs FULL reconcile passes until one finds no difference, and prints the steps to
switch. `./cutover.sh --status` checks once and exits 0 when it is safe to cut over.
[Migrating from RedisShake](migrating-from-redisshake.md#cut-over) walks through cutover and
rollback.

## Options

These options apply to `simple-migration.sh` and `continuous-migration.sh` alike, except where
noted. `continuous-migration.sh` takes no `--mode` and no file options. It adds `--live-source`,
`--reconcile-mode`, `--reconcile-interval-ms`, `--reconcile-sample-size`,
`--reconcile-spare-target-keys`, `--reconcile-spared-prefixes`, `--reconcile-spared-types`,
`--keyspace-pattern`, `--event-types`, `--max-queue-depth`, `--skip-keyspace-check`,
`--require-config-check` and `--reconciliation-signals`; see its `--help`. Each script's
`--help` also lists the environment variable for every option.

```
--source-connection-string S  redis://[:password@]host:port[/db], or rediss:// for TLS
--target-connection-string S  same format, for the Dragonfly target
--source-connection-mode M    standalone (default), sentinel or cluster
--target-connection-mode M    same, for the target
--source-require-tls          force TLS to the source (implied by a rediss:// string)
--target-require-tls          force TLS to the target (implied by a rediss:// string)
--nifi-container NAME         container to run NiFi in (default: nifi-redis-migration)
--nifi-user USER              NiFi login; by default the script reads it from the
--nifi-password PASSWORD      container's logs, or from the copy saved inside the container
--migration-id ID             checkpoint id (default: a fresh r2dfly-<epoch seconds> per run)
--cpus N                      CPU limit for the NiFi container (default: none)
--memory SIZE                 memory limit for the NiFi container, e.g. 4g (default: none)
--parallelism N               concurrent tasks for the scan/read side of the flow, and
                              RedisScanReader's Partition Count (default: 1)
--writer-concurrency N        concurrent tasks for the write side of the flow (default: 1)
--key-types LIST              comma-separated Redis types to copy (default: every core type
                              and every module type; each key is copied or dead-lettered with
                              its reason). See run-r2dfly.sh --help for the aliases and the
                              TopK and --dfly-to-dfly notes
--disallow-key-types LIST     comma-separated types to remove from what --key-types would
                              otherwise include (same names and aliases). The summary counts
                              the keys these two filters leave out, by type
--prefix-deny-list LIST       comma-separated key prefixes to skip (checked first)
--prefix-only-list LIST       comma-separated key prefixes to copy, and nothing else
--topk-mode MODE              exact (default) or off
--dfly-to-dfly                force the DUMP/RESTORE fast path on (normally auto-detected)
--no-dfly-to-dfly             force it off
--no-restore-fast-path        copy every core-type key with the typed read and write, instead
                              of probing the target and using DUMP/RESTORE where it works
--source-read-from WHERE      master (default) or replica; replica is for one-time copies
                              only, and continuous-migration.sh refuses it
--max-source-latency-ms N     pause the scan while a page takes longer than N ms (default: 0, off)
--max-source-ops-per-sec N    pause the scan while the source is above N ops/s (default: 0, off)
--no-read-encoding            skip OBJECT ENCODING for each scanned key
--claim-timeout-ms N          take over a dead node's scan partition after N ms (default: 300000)
--batch-size N                RedisBatchWriter's batch size (default: 50); --batch-size-string,
                              -hash, -list, -set, -zset and -stream override it per type
--module-batch-size N         ModuleTypeHandler's batch size (default: 50); --batch-size-json,
                              --batch-size-topk and --batch-size-bloom-cms override it per type
--toml-file FILE              load any of these from a TOML file (see "Using a TOML config
                              file" below)
--disable-provenance          turn NiFi's provenance recording off (the default; see "Disk
                              space and --disable-provenance" below)
--no-disable-provenance       keep provenance recording on, with this project's own limits
                              (1 GB of full lineage history)
--defaults                    skip the resource and parallelism question and use the defaults
--verbose                     show full image pull and build output instead of a progress bar
-y, --yes                     do not ask before stopping and clearing a pre-existing migration
```

## Transforms

These options change what lands on the target. `simple-migration.sh` and
`continuous-migration.sh` both take them, and each has a key in the `[transform]` TOML section.
`headless-migration.sh` takes all but the script, through its TOML file.

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

**TTL strategy.** `preserve` copies the TTL each key has left, less the time the key spent in
the flow, so a copy never outlives its source key. A key whose TTL ran out on the way gets 1 ms
and expires on the target too. `strip` writes every key without a TTL. `reset` adds
`--ttl-reset-offset-ms` to each remaining TTL, which gives the application time to settle after
cutover. A key without a TTL gets none either way. The reconciler expects the TTLs the strategy
produces, so it does not report them as differences.

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
  Bloom) are skipped. NiFi counts them under "Scan Keys Skipped by Type Filter", and the
  summary lists them as keys left out by the key-type filter.
- `--target-file` writes keys as the source holds them, so it refuses `--rename-prefix`,
  `--transform-script` and any `--ttl-strategy` other than `preserve`. Give those when you load
  the file with `--source-file`.
- An existing file is never overwritten; the script stops instead.
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
  members, stream field names and values, and stream consumer group and consumer names. Stream
  entry ids, scores and TTLs stay as they are.
- A FlowFile whose key name is not UTF-8 carries the exact bytes in `redis.key.base64`;
  `redis.key` then shows the name with replacement characters, for people only.

Dragonfly cuts a stream consumer name at its first zero byte (Redis keeps it), so two consumers
whose names differ only after a zero byte become one consumer on Dragonfly. Group names keep
every byte.

## Using a TOML config file

`--toml-file FILE` loads settings from a TOML file, in place of flags or alongside them.
`simple-migration.sh` and `continuous-migration.sh` look for `FILE` as given (relative to the
current directory, or absolute), then in `scripts/config/`, where the sample configs live.
`headless-migration.sh` takes a subset of the keys and looks only where you point it (see
[above](#run-a-one-time-copy-without-a-nifi-server)).

```bash
./simple-migration.sh --toml-file source-cluster-hefty-box-config.toml
```

A flag on the command line overrides the same TOML setting, wherever `--toml-file` appears. So a
config file can hold your usual settings while a flag changes one of them for a single run:

```bash
./simple-migration.sh --toml-file source-cluster-hefty-box-config.toml --parallelism 8
```

Every flag has a TOML key in a section. Most keys are the flag's name without `--` and without
the section's name: `[source] connection-string` is `--source-connection-string`, and
`[migration] batch-size` is `--batch-size`. The exceptions:

- On/off flags take `true` or `false`: `restore-fast-path = false` is `--no-restore-fast-path`,
  and `read-encoding = false` is `--no-read-encoding`.
- `[transform] script` is `--transform-script`, and `[live] source` is `--live-source`.
- The other live-phase flags keep their full name under `[live]`, for example
  `[live] reconcile-mode` (`continuous-migration.sh` only).

Leave out any key you do not need; it falls back to the script's default:

```toml
[source]
connection-string = "redis://source-host:6379"   # --source-connection-string
connection-mode = "standalone"                    # --source-connection-mode
require-tls = false                               # --source-require-tls
read-from = "master"                              # --source-read-from

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
batch-size = 50                                    # --batch-size

# continuous-migration.sh only
[live]
source = "keyspace"                                # --live-source
reconcile-mode = "SAMPLE"                          # --reconcile-mode

[run]
defaults = true                                    # --defaults
verbose = false                                    # --verbose
yes = true                                         # -y / --yes
migration-id = "r2dfly-orders"                     # --migration-id
disable-provenance = true                          # --disable-provenance / --no-disable-provenance
```

An unknown section or key, usually a typo, prints a warning and is ignored. These annotated
samples ship in `scripts/config/`:

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

NiFi's provenance repository logs a lineage event (CREATE, ATTRIBUTES_MODIFIED and so on) for
every FlowFile in the flow. Its disk use grows with the number of FlowFiles, whether or not the
migration is writing to the target. NiFi's default cap
(`nifi.provenance.repository.max.storage.size`) is 10 GB, larger than the whole disk on a small
host.

This stalled real migrations in two ways:

- On a 7.6 GB host, the repository's Lucene index filled the disk. Every write path in NiFi then
  failed with `IOException: No space left on device` in a loop, including RedisBatchWriter's
  writes. The target stayed at 0 keys, and only `nifi-app.log` showed the cause.
- With a cap, the repository still indexes every event. After about 25 million keys over several
  runs on NiFi 2.11, its index queue filled until every session commit in the flow blocked, and
  stayed blocked after the flow was stopped.

**So provenance is off by default.** `--disable-provenance` sets
`nifi.provenance.repository.implementation` to NiFi's `NoOpProvenanceRepository`, which records
nothing. It restarts the NiFi container to apply the change, and does nothing when the setting
is already in place, so it is safe on every run. Failures stay visible: every key that is not
copied has a dead-letter record with its reason, and processors raise bulletins.

Pass `--no-disable-provenance` if you want lineage history and the host has the disk. It
restores `WriteAheadProvenanceRepository` with a 1 GB cap, and restarts the container if an
earlier run turned provenance off.

## Re-running against a container with leftover queued data

If an earlier run left FlowFiles queued (for example, it was interrupted), the next run lists
them and asks:

```
Any pre-existing migration will be stopped and its queued data cleared. Proceed? [Y/n]
```

Answer `n` to stop without changing anything. Pass `-y` or `--yes` to skip the question. A
non-interactive shell proceeds without asking.

## Examples

Local test containers, no TLS:

```bash
./simple-migration.sh \
  --source-connection-string redis://source-redis:6379 \
  --target-connection-string redis://target-dragonfly:6379
```

Cloud target that requires TLS (the `rediss://` scheme turns it on):

```bash
./simple-migration.sh \
  --source-connection-string redis://192.168.1.50:6379 \
  --target-connection-string rediss://default:mypassword@my-instance.dragonflydb.cloud:6385
```

A container whose NiFi login the script cannot read any more (normally it reads the login
itself):

```bash
./simple-migration.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string redis://target-host:6379 \
  --nifi-user <username> --nifi-password <password>
```

## Migrating a Redis/Dragonfly Cluster

`--source-connection-mode` and `--target-connection-mode` each take `standalone` (default),
`sentinel` or `cluster`, so either side or both can be a Redis, Valkey or Dragonfly cluster. In
`cluster` mode, that side's connection string is a comma-separated list of seed nodes:

```bash
./simple-migration.sh \
  --source-connection-mode cluster \
  --source-connection-string redis://node1:6379,redis://node2:6379,redis://node3:6379 \
  --target-connection-string redis://target-dragonfly:6379
```

For a clustered Dragonfly target ("swarm"), set `--target-connection-mode cluster` and pass its
seed nodes. **Assign the target cluster's slots before you run this** (through Dragonfly Cloud,
or a manual `DFLYCLUSTER CONFIG` push). The tool checks that the slots are assigned; it never
assigns them.

Before it configures or starts anything, the script checks each `cluster` side:

- `CLUSTER INFO` must report `cluster_state:ok`.
- The masters' slot ranges must cover 0-16383 with no gaps. No node may be flagged
  `fail` or `fail?`, and no slot may be mid-migration (importing or migrating). Any of these
  stops the run, with the problem named, before anything changes.
- It sums `DBSIZE` over every master, not just the seed node you passed, and prints each node's
  count and the total.

If a target cluster is not fully covered and healthy, the run stops and tells you to fix it
first.

## Container resources and migration parallelism

The default configuration runs everything single-threaded with no CPU/memory limit on the
NiFi container - fine for a quick test or a small dataset. Change it when:

- **The source keyspace is large** (many millions of keys) and reading values is too slow.
  Raise `--parallelism N` to run N concurrent RedisScanReader, RedisTypeDeserializer and
  ModuleTypeHandler tasks. A standalone or Sentinel source is still scanned once, by one task;
  the extra tasks read and decode values in parallel. On a cluster source the masters are dealt
  out among the N scan tasks, each scanned once. Keep `N` under the NiFi container's CPU count
  (see `--cpus` below) and within the source's headroom.
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

`simple-migration.sh` asks for these when you run it interactively. Pass `--defaults`, or any of
the flags, to skip the question. Once NiFi is running,
`run-r2dfly.sh --parallelism N --writer-concurrency N` applies new values to the existing flow
without recreating the container. Unlike the other scripts, `run-r2dfly.sh` does not read the
NiFi login itself: pass `--nifi-user` and `--nifi-password`, or `--nifi-token`. For the sizing
formulas, see section 7 of `artifacts/NIFI_REDIS_MIGRATION_SPEC.md`. To change the same settings
by hand in the NiFi canvas, see "Tuning parallelism and container resources" in `TUTORIAL.md`.

## Advanced usage

`run-r2dfly.sh` configures and starts the flow that the migration scripts set up, and takes
options they do not pass on, such as search index control and progress polling. It needs the
NiFi login (`--nifi-user` and `--nifi-password`, or `--nifi-token`). See its help:

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
  rerun the same query on a schedule. **The processor saves the last fetched row's value for
  that column, not a computed maximum, so the query must `ORDER BY` that column ascending**, or
  rows are skipped without warning. On the first run `${cursor}` is the `Initial Cursor Value`
  property, or an empty string when that is unset. A numeric cursor with no initial value needs
  a guard, for example `CAST(COALESCE(NULLIF('${cursor}', ''), '0') AS INT)`.

Wire them as `ExecuteSQLIncremental` (or `ConsumeKafka`, `FetchFile`, `GetMongoRecord` or any
other NiFi source with a Record Reader) → `RecordToKeyRecord` → `RedisBatchWriter`.

For delimited files (TSV, CSV or a custom delimiter), `simple-ingest-file.sh --toml-file FILE`
builds this flow for you through NiFi's REST API, reading from a local path or an S3-compatible
bucket. `scripts/config/simple-ingest-file-example.toml` shows its settings.

For other sources there is no script, and `r2dfly.json` does not include these processors.
Build the flow in the NiFi canvas or through its REST API. `deploy-to-nifi.sh` loads the NAR
that holds both processors, so they are on the canvas palette once NiFi is up.
