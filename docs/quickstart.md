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

## Options

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
--prefix-deny-list LIST         comma-separated key prefixes to exclude (see run-r2dfly.sh --help)
--prefix-only-list LIST         comma-separated key prefixes to exclusively migrate (see
                               run-r2dfly.sh --help)
--topk-mode MODE               exact (default) or off (see run-r2dfly.sh --help)
--dfly-to-dfly                 force-enable the DUMP/RESTORE fast path (normally auto-detected)
--no-dfly-to-dfly              force-disable it
--toml-file FILE               load any/all of the above from a TOML config file - see
                               [Using a TOML config file](#using-a-toml-config-file) below
--disable-provenance           cap NiFi's provenance repository to a small fixed footprint
                               (default: on) - see [Disk space and
                               --disable-provenance](#disk-space-and---disable-provenance) below
--no-disable-provenance        restore NiFi's own provenance defaults (10GB, full lineage/audit
                               history) instead, if you actually want that and have the disk
--defaults                    skip the resource/parallelism prompt and use the defaults above
--verbose                     show real container image pull/build/download output instead of
                               a progress bar
-y, --yes                     don't prompt before stopping/clearing a pre-existing migration
```

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

Every key maps 1:1 to a flag above (same name, minus `--`), grouped into sections. You don't
need to set every key - anything left out falls back to `simple-migration.sh`'s own default:

```toml
[source]
connection-string = "redis://source-host:6379"   # --source-connection-string
connection-mode = "standalone"                    # --source-connection-mode
require-tls = false                               # --source-require-tls

[target]
connection-string = "rediss://default:pw@target-host:6385"  # --target-connection-string
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
prefix-deny-list = ""                              # --prefix-deny-list
prefix-only-list = ""                              # --prefix-only-list
topk-mode = "exact"                                # --topk-mode
dfly-to-dfly = true                                # --dfly-to-dfly / --no-dfly-to-dfly

[run]
defaults = true                                    # --defaults
verbose = false                                    # --verbose
yes = true                                         # -y / --yes
```

An unrecognized section or key is a warning (likely a typo), not a silent no-op or a hard
error. Two annotated samples ship in `scripts/config/`:

- `source-cluster-hefty-box-config.toml` - a real Redis Cluster source, migrated onto a bigger
  host with parallelism/resources turned up
- `key-filter-example.toml` - only `hash`/`json`/`string` keys, and only under the
  `accounting:shelter:123`, `pet:detail`, and `geo` prefixes

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

**This is capped by default** - `--disable-provenance` is on unless you say otherwise, precisely
so nobody has to discover this problem mid-migration the way it was first found. It caps
`nifi.provenance.repository.max.storage.size`/`rollover.size`/`max.storage.time` down to a small
fixed footprint (a few MB, a few minutes' retention) instead of disabling provenance tracking
outright, restarting the NiFi container to apply it if it isn't already set (a no-op, safe to
pass on every run, once it is).

Pass `--no-disable-provenance` instead if you actually want NiFi's fine-grained lineage/audit
history for the migration and the host has the disk for it (NiFi's own default is a 10GB cap) -
this also restarts the container if a previous default-on run already capped it, restoring
NiFi's own defaults.

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
