#!/usr/bin/env bash
# One-command Redis -> Dragonfly continuous migration: validates source/target connectivity,
# reports each side's product and version (INFO SERVER) - which also decides whether to enable the
# --dfly-to-dfly fast path - confirms the source actually emits keyspace notifications, builds and
# deploys the processor NAR, starts NiFi if needed, wires a keyspace-notification-driven Live
# Phase into the flow, and then runs the same initial snapshot simple-migration.sh runs - which is
# what starts the whole flow, Live Phase included. It exits with NiFi still running: the Live
# Phase keeps applying source changes to the target until stop-continuous-migration.sh stops it.
#
# simple-migration.sh is the snapshot-only tool - use that one when a single one-time copy is all
# you want and nothing should keep running afterward.
#
# "Continuous" here means "keeps applying changes", not "guaranteed to lose nothing". If the
# keyspace pub/sub connection drops, or the consumer's internal queue fills, the events in that
# window are counted but never replayed, and nothing repairs the gap - the reconciliation pass
# that would repair it is not built. Read the LIMITATIONS section in --help before planning a
# cutover around this.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/redis-lib.sh"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"
source "$PROJECT_ROOT/scripts/cluster-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") --source-connection-string S --target-connection-string S [options]

Runs an initial snapshot AND leaves a keyspace-notification-driven Live Phase running, so
changes made on the source after the snapshot keep landing on the target. Stop it with
stop-continuous-migration.sh. For a one-time copy that exits when it's done, use
simple-migration.sh instead.

Required:
  --source-connection-string S  redis://[:password@]host:port[/db] (or rediss:// for TLS -
                                 a rediss:// scheme automatically enables TLS to that side,
                                 no separate flag needed). For --source-connection-mode
                                 cluster, a comma-separated seed-node list instead, e.g.
                                 redis://host1:port1,redis://host2:port2
  --target-connection-string S  same format, for the Dragonfly target

Live Phase options:
  --keyspace-pattern P           pub/sub channel pattern the keyspace-event consumer subscribes
                                 to (env KEYSPACE_PATTERN, default: the processor's own default,
                                 __keyevent@*__:*, which covers every database on the source).
                                 Narrow it only if you know exactly which keyevent channels you
                                 need - anything it doesn't match is never seen at all.
  --event-types LIST             comma-separated keyspace event types to act on, e.g. set,del,expired
                                 (env EVENT_TYPES, default: unset - every event type the pattern
                                 delivers)
  --max-queue-depth N            how many events the consumer buffers internally before it starts
                                 dropping them (env MAX_QUEUE_DEPTH, default: the processor's own
                                 default, 10000). Dropped events are counted as "Keyspace Events
                                 Dropped (Queue Full)" and are NOT replayed later.
  --skip-keyspace-check          skip STAGE 1's notify-keyspace-events pre-flight on the source
                                 (env SKIP_KEYSPACE_CHECK, default: false). The check is the only
                                 thing that turns a misconfigured source into a clear error here
                                 rather than a processor that never starts - skipping it means
                                 the consumer fails at schedule time in STAGE 8 instead, which
                                 surfaces only as a NiFi bulletin.
  --reconciliation-signals       route the consumer's topology_drift relationship to a NiFi funnel
                                 queue instead of auto-terminating it (env RECONCILIATION_SIGNALS,
                                 default: false). The queue is durable and can be read back over
                                 NiFi's REST API, but nothing consumes it - limitation 4 below is
                                 still open - so this keeps the drift signals for inspection and
                                 nothing more. Re-running without the flag deletes the queue,
                                 which NiFi refuses while FlowFiles are still sitting in it.

Optional:
  --toml-file FILE               load settings from a TOML config file instead of/alongside
                                 flags (env TOML_FILE). FILE is looked up as given first (a
                                 path relative to the current directory, or absolute); if
                                 that doesn't exist, it's looked up in scripts/config/ next -
                                 that's where the sample configs below live, and the natural
                                 place to keep your own. Any flag also given on the command
                                 line overrides the corresponding TOML setting, regardless of
                                 where --toml-file appears among the other flags. See
                                 scripts/config/*.toml for annotated examples (a hefty-box
                                 cluster-source config, a prefix-filtered one) and
                                 docs/quickstart.md for the full key reference. The Live Phase
                                 settings above live in a [live] section of that file.
  --source-connection-mode M    standalone, sentinel, or cluster (env SOURCE_CONNECTION_MODE,
                                 default: standalone). cluster runs a pre-flight topology
                                 health check and cluster-aware key-count reconciliation
                                 (full 16384-slot coverage, no failed/mid-migration nodes)
                                 before anything is configured or started.
  --target-connection-mode M    same, for the target. A pre-provisioned Dragonfly cluster
                                 ("swarm") target only - this tool verifies the target
                                 cluster's slots are already fully assigned and healthy, it
                                 never bootstraps/provisions them.
  --source-require-tls          force TLS to the source (implied automatically by a
                                 rediss:// source connection string)
  --target-require-tls          force TLS to the target (implied automatically by a
                                 rediss:// target connection string)
  --nifi-container NAME         container to run NiFi in (env NIFI_CONTAINER_NAME,
                                 default: nifi-redis-migration)
  --migration-id ID              checkpoint id forwarded to run-r2dfly.sh (env MIGRATION_ID,
                                 default: a fresh id each run, e.g. r2dfly-1717000000) - also
                                 namespaces SearchIndexExporter/SearchIndexRehydrator's cached
                                 definitions, so overriding it only makes sense alongside
                                 --nifi-user/--nifi-password for resuming a specific prior run's
                                 checkpoint
  --nifi-user USER              use existing NiFi credentials instead of reading the
  --nifi-password PASSWORD      auto-generated ones from the container's logs
  --cpus N                      CPU limit for the NiFi container (env NIFI_CPUS, default: none -
                                 only applies when the container is first created)
  --memory SIZE                 memory limit for the NiFi container, e.g. 4g (env NIFI_MEMORY,
                                 default: none - only applies when the container is first
                                 created; NiFi's JVM max heap is derived from this automatically
                                 unless NIFI_JVM_HEAP_MAX is also set)
  --parallelism N                concurrent tasks driven through the scan/read side of the flow
                                 (RedisScanReader/RedisTypeDeserializer/ModuleTypeHandler) and
                                 RedisScanReader's Partition Count (env PARALLELISM, default: 1)
  --writer-concurrency N          concurrent tasks for RedisBatchWriter, the write side (env
                                 WRITER_CONCURRENCY, default: 1)
  --key-types LIST               comma-separated Redis types to scan/migrate, forwarded as-is
                                 to run-r2dfly.sh (env KEY_TYPES, default: run-r2dfly.sh's own
                                 default - string,hash,set,zset,list,stream,ReJSON-RL). Accepts
                                 the real Redis type strings, or case-insensitive aliases: json
                                 (ReJSON-RL), topk (TopK-TYPE), sortedset (zset), streams
                                 (stream). See run-r2dfly.sh --help for the full type/TopK
                                 handling notes.
  --disallow-key-types LIST       comma-separated Redis types to exclude from whatever
                                 --key-types would otherwise include, forwarded as-is to
                                 run-r2dfly.sh (env DISALLOW_KEY_TYPES, default: unset/none
                                 excluded). Accepts the same type strings/aliases as --key-types
                                 above. See run-r2dfly.sh --help for exactly when this is applied
                                 relative to --topk-mode/--dfly-to-dfly.
  --prefix-deny-list LIST         comma-separated key prefixes to exclude, forwarded as-is to
                                 run-r2dfly.sh (env PREFIX_DENY_LIST, default: unset/no prefixes
                                 excluded). Checked before --prefix-only-list.
  --prefix-only-list LIST         comma-separated key prefixes to exclusively migrate, forwarded
                                 as-is to run-r2dfly.sh (env PREFIX_ONLY_LIST, default: unset/all
                                 prefixes). See run-r2dfly.sh --help for the full prefix-filter
                                 semantics.
  --topk-mode MODE               how to migrate TopK keys, forwarded as-is to run-r2dfly.sh
                                 (env TOPK_MODE, default: run-r2dfly.sh's own default - exact).
                                 See run-r2dfly.sh --help for the full exact/off explanation.
  --dfly-to-dfly                 force-enable run-r2dfly.sh's DUMP/RESTORE fast path (env
                                 DFLY_TO_DFLY=true). Normally decided automatically: STAGE 1
                                 below queries INFO SERVER on both sides and enables this on its
                                 own when both report the exact same dragonfly_version - pass
                                 this explicitly to force it on anyway (e.g. despite a detected
                                 version mismatch - DUMP payloads aren't portable across
                                 mismatched Dragonfly versions, so that's a real risk, not just
                                 a formality).
  --no-dfly-to-dfly              force-disable it instead, even if both sides auto-detect as
                                 matching-version Dragonfly (env DFLY_TO_DFLY=false)
  --batch-size N                 RedisBatchWriter's default batch size, forwarded as-is to
                                 run-r2dfly.sh (env BATCH_SIZE, default: run-r2dfly.sh's own
                                 default - 50). See run-r2dfly.sh --help for the full per-type
                                 override list below and how they interact with this default.
  --batch-size-string N           override --batch-size for string keys only (env BATCH_SIZE_STRING)
  --batch-size-hash N             same, for hash keys (env BATCH_SIZE_HASH)
  --batch-size-list N             same, for list keys (env BATCH_SIZE_LIST)
  --batch-size-set N              same, for set keys (env BATCH_SIZE_SET)
  --batch-size-zset N             same, for sorted set (zset) keys (env BATCH_SIZE_ZSET)
  --batch-size-stream N           same, for stream keys (env BATCH_SIZE_STREAM)
  --module-batch-size N          ModuleTypeHandler's default batch size, forwarded as-is to
                                 run-r2dfly.sh (env MODULE_BATCH_SIZE, default: run-r2dfly.sh's
                                 own default - 50)
  --batch-size-json N              override --module-batch-size for ReJSON-RL keys only (env
                                 BATCH_SIZE_JSON)
  --batch-size-topk N              same, for TopK keys under --topk-mode exact, the default (env
                                 BATCH_SIZE_TOPK)
  --batch-size-bloom-cms N         same, for Bloom/Cuckoo Filter/CMS keys under --dfly-to-dfly
                                 (env BATCH_SIZE_BLOOM_CMS)
  --defaults                    skip the interactive resource/parallelism prompt and use the
                                 defaults above (env ASSUME_DEFAULTS)
  --disable-provenance          cap NiFi's provenance repository to a small fixed footprint
                                 instead of its 10GB-default cap (env DISABLE_PROVENANCE=true,
                                 default: true - already the default, this flag exists mainly
                                 for symmetry/explicitness). NiFi's provenance repository logs
                                 fine-grained per-FlowFile lineage events, so its disk usage
                                 grows with FlowFile *count*, independent of whether the
                                 migration is actually making progress - confirmed directly as
                                 the root cause of a real stuck migration, where a 7.6GB host's
                                 disk filled entirely (well before RedisBatchWriter itself ran
                                 out of room) and every write path in NiFi started failing with
                                 "No space left on device" in a loop, silently, with the target
                                 sitting at 0 keys/0 bytes written the whole time and no clearer
                                 error surfaced anywhere else. Matters more here than for a
                                 snapshot run: the Live Phase keeps producing FlowFiles for as
                                 long as it's left running. Restarts the NiFi container to apply
                                 the cap if it isn't already set (skipped if it already is -
                                 safe to pass on every run). See deploy-to-nifi.sh for the exact
                                 properties this changes.
  --no-disable-provenance        restore this project's own provenance repository defaults (1GB, full
                                 lineage/audit history) instead - only worth it if you actually
                                 want that history and the migration host has the disk for it
                                 (env DISABLE_PROVENANCE=false). Also restarts the container if
                                 a previous default-on run already capped it.
  --verbose                     show real container image pull/build/download output instead
                                 of a progress bar (env VERBOSE)
  -y, --yes                     don't prompt before stopping/clearing a pre-existing migration
  -h, --help                    this help

LIMITATIONS - read these before planning a cutover around continuous mode:
  1. Continuous mode captures ongoing changes but does not yet self-heal a dropped connection's
     gap. If the keyspace pub/sub connection drops, events during the outage are lost; the
     consumer records "Keyspace Pub/Sub Disconnects" and "Keyspace Pub/Sub Downtime (ms)"
     counters so the gap is visible, but nothing repairs it. The reconciliation/repair pass
     this would depend on is not built.
  2. If the consumer's internal queue fills (--max-queue-depth), events are dropped and counted
     as "Keyspace Events Dropped (Queue Full)". Also not repaired.
  3. During the initial snapshot the Live Phase and the scanner both write to the target. The
     live path fetches the key's CURRENT value, so a live update can be overwritten by the
     scanner's older in-flight batch for the same key. That key stays stale until it changes
     again. This is a known narrow window in this first wiring pass.
  4. Cluster topology drift is detected and counted, but nothing acts on it. The topology_drift
     relationship is auto-terminated by default; --reconciliation-signals queues it at a funnel
     instead, which keeps the signals but still leaves nothing reading them.
  5. The Live Phase starts together with the initial scan, when run-r2dfly.sh starts the flow,
     so changes made on the source between this script starting and that moment are not captured
     by the Live Phase (the initial snapshot covers whatever is on the source when it scans).
     Additionally, run-r2dfly.sh stops and starts the whole process group to apply
     configuration, so re-running a migration against a flow whose Live Phase was already
     running tears the pub/sub subscription down for that reconfigure window, and events during
     it are lost.
  6. RedisBatchWriter batches its writes: its deployed defaults are batch-size 50 and
     batch-timeout-ms 30000, so a single live change can wait up to 30 seconds on the target
     before it lands. That is throughput tuning, not a correctness bug, but it means
     "continuous" is not "immediate".

Example:
  $(basename "$0") \\
    --source-connection-string redis://source-host:6379 \\
    --target-connection-string rediss://default:password@target-host:6385
EOF
}

SOURCE_CONNECTION_STRING=""
TARGET_CONNECTION_STRING=""
SOURCE_CONNECTION_MODE="${SOURCE_CONNECTION_MODE:-standalone}"
TARGET_CONNECTION_MODE="${TARGET_CONNECTION_MODE:-standalone}"
SOURCE_REQUIRE_TLS="false"
TARGET_REQUIRE_TLS="false"
NIFI_CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_CPUS="${NIFI_CPUS:-}"
NIFI_MEMORY="${NIFI_MEMORY:-}"
PARALLELISM="${PARALLELISM:-1}"
WRITER_CONCURRENCY="${WRITER_CONCURRENCY:-1}"
KEY_TYPES="${KEY_TYPES:-}"
DISALLOW_KEY_TYPES="${DISALLOW_KEY_TYPES:-}"
PREFIX_DENY_LIST="${PREFIX_DENY_LIST:-}"
PREFIX_ONLY_LIST="${PREFIX_ONLY_LIST:-}"
TOPK_MODE="${TOPK_MODE:-}"
# Left unset (as opposed to "true"/"false") means "decide automatically from the STAGE 1
# server-type detection below"; --dfly-to-dfly/--no-dfly-to-dfly (or the env var) pin it instead.
DFLY_TO_DFLY="${DFLY_TO_DFLY:-}"
# All left unset by default (as opposed to run-r2dfly.sh's own global --batch-size/
# --module-batch-size, which do have real numeric defaults) - unset here means "let
# run-r2dfly.sh apply its own default", forwarded only when actually set (see RUN_ARGS below).
BATCH_SIZE="${BATCH_SIZE:-}"
BATCH_SIZE_STRING="${BATCH_SIZE_STRING:-}"
BATCH_SIZE_HASH="${BATCH_SIZE_HASH:-}"
BATCH_SIZE_LIST="${BATCH_SIZE_LIST:-}"
BATCH_SIZE_SET="${BATCH_SIZE_SET:-}"
BATCH_SIZE_ZSET="${BATCH_SIZE_ZSET:-}"
BATCH_SIZE_STREAM="${BATCH_SIZE_STREAM:-}"
MODULE_BATCH_SIZE="${MODULE_BATCH_SIZE:-}"
BATCH_SIZE_JSON="${BATCH_SIZE_JSON:-}"
BATCH_SIZE_TOPK="${BATCH_SIZE_TOPK:-}"
BATCH_SIZE_BLOOM_CMS="${BATCH_SIZE_BLOOM_CMS:-}"
# Unset means "leave the RedisKeyspaceEventConsumer processor's own default in place" - STAGE 7
# sends JSON null for each of these, which is how the NiFi API explicitly clears an optional
# property back to its default rather than inheriting a previous run's value.
KEYSPACE_PATTERN="${KEYSPACE_PATTERN:-}"
EVENT_TYPES="${EVENT_TYPES:-}"
MAX_QUEUE_DEPTH="${MAX_QUEUE_DEPTH:-}"
SKIP_KEYSPACE_CHECK="${SKIP_KEYSPACE_CHECK:-false}"
RECONCILIATION_SIGNALS="${RECONCILIATION_SIGNALS:-false}"
ASSUME_DEFAULTS="${ASSUME_DEFAULTS:-false}"
DISABLE_PROVENANCE="${DISABLE_PROVENANCE:-true}"
RESOURCE_FLAGS_GIVEN="false"
VERBOSE="${VERBOSE:-false}"
ASSUME_YES="false"
TOML_FILE="${TOML_FILE:-}"
MIGRATION_ID="${MIGRATION_ID:-r2dfly-$(date +%s)}"

# Gives a clear error for a flag missing its value (e.g. a copy-paste that dropped an
# argument) instead of a raw "$2: unbound variable" from set -u.
require_value() {
  if [[ $# -lt 2 ]]; then
    echo "error: $1 requires a value" >&2
    usage; exit 1
  fi
}

# resolve_toml_path <file> - <file> as given first (relative to cwd, or absolute), else
# scripts/config/<file> - that's the "default place to look" the sample configs live in.
resolve_toml_path() {
  local f="$1"
  if [[ -f "$f" ]]; then
    echo "$f"; return 0
  fi
  if [[ -f "$PROJECT_ROOT/scripts/config/$f" ]]; then
    echo "$PROJECT_ROOT/scripts/config/$f"; return 0
  fi
  return 1
}

# load_toml_config <path> - parses a TOML config (via the toolbox image's python3, which has
# tomllib built in - verified against the actual alpine:3.20 base this project's toolbox image
# uses, python 3.12) and eval's the settings it contains as this script's own variables, using
# the exact same names the flags below assign to - so a TOML setting behaves identically to
# having passed the equivalent flag, just applied earlier (any flag given on the command line
# still overrides it, since the normal flag-parsing loop runs after this and simply reassigns).
# Unknown sections/keys are warned about on stderr (likely a typo) rather than silently ignored.
load_toml_config() {
  local path="$1" out
  if ! out="$(toolbox_run python3 -c "$(cat <<'PYEOF'
import sys, tomllib, shlex

MAPPING = {
    ("source", "connection-string"): "SOURCE_CONNECTION_STRING",
    ("source", "connection-mode"): "SOURCE_CONNECTION_MODE",
    ("source", "require-tls"): "SOURCE_REQUIRE_TLS",
    ("target", "connection-string"): "TARGET_CONNECTION_STRING",
    ("target", "connection-mode"): "TARGET_CONNECTION_MODE",
    ("target", "require-tls"): "TARGET_REQUIRE_TLS",
    ("nifi", "container"): "NIFI_CONTAINER_NAME",
    ("nifi", "user"): "NIFI_USER",
    ("nifi", "password"): "NIFI_PASS",
    ("resources", "cpus"): "NIFI_CPUS",
    ("resources", "memory"): "NIFI_MEMORY",
    ("resources", "parallelism"): "PARALLELISM",
    ("resources", "writer-concurrency"): "WRITER_CONCURRENCY",
    ("migration", "key-types"): "KEY_TYPES",
    ("migration", "disallow-key-types"): "DISALLOW_KEY_TYPES",
    ("migration", "prefix-deny-list"): "PREFIX_DENY_LIST",
    ("migration", "prefix-only-list"): "PREFIX_ONLY_LIST",
    ("migration", "topk-mode"): "TOPK_MODE",
    ("migration", "dfly-to-dfly"): "DFLY_TO_DFLY",
    ("migration", "batch-size"): "BATCH_SIZE",
    ("migration", "batch-size-string"): "BATCH_SIZE_STRING",
    ("migration", "batch-size-hash"): "BATCH_SIZE_HASH",
    ("migration", "batch-size-list"): "BATCH_SIZE_LIST",
    ("migration", "batch-size-set"): "BATCH_SIZE_SET",
    ("migration", "batch-size-zset"): "BATCH_SIZE_ZSET",
    ("migration", "batch-size-stream"): "BATCH_SIZE_STREAM",
    ("migration", "module-batch-size"): "MODULE_BATCH_SIZE",
    ("migration", "batch-size-json"): "BATCH_SIZE_JSON",
    ("migration", "batch-size-topk"): "BATCH_SIZE_TOPK",
    ("migration", "batch-size-bloom-cms"): "BATCH_SIZE_BLOOM_CMS",
    ("live", "keyspace-pattern"): "KEYSPACE_PATTERN",
    ("live", "event-types"): "EVENT_TYPES",
    ("live", "max-queue-depth"): "MAX_QUEUE_DEPTH",
    ("live", "skip-keyspace-check"): "SKIP_KEYSPACE_CHECK",
    ("live", "reconciliation-signals"): "RECONCILIATION_SIGNALS",
    ("run", "defaults"): "ASSUME_DEFAULTS",
    ("run", "verbose"): "VERBOSE",
    ("run", "yes"): "ASSUME_YES",
}
RESOURCE_KEYS = {("resources", k) for k in ("cpus", "memory", "parallelism", "writer-concurrency")}

try:
    data = tomllib.load(sys.stdin.buffer)
except tomllib.TOMLDecodeError as e:
    print(f"invalid TOML: {e}", file=sys.stderr)
    sys.exit(1)

resource_field_present = False
for (section, key), varname in MAPPING.items():
    section_data = data.get(section)
    if not isinstance(section_data, dict) or key not in section_data:
        continue
    value = section_data[key]
    value = "true" if value is True else "false" if value is False else str(value)
    print(f"{varname}={shlex.quote(value)}")
    if (section, key) in RESOURCE_KEYS:
        resource_field_present = True

if resource_field_present:
    print("RESOURCE_FLAGS_GIVEN=true")

known_keys_by_section = {}
for section, key in MAPPING:
    known_keys_by_section.setdefault(section, set()).add(key)
for section, contents in data.items():
    if section not in known_keys_by_section:
        print(f"warning: unrecognized TOML section [{section}] in config - ignored", file=sys.stderr)
        continue
    if not isinstance(contents, dict):
        continue
    for key in contents:
        if key not in known_keys_by_section[section]:
            print(f"warning: unrecognized TOML key '{key}' in [{section}] - ignored", file=sys.stderr)
PYEOF
)" < "$path")"; then
    echo "error: failed to parse TOML config '$path'" >&2
    exit 1
  fi
  eval "$out"
}

# --toml-file is handled as its own pre-pass, stripped out of "$@" here, so the settings it
# loads become this script's baseline BEFORE the normal flag-parsing loop below runs - any
# other flag (wherever it appears in the actual command line) then simply overrides that
# baseline the same way it always would, since the loop doesn't know or care where a value
# came from.
ARGS=("$@")
FILTERED_ARGS=()
i=0
while [[ $i -lt ${#ARGS[@]} ]]; do
  if [[ "${ARGS[$i]}" == "--toml-file" ]]; then
    if [[ $((i + 1)) -ge ${#ARGS[@]} ]]; then
      echo "error: --toml-file requires a value" >&2; usage; exit 1
    fi
    TOML_FILE="${ARGS[$((i + 1))]}"
    i=$((i + 2))
    continue
  fi
  FILTERED_ARGS+=("${ARGS[$i]}")
  i=$((i + 1))
done
# A zero-element array expanded as "${arr[@]}" trips "unbound variable" under set -u on this
# project's oldest supported bash (macOS's stock 3.2) - only fixed in bash 4.4+. Guard it
# instead of assuming --toml-file always leaves other args behind (e.g. `--toml-file f.toml`
# alone, with nothing else on the command line, is a valid and expected way to run this).
if [[ ${#FILTERED_ARGS[@]} -gt 0 ]]; then
  set -- "${FILTERED_ARGS[@]}"
else
  set --
fi

if [[ -n "$TOML_FILE" ]]; then
  TOML_PATH="$(resolve_toml_path "$TOML_FILE")" || {
    echo "error: TOML config '$TOML_FILE' not found (looked for it as given, and under $PROJECT_ROOT/scripts/config/)" >&2
    exit 1
  }
  echo "==> loading settings from $TOML_PATH"
  redis_lib_pick_runtime
  load_toml_config "$TOML_PATH"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-connection-string) require_value "$@"; SOURCE_CONNECTION_STRING="$2"; shift 2 ;;
    --target-connection-string) require_value "$@"; TARGET_CONNECTION_STRING="$2"; shift 2 ;;
    --source-connection-mode) require_value "$@"; SOURCE_CONNECTION_MODE="$2"; shift 2 ;;
    --target-connection-mode) require_value "$@"; TARGET_CONNECTION_MODE="$2"; shift 2 ;;
    --source-require-tls) SOURCE_REQUIRE_TLS="true"; shift ;;
    --target-require-tls) TARGET_REQUIRE_TLS="true"; shift ;;
    --nifi-container) require_value "$@"; NIFI_CONTAINER_NAME="$2"; shift 2 ;;
    --migration-id) require_value "$@"; MIGRATION_ID="$2"; shift 2 ;;
    --nifi-user) require_value "$@"; NIFI_USER="$2"; shift 2 ;;
    --nifi-password) require_value "$@"; NIFI_PASS="$2"; shift 2 ;;
    --cpus) require_value "$@"; NIFI_CPUS="$2"; RESOURCE_FLAGS_GIVEN="true"; shift 2 ;;
    --memory) require_value "$@"; NIFI_MEMORY="$2"; RESOURCE_FLAGS_GIVEN="true"; shift 2 ;;
    --parallelism) require_value "$@"; PARALLELISM="$2"; RESOURCE_FLAGS_GIVEN="true"; shift 2 ;;
    --writer-concurrency) require_value "$@"; WRITER_CONCURRENCY="$2"; RESOURCE_FLAGS_GIVEN="true"; shift 2 ;;
    --key-types) require_value "$@"; KEY_TYPES="$2"; shift 2 ;;
    --disallow-key-types) require_value "$@"; DISALLOW_KEY_TYPES="$2"; shift 2 ;;
    --prefix-deny-list) require_value "$@"; PREFIX_DENY_LIST="$2"; shift 2 ;;
    --prefix-only-list) require_value "$@"; PREFIX_ONLY_LIST="$2"; shift 2 ;;
    --topk-mode) require_value "$@"; TOPK_MODE="$2"; shift 2 ;;
    --dfly-to-dfly) DFLY_TO_DFLY="true"; shift ;;
    --no-dfly-to-dfly) DFLY_TO_DFLY="false"; shift ;;
    --batch-size) require_value "$@"; BATCH_SIZE="$2"; shift 2 ;;
    --batch-size-string) require_value "$@"; BATCH_SIZE_STRING="$2"; shift 2 ;;
    --batch-size-hash) require_value "$@"; BATCH_SIZE_HASH="$2"; shift 2 ;;
    --batch-size-list) require_value "$@"; BATCH_SIZE_LIST="$2"; shift 2 ;;
    --batch-size-set) require_value "$@"; BATCH_SIZE_SET="$2"; shift 2 ;;
    --batch-size-zset) require_value "$@"; BATCH_SIZE_ZSET="$2"; shift 2 ;;
    --batch-size-stream) require_value "$@"; BATCH_SIZE_STREAM="$2"; shift 2 ;;
    --module-batch-size) require_value "$@"; MODULE_BATCH_SIZE="$2"; shift 2 ;;
    --batch-size-json) require_value "$@"; BATCH_SIZE_JSON="$2"; shift 2 ;;
    --batch-size-topk) require_value "$@"; BATCH_SIZE_TOPK="$2"; shift 2 ;;
    --batch-size-bloom-cms) require_value "$@"; BATCH_SIZE_BLOOM_CMS="$2"; shift 2 ;;
    --keyspace-pattern) require_value "$@"; KEYSPACE_PATTERN="$2"; shift 2 ;;
    --event-types) require_value "$@"; EVENT_TYPES="$2"; shift 2 ;;
    --max-queue-depth) require_value "$@"; MAX_QUEUE_DEPTH="$2"; shift 2 ;;
    --skip-keyspace-check) SKIP_KEYSPACE_CHECK="true"; shift ;;
    --reconciliation-signals) RECONCILIATION_SIGNALS="true"; shift ;;
    --defaults) ASSUME_DEFAULTS="true"; shift ;;
    --disable-provenance) DISABLE_PROVENANCE="true"; shift ;;
    --no-disable-provenance) DISABLE_PROVENANCE="false"; shift ;;
    --verbose) VERBOSE="true"; shift ;;
    -y|--yes) ASSUME_YES="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

echo "==> mode: continuous (initial snapshot, then an ongoing keyspace-notification sync that keeps running after this script exits - stop it with stop-continuous-migration.sh)"
if [[ -z "$SOURCE_CONNECTION_STRING" || -z "$TARGET_CONNECTION_STRING" ]]; then
  echo "error: --source-connection-string and --target-connection-string are required" >&2
  usage; exit 1
fi
if ! [[ "$PARALLELISM" =~ ^[0-9]+$ ]] || [[ "$PARALLELISM" -lt 1 ]]; then
  echo "error: --parallelism must be a positive integer (got '$PARALLELISM')" >&2
  usage; exit 1
fi
if ! [[ "$WRITER_CONCURRENCY" =~ ^[0-9]+$ ]] || [[ "$WRITER_CONCURRENCY" -lt 1 ]]; then
  echo "error: --writer-concurrency must be a positive integer (got '$WRITER_CONCURRENCY')" >&2
  usage; exit 1
fi
if [[ -n "$MAX_QUEUE_DEPTH" ]]; then
  if ! [[ "$MAX_QUEUE_DEPTH" =~ ^[0-9]+$ ]] || [[ "$MAX_QUEUE_DEPTH" -lt 1 ]]; then
    echo "error: --max-queue-depth must be a positive integer (got '$MAX_QUEUE_DEPTH')" >&2
    usage; exit 1
  fi
fi
SKIP_KEYSPACE_CHECK="$(tr '[:upper:]' '[:lower:]' <<< "$SKIP_KEYSPACE_CHECK")"
RECONCILIATION_SIGNALS="$(tr '[:upper:]' '[:lower:]' <<< "$RECONCILIATION_SIGNALS")"
SOURCE_CONNECTION_MODE="$(tr '[:upper:]' '[:lower:]' <<< "$SOURCE_CONNECTION_MODE")"
TARGET_CONNECTION_MODE="$(tr '[:upper:]' '[:lower:]' <<< "$TARGET_CONNECTION_MODE")"
case "$SOURCE_CONNECTION_MODE" in
  standalone|sentinel|cluster) ;;
  *) echo "error: --source-connection-mode must be standalone, sentinel, or cluster (got '$SOURCE_CONNECTION_MODE')" >&2; usage; exit 1 ;;
esac
case "$TARGET_CONNECTION_MODE" in
  standalone|sentinel|cluster) ;;
  *) echo "error: --target-connection-mode must be standalone, sentinel, or cluster (got '$TARGET_CONNECTION_MODE')" >&2; usage; exit 1 ;;
esac

# A rediss:// scheme already says TLS is required - no need to also pass
# --source-require-tls/--target-require-tls separately in that case.
if [[ "$SOURCE_CONNECTION_STRING" == rediss://* ]]; then
  SOURCE_REQUIRE_TLS="true"
fi
if [[ "$TARGET_CONNECTION_STRING" == rediss://* ]]; then
  TARGET_REQUIRE_TLS="true"
fi

export NIFI_CONTAINER_NAME
export VERBOSE
export DISABLE_PROVENANCE
redis_lib_pick_runtime
nifi_lib_check_disk_space

SOURCE_CONTAINER="$(redis_lib_extract_host "$SOURCE_CONNECTION_STRING")"
TARGET_CONTAINER="$(redis_lib_extract_host "$TARGET_CONNECTION_STRING")"

redis_dbsize() {
  local out
  # Guard + fallback-to-0 mirrors cluster-lib.sh's own DBSIZE handling (cluster_lib_verify,
  # cluster_lib_dbsize_sum_nodes) and this script's own TOTAL_QUEUED below: a transient
  # connection blip here would otherwise abort the whole script under set -e/pipefail.
  out="$(redis_cli "$1" "$2" DBSIZE 2>/dev/null | tr -d '\r')" || true
  [[ "$out" =~ ^[0-9]+$ ]] || out=0
  echo "$out"
}

echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 1/9: Validating source and target connectivity, detecting server type, checking keyspace notifications"
echo "=================================================="
# Retried rather than one-shot because a single PING is too thin a thread to hang the whole
# migration on: this exact check has failed against a healthy remote host and then succeeded
# moments later, unchanged. redis_cli's stderr is captured (it used to go to /dev/null) so a
# genuine misconfiguration reports redis-cli's own reason instead of just "could not connect".
check_connection() {
  local label="$1" container="$2" connstr="$3" reply attempt delay errfile
  errfile="$(mktemp)"
  for attempt in 1 2 3; do
    # The `|| true` is load-bearing under this script's `set -e`/`pipefail`: without it, a
    # connection failure makes this assignment itself fail (redis_cli's non-zero exit propagates
    # through the pipeline), which would abort the script right here - silently, before the
    # deliberate "could not connect" error below ever runs.
    reply="$(redis_cli "$container" "$connstr" PING 2>"$errfile" | tr -d '\r')" || true
    if [[ "$reply" == "PONG" ]]; then
      rm -f "$errfile"
      echo "  $label: OK"
      return 0
    fi
    if [[ $attempt -lt 3 ]]; then
      delay=$((2 ** attempt))
      echo "  $label: attempt $attempt/3 failed, retrying in ${delay}s..." >&2
      sleep "$delay"
    fi
  done
  # An application-level rejection (bad AUTH, wrong user) is a successful exec that prints its
  # error to stdout, so $errfile stays empty and the final reply is the only diagnostic we have.
  if [[ ! -s "$errfile" && -n "$reply" ]]; then
    printf '%s\n' "$reply" >"$errfile"
  fi
  echo "error: could not connect to $label ($connstr)" >&2
  echo "  last error:" >&2
  sed 's/^/    /' "$errfile" >&2
  rm -f "$errfile"
  exit 1
}
check_connection "source" "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING"
check_connection "target" "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING"

SOURCE_SERVER_KIND="$(redis_lib_detect_product_kind "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
TARGET_SERVER_KIND="$(redis_lib_detect_product_kind "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
echo "  source: $(redis_lib_product_label "$SOURCE_SERVER_KIND")"
echo "  target: $(redis_lib_product_label "$TARGET_SERVER_KIND")"

if [[ -z "$DFLY_TO_DFLY" ]]; then
  if [[ "$SOURCE_SERVER_KIND" == dragonfly:* && "$TARGET_SERVER_KIND" == dragonfly:* ]]; then
    SOURCE_DFLY_VERSION="${SOURCE_SERVER_KIND#dragonfly:}"
    TARGET_DFLY_VERSION="${TARGET_SERVER_KIND#dragonfly:}"
    if [[ "$SOURCE_DFLY_VERSION" == "$TARGET_DFLY_VERSION" ]]; then
      DFLY_TO_DFLY="true"
      echo "  both sides are Dragonfly $SOURCE_DFLY_VERSION - enabling --dfly-to-dfly (DUMP/RESTORE fast path for ReJSON-RL/TopK/Bloom/CMS keys)"
    else
      DFLY_TO_DFLY="false"
      echo "  both sides are Dragonfly but versions differ ($SOURCE_DFLY_VERSION vs $TARGET_DFLY_VERSION) - NOT auto-enabling --dfly-to-dfly (DUMP payloads aren't portable across mismatched versions); pass --dfly-to-dfly explicitly to force it anyway"
    fi
  else
    DFLY_TO_DFLY="false"
  fi
fi

# Runs after the --dfly-to-dfly decision above because two of the gaps this warns about (Bloom
# and CMS) depend on it rather than on the target's own command support - see
# redis_lib_warn_capability_gaps. Warnings go to stderr and never abort the run: a capable source
# paired with a less capable target is a legitimate migration for every other key type.
SOURCE_CAPS="$(redis_lib_detect_capabilities "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
TARGET_CAPS="$(redis_lib_detect_capabilities "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
SOURCE_INDEX_COUNT="$(redis_lib_count_search_indexes "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING" "$SOURCE_CAPS")"
echo "  source module support: ${SOURCE_CAPS:-none detected}  (search indexes: $SOURCE_INDEX_COUNT)"
echo "  target module support: ${TARGET_CAPS:-none detected}"
redis_lib_warn_capability_gaps "$SOURCE_CAPS" "$TARGET_CAPS" "$DFLY_TO_DFLY" "$SOURCE_INDEX_COUNT"

# get_dbsize <mode> <container> <connstr> - lightweight key count, cluster-aware (sums across
# masters) when mode is "cluster". Used below for the end-of-run summary, where topology
# health has already been gated by cluster_lib_verify at the start of this stage.
get_dbsize() {
  local mode="$1" container="$2" connstr="$3"
  if [[ "$mode" == "cluster" ]]; then
    cluster_lib_dbsize_sum "$container" "$connstr"
  else
    redis_dbsize "$container" "$connstr"
  fi
}

# A cluster side gets a full pre-flight check (topology health + full slot coverage), not just
# a key count - cluster_lib_verify aborts the script (via set -e, since it returns 1 on
# failure) before anything is configured or started if the cluster isn't healthy.
if [[ "$SOURCE_CONNECTION_MODE" == "cluster" ]]; then
  echo "  verifying source cluster topology"
  SOURCE_DBSIZE_START="$(cluster_lib_verify "source" "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
else
  SOURCE_DBSIZE_START="$(redis_dbsize "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
fi
if [[ "$TARGET_CONNECTION_MODE" == "cluster" ]]; then
  echo "  verifying target cluster topology"
  TARGET_DBSIZE_START="$(cluster_lib_verify "target" "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
else
  TARGET_DBSIZE_START="$(redis_dbsize "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
fi
echo "  source currently has $SOURCE_DBSIZE_START keys"
echo "  target currently has $TARGET_DBSIZE_START keys"

if [[ "$SKIP_KEYSPACE_CHECK" != "true" ]]; then
  KEYSPACE_CHECK_RC=0
  KEYSPACE_EVENTS_VALUE="$(redis_lib_check_keyspace_events "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")" || KEYSPACE_CHECK_RC=$?
  if [[ $KEYSPACE_CHECK_RC -eq 0 ]]; then
    echo "  source keyspace notifications: OK"
  else
    echo "error: the source's notify-keyspace-events is '${KEYSPACE_EVENTS_VALUE:-<empty>}' - the Live Phase needs it to contain both 'A' (every key-event class) and 'E' (keyevent channels), or it sees nothing at all" >&2
    echo "       Enable it with: CONFIG SET notify-keyspace-events AE" >&2
    echo "       Managed services (ElastiCache, MemoryDB and friends) reject CONFIG SET - set the" >&2
    echo "       equivalent parameter-group value there instead and wait for it to apply." >&2
    echo "       The setting must also persist across restarts (redis.conf / the parameter group," >&2
    echo "       not just a runtime CONFIG SET): a source that loses it on a failover silently" >&2
    echo "       stops feeding the Live Phase, with no error on either side." >&2
    echo "       --skip-keyspace-check bypasses this check, but it doesn't make the requirement go" >&2
    echo "       away - it just moves the failure to STAGE 8, where a misconfigured source shows up" >&2
    echo "       only as a NiFi bulletin on a processor that never reaches RUNNING." >&2
    exit 1
  fi
else
  echo "warning: --skip-keyspace-check given - the source's notify-keyspace-events setting was not verified. If it doesn't contain both 'A' and 'E', RedisKeyspaceEventConsumer will fail at schedule time in STAGE 8 instead." >&2
fi

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 2/9: Container resources and migration parallelism"
echo "=================================================="
if [[ "$RESOURCE_FLAGS_GIVEN" != "true" && "$ASSUME_DEFAULTS" != "true" ]]; then
  if [[ -t 0 ]]; then
    read -r -p "Use default container resources (no CPU/memory limit) and parallelism (1x)? [Y/n] " REPLY
    if [[ "$REPLY" == [nN]* ]]; then
      read -r -p "  CPU limit for the NiFi container, e.g. 2 (blank = no limit): " NIFI_CPUS
      read -r -p "  Memory limit for the NiFi container, e.g. 4g (blank = no limit): " NIFI_MEMORY
      read -r -p "  Migration parallelism / partition count [1]: " REPLY_PARALLELISM
      PARALLELISM="${REPLY_PARALLELISM:-1}"
      read -r -p "  Writer concurrency [1]: " REPLY_WRITER
      WRITER_CONCURRENCY="${REPLY_WRITER:-1}"
      if ! [[ "$PARALLELISM" =~ ^[0-9]+$ ]] || [[ "$PARALLELISM" -lt 1 ]]; then
        echo "error: parallelism must be a positive integer (got '$PARALLELISM')" >&2; exit 1
      fi
      if ! [[ "$WRITER_CONCURRENCY" =~ ^[0-9]+$ ]] || [[ "$WRITER_CONCURRENCY" -lt 1 ]]; then
        echo "error: writer concurrency must be a positive integer (got '$WRITER_CONCURRENCY')" >&2; exit 1
      fi
    fi
  else
    echo "  non-interactive shell - using defaults (pass --cpus/--memory/--parallelism/--writer-concurrency to customize)"
  fi
fi
export NIFI_CPUS NIFI_MEMORY
echo "  cpus=${NIFI_CPUS:-<none>}  memory=${NIFI_MEMORY:-<none>}  parallelism=$PARALLELISM  writer-concurrency=$WRITER_CONCURRENCY"

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 3/9: Starting NiFi and deploying the processor NAR"
echo "=================================================="
"$PROJECT_ROOT/scripts/deploy-to-nifi.sh"

# Credentials are only ever printed once, in the container's own logs on first creation - grab
# them now (rather than waiting for STAGE 5's own authentication step) so the connection-info
# message below can tell the user how to log in right away.
if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
  # `|| true`: a no-match grep (e.g. credentials already rotated past the container's log
  # buffer) fails this assignment and would otherwise silently abort the script before the "no
  # NiFi credentials" error below can run.
  CREDS="$($RUNTIME logs "$NIFI_CONTAINER_NAME" 2>&1 | grep -A1 "Generated Username" | tail -2)" || true
  NIFI_USER="$(sed -n '1s/.*\[\(.*\)\]/\1/p' <<< "$CREDS")"
  NIFI_PASS="$(sed -n '2s/.*\[\(.*\)\]/\1/p' <<< "$CREDS")"
  # The credentials line is eventually rotated out of the container's logs for good, so fall
  # back to the copy deploy-to-nifi.sh saved inside the container while it was still there.
  if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
    nifi_lib_load_state "$NIFI_CONTAINER_NAME" "$NIFI_CREDS_FILE_PATH"
  fi
fi
if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
  echo "error: no NiFi credentials given, and none could be read from the container's logs" >&2
  echo "       (they're only printed once, on first container creation) or from the copy" >&2
  echo "       saved inside the container - pass --nifi-user/--nifi-password explicitly if" >&2
  echo "       you already have credentials." >&2
  exit 1
fi

# NiFi's self-signed cert's SAN list only covers localhost/127.0.0.1/the container's own
# internal hostname (see docs/TUTORIAL.md) - a browser on a different machine than this one
# (e.g. you're SSH'd into a remote migration host) needs that exact hostname mapped to this
# machine's IP in its own /etc/hosts, or it hits "Invalid SNI" rather than reaching NiFi at
# all. Printed unconditionally since it's harmless when everything's local (you can just
# ignore it and use localhost:8443 as usual).
#
# The suggested IP has to be this machine's *public*, internet-routable address, not a local/
# private one - the whole point is a browser on a genuinely different machine (e.g. a laptop
# reaching a remote EC2 migration host over SSH) resolving the hostname to something it can
# actually route to, and a private IP (what hostname -I/ipconfig getifaddr report) is only
# reachable from inside the same private network. curl ifconfig.me asks an external service
# what address this host's traffic is actually seen coming from, which is the same address a
# browser anywhere else on the internet would need to reach it at. Falls back to the
# private-IP detection (still useful if this really is a same-machine/same-LAN setup, or
# ifconfig.me is unreachable - e.g. no outbound internet from this host) rather than leaving
# the placeholder unconditionally.
NIFI_INTERNAL_HOSTNAME="$(runtime_exec "$NIFI_CONTAINER_NAME" hostname || true)"
THIS_HOST_IP="$(curl -s --max-time 3 ifconfig.me 2>/dev/null || true)"
[[ "$THIS_HOST_IP" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || THIS_HOST_IP=""
# `|| true` right before the closing paren (not just on `hostname`'s own redirect) - under
# pipefail, a host without GNU hostname's -I flag (e.g. macOS's BSD hostname) makes the whole
# pipeline "fail" even though awk itself succeeds trivially on empty input, and this assignment
# sits as the right-hand side of `&&` - the one list position set -e does NOT exempt - so
# without this, a plain fallback failing would abort the whole script instead of falling
# through to the next line.
[[ -z "$THIS_HOST_IP" ]] && THIS_HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
[[ -z "$THIS_HOST_IP" ]] && THIS_HOST_IP="$(ipconfig getifaddr en0 2>/dev/null || true)"
[[ -z "$THIS_HOST_IP" ]] && THIS_HOST_IP="<this machine's IP>"
if [[ -n "$NIFI_INTERNAL_HOSTNAME" ]]; then
  echo
  echo "To connect to the NiFi server please add $NIFI_INTERNAL_HOSTNAME to your /etc/hosts file mapping it to $THIS_HOST_IP and use this URL: https://$NIFI_INTERNAL_HOSTNAME:8443  You can authenticate with USER: $NIFI_USER and PASSWORD: $NIFI_PASS"
fi

echo
if [[ -t 0 ]]; then
  read -r -p "Press Enter once you've confirmed you can reach the NiFi UI to continue... " _
else
  echo "  non-interactive shell - continuing without waiting for UI confirmation"
fi

# Stashed now, before STAGE 4 below rewrites localhost/127.0.0.1 to the NiFi container's own
# host-gateway alias: simple-troubleshoot.sh's reachability checks run from the host/toolbox
# side (see redis_cli in redis-lib.sh), same as this script's own STAGE 1 check_connection
# above, so they need the original, host-reachable connection strings, not the
# container-network-namespace-adapted ones.
nifi_lib_save_state "$NIFI_CONTAINER_NAME" "$NIFI_STATE_FILE_PATH" \
  SOURCE_CONNECTION_STRING SOURCE_CONNECTION_MODE \
  TARGET_CONNECTION_STRING TARGET_CONNECTION_MODE \
  MIGRATION_ID

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 4/9: Adapting localhost URLs for the NiFi container"
echo "=================================================="
# NiFi runs in its own container with its own network namespace, so "localhost"/"127.0.0.1"
# in a connection string means "the host machine" from where this script runs, but means
# "myself" from inside the NiFi container. Detect that mismatch by actually testing whether
# the NiFi container can open a TCP connection to the address as given, and only rewrite it
# (to the runtime's own host-gateway alias - host.containers.internal for podman,
# host.docker.internal for docker, see deploy-to-nifi.sh's --add-host for the Docker Engine
# on Linux case) if that test fails.
HOST_GATEWAY_ALIAS="host.containers.internal"
[[ "$RUNTIME" == "docker" ]] && HOST_GATEWAY_ALIAS="host.docker.internal"
# The gateway alias isn't always reachable - confirmed directly on a rootless-podman/EC2 box
# using the default slirp4netns network: host.containers.internal resolved fine in the
# container's own /etc/hosts, but a TCP dial to it failed with "Network is unreachable" (not
# just "refused"), so the migration silently stayed pointed at localhost, unable to connect at
# all. This machine's own real IP(s) (what a genuinely separate machine on the network would use
# to reach it) worked instead, so try those next before giving up. `hostname -I` covers Linux;
# `ipconfig getifaddr en0` covers macOS (BSD hostname has no -I flag) for the case this script
# itself runs on a Mac talking to a local podman/docker daemon.
declare -a FALLBACK_HOSTS=("$HOST_GATEWAY_ALIAS")
for ip in $(hostname -I 2>/dev/null || true); do
  FALLBACK_HOSTS+=("$ip")
done
MAC_IP="$(ipconfig getifaddr en0 2>/dev/null || true)"
[[ -n "$MAC_IP" ]] && FALLBACK_HOSTS+=("$MAC_IP")
# nifi_probe_ping <redis-uri> - attempts a real Redis/Dragonfly PING against <redis-uri>, run
# from inside the NiFi container's OWN network namespace, and echoes the reply stripped of \r
# (mirroring check_connection's own `redis_cli ... PING | tr -d '\r'` pattern above) so the
# caller can require it be exactly "PONG". A bare TCP connect isn't enough to prove the thing
# listening on the other end is actually Redis/Dragonfly - confirmed directly: NiFi's own
# internal Site-to-Site socket (nifi.remote.input.socket.port) can coincidentally share a port
# number with an unrelated real Redis elsewhere, and happily accepts the TCP connection itself.
#
# This runs via `--network container:$NIFI_CONTAINER_NAME` (joining NiFi's netns byte-for-byte,
# so "localhost" here means exactly what "localhost" means to NiFi's own JVM) plus the toolbox
# image's redis-cli, rather than calling redis_cli() from redis-lib.sh directly: that helper
# execs redis-cli inside whatever container name it's given, which only works when that
# container already has redis-cli - true for a real Redis/Dragonfly container, but not for
# apache/nifi's own image (confirmed directly against docker.io/apache/nifi:2.11.0: it has
# bash and curl, no redis-cli). Its own fallback would then run redis-cli via the toolbox
# container on --network host instead - testing reachability from this script's own host, not
# from NiFi's netns - which is a different, wrong question here: this project's NiFi container
# runs bridge-networked (see deploy-to-nifi.sh's `-p` port publish, no --network host), so the
# two vantage points can disagree, and host-vantage is exactly the blind spot this check exists
# to close.
nifi_probe_ping() {
  local uri="$1"
  toolbox_lib_ensure_image
  $RUNTIME run --rm --network "container:$NIFI_CONTAINER_NAME" "$TOOLBOX_IMAGE" \
    redis-cli -u "$uri" PING 2>/dev/null | tr -d '\r'
}
adapt_one_connstr_for_nifi() {
  local label="$1" connstr="$2" host port candidate fixed reply
  host="$(redis_lib_extract_host "$connstr")"
  if [[ "$host" != "localhost" && "$host" != "127.0.0.1" ]]; then
    echo "$connstr"
    return 0
  fi
  port="$(redis_lib_extract_port "$connstr")"
  # `|| true`: see check_connection above - a probe failure (refused, TLS garbage, timeout)
  # must degrade to "try the next candidate" rather than aborting under set -e/pipefail.
  reply="$(nifi_probe_ping "$connstr")" || true
  if [[ "$reply" == "PONG" ]]; then
    echo "  $label ($host:$port): answered PING from the NiFi container - leaving as-is" >&2
    echo "$connstr"
    return 0
  fi
  echo "  $label ($host:$port): did NOT answer PING from the NiFi container - trying ${FALLBACK_HOSTS[*]}" >&2
  for candidate in "${FALLBACK_HOSTS[@]}"; do
    fixed="${connstr/$host/$candidate}"
    reply="$(nifi_probe_ping "$fixed")" || true
    if [[ "$reply" == "PONG" ]]; then
      echo "  $label: $candidate:$port answered PING - using it instead" >&2
      echo "$fixed"
      return 0
    fi
  done
  echo "  warning: none of {${FALLBACK_HOSTS[*]}}:$port answered PING from the NiFi container - leaving $label URL unchanged; the migration will likely fail to connect" >&2
  echo "$connstr"
}
# adapt_connection_string_for_nifi <label> <connstr> - applies adapt_one_connstr_for_nifi to
# each comma-separated node, so a --*-connection-mode cluster seed list (one full URI per
# node) gets each node rewritten independently - e.g. a local multi-container cluster test
# using several distinct localhost:port entries. A one-element split for a plain
# STANDALONE/SENTINEL connstr, unchanged from before.
adapt_connection_string_for_nifi() {
  local label="$1" connstr="$2" part
  local -a parts out=()
  IFS=',' read -ra parts <<< "$connstr"
  for part in "${parts[@]}"; do
    out+=("$(adapt_one_connstr_for_nifi "$label" "$part")")
  done
  local IFS=','
  echo "${out[*]}"
}
SOURCE_CONNECTION_STRING="$(adapt_connection_string_for_nifi source "$SOURCE_CONNECTION_STRING")"
TARGET_CONNECTION_STRING="$(adapt_connection_string_for_nifi target "$TARGET_CONNECTION_STRING")"

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 5/9: Authenticating with NiFi"
echo "=================================================="
echo "  authenticated as $NIFI_USER"
CONTAINER_NAME="$NIFI_CONTAINER_NAME"
nifi_lib_init

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 6/9: Checking for a pre-existing migration"
echo "=================================================="
# This has to happen BEFORE the Live Phase is wired and started (STAGES 7 and 8): clearing a
# pre-existing migration drops every queued FlowFile in the process group, and the Live Phase's
# captured keyspace events sit in those same queues. Run it afterwards and it would throw away
# exactly the events the Live Phase exists to collect.
#
# `|| true` throughout STAGE 6: under set -e/pipefail, a transient NiFi API hiccup would
# otherwise abort the script silently at whichever assignment hit it, before the deliberate
# error checks below (or the equally deliberate "no queue" fallback) ever get a chance to run.
ROOT_ID="$(nifi_cli get-root-id -ot simple | tr -d '[:space:]')" || true
if [[ -z "$ROOT_ID" ]]; then
  echo "error: could not reach NiFi's REST API to determine the root process group id (check NiFi container health/logs)" >&2
  exit 1
fi
PG_ID="$(nifi_api_get "flow/process-groups/$ROOT_ID" | py3 "
import json, sys
d = json.load(sys.stdin)
for g in d['processGroupFlow']['flow']['processGroups']:
    if g['component']['name'] == 'R2Dfly Migration':
        print(g['component']['id'])
        break
")" || true
if [[ -z "$PG_ID" ]]; then
  echo "error: no 'R2Dfly Migration' process group found - deploy-to-nifi.sh should have created it" >&2
  exit 1
fi

CONN_JSON="$(nifi_api_get "process-groups/$PG_ID/connections")" || true
TOTAL_QUEUED="$(py3 "
import json, sys
d = json.load(sys.stdin)
# queuedCount is a JSON string, and NiFi comma-formats it once it's large enough (e.g. '6,930'),
# not just the plain digit string ('0') int() alone can handle - strip separators first.
def queued(c):
    return int(c['status']['aggregateSnapshot']['queuedCount'].replace(',', ''))
print(sum(queued(c) for c in d['connections']))
" <<< "$CONN_JSON")" || true
[[ "$TOTAL_QUEUED" =~ ^[0-9]+$ ]] || TOTAL_QUEUED=0

if [[ "$TOTAL_QUEUED" -gt 0 ]]; then
  echo "  found $TOTAL_QUEUED FlowFiles queued from a previous run:"
  py3 "
import json, sys
d = json.load(sys.stdin)
for c in d['connections']:
    size = int(c['status']['aggregateSnapshot']['queuedCount'].replace(',', ''))
    if size > 0:
        name = c['component'].get('name') or (c['component']['source']['name'] + ' -> ' + c['component']['destination']['name'])
        print(f'    {name}: {size} queued')
" <<< "$CONN_JSON" || true
  if [[ "$ASSUME_YES" != "true" ]]; then
    if [[ -t 0 ]]; then
      read -r -p "  Any pre-existing migration will be stopped and its queued data cleared. Proceed? [Y/n] " REPLY
      if [[ "$REPLY" == [nN]* ]]; then
        echo "  aborted - nothing was changed."
        exit 1
      fi
    else
      echo "  non-interactive shell - proceeding automatically (pass --yes to suppress this notice)"
    fi
  fi

  echo "  stopping the flow and clearing queues"
  nifi_cli pg-stop -pgid "$PG_ID" || true
  sleep 3

  CONN_IDS_FILE="$(mktemp)"
  py3 "
import json, sys
d = json.load(sys.stdin)
for c in d['connections']:
    print(c['id'])
" > "$CONN_IDS_FILE" <<< "$CONN_JSON" || true
  while IFS= read -r conn_id; do
    [[ -n "$conn_id" ]] || continue
    RESP="$(nifi_api_post "flowfile-queues/$conn_id/drop-requests" '{}')" || true
    DROP_ID="$(py3 "import json,sys; print(json.load(sys.stdin)['dropRequest']['id'])" 2>/dev/null <<< "$RESP" || true)"
    if [[ -n "$DROP_ID" ]]; then
      for i in {1..15}; do
        STATUS="$(nifi_api_get "flowfile-queues/$conn_id/drop-requests/$DROP_ID" | py3 "import json,sys; d=json.load(sys.stdin)['dropRequest']; print('done' if d['finished'] else 'pending')" 2>/dev/null || true)"
        if [[ "$STATUS" == "done" ]]; then
          break
        fi
        sleep 1
      done
    fi
  done < "$CONN_IDS_FILE"
  rm -f "$CONN_IDS_FILE"
  echo "  queues cleared"
else
  echo "  no pre-existing migration state found"
fi

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 7/9: Wiring the Live Phase"
echo "=================================================="
# --no-export-baseline: the builder can write the project's own artifacts/r2dfly.json baseline as
# a side effect, and a user's migration run has no business rewriting a checked-in project
# artifact. An export taken mid-run would also have this run's connection strings, parallelism and
# Live Phase tuning baked into it, which is not what that baseline is supposed to represent.
BUILD_LIVE_ARGS=()
[[ "$RECONCILIATION_SIGNALS" == "true" ]] && BUILD_LIVE_ARGS+=(--reconciliation-signals)
# ${arr+"${arr[@]}"}: expanding an empty array as "${arr[@]}" is an unbound-variable error under
# set -u on bash 3.2, the same trap the --toml-file filtering above guards against.
"$PROJECT_ROOT/scripts/build-live-phase-flow.sh" \
  --nifi-container "$NIFI_CONTAINER_NAME" \
  --nifi-user "$NIFI_USER" --nifi-password "$NIFI_PASS" \
  --no-export-baseline ${BUILD_LIVE_ARGS+"${BUILD_LIVE_ARGS[@]}"}

# One pass over the processor list for all three ids (instead of three separate py3 invocations
# re-parsing the same JSON) - each py3 call is a container exec via the toolbox, not free.
# Pre-seeding every name to '' keeps the '|'-joined field count constant, so the read below
# always lands each id in the right variable even when one processor is missing.
LIVE_PROC_LIST_JSON="$(nifi_api_get "process-groups/$PG_ID/processors")" || true
LIVE_PROC_IDS="$(echo "$LIVE_PROC_LIST_JSON" | py3 "
import json, sys
d = json.load(sys.stdin)
order = ['RedisKeyspaceEventConsumer', 'RedisSingleKeyFetch', 'DeleteRedisKey']
ids = {name: '' for name in order}
for p in d['processors']:
    t = p['component']['type']
    for name in order:
        if not ids[name] and t.endswith(name):
            ids[name] = p['component']['id']
            break
print('|'.join(ids[name] for name in order))
")" || true
IFS='|' read -r PROC_KEYSPACE PROC_FETCH PROC_DELETE <<< "$LIVE_PROC_IDS"
if [[ -z "$PROC_KEYSPACE" ]]; then
  echo "error: could not find the RedisKeyspaceEventConsumer processor in $PG_ID - build-live-phase-flow.sh should have created it" >&2; exit 1
fi
if [[ -z "$PROC_FETCH" ]]; then
  echo "error: could not find the RedisSingleKeyFetch processor in $PG_ID - build-live-phase-flow.sh should have created it" >&2; exit 1
fi
if [[ -z "$PROC_DELETE" ]]; then
  echo "error: could not find the DeleteRedisKey processor in $PG_ID - build-live-phase-flow.sh should have created it" >&2; exit 1
fi

# json_or_null <value> - "null" (unquoted, NiFi's way of explicitly clearing an optional property
# back to its own default) or the value quoted as a JSON string. NiFi's processor-property PUT is
# a partial merge, not a full replace: a key simply absent from the body leaves whatever value a
# PREVIOUS run left there untouched. All three Live Phase properties below are optional, so each
# one goes into every PUT body one way or the other - otherwise an unset --keyspace-pattern would
# silently inherit a narrow pattern some earlier run set, and the Live Phase would quietly see
# only part of the keyspace with nothing logged as wrong.
json_or_null() {
  if [[ -n "$1" ]]; then echo "\"$1\""; else echo "null"; fi
}

# nifi_api_put_checked <path> <body> - like nifi_api_put, but treats a non-JSON response (NiFi
# returns a plain-text body, not JSON, for some rejections - e.g. "Cannot modify configuration
# of ... because it is currently not disabled") as a hard failure instead of silently discarding
# it.
nifi_api_put_checked() {
  local path="$1" body="$2" resp
  # `|| true`: a bare connectivity failure here (as opposed to a non-JSON rejection body, which
  # this function exists to catch) would otherwise abort the script via set -e before ever
  # reaching this function's own check below - an empty $resp fails that same json.load check
  # anyway, so it cascades into the same "PUT ... was rejected" error correctly.
  resp="$(nifi_api_put "$path" "$body")" || true
  if ! echo "$resp" | py3 "import json,sys; json.load(sys.stdin)" >/dev/null 2>&1; then
    echo "error: PUT $path was rejected: $resp" >&2
    exit 1
  fi
  echo "$resp"
}

echo "  keyspace-pattern=${KEYSPACE_PATTERN:-<processor default>}  event-types=${EVENT_TYPES:-<all>}  max-queue-depth=${MAX_QUEUE_DEPTH:-<processor default>}"
CONSUMER_VER="$(nifi_current_version "processors/$PROC_KEYSPACE")" || true
LIVE_PROPS="\"keyspace-pattern\":$(json_or_null "$KEYSPACE_PATTERN")"
LIVE_PROPS="$LIVE_PROPS,\"event-types\":$(json_or_null "$EVENT_TYPES")"
LIVE_PROPS="$LIVE_PROPS,\"max-queue-depth\":$(json_or_null "$MAX_QUEUE_DEPTH")"
LIVE_BODY="{\"revision\":{\"version\":$CONSUMER_VER},\"component\":{\"id\":\"$PROC_KEYSPACE\",\"config\":{\"properties\":{$LIVE_PROPS}}}}"
nifi_api_put_checked "processors/$PROC_KEYSPACE" "$LIVE_BODY" > /dev/null

# Wired, not started. The two connection-pool controller services these processors depend on are
# still blank and DISABLED at this point - deploy-to-nifi.sh leaves them that way deliberately
# (its "the two connection-pool services are expected to stay DISABLED/INVALID here" comment), and
# nothing configures or enables them until STAGE 8's run-r2dfly.sh does. NiFi refuses to start a
# processor whose required controller service is disabled and reports it INVALID, so trying to
# start the Live Phase here would fail outright on a first run. run-r2dfly.sh's own pg-start
# brings the whole process group up at the end, the three Live Phase processors included, which
# is where they actually come alive.
echo "  Live Phase wired - it starts with the rest of the flow in STAGE 8, once the connection pools are configured and enabled"

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 8/9: Initial snapshot and Live Phase start"
echo "=================================================="
RUN_ARGS=(
  --nifi-container "$NIFI_CONTAINER_NAME"
  --nifi-user "$NIFI_USER" --nifi-password "$NIFI_PASS"
  --migration-id "$MIGRATION_ID"
  --source-connection-string "$SOURCE_CONNECTION_STRING"
  --source-connection-mode "$SOURCE_CONNECTION_MODE"
  --target-connection-string "$TARGET_CONNECTION_STRING"
  --target-connection-mode "$TARGET_CONNECTION_MODE"
  --parallelism "$PARALLELISM"
  --writer-concurrency "$WRITER_CONCURRENCY"
  --start
)
[[ "$SOURCE_REQUIRE_TLS" == "true" ]] && RUN_ARGS+=(--source-require-tls)
[[ "$TARGET_REQUIRE_TLS" == "true" ]] && RUN_ARGS+=(--target-require-tls)
[[ -n "$KEY_TYPES" ]] && RUN_ARGS+=(--key-types "$KEY_TYPES")
[[ -n "$DISALLOW_KEY_TYPES" ]] && RUN_ARGS+=(--disallow-key-types "$DISALLOW_KEY_TYPES")
[[ -n "$PREFIX_DENY_LIST" ]] && RUN_ARGS+=(--prefix-deny-list "$PREFIX_DENY_LIST")
[[ -n "$PREFIX_ONLY_LIST" ]] && RUN_ARGS+=(--prefix-only-list "$PREFIX_ONLY_LIST")
[[ -n "$TOPK_MODE" ]] && RUN_ARGS+=(--topk-mode "$TOPK_MODE")
[[ "$DFLY_TO_DFLY" == "true" ]] && RUN_ARGS+=(--dfly-to-dfly)
[[ -n "$BATCH_SIZE" ]] && RUN_ARGS+=(--batch-size "$BATCH_SIZE")
[[ -n "$BATCH_SIZE_STRING" ]] && RUN_ARGS+=(--batch-size-string "$BATCH_SIZE_STRING")
[[ -n "$BATCH_SIZE_HASH" ]] && RUN_ARGS+=(--batch-size-hash "$BATCH_SIZE_HASH")
[[ -n "$BATCH_SIZE_LIST" ]] && RUN_ARGS+=(--batch-size-list "$BATCH_SIZE_LIST")
[[ -n "$BATCH_SIZE_SET" ]] && RUN_ARGS+=(--batch-size-set "$BATCH_SIZE_SET")
[[ -n "$BATCH_SIZE_ZSET" ]] && RUN_ARGS+=(--batch-size-zset "$BATCH_SIZE_ZSET")
[[ -n "$BATCH_SIZE_STREAM" ]] && RUN_ARGS+=(--batch-size-stream "$BATCH_SIZE_STREAM")
[[ -n "$MODULE_BATCH_SIZE" ]] && RUN_ARGS+=(--module-batch-size "$MODULE_BATCH_SIZE")
[[ -n "$BATCH_SIZE_JSON" ]] && RUN_ARGS+=(--batch-size-json "$BATCH_SIZE_JSON")
[[ -n "$BATCH_SIZE_TOPK" ]] && RUN_ARGS+=(--batch-size-topk "$BATCH_SIZE_TOPK")
[[ -n "$BATCH_SIZE_BLOOM_CMS" ]] && RUN_ARGS+=(--batch-size-bloom-cms "$BATCH_SIZE_BLOOM_CMS")

"$PROJECT_ROOT/scripts/run-r2dfly.sh" "${RUN_ARGS[@]}"

processor_run_status() {
  nifi_api_get "processors/$1" | py3 "
import json, sys
d = json.load(sys.stdin)
print(d.get('status', {}).get('runStatus', ''))
"
}

# report_processor_failure <proc-id> <label> - dumps both validation errors AND recent bulletins
# for a processor that wouldn't start. Bulletins are the load-bearing half: a processor whose
# @OnScheduled throws never reaches RUNNING and has no validation errors at all, which is exactly
# what RedisKeyspaceEventConsumer does when the source's notify-keyspace-events setting is wrong.
# Without the bulletins that failure looks like a bare timeout with nothing to act on.
report_processor_failure() {
  local proc_id="$1" label="$2"
  echo "  $label validation errors:" >&2
  nifi_api_get "processors/$proc_id" | py3 "
import json, sys
d = json.load(sys.stdin)
errs = d.get('component', {}).get('validationErrors') or []
for e in errs:
    print(f'    {e}')
if not errs:
    print('    (none reported - see the bulletins below)')
" >&2 || true
  echo "  $label recent bulletins:" >&2
  nifi_api_get "flow/bulletin-board?limit=100" | py3 "
import json, sys
d = json.load(sys.stdin)
found = False
for b in d.get('bulletinBoard', {}).get('bulletins', []):
    inner = b.get('bulletin', {})
    if b.get('sourceId') == '$proc_id' or inner.get('sourceId') == '$proc_id':
        found = True
        print(f\"    [{inner.get('level', '')}] {inner.get('timestamp', '')} {inner.get('message', '')}\")
if not found:
    print('    (none)')
" >&2 || true
}

# start_and_wait_running <proc-id> <label> [timeout-seconds] - starts a processor and polls until
# NiFi actually reports it Running. The revision is re-read immediately before the write because
# every write to a component increments it and a stale one is rejected outright. Returns 1 on
# timeout so the caller decides what a failure means.
start_and_wait_running() {
  local proc_id="$1" label="$2" timeout="${3:-60}" waited=0 ver status
  ver="$(nifi_current_version "processors/$proc_id")" || true
  nifi_api_put "processors/$proc_id/run-status" "{\"revision\":{\"version\":$ver},\"state\":\"RUNNING\",\"disconnectedNodeAcknowledged\":false}" > /dev/null
  while true; do
    status="$(processor_run_status "$proc_id")" || status=""
    if [[ "$status" == "Running" ]]; then
      echo "  $label: Running"
      return 0
    fi
    if [[ "$waited" -ge "$timeout" ]]; then
      echo "error: $label did not reach RUNNING within ${timeout}s (last status: ${status:-unknown})" >&2
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

# run-r2dfly.sh stops the WHOLE process group to apply its configuration (`nifi_cli pg-stop -pgid
# $PG_ID`, its line 727) and starts it again at the end (`pg-start`, its line 1304), so the Live
# Phase comes up underneath this script rather than by anything here. This pass confirms it
# actually did, instead of assuming it. Checked downstream-first, and anything still stopped is
# started in that same order, so the consumer never emits a keyspace event into a chain whose next
# processor is stopped.
echo
echo "  verifying the Live Phase came up with the flow"
for live_proc in "$PROC_DELETE:DeleteRedisKey" "$PROC_FETCH:RedisSingleKeyFetch" "$PROC_KEYSPACE:RedisKeyspaceEventConsumer"; do
  LIVE_PROC_ID="${live_proc%%:*}"
  LIVE_PROC_LABEL="${live_proc#*:}"
  LIVE_PROC_STATUS="$(processor_run_status "$LIVE_PROC_ID")" || LIVE_PROC_STATUS=""
  if [[ "$LIVE_PROC_STATUS" == "Running" ]]; then
    echo "  $LIVE_PROC_LABEL: Running"
    continue
  fi
  echo "  $LIVE_PROC_LABEL: ${LIVE_PROC_STATUS:-unknown} - starting it"
  # A hard failure, never a warning: a consumer that silently isn't running is precisely the
  # failure continuous mode exists to avoid, and it would look identical to a quiet source.
  if ! start_and_wait_running "$LIVE_PROC_ID" "$LIVE_PROC_LABEL"; then
    report_processor_failure "$LIVE_PROC_ID" "$LIVE_PROC_LABEL"
    echo "error: the Live Phase is not running, so source changes are NOT being applied to the target. The initial snapshot above did complete. Run diagnose-r2dfly.sh for the full component state." >&2
    exit 1
  fi
done

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 9/9: Migration summary"
echo "=================================================="
SOURCE_DBSIZE_END="$(get_dbsize "$SOURCE_CONNECTION_MODE" "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
TARGET_DBSIZE_END="$(get_dbsize "$TARGET_CONNECTION_MODE" "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
MIGRATED=$(( TARGET_DBSIZE_END - TARGET_DBSIZE_START ))
echo "  mode: continuous"
echo "  source keys: $SOURCE_DBSIZE_END"
echo "  target keys: $TARGET_DBSIZE_END (was $TARGET_DBSIZE_START before this run)"
echo "  keys migrated this run: $MIGRATED"
echo "  the initial snapshot is complete, and the Live Phase is still running."
echo "  NiFi keeps running unsupervised after this script exits, and the Live Phase keeps applying"
echo "  source changes to the target until it is stopped."
echo
echo "  LIMITATIONS - these are real, and none of them are repaired automatically:"
echo "    1. Continuous mode captures ongoing changes but does not yet self-heal a dropped"
echo "       connection's gap. If the keyspace pub/sub connection drops, events during the outage"
echo "       are lost; the consumer records 'Keyspace Pub/Sub Disconnects' and 'Keyspace Pub/Sub"
echo "       Downtime (ms)' counters so the gap is visible, but nothing repairs it. The"
echo "       reconciliation/repair pass this would depend on is not built."
echo "    2. If the consumer's internal queue fills (--max-queue-depth), events are dropped and"
echo "       counted as 'Keyspace Events Dropped (Queue Full)'. Also not repaired."
echo "    3. During the initial snapshot the Live Phase and the scanner both write to the target."
echo "       The live path fetches the key's CURRENT value, so a live update can be overwritten by"
echo "       the scanner's older in-flight batch for the same key. That key stays stale until it"
echo "       changes again. This is a known narrow window in this first wiring pass."
echo "    4. Cluster topology drift is detected and counted but the topology_drift relationship is"
echo "       auto-terminated - nothing acts on it."
echo "    5. The Live Phase starts together with the initial scan, when run-r2dfly.sh starts the"
echo "       flow, so changes made on the source between this script starting and that moment are"
echo "       not captured by the Live Phase (the initial snapshot covers whatever is on the source"
echo "       when it scans). Additionally, run-r2dfly.sh stops and starts the whole process group"
echo "       to apply configuration, so re-running a migration against a flow whose Live Phase was"
echo "       already running tears the pub/sub subscription down for that reconfigure window, and"
echo "       events during it are lost."
echo "    6. RedisBatchWriter batches its writes: its deployed defaults are batch-size 50 and"
echo "       batch-timeout-ms 30000, so a single live change can wait up to 30 seconds on the"
echo "       target before it lands. That is throughput tuning, not a correctness bug, but it"
echo "       means 'continuous' is not 'immediate'."
echo "    7. RedisSingleKeyFetch's module_type relationship is auto-terminated, not routed to"
echo "       ModuleTypeHandler. A live change to a non-core-type key (ReJSON-RL, TopK-TYPE, and"
echo "       other module types) is silently dropped - no counter, no bulletin. This only affects"
echo "       changes made DURING continuous mode; the initial snapshot handles these types fully."
echo
echo "  Stop the Live Phase with:"
echo "    ./stop-continuous-migration.sh --nifi-container $NIFI_CONTAINER_NAME --nifi-user $NIFI_USER --nifi-password <the password you used>"

echo
echo "Run simple-troubleshoot.sh if there appears to be any issue with the migration."
echo
echo "Done."
