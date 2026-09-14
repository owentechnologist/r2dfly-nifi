#!/usr/bin/env bash
# One-command Redis -> Dragonfly migration: validates source/target connectivity, reports each
# side's product and version (INFO SERVER) - which also decides whether to enable the
# --dfly-to-dfly fast path - warns about module/data types the source holds that this tool can't
# land on this particular target, builds and deploys the processor NAR, starts NiFi if needed,
# and runs the standard migration via run-r2dfly.sh. For anything beyond the
# default migration (custom key-type filters, TopK modes, batch tuning, prefix filters, etc.),
# use run-r2dfly.sh directly - see ./run-r2dfly.sh --help.
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

Required:
  --source-connection-string S  redis://[:password@]host:port[/db] (or rediss:// for TLS -
                                 a rediss:// scheme automatically enables TLS to that side,
                                 no separate flag needed). For --source-connection-mode
                                 cluster, a comma-separated seed-node list instead, e.g.
                                 redis://host1:port1,redis://host2:port2
  --target-connection-string S  same format, for the Dragonfly target

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
                                 docs/quickstart.md for the full key reference.
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
                                 error surfaced anywhere else. Restarts the NiFi container to
                                 apply the cap if it isn't already set (skipped if it already
                                 is - safe to pass on every run). See deploy-to-nifi.sh for the
                                 exact properties this changes.
  --no-disable-provenance        restore NiFi's own provenance repository defaults (10GB, full
                                 lineage/audit history) instead - only worth it if you actually
                                 want that history and the migration host has the disk for it
                                 (env DISABLE_PROVENANCE=false). Also restarts the container if
                                 a previous default-on run already capped it.
  --verbose                     show real container image pull/build/download output instead
                                 of a progress bar (env VERBOSE)
  -y, --yes                     don't prompt before stopping/clearing a pre-existing migration
  -h, --help                    this help

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
    --defaults) ASSUME_DEFAULTS="true"; shift ;;
    --disable-provenance) DISABLE_PROVENANCE="true"; shift ;;
    --no-disable-provenance) DISABLE_PROVENANCE="false"; shift ;;
    --verbose) VERBOSE="true"; shift ;;
    -y|--yes) ASSUME_YES="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

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
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 1/8: Validating source and target connectivity, detecting server type"
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

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 2/8: Container resources and migration parallelism"
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
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 3/8: Starting NiFi and deploying the processor NAR"
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
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 4/8: Adapting localhost URLs for the NiFi container"
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
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 5/8: Authenticating with NiFi"
echo "=================================================="
echo "  authenticated as $NIFI_USER"
CONTAINER_NAME="$NIFI_CONTAINER_NAME"
nifi_lib_init

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 6/8: Checking for a pre-existing migration"
echo "=================================================="
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
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 7/8: Configuring and starting the migration"
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

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] STAGE 8/8: Migration summary"
echo "=================================================="
SOURCE_DBSIZE_END="$(get_dbsize "$SOURCE_CONNECTION_MODE" "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
TARGET_DBSIZE_END="$(get_dbsize "$TARGET_CONNECTION_MODE" "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
MIGRATED=$(( TARGET_DBSIZE_END - TARGET_DBSIZE_START ))
echo "  source keys: $SOURCE_DBSIZE_END"
echo "  target keys: $TARGET_DBSIZE_END (was $TARGET_DBSIZE_START before this run)"
echo "  keys migrated this run: $MIGRATED"

echo
echo "Run simple-troubleshoot.sh if there appears to be any issue with the migration."
echo
echo "Done."
