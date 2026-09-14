#!/usr/bin/env bash
# Configures the source/target connection details on an already-imported "R2Dfly
# Migration" flow (see deploy-to-nifi.sh / r2dfly.json), enables the resulting
# controller services, and offers to start the flow.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"
source "$PROJECT_ROOT/scripts/redis-lib.sh"
source "$PROJECT_ROOT/scripts/cluster-lib.sh"
# Must be called from this true top level, before any trap of this script's own - see
# toolbox_lib_enable_reuse's comment in toolbox-lib.sh. Makes the DBSIZE poll loop below (and
# any per-node cluster call) reuse one toolbox container instead of spinning a fresh one per
# call, for any target not reachable via a local container exec (i.e. any real remote/cloud
# target).
toolbox_lib_enable_reuse

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

NiFi connection/auth:
  --nifi-container NAME        container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER             NiFi single-user login username (env NIFI_USER)
  --nifi-password PASSWORD     NiFi single-user login password (env NIFI_PASS)
  --nifi-token TOKEN           use an existing bearer token instead of user/password (env NIFI_TOKEN)

Source:
  --source-connection-string S  e.g. redis://[:password@]host:port[/db] (env SOURCE_CONNECTION_STRING).
                                 For --source-connection-mode cluster, a comma-separated list of
                                 seed nodes instead, e.g. redis://host1:port1,redis://host2:port2
  --source-connection-mode M    standalone, sentinel, or cluster (env SOURCE_CONNECTION_MODE,
                                 default: standalone)
  --source-require-tls          require TLS to the source (env SOURCE_REQUIRE_TLS, default: false)

Scan behavior:
  --key-types LIST              comma-separated Redis types to scan/migrate (env KEY_TYPES,
                                 default: string,hash,set,zset,list,stream,ReJSON-RL - ReJSON-RL
                                 routes through ModuleTypeHandler, not RedisTypeDeserializer.
                                 TopK is handled separately, see --topk-mode). Accepts the real
                                 Redis type strings, or these case-insensitive aliases: json
                                 (ReJSON-RL), topk (TopK-TYPE), sortedset (zset), streams
                                 (stream), cuckoofilter/cf (MBbloomCF) - string/hash/list match
                                 themselves either way. The single value "all" (used alone, not
                                 combined with other types) expands to the default list above,
                                 which --topk-mode/--dfly-to-dfly then extend exactly as they
                                 would for the plain default (see below).
                                 Without --dfly-to-dfly, any type other than the default list
                                 above is rejected up front: MBbloom--/MBbloomCF/CMSk-TYPE
                                 (Bloom/Cuckoo Filter/CMS) only reconstruct via
                                 --dfly-to-dfly's DUMP/RESTORE fast path (verified directly,
                                 including a real DUMP/RESTORE round-trip of a Cuckoo Filter key
                                 between two current Dragonfly instances - note Cuckoo Filter
                                 support is itself version-gated on Dragonfly's side, e.g.
                                 present in df-v1.40.2 but not df-v1.39.0). With --dfly-to-dfly
                                 on, nothing is rejected here - an unsupported type just fails
                                 safely per-key at the processor instead
                                 (redis.incompatible.reason), the same runtime safety net
                                 Bloom/Cuckoo Filter/CMS already rely on.
  --disallow-key-types LIST      comma-separated Redis types to exclude from whatever
                                 --key-types would otherwise include (env DISALLOW_KEY_TYPES,
                                 default: unset/none excluded). Accepts the exact same real type
                                 strings/aliases as --key-types above, including "all". Applied
                                 as a final subtraction after --key-types' own default/"all"
                                 expansion AND the --topk-mode/--dfly-to-dfly auto-additions
                                 below, regardless of how a type got included - e.g. --key-types
                                 all --disallow-key-types stream,cf migrates everything the
                                 default list would, minus streams and Cuckoo Filter keys.
                                 Equivalent to --topk-mode off for TopK-TYPE specifically, since
                                 both just keep it out of the scan filter.
  --migration-id ID              checkpoint id RedisScanReader uses in the Cursor State Cache
                                 (env MIGRATION_ID, default: a fresh id each run, e.g.
                                 r2dfly-<timestamp>). Re-using an old id resumes/no-ops against
                                 a scan already marked complete for that id - see TUTORIAL.md's
                                 troubleshooting section. Pass one explicitly to intentionally
                                 resume an interrupted scan. Also names this run's log file,
                                 ../logs/<migration-id>.log (one level up from scripts/) - a
                                 mirror of every settings/activity line this script prints,
                                 kept after the terminal scrollback is gone.
  --prefix-deny-list LIST        comma-separated key prefixes to exclude (env PREFIX_DENY_LIST,
                                 default: unset/none). A key is skipped if it starts with any
                                 of these prefixes. Checked before --prefix-only-list.
  --prefix-only-list LIST        comma-separated key prefixes to exclusively migrate (env
                                 PREFIX_ONLY_LIST, default: unset/all prefixes). If set, a key
                                 is skipped unless it starts with one of these prefixes. Search
                                 index migration below is unaffected by this - it never touches
                                 the actual keyspace, so there's no temp-key prefix to worry
                                 about pairing this with anymore.
  --topk-mode MODE               how to migrate TopK keys (env TOPK_MODE, default: exact):
                                   exact - adds TopK-TYPE to the scan filter and lets
                                           ModuleTypeHandler reconstruct each key directly as it
                                           flows through the normal pipeline: exact counts via
                                           TOPK.INCRBY (chunked into <=100000-sized increments per
                                           item, since a real item's exact count can exceed
                                           TOPK.INCRBY's own per-call limit - verified live against
                                           production-scale data), or under --dfly-to-dfly, its
                                           DUMP/RESTORE fast path instead (also exact, and falls
                                           back to TOPK.INCRBY for any individual key where
                                           DUMP/RESTORE itself fails)
                                   off   - skip TopK keys entirely
                                 (There used to be a separate "approximate" mode reconstructing
                                 via TOPK.ADD, membership only, plus an "exact" mode that routed
                                 TopK-TYPE around this entirely via a Lua/zset-hash round-trip on
                                 the side - both retired once ModuleTypeHandler's own
                                 reconstruction became exact-count natively, making the
                                 distinction pointless.)
  --ignore-search-indexes        skip search index migration entirely (env
                                 IGNORE_SEARCH_INDEXES=true, default: false - search indexes
                                 are migrated by default). Driven by two NiFi processors already
                                 in the flow, run before the main flow starts (not after, unlike
                                 the old design - see SearchIndexRehydrator's own class doc for
                                 why that dependency went away): SearchIndexExporter runs
                                 FT._LIST/FT.INFO on the source and caches a normalized
                                 definition of each index (identical to what --topk-mode exact's
                                 doc above says about ModuleTypeHandler - all done directly in
                                 Java now, no Lua/temp keys involved); SearchIndexRehydrator
                                 reads the cache back and rebuilds each index with FT.CREATE on
                                 the target, broadcasting to every master node in cluster mode.
                                 Covers TEXT/NUMERIC/TAG/GEO/VECTOR fields - a VECTOR field
                                 FT.INFO didn't report a reconstructable algorithm/data_type/
                                 dim/distance_metric for is skipped, with a warning, but that's
                                 the only case that's not currently migrated.

Target:
  --target-connection-string S   same format as above (env TARGET_CONNECTION_STRING)
  --target-connection-mode M     standalone, sentinel, or cluster (env TARGET_CONNECTION_MODE,
                                 default: standalone) - a pre-provisioned Dragonfly cluster
                                 ("swarm") target only: this tool never bootstraps a target
                                 cluster's slot layout, only verifies it (see cluster-lib.sh)
  --target-require-tls           require TLS to the target (env TARGET_REQUIRE_TLS, default: false)

Parallelism (default configuration below runs everything single-threaded; raise these to drive
more throughput through the flow - see NIFI_REDIS_MIGRATION_SPEC.md section 7 for sizing
guidance, e.g. N <= source Redis / NiFi container CPU headroom, W <= target Dragonfly core count):
  --parallelism N                 concurrent tasks for RedisScanReader, RedisTypeDeserializer,
                                 and ModuleTypeHandler - the scan/read side of the flow (env
                                 PARALLELISM, default: 1). Also sets RedisScanReader's Partition
                                 Count to N, since NiFi requires the two to match. Redis SCAN
                                 has no native range-partitioning, so each of the N tasks scans
                                 the full keyspace and filters to its own partition by key hash -
                                 more source-side scan overhead per task added, in exchange for
                                 more parallel throughput through the pipeline.
  --writer-concurrency N          concurrent tasks for RedisBatchWriter - the write side (env
                                 WRITER_CONCURRENCY, default: 1). Independent of --parallelism;
                                 no benefit raising it past the target Dragonfly's core count.

Batch tuning (both default to the same values the processors themselves default to; raise
these for a remote target where the default may leave throughput on the table, e.g. a real
network round trip per pipeline flush rather than a near-zero-latency local one):
  --batch-size N                 RedisBatchWriter's default batch size, i.e. how many keys of a
                                  given core type (string/hash/list/set/zset/stream) it pipelines
                                  per write flush, for any type without its own --batch-size-<type>
                                  override below (env BATCH_SIZE, default: 50)
  --batch-size-string N           override --batch-size for string keys only, independent of every
                                  other type (env BATCH_SIZE_STRING, default: unset - uses --batch-size)
  --batch-size-hash N             same, for hash keys (env BATCH_SIZE_HASH)
  --batch-size-list N             same, for list keys (env BATCH_SIZE_LIST)
  --batch-size-set N              same, for set keys (env BATCH_SIZE_SET)
  --batch-size-zset N             same, for sorted set (zset) keys (env BATCH_SIZE_ZSET)
  --batch-size-stream N           same, for stream keys (env BATCH_SIZE_STREAM)
  --batch-timeout-ms N           how long RedisBatchWriter awaits one batch's pipeline
                                  (env BATCH_TIMEOUT_MS, default: 30000)
  --module-batch-size N          ModuleTypeHandler's default batch size, i.e. how many keys of a
                                  given module-key category (json/topk, and bloom-cms under
                                  --dfly-to-dfly) it pipelines per read/write flush, for any
                                  category without its own --batch-size-<category> override below
                                  (env MODULE_BATCH_SIZE, default: 50)
  --batch-size-json N              override --module-batch-size for ReJSON-RL keys only (env
                                  BATCH_SIZE_JSON, default: unset - uses --module-batch-size)
  --batch-size-topk N              same, for TopK keys (env BATCH_SIZE_TOPK) - only applies
                                  under --topk-mode exact, the default; no effect with --topk-mode off
  --batch-size-bloom-cms N         same, for Bloom/Cuckoo Filter/CMS keys, only ever migrated at
                                  all under --dfly-to-dfly (env BATCH_SIZE_BLOOM_CMS)
  --module-batch-timeout-ms N    how long ModuleTypeHandler awaits one batch's pipeline
                                  (env MODULE_BATCH_TIMEOUT_MS, default: 10000)
  --dfly-to-dfly                 enable when BOTH source and target are Dragonfly (env
                                 DFLY_TO_DFLY=true, default: false). Uses DUMP/RESTORE - a
                                 byte-for-byte copy of a key's internal serialization - instead
                                 of type-specific read/reconstruct commands, for ReJSON-RL,
                                 TopK-TYPE (its normal TOPK.INCRBY-based reconstruction is
                                 already exact counts, same as this path), MBbloom-- (Bloom filter), MBbloomCF
                                 (Cuckoo Filter), and CMSk-TYPE (Count-Min Sketch) keys via
                                 ModuleTypeHandler, AND string/hash/list/set/zset keys via
                                 RedisTypeDeserializer/RedisBatchWriter - roughly halves round
                                 trips per key, and for the latter group also avoids a write
                                 command whose argument count scales with the key's own size
                                 (RESTORE's argument count is fixed regardless of element count,
                                 unlike ZADD/SADD/RPUSH/HSET - relevant for any multi-million-
                                 element aggregate). ReJSON-RL/TopK-TYPE fall back to their
                                 normal reconstruction if DUMP/RESTORE fails for a given key;
                                 string/hash/list/set/zset keys fall back to the normal type-
                                 specific read if the DUMP payload exceeds --max-dump-payload-
                                 bytes; Bloom, Cuckoo Filter, and CMS have no other reconstruction
                                 path, so they're only migrated at all when this is set (auto-
                                 added to --key-types). Verified directly between two real
                                 Dragonfly instances - NOT safe against a real Redis source/
                                 target, or mismatched Dragonfly versions, since DUMP payloads
                                 aren't a portable format (Cuckoo Filter support itself is
                                 version-gated on Dragonfly's side too - e.g. present in
                                 df-v1.40.2, absent in df-v1.39.0 - one more reason the version
                                 match this flag depends on has to be exact, not just "both sides
                                 are Dragonfly").
  --max-dump-payload-bytes N      under --dfly-to-dfly, a string/hash/list/set/zset key whose
                                 DUMP payload exceeds this many bytes falls back to the normal
                                 type-specific read instead - a defensive cap against the
                                 target's max-bulk-string-length limit, independent of how many
                                 elements the key contains (env MAX_DUMP_PAYLOAD_BYTES, default:
                                 67108864, i.e. 64 MiB)

Starting the flow:
  --start                      start the flow automatically, no prompt
  --no-start                   never start the flow, no prompt
  (default: ask interactively; non-interactive shells default to not starting)

Progress tracking (only runs if the flow gets started; each round also prints a line of NiFi's
own live flow status alongside the DBSIZE numbers - queued FlowFiles/bytes, active thread
count, and recent bytes written, straight from the flow's own status API):
  --source-container NAME      container to run 'redis-cli DBSIZE' against for the source
                               (env SOURCE_CONTAINER, default: guessed from the connection
                               string's hostname)
  --target-container NAME      same, for the target (env TARGET_CONTAINER)
  --poll-interval SECONDS      how often to check DBSIZE (env POLL_INTERVAL, default: 2)
  --poll-timeout SECONDS       give up if target's key count hasn't grown at all for this many
                                seconds (env POLL_TIMEOUT, default: 300, UNLESS auto-tuned - see
                                below) - a STALL timeout, not a total-time one: a migration
                                that's still actively growing the target (at any rate) is never
                                cut off by this, no matter how long it takes in total. Passing
                                this explicitly (flag or env var) disables auto-tuning for it.
  --poll-max-duration SECONDS  hard ceiling on total polling time regardless of ongoing
                                progress, as a backstop (env POLL_MAX_DURATION, default: 3600,
                                UNLESS auto-tuned - see below). Passing this explicitly (flag or
                                env var) disables auto-tuning for it.
  --poll-quiet-rounds N        treat the scan as complete after this many consecutive
                                unchanged target counts (env POLL_QUIET_ROUNDS, default: 3) -
                                needed because a --key-types filter means target will never
                                reach source's full count. Raise this for a large migration
                                where a slow batch could look like a stall for a few rounds.
  --no-poll                    don't track/report DBSIZE after starting the flow (also skips
                                the latency probe/auto-tuning below, since neither matters if
                                nothing is being polled)

Auto-tuned poll timeouts: unless --poll-timeout/--poll-max-duration (or their env vars) are
passed explicitly, this measures round-trip latency to source and target (50 back-to-back
PINGs in one redis-cli call each, timed - not redis-cli's own --latency, which never
terminates on its own and has no portable way to bound it: this project must run unmodified on
a bare macOS host, which has neither GNU coreutils' timeout nor gtimeout), combines that with
the source's current key count and this run's --batch-size/--writer-concurrency, and - only
ever upward, never below the plain defaults above - raises --poll-max-duration (and, capped
more conservatively, --poll-timeout) to something sized for the actual migration instead of a
one-size-fits-all constant. It's a rough, openly-approximate estimate (round trips ~= keys /
(batch-size * writer-concurrency), times measured latency, times a 5x safety margin for
payload transfer time/processing/per-type reconstruction overhead none of this measures) -
skipped silently, falling back to the plain defaults, if either side's latency can't be
measured at all.

Passing secrets as environment variables instead of arguments avoids putting them in
shell history. Flags take precedence over environment variables.

Example:
  NIFI_USER=... NIFI_PASS=... $(basename "$0") \\
    --source-connection-string redis://source-redis:6379 \\
    --target-connection-string redis://target-dragonfly:6379
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"
SOURCE_CONNECTION_STRING="${SOURCE_CONNECTION_STRING:-}"
SOURCE_CONNECTION_MODE="${SOURCE_CONNECTION_MODE:-standalone}"
SOURCE_REQUIRE_TLS="${SOURCE_REQUIRE_TLS:-false}"
TARGET_CONNECTION_STRING="${TARGET_CONNECTION_STRING:-}"
TARGET_CONNECTION_MODE="${TARGET_CONNECTION_MODE:-standalone}"
TARGET_REQUIRE_TLS="${TARGET_REQUIRE_TLS:-false}"
START_MODE="ask"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-}"
TARGET_CONTAINER="${TARGET_CONTAINER:-}"
POLL_INTERVAL="${POLL_INTERVAL:-2}"
POLL_TIMEOUT_EXPLICIT=false
[[ -n "${POLL_TIMEOUT:-}" ]] && POLL_TIMEOUT_EXPLICIT=true
POLL_TIMEOUT="${POLL_TIMEOUT:-300}"
POLL_MAX_DURATION_EXPLICIT=false
[[ -n "${POLL_MAX_DURATION:-}" ]] && POLL_MAX_DURATION_EXPLICIT=true
POLL_MAX_DURATION="${POLL_MAX_DURATION:-3600}"
POLL_QUIET_ROUNDS="${POLL_QUIET_ROUNDS:-90}"
POLL_MODE="yes"
# The universal set - every one of these has a real reconstruction path regardless of
# --dfly-to-dfly, with exact counts either way (TopK-TYPE included - see --topk-mode above).
# Anything else (MBbloom--, CMSk-TYPE, MBbloomCF/cuckoofilter) only has a reconstruction path via
# --dfly-to-dfly's DUMP/RESTORE fast path - see the validation block below.
DEFAULT_KEY_TYPES="string,hash,set,zset,list,stream,ReJSON-RL"
KEY_TYPES="${KEY_TYPES:-$DEFAULT_KEY_TYPES}"
DISALLOW_KEY_TYPES="${DISALLOW_KEY_TYPES:-}"
PREFIX_DENY_LIST="${PREFIX_DENY_LIST:-}"
PREFIX_ONLY_LIST="${PREFIX_ONLY_LIST:-}"
MIGRATION_ID="${MIGRATION_ID:-}"
TOPK_MODE="${TOPK_MODE:-exact}"
IGNORE_SEARCH_INDEXES="${IGNORE_SEARCH_INDEXES:-false}"
BATCH_SIZE="${BATCH_SIZE:-50}"
BATCH_SIZE_STRING="${BATCH_SIZE_STRING:-}"
BATCH_SIZE_HASH="${BATCH_SIZE_HASH:-}"
BATCH_SIZE_LIST="${BATCH_SIZE_LIST:-}"
BATCH_SIZE_SET="${BATCH_SIZE_SET:-}"
BATCH_SIZE_ZSET="${BATCH_SIZE_ZSET:-}"
BATCH_SIZE_STREAM="${BATCH_SIZE_STREAM:-}"
BATCH_TIMEOUT_MS="${BATCH_TIMEOUT_MS:-30000}"
MODULE_BATCH_SIZE="${MODULE_BATCH_SIZE:-50}"
BATCH_SIZE_JSON="${BATCH_SIZE_JSON:-}"
BATCH_SIZE_TOPK="${BATCH_SIZE_TOPK:-}"
BATCH_SIZE_BLOOM_CMS="${BATCH_SIZE_BLOOM_CMS:-}"
MODULE_BATCH_TIMEOUT_MS="${MODULE_BATCH_TIMEOUT_MS:-10000}"
DFLY_TO_DFLY="${DFLY_TO_DFLY:-false}"
MAX_DUMP_PAYLOAD_BYTES="${MAX_DUMP_PAYLOAD_BYTES:-67108864}"
PARALLELISM="${PARALLELISM:-1}"
WRITER_CONCURRENCY="${WRITER_CONCURRENCY:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) NIFI_TOKEN="$2"; shift 2 ;;
    --source-connection-string) SOURCE_CONNECTION_STRING="$2"; shift 2 ;;
    --source-connection-mode) SOURCE_CONNECTION_MODE="$2"; shift 2 ;;
    --source-require-tls) SOURCE_REQUIRE_TLS="true"; shift ;;
    --target-connection-string) TARGET_CONNECTION_STRING="$2"; shift 2 ;;
    --target-connection-mode) TARGET_CONNECTION_MODE="$2"; shift 2 ;;
    --target-require-tls) TARGET_REQUIRE_TLS="true"; shift ;;
    --start) START_MODE="yes"; shift ;;
    --no-start) START_MODE="no"; shift ;;
    --source-container) SOURCE_CONTAINER="$2"; shift 2 ;;
    --target-container) TARGET_CONTAINER="$2"; shift 2 ;;
    --poll-interval) POLL_INTERVAL="$2"; shift 2 ;;
    --poll-timeout) POLL_TIMEOUT="$2"; POLL_TIMEOUT_EXPLICIT=true; shift 2 ;;
    --poll-max-duration) POLL_MAX_DURATION="$2"; POLL_MAX_DURATION_EXPLICIT=true; shift 2 ;;
    --poll-quiet-rounds) POLL_QUIET_ROUNDS="$2"; shift 2 ;;
    --no-poll) POLL_MODE="no"; shift ;;
    --key-types) KEY_TYPES="$2"; shift 2 ;;
    --disallow-key-types) DISALLOW_KEY_TYPES="$2"; shift 2 ;;
    --prefix-deny-list) PREFIX_DENY_LIST="$2"; shift 2 ;;
    --prefix-only-list) PREFIX_ONLY_LIST="$2"; shift 2 ;;
    --migration-id) MIGRATION_ID="$2"; shift 2 ;;
    --topk-mode) TOPK_MODE="$2"; shift 2 ;;
    --ignore-search-indexes) IGNORE_SEARCH_INDEXES="true"; shift ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --batch-size-string) BATCH_SIZE_STRING="$2"; shift 2 ;;
    --batch-size-hash) BATCH_SIZE_HASH="$2"; shift 2 ;;
    --batch-size-list) BATCH_SIZE_LIST="$2"; shift 2 ;;
    --batch-size-set) BATCH_SIZE_SET="$2"; shift 2 ;;
    --batch-size-zset) BATCH_SIZE_ZSET="$2"; shift 2 ;;
    --batch-size-stream) BATCH_SIZE_STREAM="$2"; shift 2 ;;
    --batch-timeout-ms) BATCH_TIMEOUT_MS="$2"; shift 2 ;;
    --module-batch-size) MODULE_BATCH_SIZE="$2"; shift 2 ;;
    --batch-size-json) BATCH_SIZE_JSON="$2"; shift 2 ;;
    --batch-size-topk) BATCH_SIZE_TOPK="$2"; shift 2 ;;
    --batch-size-bloom-cms) BATCH_SIZE_BLOOM_CMS="$2"; shift 2 ;;
    --module-batch-timeout-ms) MODULE_BATCH_TIMEOUT_MS="$2"; shift 2 ;;
    --dfly-to-dfly) DFLY_TO_DFLY="true"; shift ;;
    --max-dump-payload-bytes) MAX_DUMP_PAYLOAD_BYTES="$2"; shift 2 ;;
    --parallelism) PARALLELISM="$2"; shift 2 ;;
    --writer-concurrency) WRITER_CONCURRENCY="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$SOURCE_CONNECTION_STRING" ]]; then
  echo "error: --source-connection-string (or SOURCE_CONNECTION_STRING) is required" >&2; usage; exit 1
fi
if [[ -z "$TARGET_CONNECTION_STRING" ]]; then
  echo "error: --target-connection-string (or TARGET_CONNECTION_STRING) is required" >&2; usage; exit 1
fi
if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  echo "error: NiFi credentials required - pass --nifi-token, or both --nifi-user and --nifi-password" >&2; usage; exit 1
fi

# A fresh id each run means each `run-r2dfly.sh` invocation does a real scan instead of
# silently no-op'ing against a prior run's "partition already complete" checkpoint. Established
# this early (right after argument parsing) so the migration log below can be named after it
# and capture everything from here on, including the --topk-mode validation right below.
[[ -n "$MIGRATION_ID" ]] || MIGRATION_ID="r2dfly-$(date +%s)"

# Every run's settings and activity get mirrored into a per-migration log file, in addition to
# the terminal, so a summary of what a given migration run did/was configured with survives
# after the terminal scrollback is gone. Named after MIGRATION_ID so it lines up with the same
# id used for checkpointing and the search-index temp-key namespacing above.
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
MIGRATION_LOG="$LOG_DIR/${MIGRATION_ID}.log"
exec > >(tee -a "$MIGRATION_LOG") 2>&1
echo "==> r2dfly version $R2DFLY_VERSION"
echo "==> logging this migration run to $MIGRATION_LOG"

case "$TOPK_MODE" in
  exact|off) ;;
  *) echo "error: --topk-mode must be exact or off (got '$TOPK_MODE')" >&2; usage; exit 1 ;;
esac

if ! [[ "$PARALLELISM" =~ ^[0-9]+$ ]] || [[ "$PARALLELISM" -lt 1 ]]; then
  echo "error: --parallelism must be a positive integer (got '$PARALLELISM')" >&2; usage; exit 1
fi
if ! [[ "$WRITER_CONCURRENCY" =~ ^[0-9]+$ ]] || [[ "$WRITER_CONCURRENCY" -lt 1 ]]; then
  echo "error: --writer-concurrency must be a positive integer (got '$WRITER_CONCURRENCY')" >&2; usage; exit 1
fi
# `${var,,}` (bash 4+ lowercasing) is deliberately avoided here - this project's scripts must
# stay usable under macOS's stock bash 3.2 (see redis-lib.sh/cluster-lib.sh's own notes on this),
# so `tr` does the lowercase+dash conversion instead, same as everywhere else in this file.
for _bs_flag in BATCH_SIZE_STRING BATCH_SIZE_HASH BATCH_SIZE_LIST BATCH_SIZE_SET BATCH_SIZE_ZSET \
    BATCH_SIZE_STREAM BATCH_SIZE_JSON BATCH_SIZE_TOPK BATCH_SIZE_BLOOM_CMS; do
  _bs_val="${!_bs_flag}"
  if [[ -n "$_bs_val" ]] && { ! [[ "$_bs_val" =~ ^[0-9]+$ ]] || [[ "$_bs_val" -lt 1 ]]; }; then
    _bs_flag_disp="--$(tr '[:upper:]_' '[:lower:]-' <<< "$_bs_flag")"
    echo "error: $_bs_flag_disp must be a positive integer (got '$_bs_val')" >&2; usage; exit 1
  fi
done
unset _bs_flag _bs_val _bs_flag_disp

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

[[ -n "$SOURCE_CONTAINER" ]] || SOURCE_CONTAINER="$(redis_lib_extract_host "$SOURCE_CONNECTION_STRING")"
[[ -n "$TARGET_CONTAINER" ]] || TARGET_CONTAINER="$(redis_lib_extract_host "$TARGET_CONNECTION_STRING")"

# --key-types accepts friendly/short aliases (case-insensitively) alongside the real Redis type
# strings the rest of this script and the NiFi flow actually match against - translate those
# here, once, so everything downstream (the TopK-TYPE/dfly-to-dfly auto-additions below, the
# processor property, the summary echo) only ever sees real type strings. Anything not in this
# table (e.g. MBbloom--, CMSk-TYPE, or a real type string already) passes through unchanged.
# The lone value "all" is a separate special case - not a per-token alias, since it replaces the
# whole list - expanding to DEFAULT_KEY_TYPES lets the existing topk-mode/dfly-to-dfly
# auto-addition logic below add TopK-TYPE/MBbloom--/CMSk-TYPE on top of it exactly as it already
# does for the plain default, instead of duplicating that conditional logic here.
normalize_key_types() {
  local input="$1" part lower
  local -a parts out=()
  if [[ "$(tr '[:upper:]' '[:lower:]' <<< "$input" | tr -d '[:space:]')" == "all" ]]; then
    echo "$DEFAULT_KEY_TYPES"
    return
  fi
  IFS=',' read -ra parts <<< "$input"
  for part in "${parts[@]}"; do
    lower="$(tr '[:upper:]' '[:lower:]' <<< "$part")"
    case "$lower" in
      json|rejson-rl) out+=("ReJSON-RL") ;;
      topk|topk-type) out+=("TopK-TYPE") ;;
      sortedset|zset) out+=("zset") ;;
      stream|streams) out+=("stream") ;;
      cuckoofilter|cf) out+=("MBbloomCF") ;;
      string|hash|list) out+=("$lower") ;;
      *) out+=("$part") ;;
    esac
  done
  local IFS=','
  echo "${out[*]}"
}
KEY_TYPES="$(normalize_key_types "$KEY_TYPES")"

# --topk-mode exact (the default) reconstructs TopK keys via RedisScanReader -> ModuleTypeHandler,
# so it needs TopK-TYPE in the scan filter; off skips them, deliberately leaving TopK-TYPE out so
# RedisScanReader/ModuleTypeHandler never touch the original key.
if [[ "$TOPK_MODE" == "exact" ]]; then
  shopt -s nocasematch
  if [[ ",$KEY_TYPES," != *",TopK-TYPE,"* ]]; then
    KEY_TYPES="$KEY_TYPES,TopK-TYPE"
  fi
  shopt -u nocasematch
fi

# Bloom (MBbloom--), Cuckoo Filter (MBbloomCF), and CMS (CMSk-TYPE) have no reconstruction path
# in ModuleTypeHandler other than --dfly-to-dfly's DUMP/RESTORE fast path, so they're only worth
# including in the scan filter at all once that's enabled - otherwise RedisScanReader would just
# skip them anyway. Cuckoo Filter support is itself version-gated on Dragonfly's side (confirmed
# present in df-v1.40.2, absent in df-v1.39.0), same reasoning as Bloom/CMS otherwise - all three
# verified directly with a real DUMP/RESTORE round-trip between two current Dragonfly instances.
if [[ "$DFLY_TO_DFLY" == "true" ]]; then
  shopt -s nocasematch
  [[ ",$KEY_TYPES," == *",MBbloom--,"* ]] || KEY_TYPES="$KEY_TYPES,MBbloom--"
  [[ ",$KEY_TYPES," == *",MBbloomCF,"* ]] || KEY_TYPES="$KEY_TYPES,MBbloomCF"
  [[ ",$KEY_TYPES," == *",CMSk-TYPE,"* ]] || KEY_TYPES="$KEY_TYPES,CMSk-TYPE"
  shopt -u nocasematch
fi

# --disallow-key-types removes types from the fully-resolved KEY_TYPES (after the default
# expansion above AND the topk-mode/dfly-to-dfly auto-additions), regardless of how a type ended
# up there - it's a final subtraction, not just a filter on what --key-types itself listed. Runs
# through the exact same alias table as --key-types (normalize_key_types) so "topk"/"json"/
# "sortedset"/etc. mean the same thing on both flags. Applied before the BAD_TYPES validation
# below so disallowing an unsupported type (e.g. --disallow-key-types cuckoofilter without
# --dfly-to-dfly) removes it cleanly instead of tripping that check on a type the user just said
# not to migrate. --disallow-key-types topk and --topk-mode off end up equivalent for TopK-TYPE -
# both just keep it out of the scan filter.
if [[ -n "$DISALLOW_KEY_TYPES" ]]; then
  DISALLOW_KEY_TYPES="$(normalize_key_types "$DISALLOW_KEY_TYPES")"
  IFS=',' read -ra _KT_KEEP <<< "$KEY_TYPES"
  IFS=',' read -ra _KT_DENY <<< "$DISALLOW_KEY_TYPES"
  _KT_OUT=()
  shopt -s nocasematch
  for _kt in "${_KT_KEEP[@]}"; do
    _kt_denied=false
    for _dt in "${_KT_DENY[@]}"; do
      [[ "$_kt" == "$_dt" ]] && { _kt_denied=true; break; }
    done
    [[ "$_kt_denied" == "true" ]] || _KT_OUT+=("$_kt")
  done
  shopt -u nocasematch
  # `${_KT_OUT[*]}` under this script's `set -u` throws "unbound variable" when _KT_OUT has zero
  # elements (every type got denied) - a real bash quirk with empty arrays, not something a
  # length check can be skipped for. Handled explicitly rather than expanding the array at all
  # in that case. `$(...)` runs in a subshell for the non-empty case, so the `IFS=','` used to
  # join with commas doesn't leak out and change word-splitting for the rest of this script -
  # unlike a bare `IFS=',' KEY_TYPES=...` prefix assignment with no command, which would set IFS
  # permanently in the current shell.
  if [[ ${#_KT_OUT[@]} -eq 0 ]]; then
    KEY_TYPES=""
  else
    KEY_TYPES="$(IFS=','; echo "${_KT_OUT[*]}")"
  fi
  unset _KT_KEEP _KT_DENY _KT_OUT _kt _kt_denied _dt
  if [[ -z "$KEY_TYPES" ]]; then
    echo "error: --disallow-key-types removed every type --key-types would otherwise have included - nothing left to migrate" >&2
    usage; exit 1
  fi
fi

# Reject any --key-types token this tool can't actually migrate WITHOUT --dfly-to-dfly - but
# only when --dfly-to-dfly is off. With it on, anything ModuleTypeHandler can't handle already
# fails safely and per-key at the processor (redis.incompatible.reason: module_type or
# dump_restore_failed) - the same runtime safety net --dfly-to-dfly already relies on for
# Bloom/Cuckoo Filter/CMS, so it's pointless to duplicate that check here for a mode that
# already self-diagnoses.
if [[ "$DFLY_TO_DFLY" != "true" ]]; then
  UNIVERSAL_KEY_TYPES=(string hash set zset list stream rejson-rl topk-type)
  BAD_TYPES=()
  IFS=',' read -ra KEY_TYPES_CHECK <<< "$KEY_TYPES"
  for kt in "${KEY_TYPES_CHECK[@]}"; do
    kt_lower="$(tr '[:upper:]' '[:lower:]' <<< "$kt")"
    match=false
    for u in "${UNIVERSAL_KEY_TYPES[@]}"; do
      [[ "$kt_lower" == "$u" ]] && { match=true; break; }
    done
    [[ "$match" == "true" ]] || BAD_TYPES+=("$kt")
  done
  if [[ ${#BAD_TYPES[@]} -gt 0 ]]; then
    echo "error: --key-types includes type(s) with no reconstruction path unless both source and target are Dragonfly:" >&2
    for bt in "${BAD_TYPES[@]}"; do
      case "$(tr '[:upper:]' '[:lower:]' <<< "$bt")" in
        mbbloomcf)
          echo "  - $bt: Cuckoo Filter keys only reconstruct via --dfly-to-dfly's DUMP/RESTORE fast path (both sides must be Dragonfly, and both must actually support Cuckoo Filter - it's version-gated, e.g. present in df-v1.40.2 but not df-v1.39.0) - pass --dfly-to-dfly, or drop this type" >&2 ;;
        mbbloom--)
          echo "  - $bt: Bloom Filter keys only reconstruct via --dfly-to-dfly's DUMP/RESTORE fast path (both sides must be Dragonfly) - pass --dfly-to-dfly, or drop this type" >&2 ;;
        cmsk-type)
          echo "  - $bt: Count-Min Sketch keys only reconstruct via --dfly-to-dfly's DUMP/RESTORE fast path (both sides must be Dragonfly) - pass --dfly-to-dfly, or drop this type" >&2 ;;
        *)
          echo "  - $bt: not a recognized/supported Redis type for this tool" >&2 ;;
      esac
    done
    usage; exit 1
  fi
fi

# Connection strings deliberately excluded (may carry passwords) - everything else here is
# the fully-resolved configuration for this run, gathered in one place for the migration log
# even though most of it is also echoed individually as each setting gets applied below.
echo "==> migration settings for '$MIGRATION_ID':"
echo "  source-connection-mode=$SOURCE_CONNECTION_MODE  source-require-tls=$SOURCE_REQUIRE_TLS"
echo "  target-connection-mode=$TARGET_CONNECTION_MODE  target-require-tls=$TARGET_REQUIRE_TLS"
echo "  key-types=$KEY_TYPES  disallow-key-types=${DISALLOW_KEY_TYPES:-<none>}"
echo "  topk-mode=$TOPK_MODE  ignore-search-indexes=$IGNORE_SEARCH_INDEXES  dfly-to-dfly=$DFLY_TO_DFLY"
echo "  prefix-deny-list=${PREFIX_DENY_LIST:-<none>}  prefix-only-list=${PREFIX_ONLY_LIST:-<none>}"
echo "  parallelism=$PARALLELISM  writer-concurrency=$WRITER_CONCURRENCY"
echo "  batch-size=$BATCH_SIZE (string=${BATCH_SIZE_STRING:-default} hash=${BATCH_SIZE_HASH:-default} list=${BATCH_SIZE_LIST:-default} set=${BATCH_SIZE_SET:-default} zset=${BATCH_SIZE_ZSET:-default} stream=${BATCH_SIZE_STREAM:-default})  batch-timeout-ms=$BATCH_TIMEOUT_MS"
echo "  module-batch-size=$MODULE_BATCH_SIZE (json=${BATCH_SIZE_JSON:-default} topk=${BATCH_SIZE_TOPK:-default} bloom-cms=${BATCH_SIZE_BLOOM_CMS:-default})  module-batch-timeout-ms=$MODULE_BATCH_TIMEOUT_MS"

nifi_lib_pick_runtime
nifi_lib_init

echo "==> locating the R2Dfly Migration process group"
# `|| true` throughout this section: under set -e/pipefail, any NiFi API hiccup (transient
# connectivity, a non-JSON response) would otherwise abort the script silently right at the
# failing assignment, before the deliberate `-z`/error checks below ever get a chance to run.
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
  echo "error: no 'R2Dfly Migration' process group found under root. Run ./deploy-to-nifi.sh first." >&2
  exit 1
fi
echo "found process group: $PG_ID"

echo "==> locating the source/target connection-pool controller services"
SVC_LIST_JSON="$(nifi_api_get "flow/process-groups/$PG_ID/controller-services")" || true
SVC_IDS="$(echo "$SVC_LIST_JSON" | py3 "
import json, sys
d = json.load(sys.stdin)
src = tgt = ''
for s in d['controllerServices']:
    t = s['component']['type']
    if not src and t.endswith('StandardRedisConnectionPoolService'):
        src = s['id']
    elif not tgt and t.endswith('StandardDragonflyConnectionPoolService'):
        tgt = s['id']
print(f'{src}|{tgt}')
")" || true
IFS='|' read -r SVC_SOURCE SVC_TARGET <<< "$SVC_IDS"
if [[ -z "$SVC_SOURCE" ]]; then
  echo "error: could not find the source Redis connection-pool service in $PG_ID" >&2; exit 1
fi
if [[ -z "$SVC_TARGET" ]]; then
  echo "error: could not find the target Dragonfly connection-pool service in $PG_ID" >&2; exit 1
fi

echo "==> locating the RedisScanReader, RedisBatchWriter, and ModuleTypeHandler processors"
PROC_LIST_JSON="$(nifi_api_get "process-groups/$PG_ID/processors")" || true
# One pass over PROC_LIST_JSON for all six processor ids (instead of six separate py3
# invocations re-parsing the same JSON) - each py3 call is a container exec via the toolbox,
# not free. PROC_MTH/PROC_SIDX_EXPORT/PROC_SIDX_REHYDRATE intentionally have no error check
# below - an empty result is handled as "older flow definition without these, skip that step"
# further down, so a transient API failure just falls into that same pre-existing tolerance.
PROC_IDS="$(echo "$PROC_LIST_JSON" | py3 "
import json, sys
d = json.load(sys.stdin)
order = ['RedisScanReader', 'RedisBatchWriter', 'ModuleTypeHandler', 'RedisTypeDeserializer', 'SearchIndexExporter', 'SearchIndexRehydrator']
ids = {name: '' for name in order}
for p in d['processors']:
    t = p['component']['type']
    for name in order:
        if not ids[name] and t.endswith(name):
            ids[name] = p['component']['id']
            break
print('|'.join(ids[name] for name in order))
")" || true
IFS='|' read -r PROC_SCAN PROC_WRITER PROC_MTH PROC_DESER PROC_SIDX_EXPORT PROC_SIDX_REHYDRATE <<< "$PROC_IDS"
if [[ -z "$PROC_SCAN" ]]; then
  echo "error: could not find the RedisScanReader processor in $PG_ID" >&2; exit 1
fi
if [[ -z "$PROC_WRITER" ]]; then
  echo "error: could not find the RedisBatchWriter processor in $PG_ID" >&2; exit 1
fi
if [[ -z "$PROC_DESER" ]]; then
  echo "error: could not find the RedisTypeDeserializer processor in $PG_ID" >&2; exit 1
fi

# pg-stop/pg-disable-services return as soon as NiFi *accepts* the request, not once every
# processor/service has actually finished transitioning - a processor mid-onTrigger() (e.g.
# RedisScanReader blocked borrowing a connection) keeps running for a while after pg-stop
# returns. Found in practice: a later property PUT was rejected with "... while the Processor
# is running" even though pg-stop had already been called and returned successfully. Poll the
# actual state afterward instead of assuming either command completed synchronously.
wait_for_processors_stopped() {
  local pgid="$1" timeout="${2:-30}" waited=0 running rc
  while true; do
    # rc is checked explicitly (not just `|| true`'d away) so a transient API failure is treated
    # as "unknown, keep polling" rather than being indistinguishable from `running` legitimately
    # coming back empty - the latter means "confirmed nothing running", the former means we
    # simply couldn't tell this round. Conflating them would let a bad poll falsely report
    # success and let the caller proceed to reconfigure/restart processors that might still
    # actually be running - precisely the silent-misconfiguration bug described above.
    rc=0
    running="$(nifi_api_get "process-groups/$pgid/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
print(','.join(p['component']['name'] for p in d['processors'] if p.get('status', {}).get('runStatus') == 'Running'))
")" || rc=$?
    [[ $rc -eq 0 && -z "$running" ]] && return 0
    if [[ "$waited" -ge "$timeout" ]]; then
      if [[ $rc -ne 0 ]]; then
        echo "warning: could not confirm processor status after ${timeout}s (NiFi API call failing) - proceeding anyway" >&2
      else
        echo "warning: still running after ${timeout}s: $running - proceeding anyway (a later property PUT may be rejected)" >&2
      fi
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

wait_for_services_disabled() {
  local pgid="$1" timeout="${2:-30}" waited=0 enabled rc
  while true; do
    # See wait_for_processors_stopped above for why rc is checked explicitly rather than just
    # `|| true`'d away.
    rc=0
    enabled="$(nifi_api_get "flow/process-groups/$pgid/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
print(','.join(s['component']['name'] for s in d['controllerServices'] if s['component'].get('state') != 'DISABLED'))
")" || rc=$?
    [[ $rc -eq 0 && -z "$enabled" ]] && return 0
    if [[ "$waited" -ge "$timeout" ]]; then
      if [[ $rc -ne 0 ]]; then
        echo "warning: could not confirm controller-service status after ${timeout}s (NiFi API call failing) - proceeding anyway" >&2
      else
        echo "warning: still not disabled after ${timeout}s: $enabled - proceeding anyway (a later property PUT may be rejected)" >&2
      fi
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

# Stop first (a no-op if already stopped) - NiFi refuses to change a running processor's
# properties, and RedisScanReader may still be running from a previous configure/start.
echo "==> stopping the flow to apply configuration changes"
nifi_cli pg-stop -pgid "$PG_ID" || true
wait_for_processors_stopped "$PG_ID"

# Same requirement applies to controller services, but for *enabled*, not running: NiFi
# rejects a property PUT to an enabled service outright (a real bug found in practice - every
# run-r2dfly.sh run after the very first silently failed to update connection-string,
# since pg-stop only covers processors and nothing here ever disabled the services first, or
# checked whether the PUT actually succeeded; RedisBatchWriter kept quietly writing wherever
# it was first pointed at, no matter what --target-connection-string said afterward).
echo "==> disabling controller services to apply configuration changes"
nifi_cli pg-disable-services -pgid "$PG_ID" || true
wait_for_services_disabled "$PG_ID"

# nifi_api_put_checked <path> <body> - like nifi_api_put, but treats a non-JSON response (NiFi
# returns a plain-text body, not JSON, for some rejections - e.g. "Cannot modify configuration
# of ... because it is currently not disabled") as a hard failure instead of silently discarding
# it. Every controller-service property update in this script goes through this now.
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

echo "==> setting RedisScanReader's key type filter, prefix filters, migration id, and parallelism"
echo "  key-type-filter=$KEY_TYPES  migration-id=$MIGRATION_ID  parallelism=$PARALLELISM"
echo "  prefix-deny-list=${PREFIX_DENY_LIST:-<none>}  prefix-only-list=${PREFIX_ONLY_LIST:-<none>}"
PROC_VER="$(nifi_current_version "processors/$PROC_SCAN")" || true
# Both are optional properties, but NiFi's processor-property PUT is a partial merge, not a
# full replace: a key simply absent from this JSON body leaves whatever value a PREVIOUS run
# left there untouched (discovered the hard way - a stale --prefix-only-list from an earlier,
# unrelated run silently filtered out an entire later migration's keys, with zero errors or
# failures logged, since "didn't match the only-list" and "not scanned" look identical).
# Passing JSON null (not an empty string, which the property's own validator rejects) is how
# to explicitly clear an optional property via this API - always set both, every run.
PREFIX_DENY_JSON="null"
[[ -n "$PREFIX_DENY_LIST" ]] && PREFIX_DENY_JSON="\"$PREFIX_DENY_LIST\""
PREFIX_ONLY_JSON="null"
[[ -n "$PREFIX_ONLY_LIST" ]] && PREFIX_ONLY_JSON="\"$PREFIX_ONLY_LIST\""
# partition-count must match concurrentlySchedulableTaskCount (NiFi's "Concurrent Tasks") for
# RedisScanReader's hash-slot partitioning to actually claim one partition per task - always
# drive both from the same --parallelism value so they can't drift out of sync.
PROC_PROPS="\"key-type-filter\":\"$KEY_TYPES\",\"migration-id\":\"$MIGRATION_ID\",\"prefix-deny-list\":$PREFIX_DENY_JSON,\"prefix-only-list\":$PREFIX_ONLY_JSON,\"partition-count\":\"$PARALLELISM\""
PROC_BODY="{\"revision\":{\"version\":$PROC_VER},\"component\":{\"id\":\"$PROC_SCAN\",\"config\":{\"properties\":{$PROC_PROPS},\"concurrentlySchedulableTaskCount\":$PARALLELISM}}}"
nifi_api_put_checked "processors/$PROC_SCAN" "$PROC_BODY" > /dev/null

echo "==> setting RedisTypeDeserializer's parallelism and dfly-to-dfly optimizations"
echo "  parallelism=$PARALLELISM  dfly-to-dfly=$DFLY_TO_DFLY  max-dump-payload-bytes=$MAX_DUMP_PAYLOAD_BYTES"
PROC_VER="$(nifi_current_version "processors/$PROC_DESER")" || true
DESER_PROPS="\"dfly-to-dfly\":\"$DFLY_TO_DFLY\",\"max-dump-payload-bytes\":\"$MAX_DUMP_PAYLOAD_BYTES\""
PROC_BODY="{\"revision\":{\"version\":$PROC_VER},\"component\":{\"id\":\"$PROC_DESER\",\"config\":{\"properties\":{$DESER_PROPS},\"concurrentlySchedulableTaskCount\":$PARALLELISM}}}"
nifi_api_put_checked "processors/$PROC_DESER" "$PROC_BODY" > /dev/null

# json_or_null <value> - "null" (unquoted, NiFi's way of explicitly clearing an optional
# property - see the prefix-deny-list/prefix-only-list comment above, same partial-merge PUT
# gotcha applies here) or the value quoted as a JSON string. Every per-type/per-category batch
# size below is optional and must always be included in its PUT body, one way or the other, for
# the same reason prefix-deny-list/prefix-only-list are.
json_or_null() {
  if [[ -n "$1" ]]; then echo "\"$1\""; else echo "null"; fi
}

echo "==> setting RedisBatchWriter's batch size (default + per-type overrides), timeout, and writer concurrency"
echo "  batch-size=$BATCH_SIZE  batch-timeout-ms=$BATCH_TIMEOUT_MS  writer-concurrency=$WRITER_CONCURRENCY"
echo "  batch-size-string=${BATCH_SIZE_STRING:-<none>}  batch-size-hash=${BATCH_SIZE_HASH:-<none>}  batch-size-list=${BATCH_SIZE_LIST:-<none>}"
echo "  batch-size-set=${BATCH_SIZE_SET:-<none>}  batch-size-zset=${BATCH_SIZE_ZSET:-<none>}  batch-size-stream=${BATCH_SIZE_STREAM:-<none>}"
PROC_VER="$(nifi_current_version "processors/$PROC_WRITER")" || true
WRITER_PROPS="\"batch-size\":\"$BATCH_SIZE\",\"batch-timeout-ms\":\"$BATCH_TIMEOUT_MS\""
WRITER_PROPS="$WRITER_PROPS,\"batch-size-string\":$(json_or_null "$BATCH_SIZE_STRING")"
WRITER_PROPS="$WRITER_PROPS,\"batch-size-hash\":$(json_or_null "$BATCH_SIZE_HASH")"
WRITER_PROPS="$WRITER_PROPS,\"batch-size-list\":$(json_or_null "$BATCH_SIZE_LIST")"
WRITER_PROPS="$WRITER_PROPS,\"batch-size-set\":$(json_or_null "$BATCH_SIZE_SET")"
WRITER_PROPS="$WRITER_PROPS,\"batch-size-zset\":$(json_or_null "$BATCH_SIZE_ZSET")"
WRITER_PROPS="$WRITER_PROPS,\"batch-size-stream\":$(json_or_null "$BATCH_SIZE_STREAM")"
PROC_BODY="{\"revision\":{\"version\":$PROC_VER},\"component\":{\"id\":\"$PROC_WRITER\",\"config\":{\"properties\":{$WRITER_PROPS},\"concurrentlySchedulableTaskCount\":$WRITER_CONCURRENCY}}}"
nifi_api_put_checked "processors/$PROC_WRITER" "$PROC_BODY" > /dev/null

if [[ -n "$PROC_MTH" ]]; then
  echo "==> setting ModuleTypeHandler's batch size (default + per-category overrides), timeout, dfly-to-dfly optimizations, and parallelism"
  echo "  batch-size=$MODULE_BATCH_SIZE  batch-timeout-ms=$MODULE_BATCH_TIMEOUT_MS  dfly-to-dfly=$DFLY_TO_DFLY  parallelism=$PARALLELISM"
  echo "  batch-size-json=${BATCH_SIZE_JSON:-<none>}  batch-size-topk=${BATCH_SIZE_TOPK:-<none>}  batch-size-bloom-cms=${BATCH_SIZE_BLOOM_CMS:-<none>}"
  PROC_VER="$(nifi_current_version "processors/$PROC_MTH")" || true
  MTH_PROPS="\"batch-size\":\"$MODULE_BATCH_SIZE\",\"batch-timeout-ms\":\"$MODULE_BATCH_TIMEOUT_MS\",\"dfly-to-dfly\":\"$DFLY_TO_DFLY\""
  MTH_PROPS="$MTH_PROPS,\"batch-size-json\":$(json_or_null "$BATCH_SIZE_JSON")"
  MTH_PROPS="$MTH_PROPS,\"batch-size-topk\":$(json_or_null "$BATCH_SIZE_TOPK")"
  MTH_PROPS="$MTH_PROPS,\"batch-size-bloom-cms\":$(json_or_null "$BATCH_SIZE_BLOOM_CMS")"
  PROC_BODY="{\"revision\":{\"version\":$PROC_VER},\"component\":{\"id\":\"$PROC_MTH\",\"config\":{\"properties\":{$MTH_PROPS},\"concurrentlySchedulableTaskCount\":$PARALLELISM}}}"
  nifi_api_put_checked "processors/$PROC_MTH" "$PROC_BODY" > /dev/null
else
  echo "==> ModuleTypeHandler processor not found; skipping its batch tuning (older flow definition?)"
fi

# ensure_ssl_context_service - reuses an existing SSL Context Service in this process group if
# one's already there, else creates one pointed at the NiFi container's own JVM default
# truststore (found via $JAVA_HOME, not a hardcoded JDK version string, since that changes
# with image updates) - it already trusts publicly-CA-signed endpoints like DragonflyDB
# Cloud's out of the box, no client certificate needed. NiFi's StandardSSLContextService
# requires *some* keystore or truststore to be populated (a bare instance with neither is
# invalid), so a "just trust the system CAs" bare config isn't an option here.
# Prints the service's id on success. Both connection pools' require-tls=true needs this,
# per AbstractRedisConnectionPoolService's customValidate - required=true and no default, so
# NiFi refuses to validate the service without one.
SSL_CONTEXT_SVC=""
ensure_ssl_context_service() {
  if [[ -n "$SSL_CONTEXT_SVC" ]]; then
    echo "$SSL_CONTEXT_SVC"
    return
  fi

  local svc_id resp
  # `|| true`: a lookup failure here falls through to the "not found, create one" branch below
  # exactly like a genuine not-found does today - consistent with this function's existing
  # design, which already treats the two the same way.
  svc_id="$(echo "$SVC_LIST_JSON" | py3 "
import json, sys
d = json.load(sys.stdin)
for s in d['controllerServices']:
    for api in s['component'].get('controllerServiceApis', []):
        if 'SSLContextService' in api.get('type', ''):
            print(s['id']); sys.exit()
")" || true

  if [[ -z "$svc_id" ]]; then
    echo "==> creating an SSL Context Service" >&2
    # `|| true`: without it, a bare connectivity failure would abort the script here instead of
    # reaching the deliberate "-z svc_id" error check right below, which already exists to
    # handle exactly this (a non-JSON/empty response).
    resp="$(nifi_api_post "process-groups/$PG_ID/controller-services" \
      '{"revision":{"version":0},"component":{"type":"org.apache.nifi.ssl.StandardSSLContextService","bundle":{"group":"org.apache.nifi","artifact":"nifi-ssl-context-service-nar","version":"2.11.0"}}}')" || true
    svc_id="$(echo "$resp" | py3 "import json,sys
d = json.load(sys.stdin)
print(d.get('component', {}).get('id', ''))" 2>/dev/null)"
    if [[ -z "$svc_id" ]]; then
      echo "error: could not create an SSL Context Service - raw API response: $resp" >&2
      exit 1
    fi
  fi

  # Applied unconditionally every run, whether the service was just created or already existed
  # from a prior run - see this function's own doc comment above for why a truststore is needed.
  local java_home cacerts_path ver body
  # Checked explicitly (not just `|| true`) rather than silently falling through: an empty
  # java_home would still produce a plausible-looking (but wrong) cacerts_path, silently
  # misconfiguring the SSL Context Service's truststore path instead of failing loudly - worse
  # than the plain crash this is replacing.
  java_home="$(runtime_exec "$CONTAINER_NAME" sh -c 'echo $JAVA_HOME')" || true
  if [[ -z "$java_home" ]]; then
    echo "error: could not read \$JAVA_HOME from container '$CONTAINER_NAME'" >&2
    exit 1
  fi
  cacerts_path="${java_home}/lib/security/cacerts"
  echo "==> pointing the SSL Context Service at the container's JVM default truststore ($cacerts_path)" >&2
  ver="$(nifi_current_version "controller-services/$svc_id")" || true
  body="{\"revision\":{\"version\":$ver},\"component\":{\"id\":\"$svc_id\",\"properties\":{\"Truststore Filename\":\"$cacerts_path\",\"Truststore Password\":\"changeit\",\"Truststore Type\":\"PKCS12\"}}}"
  resp="$(nifi_api_put_checked "controller-services/$svc_id" "$body")"

  # In case the guessed property names above are wrong, print what NiFi actually expects so a
  # fix is a one-line change instead of another round-trip.
  echo "$resp" | py3 "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit()
print('  SSL Context Service properties (name -> current value):', file=sys.stderr)
for name, value in d.get('component', {}).get('properties', {}).items():
    print(f'    {name!r}: {value!r}', file=sys.stderr)
" >&2

  SSL_CONTEXT_SVC="$svc_id"
  echo "$SSL_CONTEXT_SVC"
}

# set_pool_properties <svc_id> <conn_str> <require_tls> <connection_mode> - connection-mode is
# always included explicitly (never conditionally omitted), same reasoning as
# prefix-deny-list/prefix-only-list above: NiFi's property PUT is a partial merge, so leaving
# it out would leave whatever mode a previous run left in place instead of resetting it.
set_pool_properties() {
  local svc_id="$1" conn_str="$2" require_tls="$3" connection_mode="$4"
  local mode_upper ver body
  mode_upper="$(tr '[:lower:]' '[:upper:]' <<< "$connection_mode")"
  ver="$(nifi_current_version "controller-services/$svc_id")" || true
  if [[ "$require_tls" == "true" ]]; then
    local ssl_id
    ssl_id="$(ensure_ssl_context_service)"
    body="{\"revision\":{\"version\":$ver},\"component\":{\"id\":\"$svc_id\",\"properties\":{\"connection-string\":\"$conn_str\",\"require-tls\":\"$require_tls\",\"ssl-context-service\":\"$ssl_id\",\"connection-mode\":\"$mode_upper\"}}}"
  else
    body="{\"revision\":{\"version\":$ver},\"component\":{\"id\":\"$svc_id\",\"properties\":{\"connection-string\":\"$conn_str\",\"require-tls\":\"$require_tls\",\"connection-mode\":\"$mode_upper\"}}}"
  fi
  nifi_api_put_checked "controller-services/$svc_id" "$body" > /dev/null
}

echo "==> setting source Redis connection details (mode=$SOURCE_CONNECTION_MODE)"
set_pool_properties "$SVC_SOURCE" "$SOURCE_CONNECTION_STRING" "$SOURCE_REQUIRE_TLS" "$SOURCE_CONNECTION_MODE"

echo "==> setting target Dragonfly connection details (mode=$TARGET_CONNECTION_MODE)"
set_pool_properties "$SVC_TARGET" "$TARGET_CONNECTION_STRING" "$TARGET_REQUIRE_TLS" "$TARGET_CONNECTION_MODE"

echo "==> enabling controller services"
nifi_cli pg-enable-services -pgid "$PG_ID" || true

echo "==> controller service status"
SERVICES_OK=true
nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
bad = False
for s in d['controllerServices']:
    c = s['component']
    print(f\"  {c['name']}: state={c.get('state')} validation={c.get('validationStatus')}\")
    if c.get('validationStatus') != 'VALID':
        bad = True
        for e in (c.get('validationErrors') or []):
            print(f'    - {e}')
sys.exit(1 if bad else 0)
" || SERVICES_OK=false

echo "==> processor status"
# `|| true`: this is a read-only display, same as the controller-service check above (which
# already guards itself via `|| SERVICES_OK=false`) - a transient API failure here shouldn't
# abort the rest of the script, just skip this one report.
nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
for p in d['processors']:
    c = p['component']
    print(f\"  {c['name']}: {c.get('validationStatus')}\")
    for e in (c.get('validationErrors') or []):
        print(f'    - {e}')
" || true

if [[ "$SERVICES_OK" != "true" ]]; then
  echo "warning: one or more controller services are still invalid; the flow will not run cleanly until that's fixed." >&2
fi

# run_oneshot_processor <proc-id> <label> [timeout-seconds] - sets this run's migration-id on
# the given processor, starts it, waits for it to reach RUNNING, then stops it again. All the
# real work for SearchIndexExporter/SearchIndexRehydrator happens once, synchronously, in their
# own @OnScheduled method - a processor only reaches RUNNING once that returns, so polling for
# RUNNING is a genuine "did the one-shot work finish" signal, not just "did NiFi accept the
# request" (same distinction wait_for_processors_stopped's own comment makes for pg-stop).
# Stopping it again afterward resets it to pick up a fresh migration-id (and re-run its
# @OnScheduled) the next time this script runs. A processor whose @OnScheduled throws never
# reaches RUNNING - NiFi just keeps retrying it administratively forever - so this gives up after
# <timeout-seconds> and reports whatever status NiFi currently shows, rather than hanging.
run_oneshot_processor() {
  local proc_id="$1" label="$2" timeout="${3:-120}" waited=0 status ver body
  ver="$(nifi_current_version "processors/$proc_id")" || true
  body="{\"revision\":{\"version\":$ver},\"component\":{\"id\":\"$proc_id\",\"config\":{\"properties\":{\"migration-id\":\"$MIGRATION_ID\"}}}}"
  nifi_api_put_checked "processors/$proc_id" "$body" > /dev/null

  ver="$(nifi_current_version "processors/$proc_id")" || true
  nifi_api_put "processors/$proc_id/run-status" "{\"revision\":{\"version\":$ver},\"state\":\"RUNNING\",\"disconnectedNodeAcknowledged\":false}" > /dev/null

  while true; do
    status="$(nifi_api_get "processors/$proc_id" | py3 "
import json, sys
d = json.load(sys.stdin)
print(d.get('status', {}).get('runStatus', ''))
")" || status=""
    [[ "$status" == "Running" ]] && break
    if [[ "$waited" -ge "$timeout" ]]; then
      echo "warning: $label did not reach RUNNING within ${timeout}s (last status: ${status:-unknown}) - check its bulletins/validation errors in the NiFi UI or nifi-app.log; leaving it running so it keeps retrying" >&2
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done

  ver="$(nifi_current_version "processors/$proc_id")" || true
  nifi_api_put "processors/$proc_id/run-status" "{\"revision\":{\"version\":$ver},\"state\":\"STOPPED\",\"disconnectedNodeAcknowledged\":false}" > /dev/null
  echo "  $label completed"
}

# Runs before the main flow starts, not after it finishes: unlike the original Lua/temp-key
# design, SearchIndexRehydrator reads definitions back from the shared Cursor State Cache
# (SearchIndexExporter's own hand-off, not the migrated data itself), so it has no dependency on
# any key having actually migrated yet. Creating an index this early just means it starts out
# empty (or partially populated) and gets kept current by the search module's own normal
# on-write indexing as ModuleTypeHandler/RedisBatchWriter write matching documents during the
# migration - functionally equivalent to creating it after the fact, without needing to wait for
# (or risk skipping, if sync polling couldn't confirm completion) DBSIZE/NiFi-idle confirmation
# the way the old design had to.
if [[ "$IGNORE_SEARCH_INDEXES" != "true" ]]; then
  if [[ -n "$PROC_SIDX_EXPORT" && -n "$PROC_SIDX_REHYDRATE" ]]; then
    echo "==> exporting search index definitions on the source (SearchIndexExporter)"
    run_oneshot_processor "$PROC_SIDX_EXPORT" "SearchIndexExporter" || true
    echo "==> rebuilding search indexes on the target (SearchIndexRehydrator)"
    run_oneshot_processor "$PROC_SIDX_REHYDRATE" "SearchIndexRehydrator" || true
  else
    echo "warning: SearchIndexExporter/SearchIndexRehydrator not found in this flow (older flow definition?) - search index migration skipped. Delete and re-import the process group (reset-r2dfly-flow.sh, then deploy-to-nifi.sh) to pick them up, or pass --ignore-search-indexes to silence this." >&2
  fi
fi

should_start() {
  case "$START_MODE" in
    yes) return 0 ;;
    no) return 1 ;;
    ask)
      if [[ ! -t 0 ]]; then
        echo "==> non-interactive shell; not starting the flow (pass --start to start automatically)"
        return 1
      fi
      read -r -p "Start the R2Dfly Migration flow now? [y/N] " reply
      [[ "$reply" =~ ^[Yy]$ ]]
      ;;
  esac
}

# redis_dbsize <container> <connection-string> <connection-mode> [cached-node-list] - see
# redis_cli in redis-lib.sh for how the container-vs-local fallback works. Prints nothing
# (caller treats empty as "couldn't read it") if neither a matching container nor a local
# redis-cli binary is available. connection-mode "cluster" sums DBSIZE across all master nodes
# (cluster-lib.sh) instead of querying a single node, which would otherwise only reflect that
# node's own shard. The optional 4th arg is a node list already fetched via
# cluster_lib_each_master (see SOURCE_CLUSTER_NODES/TARGET_CLUSTER_NODES below) - when given,
# this sums DBSIZE directly over it (cluster_lib_dbsize_sum_nodes) instead of re-running
# CLUSTER NODES from scratch (cluster_lib_dbsize_sum) - callers that poll repeatedly (like
# poll_until_synced, every few seconds for the whole migration) should always pass it, since
# cluster topology essentially never changes mid-migration.
redis_dbsize() {
  local container="$1" connstr="$2" mode="${3:-standalone}" cached_nodes="${4:-}"
  # `|| true` on both branches: this function's own doc comment promises "prints nothing ...
  # caller treats empty as couldn't read it" on failure - without it, a connection failure
  # would abort the whole script under set -e instead of actually reaching that documented,
  # already-handled-by-callers behavior.
  if [[ "$mode" == "cluster" ]]; then
    if [[ -n "$cached_nodes" ]]; then
      cluster_lib_dbsize_sum_nodes "$cached_nodes" 2>/dev/null || true
    else
      cluster_lib_dbsize_sum "$container" "$connstr" 2>/dev/null || true
    fi
  else
    redis_cli "$container" "$connstr" DBSIZE 2>/dev/null | tr -d '\r' || true
  fi
}

# probe_round_trip_ms <container> <connstr> - measures round-trip latency as PROBE_PINGS
# back-to-back PINGs inside a SINGLE redis-cli invocation (`-r N`), not one redis_cli() call per
# ping - this project's own container-exec/toolbox-fallback overhead (spinning up a whole
# container per call on the fallback path) would otherwise swamp the real number entirely.
# Deliberately not redis-cli's own --latency: it never terminates on its own (loops forever
# over one \r-updated line) and there's no portable way to bound it here - this project must
# run unmodified on a bare macOS host, which has neither GNU coreutils' `timeout` nor
# `gtimeout` (confirmed directly - neither exists there by default). Timed with bash's own
# `time` builtin (TIMEFORMAT) instead of `date +%N`, for the same bare-macOS-portability reason.
# Tries the named container first, then the toolbox fallback - same order as redis_cli() in
# redis-lib.sh, but that function can't be reused here since it buffers a single reply via
# command substitution, not a timed multi-rep run. Prints an integer milliseconds estimate, or
# nothing (and a nonzero return) if neither path could reach it at all.
PROBE_PINGS=50
probe_round_trip_ms() {
  local container="$1" connstr="$2" elapsed rc
  local TIMEFORMAT='%R'
  # `|| rc=$?` (one compound statement, not two) is load-bearing under set -e: `elapsed=$(...);
  # rc=$?` as separate statements dies on the first exec attempt's failure before `rc=$?` ever
  # runs, defeating the toolbox-fallback logic right below for the common case (a real remote
  # Redis not locally exec-able) instead of just falling through to it.
  rc=0
  elapsed="$( { time "$RUNTIME" exec -i "$container" redis-cli -r "$PROBE_PINGS" PING >/dev/null 2>/dev/null; } 2>&1 )" || rc=$?
  if [[ $rc -ne 0 ]]; then
    rc=0
    local fallback_connstr="${connstr%%,*}"
    elapsed="$( { time toolbox_run redis-cli -u "$fallback_connstr" -r "$PROBE_PINGS" PING >/dev/null 2>/dev/null; } 2>&1 )" || rc=$?
    [[ $rc -eq 0 ]] || return 1
  fi
  awk -v e="$elapsed" -v n="$PROBE_PINGS" 'BEGIN { v = (e * 1000 / n) + 0.5; if (v < 1) v = 1; printf "%d", int(v) }'
}

# auto_tune_poll_durations - best-effort: extends (never shrinks) --poll-timeout/
# --poll-max-duration to fit the actual migration's size, instead of leaving a one-size-fits-
# all constant that a genuinely large migration would trip while perfectly healthy. Skips
# itself entirely - silently keeping the plain defaults/whatever was explicitly passed - if the
# user already gave both flags explicitly, or if either side's latency can't be measured.
# The model is deliberately rough (see --help): round trips ~= keys / (batch-size *
# writer-concurrency), each costing about the slower side's measured latency, times a flat
# safety multiplier for everything a bare PING round trip can't capture at all - real payload
# transfer time, per-type (JSON/TopK/search-index) reconstruction overhead, general processing.
auto_tune_poll_durations() {
  if [[ "$POLL_TIMEOUT_EXPLICIT" == "true" && "$POLL_MAX_DURATION_EXPLICIT" == "true" ]]; then
    return 0
  fi

  echo "==> estimating a generous poll timeout: probing round-trip latency to source/target (${PROBE_PINGS:-3} pings each)"
  local src_ms tgt_ms
  
  if ! src_ms="$(probe_round_trip_ms "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"; then
    echo "  couldn't measure source latency - keeping plain defaults" >&2
    return 0
  fi
  if ! tgt_ms="$(probe_round_trip_ms "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"; then
    echo "  couldn't measure target latency - keeping plain defaults" >&2
    return 0
  fi

  local rtt_ms=$(( src_ms > tgt_ms ? src_ms : tgt_ms ))
  
  local src_count
  src_count="$(redis_dbsize "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING" "$SOURCE_CONNECTION_MODE" "${SOURCE_CLUSTER_NODES:-}")" || true
  if [[ -z "$src_count" ]]; then
    echo "  couldn't read source DBSIZE - keeping plain defaults" >&2
    return 0
  fi

  local safety_multiplier=5
  local batch_sz=${BATCH_SIZE:-50}
  local concurrency=${WRITER_CONCURRENCY:-1}
  
  local per_round=$(( batch_sz * concurrency ))
  [[ $per_round -lt 1 ]] && per_round=1
  
  local batches=$(( (src_count + per_round - 1) / per_round ))
  [[ $batches -lt 1 ]] && batches=1

  local estimated_seconds=$(( (batches * rtt_ms * safety_multiplier) / 1000 ))
  [[ $estimated_seconds -lt 60 ]] && estimated_seconds=60

  echo "  source=$src_count keys, round-trip~=${rtt_ms}ms, batch-size=$batch_sz, writer-concurrency=$concurrency -> ~${estimated_seconds}s estimated duration cap"

  if [[ "$POLL_MAX_DURATION_EXPLICIT" != "true" && $estimated_seconds -gt $POLL_MAX_DURATION ]]; then
    echo "  raising --poll-max-duration from ${POLL_MAX_DURATION}s to ${estimated_seconds}s to match"
    POLL_MAX_DURATION=$estimated_seconds
  fi

  local stall_estimate=$(( estimated_seconds / 4 ))
  [[ $stall_estimate -lt 30 ]] && stall_estimate=30
  [[ $stall_estimate -gt 1800 ]] && stall_estimate=1800
  
  if [[ "$POLL_TIMEOUT_EXPLICIT" != "true" && $stall_estimate -gt $POLL_TIMEOUT ]]; then
    echo "  raising --poll-timeout from ${POLL_TIMEOUT}s to ${stall_estimate}s to match"
    POLL_TIMEOUT=$stall_estimate
  fi
}

# nifi_poll_stats <pg_id> - one-line, best-effort supplement to the DBSIZE numbers above, from
# the flow's own live status: queued FlowFiles/bytes and active thread count are real-time
# gauges (not a rolling window), so they're a genuinely useful "is the pipeline backed up or
# keeping up" signal DBSIZE alone can't show. bytesWritten/"written" IS a rolling ~5-minute
# window (NiFi's own convention, same as its UI's "5 min" stats) rather than a since-start
# cumulative total - labeled "recent" in the printed line so it isn't mistaken for one. Prints
# nothing (caller just skips the line) if the API call fails for any reason - this is a
# supplementary display, never worth failing the actual poll loop over.
nifi_poll_stats() {
  local pgid="$1"
  # `|| true`: this function's own doc comment promises "prints nothing ... if the API call
  # fails for any reason" - the python side already swallows its own parsing errors via
  # try/except, but a bare nifi_api_get connectivity failure still needs this to actually reach
  # that documented behavior instead of aborting the whole poll loop under set -e.
  nifi_api_get "flow/process-groups/$pgid/status" 2>/dev/null | py3 "
import json, sys
try:
    d = json.load(sys.stdin)
    s = d['processGroupStatus']['aggregateSnapshot']
    print(f\"{s['queuedCount']}|{s['queuedSize']}|{s['activeThreadCount']}|{s['written']}\")
except Exception:
    pass
" 2>/dev/null || true
}

# print_timeout_warning - shared by both give-up paths below: --poll-timeout/--poll-max-duration
# are both somewhat arbitrary backstops on OUR polling loop, not on the flow itself - NiFi keeps
# migrating regardless of whether this script is still watching, so giving up here is not the
# same thing as the migration being done or stuck.
print_timeout_warning() {
  echo "warning: the flow of keys from source to target may continue past this script's timeout (the timeout is somewhat arbitrary in length). Please continue to check your target server, or check the NiFi flows for migrating keys, if some keys seem missing at first." >&2
  echo "warning: run simple-troubleshoot.sh if there appears to be any issue with the migration." >&2
}

poll_until_synced() {
  local interval="${POLL_INTERVAL:-5}"
  local timeout="${POLL_TIMEOUT:-300}"
  local max_duration="${POLL_MAX_DURATION:-3600}"
  local max_quiet="${POLL_QUIET_ROUNDS:-3}"

  echo "==> tracking DBSIZE on '$SOURCE_CONTAINER' (source) and '$TARGET_CONTAINER' (target) every ${interval}s"
  echo "    (gives up after ${timeout}s with no forward progress, or ${max_duration}s total either way)"
  
  local elapsed=0 last_progress_elapsed=0 src tgt last_tgt="" quiet_rounds=0 rate pct
  
  while true; do
    src="$(redis_dbsize "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING" "$SOURCE_CONNECTION_MODE" "${SOURCE_CLUSTER_NODES:-}")" || true
    tgt="$(redis_dbsize "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING" "$TARGET_CONNECTION_MODE" "${TARGET_CLUSTER_NODES:-}")" || true
    
    if [[ -z "$src" || -z "$tgt" ]]; then
      echo "  could not read DBSIZE (source='$src' target='$tgt') - check your redis-cli path. Giving up." >&2
      return 1
    fi

    pct=0
    [[ "$src" -gt 0 ]] && pct=$((tgt * 100 / src))
    
    rate=0
    if [[ -n "$last_tgt" && "$interval" -gt 0 ]]; then
      # Multiplied by 10 to give 1 decimal place of accuracy (e.g., 15 means 1.5 keys/s)
      local raw_rate=$(( ((tgt - last_tgt) * 10) / interval ))
      rate="$(awk -v r="$raw_rate" 'BEGIN {print r/10}')"
    fi
    
    echo "  source=$src  target=$tgt  (${pct}%, ${rate} keys/s)"

    local nifi_stats queued_count queued_size active_threads written
    nifi_stats="$(nifi_poll_stats "$PG_ID")" || true
    if [[ -n "$nifi_stats" ]]; then
      IFS='|' read -r queued_count queued_size active_threads written <<< "$nifi_stats"
      echo "    nifi: ${queued_count} FlowFiles queued (${queued_size}), ${active_threads} active threads, ${written} written (recent)"
    fi

    # nifi_idle gates both completion conditions below - DBSIZE alone isn't proof the flow is
    # actually done: other in-flight FlowFiles may just not have reached
    # RedisBatchWriter/ModuleTypeHandler yet even though the target's DBSIZE has caught up or
    # gone quiet, or (for the Stable Plateau heuristic specifically, built for prefix-filtered
    # migrations) DBSIZE simply isn't moving this tick because the scan is churning through a run
    # of filtered-out keys. Only actually gate on this when the NiFi stats call itself succeeded
    # this tick - if it didn't, fall back to DBSIZE alone (today's behavior) rather than stalling
    # forever on a signal we can't read.
    local nifi_idle=true
    if [[ -n "$nifi_stats" && "$queued_count" =~ ^[0-9]+$ && "$active_threads" =~ ^[0-9]+$ ]]; then
      [[ "$queued_count" -eq 0 && "$active_threads" -eq 0 ]] || nifi_idle=false
    fi

    # Completion Condition 1: Direct Catchup
    if [[ "$tgt" -ge "$src" ]]; then
      if [[ "$nifi_idle" == "true" ]]; then
        echo "==> target has caught up to source ($tgt/$src keys, 100%) - migration appears complete"
        return 0
      fi
      echo "    target has caught up on DBSIZE, but NiFi still shows ${queued_count} queued / ${active_threads} active - waiting for the flow to drain before treating this as synced"
      last_progress_elapsed=$elapsed
      quiet_rounds=0
    # Completion Condition 2: Stable Plateau (Heuristic for filtered migrations) - only
    # evaluated once Condition 1 hasn't already caught (and isn't itself just waiting on NiFi
    # to drain), so a caught-up-but-draining tick doesn't also spam this heuristic's own
    # "waiting to drain" message right below it.
    elif [[ -n "$last_tgt" && "$tgt" -eq "$last_tgt" && "$tgt" -gt 0 ]]; then
      quiet_rounds=$((quiet_rounds + 1))
      if [[ "$quiet_rounds" -ge "$max_quiet" ]]; then
        if [[ "$nifi_idle" == "true" ]]; then
          echo "==> target stopped growing at $tgt keys (${pct}% of source) - treating scan as complete"
          return 0
        fi
        echo "    target has been stable at $tgt keys for ${quiet_rounds} rounds, but NiFi still shows ${queued_count} queued / ${active_threads} active - waiting for the flow to drain before treating this as synced"
        last_progress_elapsed=$elapsed
      fi
    elif [[ "$tgt" != "$last_tgt" ]]; then
      quiet_rounds=0
      last_progress_elapsed=$elapsed
    fi
    last_tgt="$tgt"

    local stalled_for=$((elapsed - last_progress_elapsed))
    if [[ "$stalled_for" -ge "$timeout" ]]; then
      echo "==> gave up after ${stalled_for}s with no growth (target stuck at $tgt/$src keys)."
      print_timeout_warning
      return 1
    fi
    
    if [[ "$elapsed" -ge "$max_duration" ]]; then
      echo "==> gave up after ${max_duration}s total duration limit reached."
      print_timeout_warning
      return 1
    fi

    sleep "$interval"
    elapsed=$((elapsed + interval))
  done
}


if should_start; then
  echo "==> starting the flow"
  nifi_cli pg-start -pgid "$PG_ID"
  echo "==> flow started"
  if [[ "$POLL_MODE" == "yes" ]]; then
    # Fetched once here (CLUSTER NODES, on whichever side is cluster-mode) and reused for every
    # DBSIZE call auto_tune_poll_durations/poll_until_synced make below - see redis_dbsize's own
    # comment for why re-deriving this from scratch on every poll tick would be wasteful.
    SOURCE_CLUSTER_NODES=""
    TARGET_CLUSTER_NODES=""
    [[ "$SOURCE_CONNECTION_MODE" == "cluster" ]] && SOURCE_CLUSTER_NODES="$(cluster_lib_each_master "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
    [[ "$TARGET_CONNECTION_MODE" == "cluster" ]] && TARGET_CLUSTER_NODES="$(cluster_lib_each_master "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
    auto_tune_poll_durations
    poll_until_synced || true
  fi
else
  echo "==> flow left stopped. Start it later with: nifi pg-start -pgid $PG_ID (via the NiFi CLI), or from the canvas."
fi

echo "==> migration run '$MIGRATION_ID' finished; full settings/activity log at $MIGRATION_LOG"
