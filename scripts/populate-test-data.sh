#!/usr/bin/env bash
# Populates a Redis-compatible source instance with a variety of key types for
# exercising the migration flow - both the core types RedisScanReader/
# RedisTypeDeserializer natively handle (string, hash, list, set, zset, stream)
# and the newer Redis 8 / Redis Stack module types that should route through
# RedisScanReader's "unknown" relationship and ModuleTypeHandler (JSON, TopK,
# CountMinSketch, TDigest, Bloom, CuckooFilter, TimeSeries, VectorSet).
#
# One type name is interpreted for clarity: "CountMinSum" -> Count-Min Sketch
# (CMS.*). "Arrays" is Redis's native array type (ARSET/ARGET/etc., Redis 8.8+,
# https://redis.io/docs/latest/develop/data-types/arrays/) - a sparse,
# index-addressable structure, distinct from both Lists and JSON arrays.
#
# --connection-mode cluster targets a real Redis/Valkey/Dragonfly Cluster: commands are
# partitioned client-side by each key's hash slot (CRC16, honoring {hash-tag}s) and sent
# directly to the master node that owns them - NOT via `redis-cli -c`, which only works
# interactively (it re-issues one command after seeing a MOVED reply); fed a whole batch via
# stdin, redis-cli has no chance to react per-command, so nearly every key not already on the
# first-contacted node would otherwise come back as an unhandled MOVED error and never get
# written. Bulk loading uses concurrent plain connections (`--parallelism N`), NOT
# `redis-cli --pipe` - confirmed directly (against a real Dragonfly Cloud cluster AND a local
# Dragonfly container, but not against plain Redis, where it works fine) that `--pipe`'s
# mass-insert protocol desyncs against Dragonfly specifically, corrupting the stream and
# silently losing all data with garbled "ERR unknown command" output. Concurrent plain
# connections give a real speedup against a high-latency (e.g. cloud) target - a single
# connection here does one full network round-trip per command, ~54ms measured against a real
# cloud endpoint, so --count in the thousands is impractically slow (hours) over one connection.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/redis-lib.sh"
source "$PROJECT_ROOT/scripts/cluster-lib.sh"

ALL_TYPES="string hash json array zset set list stream topk countminsketch tdigest bloom cuckoofilter timeseries vectorset"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --container NAME          container to run redis-cli against (env REDIS_CONTAINER;
                             default: guessed from --connection-string's hostname)
  --connection-string S     redis://[:password@]host:port (or rediss:// for TLS) (env
                             REDIS_CONNECTION_STRING); used directly if --container isn't
                             running locally. For --connection-mode cluster, any one reachable
                             seed node is enough - the rest of the cluster's topology is
                             discovered automatically.
  --connection-mode M       standalone or cluster (env CONNECTION_MODE, default: standalone).
                             cluster partitions commands by key hash-slot and sends each
                             directly to its owning master - see the header comment above for
                             why (plain redis-cli -c does not work for this).
  --count N                 keys to create per type (env COUNT, default: 50)
  --prefix PREFIX           key prefix (env KEY_PREFIX, default: test)
  --types LIST              comma-separated subset of types to populate (env TYPES,
                             default: all of: $ALL_TYPES)
  --parallelism N           concurrent redis-cli connections per node (env PARALLELISM,
                             default: 1). Raise this for a remote/cloud target - each
                             connection here does one full round-trip per command, so a
                             single connection against real network latency is slow at scale
                             (e.g. --count 10000 across several types can take hours over one
                             connection; try --parallelism 10-20 for a cloud target).
  --flush                   FLUSHDB the source before populating (destructive - off by
                             default; in cluster mode, issues FLUSHDB against every master)
  -h, --help                this help

Types requiring Redis 8+ or Redis Stack modules (json, array, topk, countminsketch,
tdigest, bloom, cuckoofilter, timeseries, vectorset) are skipped with a warning,
not a hard failure, if the server doesn't recognize the command.

Example:
  $(basename "$0") --connection-string redis://source-redis:6379

  $(basename "$0") --connection-mode cluster --parallelism 10 \\
    --connection-string rediss://default:pw@my-cluster.dragonflydb.cloud:6385 \\
    --count 10000 --types string,hash,zset,set,list,stream
EOF
}

CONTAINER="${REDIS_CONTAINER:-}"
CONNECTION_STRING="${REDIS_CONNECTION_STRING:-}"
CONNECTION_MODE="${CONNECTION_MODE:-standalone}"
COUNT="${COUNT:-50}"
KEY_PREFIX="${KEY_PREFIX:-test}"
TYPES="${TYPES:-$ALL_TYPES}"
TYPES="${TYPES//,/ }"
PARALLELISM="${PARALLELISM:-1}"
FLUSH="no"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container) CONTAINER="$2"; shift 2 ;;
    --connection-string) CONNECTION_STRING="$2"; shift 2 ;;
    --connection-mode) CONNECTION_MODE="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --prefix) KEY_PREFIX="$2"; shift 2 ;;
    --types) TYPES="${2//,/ }"; shift 2 ;;
    --parallelism) PARALLELISM="$2"; shift 2 ;;
    --flush) FLUSH="yes"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$CONTAINER" && -z "$CONNECTION_STRING" ]]; then
  echo "error: need --container or --connection-string (or REDIS_CONTAINER/REDIS_CONNECTION_STRING)" >&2
  usage; exit 1
fi
[[ -n "$CONTAINER" ]] || CONTAINER="$(redis_lib_extract_host "$CONNECTION_STRING")"

CONNECTION_MODE="$(echo "$CONNECTION_MODE" | tr '[:upper:]' '[:lower:]')"
case "$CONNECTION_MODE" in
  standalone|cluster) ;;
  *) echo "error: --connection-mode must be standalone or cluster (got '$CONNECTION_MODE')" >&2; usage; exit 1 ;;
esac
if ! [[ "$PARALLELISM" =~ ^[0-9]+$ ]] || [[ "$PARALLELISM" -lt 1 ]]; then
  echo "error: --parallelism must be a positive integer (got '$PARALLELISM')" >&2; usage; exit 1
fi

redis_lib_pick_runtime

POPULATE_TMP="$(mktemp -d)"
trap 'rm -rf "$POPULATE_TMP"' EXIT

# Cluster topology (masters + slot ranges), discovered once and reused for every type below -
# avoids re-querying CLUSTER NODES per type. MASTER_HOST/MASTER_PORT/MASTER_START/MASTER_END
# are parallel arrays, one entry per master, in no particular order.
MASTER_HOST=() MASTER_PORT=() MASTER_START=() MASTER_END=()
if [[ "$CONNECTION_MODE" == "cluster" ]]; then
  echo "==> discovering cluster topology"
  CLUSTER_SEED="${CONNECTION_STRING%%,*}"
  TOPOLOGY_REPORT="$(_cluster_lib_topology "$CONTAINER" "$CLUSTER_SEED")"
  TOPOLOGY_STATUS="$(echo "$TOPOLOGY_REPORT" | awk -F'\t' '$1=="STATUS"{print $2; exit}')"
  if [[ "$TOPOLOGY_STATUS" != "OK" ]]; then
    echo "error: cluster topology check failed: $(echo "$TOPOLOGY_REPORT" | awk -F'\t' '$1=="STATUS"{print $3; exit}')" >&2
    exit 1
  fi
  while IFS=$'\t' read -r tag host port ranges; do
    [[ "$tag" == "MASTER" ]] || continue
    # ranges is "start-end[,start-end...]" - a master can own multiple non-contiguous ranges
    # after prior rebalancing; each becomes its own array entry pointing at the same node.
    IFS=',' read -ra parts <<< "$ranges"
    for part in "${parts[@]}"; do
      [[ "$part" == "(none)" ]] && continue
      local_start="${part%%-*}"
      local_end="${part##*-}"
      MASTER_HOST+=("$host")
      MASTER_PORT+=("$port")
      MASTER_START+=("$local_start")
      MASTER_END+=("$local_end")
    done
  done <<< "$(echo "$TOPOLOGY_REPORT" | grep '^MASTER')"
  echo "  ${#MASTER_HOST[@]} slot range(s) across $(echo "$TOPOLOGY_REPORT" | grep -c '^MASTER') master node(s)"
fi

gen_string()   { for i in $(seq 1 "$COUNT"); do echo "SET ${KEY_PREFIX}:string:$i \"string-value-$i\""; done; }
gen_hash()     { for i in $(seq 1 "$COUNT"); do echo "HSET ${KEY_PREFIX}:hash:$i field1 value1-$i field2 value2-$i field3 value3-$i"; done; }
gen_json()     { for i in $(seq 1 "$COUNT"); do echo "JSON.SET ${KEY_PREFIX}:json:$i . '{\"id\":$i,\"name\":\"item-$i\",\"active\":true}'"; done; }
gen_array()    { for i in $(seq 1 "$COUNT"); do echo "ARSET ${KEY_PREFIX}:array:$i 0 elem1-$i elem2-$i elem3-$i"; done; }
gen_zset()     { for i in $(seq 1 "$COUNT"); do echo "ZADD ${KEY_PREFIX}:zset:$i 1 member1-$i 2 member2-$i 3 member3-$i"; done; }
gen_set()      { for i in $(seq 1 "$COUNT"); do echo "SADD ${KEY_PREFIX}:set:$i member1-$i member2-$i member3-$i"; done; }
gen_list()     { for i in $(seq 1 "$COUNT"); do echo "RPUSH ${KEY_PREFIX}:list:$i item1-$i item2-$i item3-$i"; done; }
gen_stream()   { for i in $(seq 1 "$COUNT"); do echo "XADD ${KEY_PREFIX}:stream:$i * field1 value1-$i field2 value2-$i"; done; }
gen_topk() {
  for i in $(seq 1 "$COUNT"); do
    echo "TOPK.RESERVE ${KEY_PREFIX}:topk:$i 50 2000 7 0.925"
    echo "TOPK.ADD ${KEY_PREFIX}:topk:$i itemA-$i itemB-$i itemC-$i"
  done
}
gen_countminsketch() {
  for i in $(seq 1 "$COUNT"); do
    echo "CMS.INITBYDIM ${KEY_PREFIX}:countminsketch:$i 2000 5"
    echo "CMS.INCRBY ${KEY_PREFIX}:countminsketch:$i itemA-$i 1 itemB-$i 2"
  done
}
gen_tdigest() {
  for i in $(seq 1 "$COUNT"); do
    echo "TDIGEST.CREATE ${KEY_PREFIX}:tdigest:$i"
    echo "TDIGEST.ADD ${KEY_PREFIX}:tdigest:$i 1.5 2.5 3.5"
  done
}
gen_bloom() {
  for i in $(seq 1 "$COUNT"); do
    echo "BF.RESERVE ${KEY_PREFIX}:bloom:$i 0.01 1000"
    echo "BF.ADD ${KEY_PREFIX}:bloom:$i item-$i"
  done
}
gen_cuckoofilter() {
  for i in $(seq 1 "$COUNT"); do
    echo "CF.RESERVE ${KEY_PREFIX}:cuckoofilter:$i 1000"
    echo "CF.ADD ${KEY_PREFIX}:cuckoofilter:$i item-$i"
  done
}
gen_timeseries() {
  for i in $(seq 1 "$COUNT"); do
    echo "TS.CREATE ${KEY_PREFIX}:timeseries:$i"
    echo "TS.ADD ${KEY_PREFIX}:timeseries:$i * $i"
  done
}
gen_vectorset() { for i in $(seq 1 "$COUNT"); do echo "VADD ${KEY_PREFIX}:vectorset:$i VALUES 4 1.0 2.0 3.0 4.0 elem-$i"; done; }

# split_into_chunks <text> <outdir> <prefix> - splits <text> into up to $PARALLELISM files
# under <outdir>/<prefix>*, always on an even line boundary. Types that emit two lines per key
# (RESERVE+ADD, CREATE+ADD, etc.) must never be split mid-pair - forcing an even chunk size
# guarantees that uniformly, regardless of type; it's a no-op concern for single-line types.
split_into_chunks() {
  local text="$1" outdir="$2" prefix="$3" total chunk_size
  # `|| true`: `grep -c` exits 1 when the count is 0 - without this, an empty/whitespace-only
  # $text would abort the script under set -e right here, bypassing the very next line's own
  # graceful handling of exactly that case.
  total="$(printf '%s\n' "$text" | grep -c .)" || true
  [[ "$total" -eq 0 ]] && return 0
  chunk_size=$(( (total + PARALLELISM - 1) / PARALLELISM ))
  (( chunk_size % 2 != 0 )) && chunk_size=$((chunk_size + 1))
  (( chunk_size < 2 )) && chunk_size=2
  printf '%s\n' "$text" | split -l "$chunk_size" - "$outdir/$prefix"
}

# partition_by_node <commands> - splits <commands> (one Redis command per line, key as the
# 2nd whitespace-separated token - true for every gen_* function above) by each line's key's
# hash slot (CRC16, honoring {hash-tag}s - same algorithm Redis Cluster itself uses), writing
# each master's share to $POPULATE_TMP/node<i>.cmds. A RESERVE+ADD/CREATE+ADD pair always
# shares the same key, so both lines always land in the same node's file, in original order.
# py3 runs the python interpreter inside an isolated toolbox container with no access to the
# host filesystem (see toolbox-lib.sh) - it can only exchange data via stdin/stdout, never by
# writing files directly, so the actual per-node file split happens here in bash afterward.
partition_by_node() {
  local commands="$1"
  local tagged
  tagged="$( {
      for i in "${!MASTER_HOST[@]}"; do
        printf '%s\t%s\t%s\n' "$i" "${MASTER_START[$i]}" "${MASTER_END[$i]}"
      done
      echo '---COMMANDS---'
      printf '%s\n' "$commands"
    } | py3 "
import sys

ranges = []
reading_ranges = True
CRC16_TABLE = []
for i in range(256):
    c = i << 8
    for _ in range(8):
        c = ((c << 1) ^ 0x1021) if (c & 0x8000) else (c << 1)
    CRC16_TABLE.append(c & 0xFFFF)

def crc16(data):
    crc = 0
    for b in data:
        crc = ((crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ b) & 0xFF]) & 0xFFFF
    return crc

def hash_slot(key):
    if '{' in key:
        start = key.index('{')
        end = key.find('}', start + 1)
        if end > start + 1:
            key = key[start+1:end]
    return crc16(key.encode()) % 16384

for line in sys.stdin:
    line = line.rstrip('\n')
    if reading_ranges:
        if line == '---COMMANDS---':
            reading_ranges = False
            continue
        idx, s, e = line.split('\t')
        ranges.append((int(idx), int(s), int(e)))
        continue
    if not line:
        continue
    key = line.split(' ')[1]
    slot = hash_slot(key)
    node_idx = 0
    for idx, s, e in ranges:
        if s <= slot <= e:
            node_idx = idx
            break
    print(f'{node_idx}\t{line}')
"
  )"
  # Single awk pass over $tagged, fanning each line out to its node file by the leading index -
  # avoids re-scanning the whole tagged output once per master (was O(masters * lines)).
  printf '%s\n' "$tagged" | awk -F'\t' -v outdir="$POPULATE_TMP" '{ idx = $1; sub(/^[0-9]+\t/, ""); print > (outdir "/node" idx ".cmds") }'
}

# run_on <container> <connstr> <commands> - dispatches <commands> across up to $PARALLELISM
# concurrent plain redis-cli connections (NOT --pipe - see header comment) and returns their
# combined output on stdout.
run_on() {
  local container="$1" connstr="$2" commands="$3"
  local chunkdir="$POPULATE_TMP/run-$$-$RANDOM"
  mkdir -p "$chunkdir"
  split_into_chunks "$commands" "$chunkdir" "c"
  local f pids=()
  for f in "$chunkdir"/c*; do
    [[ -e "$f" ]] || continue
    ( redis_cli "$container" "$connstr" < "$f" > "$f.out" 2>&1 ) &
    pids+=("$!")
  done
  local pid
  for pid in "${pids[@]:-}"; do
    [[ -n "$pid" ]] && wait "$pid" || true
  done
  # `|| true`: if the glob matched no files (no chunks were ever written), `cat` exits nonzero
  # and would abort the script under set -e - defensive, since callers already guard against an
  # empty $commands that would cause this today.
  cat "$chunkdir"/*.out 2>/dev/null || true
  rm -rf "$chunkdir"
}

run_batch() {
  local label="$1" commands="$2"
  [[ -z "$commands" ]] && return 0
  local output="" error_lines

  if [[ "$CONNECTION_MODE" == "cluster" ]]; then
    rm -f "$POPULATE_TMP"/node*.cmds
    partition_by_node "$commands"
    local idx node_file
    for idx in "${!MASTER_HOST[@]}"; do
      node_file="$POPULATE_TMP/node${idx}.cmds"
      [[ -s "$node_file" ]] || continue
      local node_connstr
      node_connstr="$(cluster_lib_node_connstr "${CONNECTION_STRING%%,*}" "${MASTER_HOST[$idx]}" "${MASTER_PORT[$idx]}")"
      output+="$(run_on "${MASTER_HOST[$idx]}" "$node_connstr" "$(cat "$node_file")")"$'\n'
    done
    rm -f "$POPULATE_TMP"/node*.cmds
  else
    output="$(run_on "$CONTAINER" "$CONNECTION_STRING" "$commands")"
  fi

  if echo "$output" | grep -qi "unknown command\|ERR unknown"; then
    echo "  $label: SKIPPED (server does not support this command - module not loaded / Redis too old)"
  elif error_lines="$(echo "$output" | grep -i "^(error)\|^ERR ")"; then
    echo "  $label: completed with errors:"
    echo "$error_lines" | sed 's/^/    /' | sort -u
  elif [[ -z "$(echo "$output" | grep -v '^$')" && -n "$commands" ]]; then
    echo "  $label: FAILED (could not reach any target node - see warnings above)"
  else
    echo "  $label: OK"
  fi
}

if [[ "$FLUSH" == "yes" ]]; then
  echo "==> flushing source database ($CONTAINER) before populating"
  if [[ "$CONNECTION_MODE" == "cluster" ]]; then
    for idx in "${!MASTER_HOST[@]}"; do
      node_connstr="$(cluster_lib_node_connstr "${CONNECTION_STRING%%,*}" "${MASTER_HOST[$idx]}" "${MASTER_PORT[$idx]}")"
      redis_cli "${MASTER_HOST[$idx]}" "$node_connstr" FLUSHDB
    done
  else
    redis_cli "$CONTAINER" "$CONNECTION_STRING" FLUSHDB
  fi
fi

echo "==> populating $COUNT keys per type ($TYPES) with prefix '${KEY_PREFIX}:' on $CONTAINER (mode=$CONNECTION_MODE, parallelism=$PARALLELISM)"
for t in $TYPES; do
  case "$t" in
    string)          run_batch "String"          "$(gen_string)" ;;
    hash)            run_batch "Hash"            "$(gen_hash)" ;;
    json)            run_batch "JSON"            "$(gen_json)" ;;
    array|arrays)    run_batch "Array"            "$(gen_array)" ;;
    zset|sortedset)  run_batch "SortedSet"       "$(gen_zset)" ;;
    set)             run_batch "Set"             "$(gen_set)" ;;
    list)            run_batch "List"            "$(gen_list)" ;;
    stream)          run_batch "Stream"          "$(gen_stream)" ;;
    topk)            run_batch "TopK"            "$(gen_topk)" ;;
    countminsketch|countminsum) run_batch "CountMinSketch" "$(gen_countminsketch)" ;;
    tdigest)         run_batch "TDigest"         "$(gen_tdigest)" ;;
    bloom)           run_batch "Bloom"           "$(gen_bloom)" ;;
    cuckoofilter)    run_batch "CuckooFilter"    "$(gen_cuckoofilter)" ;;
    timeseries)      run_batch "TimeSeries"      "$(gen_timeseries)" ;;
    vectorset|vectorsets) run_batch "VectorSet"  "$(gen_vectorset)" ;;
    *) echo "  unknown type '$t', skipping" >&2 ;;
  esac
done

echo "==> done. DBSIZE on $CONTAINER:"
if [[ "$CONNECTION_MODE" == "cluster" ]]; then
  cluster_lib_dbsize_sum "$CONTAINER" "$CONNECTION_STRING"
else
  redis_cli "$CONTAINER" "$CONNECTION_STRING" DBSIZE
fi
