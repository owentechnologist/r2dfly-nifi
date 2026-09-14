#!/usr/bin/env bash
# Diagnoses a stalled/misbehaving "R2Dfly Migration" flow that's already been configured
# and started by run-r2dfly.sh: bulletin board errors, per-processor/queue throughput (to
# tell a stuck reader from a stuck writer), controller service validity, direct source/target
# reachability (plus each side's product/version and module support, and any capability gap
# between them), and podman host health. Read-only - doesn't stop/start/reconfigure anything.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"
source "$PROJECT_ROOT/scripts/redis-lib.sh"
source "$PROJECT_ROOT/scripts/cluster-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

NiFi connection/auth:
  --nifi-container NAME        container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER             NiFi single-user login username (env NIFI_USER)
  --nifi-password PASSWORD     NiFi single-user login password (env NIFI_PASS)
  --nifi-token TOKEN           use an existing bearer token instead of user/password (env NIFI_TOKEN)

Reachability checks (optional - skipped if not given):
  --source-connection-string S  same format run-r2dfly.sh takes; PINGs the source directly
  --source-container NAME       env SOURCE_CONTAINER, default: guessed from the connection string
  --source-connection-mode M    standalone, sentinel, or cluster (env SOURCE_CONNECTION_MODE,
                                 default: standalone) - cluster sums DBSIZE across all master
                                 nodes instead of querying just the first seed node, same as
                                 simple-migration.sh/run-r2dfly.sh (see cluster-lib.sh). Does NOT
                                 re-run the topology health check - only the initial migration
                                 start does that; this only affects how the key count is read.
  --target-connection-string S  same, for the target
  --target-container NAME       env TARGET_CONTAINER, default: guessed from the connection string
  --target-connection-mode M    same as --source-connection-mode, for the target

Example:
  NIFI_USER=... NIFI_PASS=... $(basename "$0") \\
    --source-connection-string redis://source-redis:6379 \\
    --target-connection-string rediss://user:pass@r9827zzxw.dragonflydb.cloud:6385
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"
SOURCE_CONNECTION_STRING="${SOURCE_CONNECTION_STRING:-}"
TARGET_CONNECTION_STRING="${TARGET_CONNECTION_STRING:-}"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-}"
TARGET_CONTAINER="${TARGET_CONTAINER:-}"
SOURCE_CONNECTION_MODE="${SOURCE_CONNECTION_MODE:-standalone}"
TARGET_CONNECTION_MODE="${TARGET_CONNECTION_MODE:-standalone}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) NIFI_TOKEN="$2"; shift 2 ;;
    --source-connection-string) SOURCE_CONNECTION_STRING="$2"; shift 2 ;;
    --source-container) SOURCE_CONTAINER="$2"; shift 2 ;;
    --source-connection-mode) SOURCE_CONNECTION_MODE="$2"; shift 2 ;;
    --target-connection-string) TARGET_CONNECTION_STRING="$2"; shift 2 ;;
    --target-container) TARGET_CONTAINER="$2"; shift 2 ;;
    --target-connection-mode) TARGET_CONNECTION_MODE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  echo "error: NiFi credentials required - pass --nifi-token, or both --nifi-user and --nifi-password" >&2; usage; exit 1
fi
# `tr`, not `${var,,}` (bash 4+ lowercasing), for the same reason run-r2dfly.sh spells this out at
# its own lowercasing site: these scripts have to keep working under macOS's stock bash 3.2, which
# aborts outright with "bad substitution" on `${var,,}` - and being this early in the script, that
# took every check below down with it. simple-migration.sh already lowercases the same two
# variables this exact way.
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
[[ -n "$SOURCE_CONTAINER" || -z "$SOURCE_CONNECTION_STRING" ]] || SOURCE_CONTAINER="$(redis_lib_extract_host "$SOURCE_CONNECTION_STRING")"
[[ -n "$TARGET_CONTAINER" || -z "$TARGET_CONNECTION_STRING" ]] || TARGET_CONTAINER="$(redis_lib_extract_host "$TARGET_CONNECTION_STRING")"

nifi_lib_pick_runtime
nifi_lib_init

echo "==> locating the R2Dfly Migration process group"
# `|| true`: under set -e/pipefail, a transient NiFi API hiccup would otherwise abort the
# script silently right at the failing assignment, before the deliberate checks below can run.
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

echo
echo "=================================================="
echo "1/5: Bulletin board (recent processor/service errors)"
echo "=================================================="
# `|| true`: this is a best-effort, read-only report section - a transient API/parse failure
# should just skip this section (or print nothing), not abort the rest of the diagnostic run.
nifi_api_get "flow/bulletin-board?limit=100" | py3 "
import json, sys
d = json.load(sys.stdin)
bulletins = d.get('bulletinBoard', {}).get('bulletins', [])
if not bulletins:
    print('  (no bulletins - if the flow is actually failing, this often means the failure is a')
    print('   silent stall rather than a raised exception; rely on section 2 instead)')
for b in bulletins:
    bl = b['bulletin']
    print(f\"  [{bl.get('timestamp')}] {bl.get('level')} {bl.get('groupName')}/{bl.get('sourceName')}: {bl.get('message')}\")
" || true

echo
echo "=================================================="
echo "2/5: Processor throughput (tells a stuck reader from a stuck writer)"
echo "=================================================="
# `|| true`: see section 1/5 above - best-effort report section.
nifi_api_get "flow/process-groups/$PG_ID/status?recursive=true" | py3 "
import json, sys
d = json.load(sys.stdin)
snap = d['processGroupStatus']['aggregateSnapshot']
print('  processors:')
for p in snap.get('processorStatusSnapshot', []):
    s = p['processorStatusSnapshot']
    print(f\"    {s['name']:24s} runStatus={s.get('runStatus'):10s} in={s['flowFilesIn']:>6}/{s['bytesIn']:>10}B  out={s['flowFilesOut']:>6}/{s['bytesOut']:>10}B  tasks={s['tasksCompleted']} activeThreads={s.get('activeThreadCount')}\")
print('  queues (a queue stuck at a fixed non-zero count means the downstream processor is stalled):')
connections = snap.get('connectionStatusSnapshot', [])
anything_queued = False
for c in connections:
    s = c['connectionStatusSnapshot']
    print(f\"    {s['sourceName']:24s} -> {s['destinationName']:24s} queued={s['queued']}\")
    if int(str(s['queued']).replace(',', '') or 0) > 0:
        anything_queued = True
if not anything_queued:
    print('  no queue currently has anything backed up - nothing here indicates a stall right now')
" || true

echo
echo "=================================================="
echo "3/5: Controller service enabled/validation status"
echo "=================================================="
# `|| true`: see section 1/5 above - best-effort report section.
nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
for s in d['controllerServices']:
    c = s['component']
    print(f\"  {c['name']:32s} state={c.get('state'):10s} validationStatus={c.get('validationStatus')}\")
    for vs in c.get('validationErrors', []) or []:
        print(f'      validation error: {vs}')
" || true

# diag_dbsize <label> <mode> <container> <connstr> - like run-r2dfly.sh's redis_dbsize: a plain
# single-node DBSIZE would only reflect that one shard's keys for a cluster - not the full
# count - so sum across all master nodes instead once mode is "cluster" (cluster-lib.sh).
# Deliberately does NOT merge stderr into $size (no 2>&1) - redis_cli only ever streams real
# diagnostic text to stderr now (a double-failure warning, or redis-cli's own "Using a
# password..." notice whenever connstr carries credentials, which is the common case for a
# real cloud endpoint), never as part of its successful return value - merging it back in here
# would just re-mix that text into the one line meant to be a clean key count.
diag_dbsize() {
  local label="$1" mode="$2" container="$3" connstr="$4" size
  # `|| true` on both branches: under this script's set -e/pipefail, a broken connection makes
  # redis_cli/cluster_lib_dbsize_sum's own non-zero exit fail the assignment itself, which would
  # silently kill the whole diagnostic run right here instead of just reporting an empty dbsize
  # and letting section 4/5 (and the rest of the script) finish.
  if [[ "$mode" == "cluster" ]]; then
    size="$(cluster_lib_dbsize_sum "$container" "$connstr")" || true
    echo "    dbsize (summed across all master nodes): $size"
  else
    size="$(redis_cli "$container" "$connstr" DBSIZE)" || true
    echo "    dbsize: $size"
  fi
}

echo
echo "=================================================="
echo "4/5: Direct source/target reachability (bypasses NiFi entirely)"
echo "=================================================="
SOURCE_KIND=""
TARGET_KIND=""
SOURCE_CAPS=""
TARGET_CAPS=""
SOURCE_INDEX_COUNT=0
if [[ -n "$SOURCE_CONNECTION_STRING" ]]; then
  echo "  source ($SOURCE_CONTAINER, mode=$SOURCE_CONNECTION_MODE):"
  # `|| true`: a PING failure's non-zero exit would otherwise abort this whole script right here
  # (set -e/pipefail) instead of letting the failure just print and the rest of the report run.
  redis_cli "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING" PING | sed 's/^/    /' || true
  diag_dbsize "source" "$SOURCE_CONNECTION_MODE" "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING"
  SOURCE_KIND="$(redis_lib_detect_product_kind "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
  SOURCE_CAPS="$(redis_lib_detect_capabilities "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING")"
  SOURCE_INDEX_COUNT="$(redis_lib_count_search_indexes "$SOURCE_CONTAINER" "$SOURCE_CONNECTION_STRING" "$SOURCE_CAPS")"
  echo "    product: $(redis_lib_product_label "$SOURCE_KIND")"
  echo "    module support: ${SOURCE_CAPS:-none detected}  (search indexes: $SOURCE_INDEX_COUNT)"
else
  echo "  (skipped - pass --source-connection-string to check)"
fi
if [[ -n "$TARGET_CONNECTION_STRING" ]]; then
  echo "  target ($TARGET_CONTAINER, mode=$TARGET_CONNECTION_MODE):"
  redis_cli "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING" PING | sed 's/^/    /' || true
  diag_dbsize "target" "$TARGET_CONNECTION_MODE" "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING"
  TARGET_KIND="$(redis_lib_detect_product_kind "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
  TARGET_CAPS="$(redis_lib_detect_capabilities "$TARGET_CONTAINER" "$TARGET_CONNECTION_STRING")"
  echo "    product: $(redis_lib_product_label "$TARGET_KIND")"
  echo "    module support: ${TARGET_CAPS:-none detected}"
else
  echo "  (skipped - pass --target-connection-string to check)"
fi
# The capability-gap warnings need both sides, so they only run when both connection strings were
# given - the same gate everything else in this section uses. Unlike simple-migration.sh, this
# script never decides --dfly-to-dfly itself (it diagnoses a flow that's already configured), so
# the dfly-to-dfly condition two of those warnings depend on is derived here the same way
# simple-migration.sh derives it: both sides Dragonfly, on the same version.
if [[ -n "$SOURCE_CONNECTION_STRING" && -n "$TARGET_CONNECTION_STRING" ]]; then
  DFLY_TO_DFLY="false"
  if [[ "$SOURCE_KIND" == dragonfly:* && "$SOURCE_KIND" == "$TARGET_KIND" ]]; then
    DFLY_TO_DFLY="true"
  fi
  redis_lib_warn_capability_gaps "$SOURCE_CAPS" "$TARGET_CAPS" "$DFLY_TO_DFLY" "$SOURCE_INDEX_COUNT"
fi

echo
echo "=================================================="
echo "5/5: Podman host health (rules out the runtime itself as the culprit)"
echo "=================================================="
if command -v "$RUNTIME" >/dev/null 2>&1; then
  echo "  $RUNTIME system df:"
  $RUNTIME system df 2>&1 | sed 's/^/    /' || true
fi
echo "  disk space:"
df -h / 2>&1 | sed 's/^/    /' || true
echo "  memory:"
free -h 2>&1 | sed 's/^/    /' || echo "    (free not available)"
