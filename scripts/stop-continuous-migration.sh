#!/usr/bin/env bash
# Stops the Live Phase that continuous-migration.sh left running: reports the keyspace-event
# counters and the live queues as they stand, stops RedisKeyspaceEventConsumer, RedisSingleKeyFetch
# and DeleteRedisKey (and nothing else), then reports the counters again. Once this has run, source
# changes are no longer applied to the target.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

NiFi connection/auth:
  --nifi-container NAME        container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER             NiFi single-user login username (env NIFI_USER)
  --nifi-password PASSWORD     NiFi single-user login password (env NIFI_PASS)
  --nifi-token TOKEN           use an existing bearer token instead of user/password (env NIFI_TOKEN)
  -h, --help                   this help

Stops only the three Live Phase processors. A snapshot migration running in the same process
group is left alone, and the flow's other processors keep running.

Example:
  NIFI_USER=... NIFI_PASS=... $(basename "$0")
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) NIFI_TOKEN="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  echo "error: NiFi credentials required - pass --nifi-token, or both --nifi-user and --nifi-password" >&2; usage; exit 1
fi

nifi_lib_pick_runtime
nifi_lib_init

echo "==> locating the R2Dfly Migration process group"
# `|| true` throughout the discovery below: under set -e/pipefail any NiFi API hiccup would
# otherwise abort the script silently right at the failing assignment, before the deliberate
# `-z`/error checks below ever get a chance to run.
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

# One pass for all three ids. Every name is pre-seeded to '' so the joined field count stays
# constant however many are actually present - otherwise a missing processor would shift every id
# one field to the left on the read below.
PROC_IDS="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
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
IFS='|' read -r PROC_CONSUMER PROC_FETCH PROC_DELETE <<< "$PROC_IDS"
if [[ -z "$PROC_CONSUMER" && -z "$PROC_FETCH" && -z "$PROC_DELETE" ]]; then
  echo "==> this flow has no Live Phase - there is nothing to stop."
  echo "    (a continuous migration wires RedisKeyspaceEventConsumer, RedisSingleKeyFetch and DeleteRedisKey into the group)"
  exit 0
fi

# live_phase_counters - one "<name>|<count>" line per counter, in a fixed order, 0 for any counter
# NiFi has never incremented (those are simply absent from the response rather than reported as
# zero). valueCount is the raw number; the sibling `value` field is a display string NiFi
# comma-formats once it gets large, so it can't be read as an integer. Counts are summed across
# every processor reporting a given counter name.
live_phase_counters() {
  nifi_api_get "counters" | py3 "
import json, sys
names = ['Keyspace Events Dropped (Queue Full)',
         'Keyspace Pub/Sub Disconnects',
         'Keyspace Pub/Sub Downtime (ms)',
         'Cluster Topology Drift Detected (Critical)',
         'Cluster Topology Drift Detected (Advisory)',
         'Orphan Keys Found']
totals = dict((n, 0) for n in names)
try:
    d = json.load(sys.stdin)
except Exception:
    d = {}
for c in d.get('counters', {}).get('aggregateSnapshot', {}).get('counters', []):
    name = c.get('name')
    if name in totals:
        totals[name] += int(c.get('valueCount') or 0)
for n in names:
    print(f'{n}|{totals[n]}')
"
}

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Live Phase state before stopping"
echo "=================================================="
BEFORE_RAW="$(live_phase_counters)" || true
COUNTERS_BEFORE=()
while IFS= read -r line; do
  [[ -n "$line" ]] || continue
  COUNTERS_BEFORE+=("$line")
done <<< "$BEFORE_RAW"

echo "  counters (summed across every processor reporting them):"
if [[ ${#COUNTERS_BEFORE[@]} -gt 0 ]]; then
  for entry in "${COUNTERS_BEFORE[@]}"; do
    printf '    %-45s %s\n' "${entry%%|*}" "${entry##*|}"
  done
else
  echo "    warning: could not read NiFi's counters" >&2
fi

echo "  processor run status:"
nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
targets = ['RedisKeyspaceEventConsumer', 'RedisSingleKeyFetch', 'DeleteRedisKey']
d = json.load(sys.stdin)
for p in d['processors']:
    c = p['component']
    for name in targets:
        if c['type'].endswith(name):
            print(f\"    {name}: {p.get('status', {}).get('runStatus', 'unknown')}\")
" || echo "    warning: could not read processor status" >&2

# Queue depth tells the user whether the live path had actually drained before the stop: a
# non-zero queue here means events were captured but not yet applied to the target.
echo "  live queue depth:"
nifi_api_get "flow/process-groups/$PG_ID/status?recursive=true" | py3 "
import json, sys
sources = {'RedisKeyspaceEventConsumer', 'RedisSingleKeyFetch'}
d = json.load(sys.stdin)
snaps = d['processGroupStatus']['aggregateSnapshot'].get('connectionStatusSnapshots', [])
printed = False
for s in snaps:
    c = s.get('connectionStatusSnapshot', s)
    if c.get('sourceName') in sources:
        print(f\"    {c.get('sourceName')} -> {c.get('destinationName')}: {c.get('queuedCount')} queued ({c.get('queuedSize')})\")
        printed = True
if not printed:
    print('    no Live Phase connections reported')
" || echo "    warning: could not read queue depth" >&2

# stop_one <processor-id> <label> - a no-op if the id is empty (a half-wired flow) or the processor
# is already stopped. Polls for the real state afterwards: a run-status PUT returns as soon as NiFi
# accepts it, not once the processor has finished its current onTrigger.
stop_one() {
  local proc_id="$1" label="$2" timeout=60 waited=0 ver status
  if [[ -z "$proc_id" ]]; then
    echo "  $label: not present in this flow - skipping"
    return 0
  fi
  ver="$(nifi_current_version "processors/$proc_id")" || true
  if [[ -z "$ver" ]]; then
    echo "warning: could not read the current revision of $label - leaving it alone" >&2
    return 1
  fi
  nifi_api_put "processors/$proc_id/run-status" \
    "{\"revision\":{\"version\":$ver},\"state\":\"STOPPED\",\"disconnectedNodeAcknowledged\":false}" > /dev/null || true
  while true; do
    status="$(nifi_api_get "processors/$proc_id" | py3 "
import json, sys
print(json.load(sys.stdin).get('status', {}).get('runStatus', ''))
")" || status=""
    if [[ -n "$status" && "$status" != "Running" ]]; then
      echo "  $label: $status"
      return 0
    fi
    if [[ "$waited" -ge "$timeout" ]]; then
      echo "warning: $label did not stop within ${timeout}s (last status: ${status:-unknown}) - check its bulletins in the NiFi UI or run diagnose-r2dfly.sh" >&2
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Stopping the Live Phase"
echo "=================================================="
# Only these three, never pg-stop: a snapshot migration may still be running in the same process
# group, and stopping the whole group would abort it mid-scan. Source first so the chain drains -
# once the consumer stops emitting, whatever is already queued still gets fetched and written.
STOP_RC=0
stop_one "$PROC_CONSUMER" "RedisKeyspaceEventConsumer" || STOP_RC=1
stop_one "$PROC_FETCH" "RedisSingleKeyFetch" || STOP_RC=1
stop_one "$PROC_DELETE" "DeleteRedisKey" || STOP_RC=1

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Final counters"
echo "=================================================="
AFTER_RAW="$(live_phase_counters)" || true
COUNTERS_AFTER=()
while IFS= read -r line; do
  [[ -n "$line" ]] || continue
  COUNTERS_AFTER+=("$line")
done <<< "$AFTER_RAW"

if [[ ${#COUNTERS_AFTER[@]} -gt 0 && ${#COUNTERS_AFTER[@]} -eq ${#COUNTERS_BEFORE[@]} ]]; then
  printf '  %-45s %10s %10s\n' "counter" "before" "after"
  i=0
  while [[ $i -lt ${#COUNTERS_AFTER[@]} ]]; do
    printf '  %-45s %10s %10s\n' \
      "${COUNTERS_AFTER[$i]%%|*}" "${COUNTERS_BEFORE[$i]##*|}" "${COUNTERS_AFTER[$i]##*|}"
    i=$((i + 1))
  done
elif [[ ${#COUNTERS_AFTER[@]} -gt 0 ]]; then
  for entry in "${COUNTERS_AFTER[@]}"; do
    printf '  %-45s %s\n' "${entry%%|*}" "${entry##*|}"
  done
else
  echo "  warning: could not read NiFi's counters" >&2
fi

echo
echo "The Live Phase is stopped. Source changes are no longer being applied to the target."
echo "A non-zero 'Keyspace Pub/Sub Disconnects' or 'Keyspace Events Dropped (Queue Full)' count above means"
echo "source changes were missed during this run, and nothing repaired them - the target is missing those"
echo "updates until the same keys change again. There is no reconciliation pass yet."
echo
echo "Run simple-troubleshoot.sh or diagnose-r2dfly.sh if the flow looks wrong."

exit $STOP_RC
