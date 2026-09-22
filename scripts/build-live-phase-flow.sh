#!/usr/bin/env bash
# Adds the Live Phase to an already-deployed "R2Dfly Migration" flow: RedisKeyspaceEventConsumer
# -> RedisSingleKeyFetch -> the flow's existing RedisBatchWriter for creates/updates, and
# RedisKeyspaceEventConsumer -> DeleteRedisKey for deletes/expiries. Reuses the flow's own two
# connection-pool controller services and its scan-path writer rather than creating parallel
# copies. Idempotent: it creates only what's missing and converges a half-built flow (e.g. a
# previous run that died between creating the processors and wiring them) on re-run. It wires and
# validates; it never starts anything. The Live Phase comes up with the rest of the process group
# when a migration run starts the flow - it can't start before that anyway, since NiFi refuses to
# start a processor whose connection-pool controller service is still disabled.
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

Options:
  --no-export-baseline         don't re-export the updated flow over artifacts/r2dfly.json
                                (the export is taken from the live flow, so it carries whatever
                                properties the running flow currently has - including a previous
                                migration run's migration-id)
  --reconciliation-signals     route RedisKeyspaceEventConsumer's topology_drift relationship to a
                                NiFi funnel instead of auto-terminating it, so drift signals queue
                                up durably and can be read back over NiFi's REST API
                                (flowfile-queues/<connection-id>/listing-requests). Default: off.
                                Nothing consumes that queue - there is no reconciliation pass in
                                this project yet - so this only keeps the signals for inspection.
                                Re-running without the flag deletes the queue, which NiFi refuses
                                while FlowFiles are still sitting in it.
  -h, --help                   this help

Normally you don't run this directly - continuous-migration.sh calls it with the credentials it
already has. Run it by hand to wire the Live Phase into a flow without starting a migration.

Example:
  NIFI_USER=... NIFI_PASS=... $(basename "$0")
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"
EXPORT_BASELINE="true"
RECONCILIATION_SIGNALS="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) NIFI_TOKEN="$2"; shift 2 ;;
    --no-export-baseline) EXPORT_BASELINE="false"; shift ;;
    --reconciliation-signals) RECONCILIATION_SIGNALS="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  echo "error: NiFi credentials required - pass --nifi-token, or both --nifi-user and --nifi-password" >&2; usage; exit 1
fi

# Mirrored to a file as well as the terminal, same as run-r2dfly.sh does for a migration run:
# what this script creates is a one-time structural change to the flow, and the validation errors
# it prints are the only record of why a wiring attempt failed once the scrollback is gone.
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
BUILD_LOG="$LOG_DIR/build-live-phase-flow-$(date +%s).log"
exec > >(tee -a "$BUILD_LOG") 2>&1
echo "==> logging this run to $BUILD_LOG"

# --- JSON body builders. Pure string functions with no dependency on anything above, so the
#     exact bytes sent to NiFi can be produced and checked without a running NiFi. ---

# proc_create_body <type> <name> <x> <y>
proc_create_body() {
  printf '{"revision":{"version":0},"component":{"type":"%s","bundle":{"group":"io.dragonfly.nifi.redis","artifact":"nifi-redis-migration-nar","version":"1.0.0-SNAPSHOT"},"name":"%s","position":{"x":%s,"y":%s}}}' \
    "$1" "$2" "$3" "$4"
}

# proc_config_body <id> <revision-version> <properties-json-members> <auto-terminated-json-members>
# <properties-json-members> is the inside of the properties object ("k":"v",...) and
# <auto-terminated-json-members> the inside of the array ("failure",...), so callers keep control
# of which values are quoted strings and which are unquoted JSON null.
proc_config_body() {
  printf '{"revision":{"version":%s},"component":{"id":"%s","config":{"properties":{%s},"autoTerminatedRelationships":[%s]}}}' \
    "$2" "$1" "$3" "$4"
}

# conn_create_body <pg-id> <source-proc-id> <destination-id> <relationships-json-members> [destination-type]
# [destination-type] is one of NiFi's ConnectableDTO types (PROCESSOR, FUNNEL, INPUT_PORT, ...) and
# has to name what the destination id actually is, or NiFi rejects the connection. It defaults to
# PROCESSOR so the three Live Phase connections below stay byte-for-byte what they always were;
# only the reconciliation-signals queue, which ends at a funnel, passes anything else.
conn_create_body() {
  printf '{"revision":{"version":0},"component":{"source":{"id":"%s","groupId":"%s","type":"PROCESSOR"},"destination":{"id":"%s","groupId":"%s","type":"%s"},"selectedRelationships":[%s],"flowFileExpiration":"0 sec","backPressureObjectThreshold":10000,"backPressureDataSizeThreshold":"1 GB"}}' \
    "$2" "$1" "$3" "$1" "${5:-PROCESSOR}" "$4"
}

# funnel_create_body <x> <y> - no name and no properties, because a funnel has none: NiFi's
# FunnelDTO adds nothing to the id and position every component carries.
funnel_create_body() {
  printf '{"revision":{"version":0},"component":{"position":{"x":%s,"y":%s}}}' "$1" "$2"
}

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

echo "==> locating the source/target connection-pool controller services"
SVC_IDS="$(nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
src = tgt = src_state = tgt_state = ''
for s in d['controllerServices']:
    c = s['component']
    if not src and c['type'].endswith('StandardRedisConnectionPoolService'):
        src, src_state = s['id'], c.get('state', '')
    elif not tgt and c['type'].endswith('StandardDragonflyConnectionPoolService'):
        tgt, tgt_state = s['id'], c.get('state', '')
print(f'{src}|{tgt}|{src_state}|{tgt_state}')
")" || true
IFS='|' read -r SVC_SOURCE SVC_TARGET SVC_SOURCE_STATE SVC_TARGET_STATE <<< "$SVC_IDS"
if [[ -z "$SVC_SOURCE" ]]; then
  echo "error: could not find the source Redis connection-pool service in $PG_ID" >&2; exit 1
fi
if [[ -z "$SVC_TARGET" ]]; then
  echo "error: could not find the target Dragonfly connection-pool service in $PG_ID" >&2; exit 1
fi
# NiFi marks a processor INVALID while a controller service it requires is disabled, and the two
# pools stay blank and DISABLED from deploy-to-nifi.sh until a migration run sets their connection
# strings. So "the Live Phase processors are invalid" only means something has actually gone wrong
# once both pools are ENABLED - checking their state directly beats reading NiFi's error strings.
POOLS_ENABLED="false"
if [[ "$SVC_SOURCE_STATE" == "ENABLED" && "$SVC_TARGET_STATE" == "ENABLED" ]]; then
  POOLS_ENABLED="true"
fi

echo "==> looking for the writer and the three Live Phase processors"
# One pass for all four ids rather than four toolbox execs over the same JSON. Every name is
# pre-seeded to '' so the joined field count stays constant however many are actually present -
# otherwise a missing processor would shift every id one field to the left on the read below.
PROC_IDS="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
order = ['RedisBatchWriter', 'RedisKeyspaceEventConsumer', 'RedisSingleKeyFetch', 'DeleteRedisKey']
ids = {name: '' for name in order}
for p in d['processors']:
    t = p['component']['type']
    for name in order:
        if not ids[name] and t.endswith(name):
            ids[name] = p['component']['id']
            break
print('|'.join(ids[name] for name in order))
")" || true
IFS='|' read -r PROC_WRITER PROC_CONSUMER PROC_FETCH PROC_DELETE <<< "$PROC_IDS"
if [[ -z "$PROC_WRITER" ]]; then
  echo "error: could not find the RedisBatchWriter processor in $PG_ID - the Live Phase reuses the scan path's writer rather than creating its own. Run ./deploy-to-nifi.sh first." >&2
  exit 1
fi

# Read once and reused for every check below: creating one connection can't change whether a
# different one already exists, so re-fetching per check would only cost extra toolbox execs.
# The topology_drift connection is picked out of this same response rather than by a second GET;
# its id and its destination (the funnel) come back on a leading line, ahead of the src>dst pairs.
# The consumer's id is interpolated into the program because py3 takes a program and nothing else,
# so there is no argv to hand it through - it is a NiFi-generated uuid, and an empty one (nothing
# created yet, first run) simply matches nothing.
CONN_INFO="$(nifi_api_get "process-groups/$PG_ID/connections" | py3 "
import json, sys
d = json.load(sys.stdin)
consumer = '$PROC_CONSUMER'
drift = '|'
for c in d['connections']:
    comp = c['component']
    if consumer and comp['source']['id'] == consumer and 'topology_drift' in (comp.get('selectedRelationships') or []):
        drift = comp['id'] + '|' + comp['destination']['id']
        break
print(drift)
for c in d['connections']:
    print(c['component']['source']['id'] + '>' + c['component']['destination']['id'])
")" || true
IFS='|' read -r DRIFT_CONN_ID DRIFT_FUNNEL_ID <<< "$(head -n 1 <<< "$CONN_INFO")"
CONN_PAIRS="$(tail -n +2 <<< "$CONN_INFO")"

# The flag and the flow's actual state give exactly three actions, so the decision is taken once,
# here, instead of being re-derived at each of the two places that act on it (the delete has to
# happen before the consumer's auto-terminated list is PUT back, the create after it).
DRIFT_ACTION="none"
if [[ "$RECONCILIATION_SIGNALS" == "true" && -z "$DRIFT_CONN_ID" ]]; then
  DRIFT_ACTION="wire"
elif [[ "$RECONCILIATION_SIGNALS" != "true" && -n "$DRIFT_CONN_ID" ]]; then
  DRIFT_ACTION="unwire"
fi

connection_present() {
  [[ -n "$1" && -n "$2" ]] && grep -Fqx -- "$1>$2" <<< "$CONN_PAIRS"
}

# Processors present but unconnected is a real state to land in: a run that died between creating
# them and wiring them leaves exactly that. Treating "all three exist" as "already wired" would
# make that state permanently unrepairable by re-running, so the connections are checked too.
NEEDS_WORK="false"
if [[ -z "$PROC_CONSUMER" || -z "$PROC_FETCH" || -z "$PROC_DELETE" ]]; then
  NEEDS_WORK="true"
elif ! connection_present "$PROC_CONSUMER" "$PROC_FETCH" ||
     ! connection_present "$PROC_CONSUMER" "$PROC_DELETE" ||
     ! connection_present "$PROC_FETCH" "$PROC_WRITER"; then
  NEEDS_WORK="true"
# A pending --reconciliation-signals toggle counts as work for the same reason: the "already
# wired" path below exits without stopping the flow, and NiFi will not add or delete a connection,
# or change a processor's auto-terminated relationships, while the flow is running.
elif [[ "$DRIFT_ACTION" != "none" ]]; then
  NEEDS_WORK="true"
fi

# live_validation_report - per-processor PASS/FAIL for the three Live Phase processors plus every
# validation error verbatim, returning non-zero unless all three are VALID. Matched on the type
# suffix rather than the ids discovered above so it reads the same whether it runs before or after
# this script creates anything.
live_validation_report() {
  local rc=0
  nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
targets = ['RedisKeyspaceEventConsumer', 'RedisSingleKeyFetch', 'DeleteRedisKey']
d = json.load(sys.stdin)
found = {}
for p in d['processors']:
    c = p['component']
    for name in targets:
        if name not in found and c['type'].endswith(name):
            found[name] = c
ok = True
for name in targets:
    c = found.get(name)
    if c is None:
        print(f'  FAIL {name}: not present in the process group')
        ok = False
        continue
    status = c.get('validationStatus')
    print(f\"  {'PASS' if status == 'VALID' else 'FAIL'} {name}: {status}\")
    if status != 'VALID':
        ok = False
    for e in (c.get('validationErrors') or []):
        print(f'    - {e}')
sys.exit(0 if ok else 1)
" || rc=$?
  return $rc
}

# NiFi reports a just-created processor as VALIDATING for a moment before it settles, so a single
# read straight after the last PUT would report a false INVALID. Polls quietly and leaves the
# actual report to the caller.
wait_for_live_validation() {
  local timeout="${1:-60}" waited=0
  while true; do
    if live_validation_report >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$waited" -ge "$timeout" ]]; then
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

print_validation_summary() {
  local rc=0
  echo
  echo "==> Live Phase validation"
  wait_for_live_validation 60 || rc=1
  live_validation_report || true
  if [[ $rc -eq 0 ]]; then
    return 0
  fi
  if [[ "$POOLS_ENABLED" != "true" ]]; then
    echo "  the two connection-pool services are still $SVC_SOURCE_STATE/$SVC_TARGET_STATE, so the processors above"
    echo "  cannot validate yet. They will once a migration run sets the connection strings and enables the pools."
    return 0
  fi
  echo "error: the Live Phase processors above are not all VALID even though both connection pools are enabled." >&2
  return 1
}

# Reported on both exit paths: with the flag folded into NEEDS_WORK above, a run that finds
# nothing to do is exactly a run whose reconciliation-signals state already matches what was
# asked for, and that is worth saying out loud rather than leaving the operator to guess.
report_reconciliation_signals() {
  if [[ "$RECONCILIATION_SIGNALS" == "true" ]]; then
    echo "  reconciliation signals: topology_drift queues at funnel $DRIFT_FUNNEL_ID - nothing drains it;"
    echo "                          read it with GET/POST flowfile-queues/$DRIFT_CONN_ID/listing-requests"
  else
    echo "  reconciliation signals: off - topology_drift is auto-terminated (pass --reconciliation-signals to queue it)"
  fi
}

if [[ "$NEEDS_WORK" != "true" ]]; then
  echo "==> the Live Phase is already wired into this flow; leaving it as it is"
  echo "  RedisKeyspaceEventConsumer: $PROC_CONSUMER"
  echo "  RedisSingleKeyFetch:        $PROC_FETCH"
  echo "  DeleteRedisKey:             $PROC_DELETE"
  report_reconciliation_signals
  print_validation_summary || exit 1
  echo
  echo "Next: ./continuous-migration.sh --source-connection-string ... --target-connection-string ..."
  exit 0
fi

# pg-stop returns as soon as NiFi accepts the request, not once every processor has actually
# finished transitioning, and NiFi refuses to add a connection to a running processor - so poll
# the real state rather than assuming pg-stop completed synchronously (the same trap run-r2dfly.sh
# hit with a property PUT rejected as "... while the Processor is running"). runStatus alone is
# not enough: it flips to Stopped as soon as the scheduler stops issuing new triggers, but a
# processor mid-onTrigger() keeps its activeThreadCount above 0 for a while after, and NiFi
# rejects config/wiring changes while any thread is still active, independent of runStatus.
wait_for_processors_stopped() {
  local timeout="${1:-30}" waited=0 running threads rc
  while true; do
    # rc is checked explicitly rather than `|| true`'d away so a transient API failure reads as
    # "couldn't tell this round, keep polling" instead of being indistinguishable from `running`
    # legitimately coming back empty, which means "confirmed nothing running".
    rc=0
    running="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
print(','.join(p['component']['name'] for p in d['processors'] if p.get('status', {}).get('runStatus') == 'Running'))
")" || rc=$?
    threads="$(nifi_api_get "flow/process-groups/$PG_ID/status" | py3 "
import json, sys
d = json.load(sys.stdin)
print(d['processGroupStatus']['aggregateSnapshot']['activeThreadCount'])
")" || rc=$?
    [[ $rc -eq 0 && -z "$running" && "$threads" == "0" ]] && return 0
    if [[ "$waited" -ge "$timeout" ]]; then
      if [[ $rc -ne 0 ]]; then
        echo "warning: could not confirm processor status after ${timeout}s (NiFi API call failing) - proceeding anyway" >&2
      else
        echo "warning: still running after ${timeout}s: running=[$running] activeThreadCount=$threads - proceeding anyway (NiFi may reject the connections below)" >&2
      fi
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

echo "==> stopping the flow so NiFi will accept the new processors and connections"
nifi_cli pg-stop -pgid "$PG_ID" || true
wait_for_processors_stopped

# nifi_api_put_checked <path> <body> - like nifi_api_put, but treats a non-JSON response (NiFi
# returns a plain-text body, not JSON, for some rejections - e.g. "Cannot modify configuration of
# ... because it is currently not disabled") as a hard failure instead of silently discarding it.
nifi_api_put_checked() {
  local path="$1" body="$2" resp
  # `|| true`: a bare connectivity failure here (as opposed to the non-JSON rejection body this
  # function exists to catch) would otherwise abort the script before reaching the check below -
  # an empty $resp fails that same json.load anyway, so it cascades into the right error.
  resp="$(nifi_api_put "$path" "$body")" || true
  if ! echo "$resp" | py3 "import json,sys; json.load(sys.stdin)" >/dev/null 2>&1; then
    echo "error: PUT $path was rejected: $resp" >&2
    exit 1
  fi
  echo "$resp"
}

# create_processor <type> <name> <x> <y> - prints the new processor's id. Callers append
# `|| exit 1`: this runs inside a command substitution, so its own failure has to surface as the
# assignment's exit status rather than as an exit that would only kill the subshell.
create_processor() {
  local proc_type="$1" name="$2" x="$3" y="$4" resp id
  resp="$(nifi_api_post "process-groups/$PG_ID/processors" "$(proc_create_body "$proc_type" "$name" "$x" "$y")")" || true
  id="$(py3 "import json,sys; print(json.load(sys.stdin).get('component',{}).get('id',''))" <<< "$resp" 2>/dev/null)" || true
  if [[ -z "$id" ]]; then
    echo "error: could not create the $name processor ($proc_type). NiFi's response: $resp" >&2
    return 1
  fi
  echo "$id"
}

# configure_processor <id> <label> <properties-json-members> <auto-terminated-json-members>
configure_processor() {
  local id="$1" label="$2" ver
  ver="$(nifi_current_version "processors/$id")" || true
  if [[ -z "$ver" ]]; then
    echo "error: could not read the current revision of $label (processors/$id)" >&2
    exit 1
  fi
  nifi_api_put_checked "processors/$id" "$(proc_config_body "$id" "$ver" "$3" "$4")" > /dev/null
}

# ensure_connection <source-id> <destination-id> <label> <relationships-json-members> - creates
# the connection only if one between the same two processors isn't already there, so a re-run
# after a run that died between creating the processors and wiring them converges instead of
# stacking a duplicate connection alongside the original.
ensure_connection() {
  local src="$1" dst="$2" label="$3" rels="$4" resp
  if connection_present "$src" "$dst"; then
    echo "  already connected: $label"
    return 0
  fi
  resp="$(nifi_api_post "process-groups/$PG_ID/connections" "$(conn_create_body "$PG_ID" "$src" "$dst" "$rels")")" || true
  if ! py3 "import json,sys; sys.exit(0 if json.load(sys.stdin).get('id') else 1)" <<< "$resp" >/dev/null 2>&1; then
    echo "error: could not create the connection $label. NiFi's response: $resp" >&2
    exit 1
  fi
  echo "  connected: $label"
}

# drift_wire_failed <what-failed> <nifi-response> - both halves of wiring the signals queue can
# fail into the same partial state, and it must never be silent: by the time either runs, the
# consumer's auto-terminated list has already been PUT without topology_drift.
drift_wire_failed() {
  echo "error: $1. NiFi's response: $2" >&2
  echo "       topology_drift is now neither auto-terminated nor connected, so RedisKeyspaceEventConsumer" >&2
  echo "       will read INVALID until this script is re-run to converge (with or without the flag)." >&2
  exit 1
}

CREATED_CONSUMER="false"
CREATED_FETCH="false"
CREATED_DELETE="false"

# y=1000 keeps all three clear of the six processors the baseline flow already places at
# y in {0, 300, 600}, so the Live Phase reads as its own row on the canvas.
if [[ -z "$PROC_CONSUMER" ]]; then
  echo "==> creating RedisKeyspaceEventConsumer"
  PROC_CONSUMER="$(create_processor "io.dragonfly.nifi.redis.processors.RedisKeyspaceEventConsumer" "RedisKeyspaceEventConsumer" 0 1000)" || exit 1
  CREATED_CONSUMER="true"
fi
if [[ -z "$PROC_FETCH" ]]; then
  echo "==> creating RedisSingleKeyFetch"
  PROC_FETCH="$(create_processor "io.dragonfly.nifi.redis.processors.RedisSingleKeyFetch" "RedisSingleKeyFetch" 400 1000)" || exit 1
  CREATED_FETCH="true"
fi
if [[ -z "$PROC_DELETE" ]]; then
  echo "==> creating DeleteRedisKey"
  PROC_DELETE="$(create_processor "io.dragonfly.nifi.redis.processors.DeleteRedisKey" "DeleteRedisKey" 800 1000)" || exit 1
  CREATED_DELETE="true"
fi

# Ahead of the configure_processor block below, not after it: that block PUTs topology_drift back
# into the consumer's auto-terminated list, and NiFi rejects a relationship that is both
# auto-terminated and connected.
if [[ "$DRIFT_ACTION" == "unwire" ]]; then
  echo "==> removing the reconciliation-signals queue"
  DRIFT_CONN_VERSION="$(nifi_current_version "connections/$DRIFT_CONN_ID")" || true
  DRIFT_DELETE_RESP="$(nifi_api_delete "connections/$DRIFT_CONN_ID?version=$DRIFT_CONN_VERSION&clientId=build-live-phase-$$")" || true
  # Don't trust the delete response body - confirm against the connection list, the same lesson
  # reset-r2dfly-flow.sh learned for its process-group delete. Asked for the connection directly,
  # NiFi answers a deleted one with a plain-text 404 body that no JSON check can read.
  DRIFT_STILL_THERE="$(nifi_api_get "process-groups/$PG_ID/connections" | py3 "
import json, sys
d = json.load(sys.stdin)
print('yes' if any(c['component']['id'] == '$DRIFT_CONN_ID' for c in d['connections']) else 'no')
")" || true
  if [[ "$DRIFT_STILL_THERE" != "no" ]]; then
    echo "error: could not confirm the topology_drift connection ($DRIFT_CONN_ID) was deleted. NiFi's response: $DRIFT_DELETE_RESP" >&2
    echo "       NiFi refuses to delete a connection that still has FlowFiles queued, which is the expected state" >&2
    echo "       for this one - nothing drains it. Your next step is either to drop its contents yourself" >&2
    echo "       (POST flowfile-queues/$DRIFT_CONN_ID/drop-requests) and re-run, or to leave --reconciliation-signals on." >&2
    echo "       This script will not drop the queue for you: those signals were made durable on purpose." >&2
    exit 1
  fi
  # The funnel is only reachable through the connection that just went away, so leaving it behind
  # would strand it on the canvas and make every later off->on cycle add another one. Worth a
  # warning, not a failure: the queue itself, the thing the flag is about, is already gone.
  if [[ -n "$DRIFT_FUNNEL_ID" ]]; then
    DRIFT_FUNNEL_VERSION="$(nifi_current_version "funnels/$DRIFT_FUNNEL_ID")" || true
    DRIFT_FUNNEL_RESP="$(nifi_api_delete "funnels/$DRIFT_FUNNEL_ID?version=$DRIFT_FUNNEL_VERSION&clientId=build-live-phase-$$")" || true
    if ! py3 "import json,sys; sys.exit(0 if json.load(sys.stdin).get('id') else 1)" <<< "$DRIFT_FUNNEL_RESP" >/dev/null 2>&1; then
      echo "warning: the topology_drift queue is gone but its funnel ($DRIFT_FUNNEL_ID) is still on the canvas. NiFi's response: $DRIFT_FUNNEL_RESP" >&2
      echo "         Delete it by hand, or a later --reconciliation-signals run will add a second one beside it." >&2
    fi
  fi
fi

# The auto-terminated lists below are fixed, never derived from "every relationship minus the
# wired ones": NiFi rejects a relationship that is both auto-terminated and connected, and a list
# computed from a half-built flow would get exactly that wrong on a re-run. key_changed,
# key_deleted and key_expired are wired below, so none of them ever appears here - and neither
# does topology_drift under --reconciliation-signals, which connects it to a funnel instead.
CONSUMER_AUTO_TERMINATED='"failure","topology_drift"'
if [[ "$RECONCILIATION_SIGNALS" == "true" ]]; then
  CONSUMER_AUTO_TERMINATED='"failure"'
fi

echo "==> configuring the Live Phase processors"
# topology-state-cache is deliberately left unset: nothing consumes topology_drift, whether it is
# auto-terminated or queued at a funnel no downstream processor reads, so pointing the consumer at
# the shared cursor cache would only add a dependency for a signal nothing acts on.
configure_processor "$PROC_CONSUMER" "RedisKeyspaceEventConsumer" \
  "\"redis-connection-pool\":\"$SVC_SOURCE\"" \
  "$CONSUMER_AUTO_TERMINATED"
# The same five properties RedisTypeDeserializer carries in the deployed flow - RedisSingleKeyFetch
# is the single-key twin of that processor and shares its serialisation, so the two have to agree
# or a live-path key would land on the target shaped differently from a scanned one.
configure_processor "$PROC_FETCH" "RedisSingleKeyFetch" \
  "\"redis-connection-pool\":\"$SVC_SOURCE\",\"hash-field-batch-size\":\"10000\",\"list-chunk-size\":\"5000\",\"stream-read-count\":\"1000\",\"include-consumer-groups\":\"true\"" \
  '"key_missing","module_type","failure"'
configure_processor "$PROC_DELETE" "DeleteRedisKey" \
  "\"dragonfly-connection-pool\":\"$SVC_TARGET\"" \
  '"success","failure"'

echo "==> wiring the Live Phase"
ensure_connection "$PROC_CONSUMER" "$PROC_FETCH" "RedisKeyspaceEventConsumer key_changed -> RedisSingleKeyFetch" \
  '"key_changed"'
# One connection carrying both relationships, not two: DeleteRedisKey treats a delete and an
# expiry identically (DEL on the target), so splitting them would only add a second queue to watch.
ensure_connection "$PROC_CONSUMER" "$PROC_DELETE" "RedisKeyspaceEventConsumer key_deleted/key_expired -> DeleteRedisKey" \
  '"key_deleted","key_expired"'
ensure_connection "$PROC_FETCH" "$PROC_WRITER" "RedisSingleKeyFetch success -> RedisBatchWriter" \
  '"success"'

# After the configure_processor block above, which has already PUT the consumer's auto-terminated
# list without topology_drift - NiFi rejects a relationship that is both auto-terminated and
# connected, so the create has to follow that PUT exactly as the delete above has to precede it.
if [[ "$DRIFT_ACTION" == "wire" ]]; then
  # y=1300 keeps the funnel clear of the three Live Phase processors sitting at y=1000.
  DRIFT_FUNNEL_RESP="$(nifi_api_post "process-groups/$PG_ID/funnels" "$(funnel_create_body 400 1300)")" || true
  DRIFT_FUNNEL_ID="$(py3 "import json,sys; print(json.load(sys.stdin).get('component',{}).get('id',''))" <<< "$DRIFT_FUNNEL_RESP" 2>/dev/null)" || true
  if [[ -z "$DRIFT_FUNNEL_ID" ]]; then
    drift_wire_failed "could not create the funnel for the reconciliation-signals queue" "$DRIFT_FUNNEL_RESP"
  fi
  DRIFT_CONN_RESP="$(nifi_api_post "process-groups/$PG_ID/connections" \
    "$(conn_create_body "$PG_ID" "$PROC_CONSUMER" "$DRIFT_FUNNEL_ID" '"topology_drift"' "FUNNEL")")" || true
  DRIFT_CONN_ID="$(py3 "import json,sys; print(json.load(sys.stdin).get('id',''))" <<< "$DRIFT_CONN_RESP" 2>/dev/null)" || true
  if [[ -z "$DRIFT_CONN_ID" ]]; then
    drift_wire_failed "could not connect RedisKeyspaceEventConsumer topology_drift to funnel $DRIFT_FUNNEL_ID" "$DRIFT_CONN_RESP"
  fi
  echo "  connected: RedisKeyspaceEventConsumer topology_drift -> reconciliation-signals funnel"
fi

# Gates the export below: a flow whose processors are genuinely broken must not become the
# baseline every future deploy-to-nifi.sh import starts from.
print_validation_summary || exit 1

if [[ "$EXPORT_BASELINE" == "true" ]]; then
  echo
  echo "==> re-exporting the flow definition over artifacts/r2dfly.json"
  BASELINE="$PROJECT_ROOT/artifacts/r2dfly.json"
  EXPORTED="$(mktemp)"
  nifi_cli pg-export -pgid "$PG_ID" -o /tmp/r2dfly-updated.json > /dev/null || true
  "$RUNTIME" cp "$CONTAINER_NAME:/tmp/r2dfly-updated.json" "$EXPORTED" || true
  EXPORT_RC=0
  py3 "
import json, sys
expected = ['RedisScanReader', 'RedisTypeDeserializer', 'ModuleTypeHandler', 'RedisBatchWriter',
            'SearchIndexExporter', 'SearchIndexRehydrator', 'RedisKeyspaceEventConsumer',
            'RedisSingleKeyFetch', 'DeleteRedisKey']
try:
    d = json.load(sys.stdin)
except Exception as e:
    print(f'  the export is not valid JSON: {e}')
    sys.exit(1)
contents = d.get('flowContents', {})
name = contents.get('name')
if name != 'R2Dfly Migration':
    print(f'  flowContents.name is {name!r}, not \"R2Dfly Migration\"')
    sys.exit(1)
types = [p.get('type', '') for p in contents.get('processors', [])]
missing = [n for n in expected if not any(t.endswith(n) for t in types)]
if missing:
    print('  the export is missing: ' + ', '.join(missing))
    sys.exit(1)
" < "$EXPORTED" || EXPORT_RC=$?
  if [[ $EXPORT_RC -ne 0 ]]; then
    echo "error: refusing to overwrite $BASELINE - the exported flow failed the checks above." >&2
    echo "       The export is at $EXPORTED if you want to look at it." >&2
    exit 1
  fi
  # An export is taken from the LIVE flow, so every property a migration run has set is baked into
  # it - migration-id in particular, which RedisScanReader checkpoints against and
  # SearchIndexExporter/Rehydrator namespace their cached definitions with. A baseline carrying a
  # real one makes the next deploy-to-nifi.sh import start out already "checkpointed" for a run
  # that has nothing to do with it.
  BAKED_IDS="$(py3 "
import json, sys
d = json.load(sys.stdin)
for p in d.get('flowContents', {}).get('processors', []):
    value = (p.get('properties') or {}).get('migration-id')
    if value:
        print(f\"    {p.get('name')}: {value}\")
" < "$EXPORTED")" || true
  cp "$BASELINE" "$BASELINE.bak"
  cp "$EXPORTED" "$BASELINE"
  rm -f "$EXPORTED"
  echo "  previous baseline saved to $BASELINE.bak"
  echo "  wrote $BASELINE"
  if [[ -n "$BAKED_IDS" ]]; then
    echo "warning: the new baseline has a previous run's migration-id baked into it:" >&2
    echo "$BAKED_IDS" >&2
    echo "warning: every flow imported from it will start out checkpointed against that run. Restore the" >&2
    echo "         previous baseline with: cp $BASELINE.bak $BASELINE" >&2
  fi
fi

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Live Phase wiring summary"
echo "=================================================="
report_processor() {
  if [[ "$1" == "true" ]]; then
    echo "  $2 created ($3)"
  else
    echo "  $2 already present ($3)"
  fi
}
report_processor "$CREATED_CONSUMER" "RedisKeyspaceEventConsumer:" "$PROC_CONSUMER"
report_processor "$CREATED_FETCH" "RedisSingleKeyFetch:       " "$PROC_FETCH"
report_processor "$CREATED_DELETE" "DeleteRedisKey:            " "$PROC_DELETE"
echo "  RedisBatchWriter reused as the live path's writer ($PROC_WRITER)"
report_reconciliation_signals
echo
echo "Nothing was started - the Live Phase comes up with the rest of the flow. Next:"
echo "  ./continuous-migration.sh --source-connection-string redis://... --target-connection-string rediss://..."
