#!/usr/bin/env bash
# Says in plain words whether it is safe to switch the application from the source to Dragonfly,
# and drives the last steps of a cutover: wait for the Live Phase to catch up, run a final FULL
# reconcile until a pass comes back clean, and print the switch checklist.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--status | --writes-stopped] [options]

  (no mode)               wait until the Live Phase is less than --max-lag-seconds behind the
                          source, then say to stop application writes and run again with
                          --writes-stopped
  --status                check once and exit: 0 when it is safe to cut over, 1 when not
  --writes-stopped        the application no longer writes to the source: wait for the flow to
                          drain, run FULL reconcile passes until one finds no difference, and
                          print the switch checklist

Options:
  --max-lag-seconds N     replication lag allowed before writes stop (default: 5)
  --timeout-seconds N     how long each wait may take (default: 1800)
  --reconcile-rounds N    FULL passes to try for a clean one (default: 3)

It is safe to cut over when all of these hold:
  - the Live Phase has read everything the source wrote (--live-source replication reports this
    as lag; keyspace notifications carry no position, so there only the queues below count)
  - no FlowFile is queued anywhere in the flow
  - no dead-letter record is waiting for review (moving a reviewed record out of the
    dead-letter directory acknowledges it)
  - a FULL reconcile pass that started after writes stopped found no difference
Keys of a type the migration was told to skip (--key-types) are listed but do not block.

The numbers come from NiFi counters, which NiFi also exports to Prometheus as
nifi_processor_counters. In a NiFi cluster they are summed over the nodes.

NiFi connection/auth:
  --nifi-container NAME   container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER        NiFi single-user login username (env NIFI_USER)
  --nifi-password PASS    NiFi single-user login password (env NIFI_PASS)
  --nifi-token TOKEN      use an existing bearer token instead of user/password (env NIFI_TOKEN)
  -h, --help              this help
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"
MODE="wait"
MAX_LAG_SECONDS=5
TIMEOUT_SECONDS=1800
RECONCILE_ROUNDS=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --status) MODE="status"; shift ;;
    --writes-stopped) MODE="final"; shift ;;
    --max-lag-seconds) MAX_LAG_SECONDS="$2"; shift 2 ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --reconcile-rounds) RECONCILE_ROUNDS="$2"; shift 2 ;;
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) NIFI_TOKEN="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done
for n in "$MAX_LAG_SECONDS" "$TIMEOUT_SECONDS" "$RECONCILE_ROUNDS"; do
  [[ "$n" =~ ^[0-9]+$ ]] || { echo "error: '$n' is not a whole number" >&2; exit 1; }
done
if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  echo "error: NiFi credentials required - pass --nifi-token, or both --nifi-user and --nifi-password" >&2; usage; exit 1
fi

nifi_lib_pick_runtime
nifi_lib_init

ROOT_ID="$(nifi_cli get-root-id -ot simple | tr -d '[:space:]')" || true
PG_ID="$(nifi_api_get "flow/process-groups/$ROOT_ID" | py3 "
import json, sys
for g in json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']:
    if g['component']['name'] == 'R2Dfly Migration':
        print(g['component']['id'])
        break
")" || true
if [[ -z "$PG_ID" ]]; then
  echo "error: no 'R2Dfly Migration' process group found in NiFi. Is the migration running here?" >&2
  exit 1
fi
PROC_RECONCILER="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
print(next((p['component']['id'] for p in json.load(sys.stdin)['processors'] if p['component']['type'].endswith('MigrationReconciler')), ''))
")" || true
if [[ -z "$PROC_RECONCILER" ]]; then
  echo "error: this flow has no MigrationReconciler, so nothing can confirm the target matches. Run continuous-migration.sh first." >&2
  exit 1
fi

# check <clean-full-pass-since-epoch> <max-lag-seconds> - prints one line per condition and a
# last line with the verdict: LAGGING (the flow has not caught up), NOT_SAFE (caught up, no clean
# FULL pass), UNREVIEWED (clean, but dead-letter records wait for review) or SAFE. The three NiFi
# documents go to a single py3 call, which is a container exec, separated by a marker line.
check() {
  {
    nifi_api_get "counters"; echo; echo "@@"
    nifi_api_get "flow/process-groups/$PG_ID/status?recursive=true"; echo; echo "@@"
    nifi_api_get "process-groups/$PG_ID/processors"
  } | py3 "
import json, sys, time
counters_doc, status_doc, procs_doc = [json.loads(part) for part in sys.stdin.read().split('\n@@\n')]
since, max_lag = $1, $2
counters = {}
for c in counters_doc['counters']['aggregateSnapshot']['counters']:
    if not c['context'].startswith('All '):
        counters[c['name']] = counters.get(c['name'], 0) + int(c['valueCount'])
def count(name):
    return counters.get(name, 0)
running = {}
dead_letter_dir = '?'
for p in procs_doc['processors']:
    kind = p['component']['type'].rsplit('.', 1)[-1]
    running[kind] = running.get(kind, False) or p['status']['runStatus'] == 'Running'
    if kind == 'RedisDeadLetterWriter':
        dead_letter_dir = p['component']['config']['properties'].get('output-directory', '?')
lines, caught_up, clean, reviewed = [], True, True, True
def report(ok, text):
    lines.append(('  ok    ' if ok else '  WAIT  ') + text)
    return ok
if running.get('RedisReplicationReader'):
    lag, pending = count('Replication Lag (seconds)'), count('Replication Pending Keys')
    # The flag is absent until the reader has once been caught up; a missing lag is not a zero one.
    flag = counters.get('Replication Caught Up')
    ok = flag == 1 if max_lag == 0 else flag is not None and (flag == 1 or lag <= max_lag)
    caught_up &= report(ok, ('replication caught up' if flag == 1 else 'replication has not caught up yet' if flag is None else 'replication behind')
                        + f': lag {lag} s (allowed {max_lag} s), {count(\"Replication Lag (bytes)\")} bytes, {pending} changed keys not yet emitted')
elif running.get('RedisKeyspaceEventConsumer'):
    dropped = count('Keyspace Events Dropped (Queue Full)') + count('Keyspace Pub/Sub Disconnects')
    report(True, 'keyspace notifications carry no position, so the queues below are the lag' + (f'; {dropped} drops or disconnects were counted, which only the FULL reconcile repairs' if dropped else ''))
else:
    caught_up &= report(False, 'no Live Phase source is running (RedisReplicationReader or RedisKeyspaceEventConsumer)')
queued = status_doc['processGroupStatus']['aggregateSnapshot']['flowFilesQueued']
caught_up &= report(queued == 0, f'{queued} FlowFiles queued in the flow')
skipped = {}
for name, value in counters.items():
    if name.endswith(')') and 'Skipped by Type Filter (' in name:
        kind = name.rsplit('(', 1)[1][:-1]
        skipped[kind] = skipped.get(kind, 0) + value
if skipped:
    report(True, 'not copied, as the migration\'s type filter asks (--key-types): ' + ', '.join(f'{n} {t}' for t, n in sorted(skipped.items())))
pending_dl = count('Dead-Letter Records Pending')
reviewed &= report(pending_dl == 0, f'{pending_dl} dead-letter records in {dead_letter_dir} not yet reviewed' + ('; move each reviewed record out of that directory to acknowledge it' if pending_dl else ''))
clean_at = counters.get('Reconcile Last Clean Full Pass Started (epoch seconds)')
if clean_at is not None and clean_at >= since:
    report(True, 'a FULL reconcile pass that started ' + time.strftime('%H:%M:%S', time.localtime(clean_at)) + ' found no difference')
else:
    clean &= report(False, 'no clean FULL reconcile pass yet' + (' since writes stopped' if since else '') + (f' (the last pass found {count(\"Reconcile Last Pass Differences\")} differences)' if count('Reconcile Last Pass Differences') else ''))
print('\n'.join(lines))
print('LAGGING' if not caught_up else 'NOT_SAFE' if not clean else 'UNREVIEWED' if not reviewed else 'SAFE')
"
}

# verdict_of <check-output> - the last line; show_check prints the rest.
verdict_of() { tail -n 1 <<< "$1"; }
show_check() { sed '$d' <<< "$1"; }

# wait_for <description> <since> <max-lag> <accept...> - re-checks every 5 s until the verdict is
# one of <accept...>, printing the conditions whenever they change.
wait_for() {
  local what="$1" since="$2" lag="$3" out last="" waited=0
  shift 3
  echo "==> waiting until $what"
  while true; do
    out="$(check "$since" "$lag")" || out=$'  could not read NiFi\nNOT_SAFE'
    [[ "$out" != "$last" ]] && { show_check "$out"; last="$out"; }
    for accept in "$@"; do
      [[ "$(verdict_of "$out")" == "$accept" ]] && return 0
    done
    if [[ "$waited" -ge "$TIMEOUT_SECONDS" ]]; then
      echo "error: gave up after ${TIMEOUT_SECONDS}s waiting until $what" >&2
      exit 1
    fi
    sleep 5
    waited=$((waited + 5))
  done
}

# set_reconciler <STOPPED|RUNNING> [mode] - a mode is changed only while stopped, and starting
# the reconciler begins a pass at once.
set_reconciler() {
  local state="$1" mode="${2:-}" ver waited=0
  ver="$(nifi_current_version "processors/$PROC_RECONCILER")"
  nifi_api_put "processors/$PROC_RECONCILER/run-status" "{\"revision\":{\"version\":$ver},\"state\":\"$state\"}" > /dev/null
  if [[ "$state" == "STOPPED" ]]; then
    until [[ "$(nifi_api_get "processors/$PROC_RECONCILER" | py3 "import json,sys; d=json.load(sys.stdin); print(d['status']['runStatus'] + str(d['status']['aggregateSnapshot']['activeThreadCount']))")" == "Stopped0" ]]; do
      [[ "$waited" -ge 120 ]] && { echo "error: MigrationReconciler did not stop within 120s" >&2; exit 1; }
      sleep 2
      waited=$((waited + 2))
    done
    if [[ -n "$mode" ]]; then
      ver="$(nifi_current_version "processors/$PROC_RECONCILER")"
      nifi_api_put "processors/$PROC_RECONCILER" \
        "{\"revision\":{\"version\":$ver},\"component\":{\"id\":\"$PROC_RECONCILER\",\"config\":{\"properties\":{\"mode\":\"$mode\"}}}}" > /dev/null
    fi
  fi
}

reconcile_passes() {
  nifi_api_get "counters" | py3 "
import json, sys
print(sum(int(c['valueCount']) for c in json.load(sys.stdin)['counters']['aggregateSnapshot']['counters']
          if c['name'] == 'Reconcile Passes' and not c['context'].startswith('All ')))
"
}

case "$MODE" in
  status)
    OUT="$(check 0 "$MAX_LAG_SECONDS")"
    show_check "$OUT"
    if [[ "$(verdict_of "$OUT")" == "SAFE" ]]; then
      echo "SAFE to cut over, as far as NiFi can tell while the source may still be taking writes."
      echo "Run $(basename "$0") --writes-stopped once the application has stopped writing to the source."
      exit 0
    fi
    echo "NOT safe to cut over yet."
    exit 1
    ;;
  wait)
    wait_for "the Live Phase is less than $MAX_LAG_SECONDS s behind the source" 0 "$MAX_LAG_SECONDS" SAFE NOT_SAFE UNREVIEWED
    echo
    echo "The Live Phase has caught up. Stop application writes to the source now, then run:"
    echo "  $(basename "$0") --writes-stopped"
    exit 0
    ;;
esac

wait_for "every change the source made has reached the target" 0 0 SAFE NOT_SAFE UNREVIEWED
SINCE="$(date +%s)"
ORIGINAL_MODE="$(nifi_api_get "processors/$PROC_RECONCILER" | py3 "import json,sys; print(json.load(sys.stdin)['component']['config']['properties'].get('mode') or 'SAMPLE')")"
CLEAN="false"
for round in $(seq 1 "$RECONCILE_ROUNDS"); do
  BEFORE="$(reconcile_passes)"
  echo "==> final reconcile, FULL pass $round of $RECONCILE_ROUNDS"
  set_reconciler STOPPED FULL
  set_reconciler RUNNING
  waited=0
  until [[ "$(reconcile_passes)" -gt "$BEFORE" ]]; do
    [[ "$waited" -ge "$TIMEOUT_SECONDS" ]] && { echo "error: the FULL pass did not finish within ${TIMEOUT_SECONDS}s" >&2; exit 1; }
    sleep 5
    waited=$((waited + 5))
  done
  # Whatever the pass sent to repair has to land before the next pass looks again.
  wait_for "the pass's repairs have landed" "$SINCE" 0 SAFE NOT_SAFE UNREVIEWED
  VERDICT="$(verdict_of "$(check "$SINCE" 0)")"
  if [[ "$VERDICT" == "SAFE" || "$VERDICT" == "UNREVIEWED" ]]; then
    CLEAN="true"
    break
  fi
done
echo "==> putting MigrationReconciler back in $ORIGINAL_MODE mode"
set_reconciler STOPPED "$ORIGINAL_MODE"
set_reconciler RUNNING

echo
OUT="$(check "$SINCE" 0)"
show_check "$OUT"
if [[ "$CLEAN" != "true" ]]; then
  echo "NOT safe to cut over: no FULL pass came back clean in $RECONCILE_ROUNDS rounds. Is something still writing to the source?" >&2
  echo "The differences each pass found are in MigrationReconciler's report and in the NiFi log." >&2
  exit 1
fi
if [[ "$(verdict_of "$OUT")" == "UNREVIEWED" ]]; then
  echo "NOT safe to cut over yet: the target matches the source, except for the keys in the dead-letter" >&2
  echo "records above, which were not copied. Review them, move them out of the directory to accept" >&2
  echo "the loss, and run $(basename "$0") --status." >&2
  exit 1
fi
cat <<EOF

SAFE to cut over. The target matches the source as of a FULL pass that began after writes stopped.
Switch checklist:
  1. Run ./stop-continuous-migration.sh. It stops every Live Phase processor, MigrationReconciler
     included, which would otherwise copy the old source's values back over the application's
     new writes on Dragonfly.
  2. Point the application at Dragonfly and let it write again.
  3. Optional rollback path: keep the old source current from Dragonfly with
       ./continuous-migration.sh --source-connection-string <dragonfly> \\
         --target-connection-string <old source> --live-source reconcile
     Each FULL reconcile pass copies what changed on Dragonfly, so the old source trails by up
     to one --reconcile-interval-ms. Dragonfly publishes only expiry notifications and serves no
     PSYNC, so there is no continuous change stream in that direction.
EOF
