#!/usr/bin/env bash
# Stops, drains, and permanently deletes the "R2Dfly Migration" process group, so the next
# ./deploy-to-nifi.sh (or ./simple-migration.sh) run re-imports a clean, unconfigured copy of
# r2dfly.json. Use this to recover from a flow left running with a bad connection string, or
# to just start a migration over from scratch.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --nifi-container NAME     container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER          NiFi single-user login username (env NIFI_USER)
  --nifi-password PASSWORD  NiFi single-user login password (env NIFI_PASS)
  --nifi-token TOKEN        use an existing bearer token instead of user/password (env NIFI_TOKEN)
  -y, --yes                 don't prompt for confirmation before deleting
  -h, --help                this help
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"
ASSUME_YES=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) NIFI_TOKEN="$2"; shift 2 ;;
    -y|--yes) ASSUME_YES=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

nifi_lib_pick_runtime

if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  # `|| true`: under set -e/pipefail, a no-match grep (e.g. credentials already rotated past the
  # container's log buffer) fails this assignment and would silently abort the script before the
  # "no NiFi credentials" error below can run.
  CREDS="$($RUNTIME logs "$CONTAINER_NAME" 2>&1 | grep -A1 "Generated Username" | tail -2)" || true
  NIFI_USER="${NIFI_USER:-$(printf '%s\n' "$CREDS" | sed -n '1s/.*\[\(.*\)\]/\1/p')}"
  NIFI_PASS="${NIFI_PASS:-$(printf '%s\n' "$CREDS" | sed -n '2s/.*\[\(.*\)\]/\1/p')}"
  # The credentials line is eventually rotated out of the container's logs for good, so fall
  # back to the copy deploy-to-nifi.sh saved inside the container while it was still there.
  if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
    nifi_lib_load_state "$CONTAINER_NAME" "$NIFI_CREDS_FILE_PATH"
  fi
fi
if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  echo "error: no NiFi credentials given, and none could be read from the container's logs" >&2
  echo "       (they're only printed once, on first container creation) or from the copy" >&2
  echo "       saved inside the container - pass --nifi-user/--nifi-password or --nifi-token" >&2
  echo "       explicitly." >&2
  exit 1
fi

nifi_lib_init

# `|| true`: under set -e/pipefail, a transient NiFi API hiccup would otherwise abort the script
# silently right at the failing assignment, before the deliberate checks below can run.
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
  echo "==> no 'R2Dfly Migration' process group found - nothing to remove"
  exit 0
fi
echo "==> found process group: $PG_ID"

if [[ "$ASSUME_YES" != "true" ]]; then
  if [[ -t 0 ]]; then
    read -r -p "This stops the flow, drops all queued FlowFiles, and permanently deletes the 'R2Dfly Migration' process group. Proceed? [y/N] " REPLY
    if [[ "$REPLY" != [yY]* ]]; then
      echo "aborted - nothing was changed."
      exit 1
    fi
  else
    echo "non-interactive shell - proceeding automatically (pass --yes to suppress this notice)"
  fi
fi

# pg-stop/pg-disable-services return as soon as NiFi *accepts* the request, not once every
# processor/service has actually finished transitioning - poll actual state before deleting,
# since NiFi refuses to delete a process group containing a running processor or enabled
# service (see run-r2dfly.sh, which hit this exact race first). runStatus alone is not enough:
# it flips to Stopped as soon as the scheduler stops issuing new triggers, but a processor
# mid-onTrigger() keeps its activeThreadCount above 0 for a while after - and NiFi rejects
# deletes/PUTs against a group with any active threads, independent of runStatus.
wait_for_processors_stopped() {
  local pgid="$1" timeout="${2:-30}" waited=0 running threads rc
  while true; do
    # rc is checked explicitly rather than `|| true`'d away: a failed API call must not be
    # indistinguishable from "confirmed nothing running" (empty $running), or a transient hiccup
    # would let this falsely report success and let the caller proceed to delete a process group
    # that may still contain a running processor - NiFi would then just reject the delete, but
    # silently, with no indication why.
    rc=0
    running="$(nifi_api_get "process-groups/$pgid/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
print(','.join(p['component']['name'] for p in d['processors'] if p.get('status', {}).get('runStatus') == 'Running'))
")" || rc=$?
    threads="$(nifi_api_get "flow/process-groups/$pgid/status" | py3 "
import json, sys
d = json.load(sys.stdin)
print(d['processGroupStatus']['aggregateSnapshot']['activeThreadCount'])
")" || rc=$?
    [[ $rc -eq 0 && -z "$running" && "$threads" == "0" ]] && return 0
    if [[ "$waited" -ge "$timeout" ]]; then
      if [[ $rc -ne 0 ]]; then
        echo "warning: could not confirm processor status after ${timeout}s (NiFi API call failing) - proceeding anyway" >&2
      else
        echo "warning: still running after ${timeout}s: running=[$running] activeThreadCount=$threads - proceeding anyway" >&2
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
    # See wait_for_processors_stopped above for why rc is checked explicitly.
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
        echo "warning: still not disabled after ${timeout}s: $enabled - proceeding anyway" >&2
      fi
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

echo "==> stopping the flow"
nifi_cli pg-stop -pgid "$PG_ID" || true
sleep 1
# `|| true`: wait_for_processors_stopped returns 1 on timeout by design (it already warned and
# means to proceed anyway) - unguarded, that nonzero return would abort the script right here
# under set -e instead.
wait_for_processors_stopped "$PG_ID" || true

echo "==> dropping all queued FlowFiles"
CONN_JSON="$(nifi_api_get "process-groups/$PG_ID/connections")" || true
# Trailing `|| true` on the whole piped loop below (not just the individual RESP/DROP_ID/STATUS
# assignments, which already tolerate failure) - without it, the producer side (py3 choking on
# a $CONN_JSON left empty by the guard above) would abort the whole script silently under
# set -e/pipefail instead of just skipping the drop-and-wait for this run.
echo "$CONN_JSON" | py3 "
import json, sys
d = json.load(sys.stdin)
for c in d['connections']:
    print(c['id'])
" | while IFS= read -r conn_id; do
  [[ -n "$conn_id" ]] || continue
  RESP="$(nifi_api_post "flowfile-queues/$conn_id/drop-requests" '{}')" || true
  DROP_ID="$(echo "$RESP" | py3 "import json,sys; print(json.load(sys.stdin)['dropRequest']['id'])" 2>/dev/null || true)"
  if [[ -n "$DROP_ID" ]]; then
    for i in $(seq 1 15); do
      sleep 1
      STATUS="$(nifi_api_get "flowfile-queues/$conn_id/drop-requests/$DROP_ID" | py3 "import json,sys; d=json.load(sys.stdin)['dropRequest']; print('done' if d['finished'] else 'pending')" 2>/dev/null || true)"
      [[ "$STATUS" == "done" ]] && break
    done
  fi
  # give podman a beat between connections instead of firing exec calls back-to-back
  sleep 1
done || true

echo "==> disabling controller services"
nifi_cli pg-disable-services -pgid "$PG_ID" || true
sleep 1
# `|| true`: see wait_for_processors_stopped's call above - same timeout-returns-1-by-design
# reasoning applies here.
wait_for_services_disabled "$PG_ID" || true

echo "==> deleting the process group"
PG_VERSION="$(nifi_current_version "process-groups/$PG_ID")" || true
nifi_api_delete "process-groups/$PG_ID?version=$PG_VERSION&clientId=reset-r2dfly-$$&disconnectedNodeAcknowledged=false" >/dev/null || true

# Don't trust the delete response body - confirm by re-querying the actual state, the same
# lesson learned the hard way for property PUTs (see project notes on nifi_api_put_checked).
STILL_THERE="$(nifi_api_get "flow/process-groups/$ROOT_ID" | py3 "
import json, sys
d = json.load(sys.stdin)
print('yes' if any(g['component']['name'] == 'R2Dfly Migration' for g in d['processGroupFlow']['flow']['processGroups']) else 'no')
")" || true
# Checked explicitly rather than just `|| true`'d away: an empty $STILL_THERE (API call failed)
# must not be treated the same as "no" (confirmed actually gone), or a failed verification query
# would silently report success on a delete that might not have actually happened.
if [[ -z "$STILL_THERE" ]]; then
  echo "warning: could not verify whether the process group was actually removed (NiFi API check failed) - check manually" >&2
elif [[ "$STILL_THERE" == "yes" ]]; then
  echo "error: process group still present after delete attempt - check nifi-app.log for details" >&2
  exit 1
fi

echo "==> R2Dfly Migration flow removed."
echo "==> Next: run ./deploy-to-nifi.sh (or ./simple-migration.sh) to import a fresh, unconfigured copy of the flow."
