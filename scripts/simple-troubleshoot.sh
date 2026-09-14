#!/usr/bin/env bash
# One-command troubleshooting for a migration already set up by simple-migration.sh: discovers
# the NiFi container's credentials and the source/target connection strings on its own (same
# way simple-migration.sh does), runs diagnose-r2dfly.sh against them, and adds a short
# recommendation based on what it found. For anything diagnose-r2dfly.sh's own flags cover
# beyond this (a different container's credentials, only checking one side, etc.), use
# diagnose-r2dfly.sh directly - see ./diagnose-r2dfly.sh --help.
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

Optional:
  --nifi-container NAME         container running NiFi (env NIFI_CONTAINER_NAME, default:
                                 nifi-redis-migration)
  --nifi-user USER              use existing NiFi credentials instead of reading the
  --nifi-password PASSWORD      auto-generated ones from the container's logs
  --source-connection-string S  override the source connection string instead of recovering it
                                 from the state simple-migration.sh saved (needed if that
                                 migration predates this script, or the container was recreated)
  --source-connection-mode M    same, for the source's connection mode (standalone, sentinel,
                                 cluster) - cluster sums DBSIZE across all master nodes instead
                                 of just the first seed node (see diagnose-r2dfly.sh)
  --target-connection-string S  same, for the target
  --target-connection-mode M    same, for the target's connection mode
  -h, --help                    this help

Example:
  $(basename "$0")
  $(basename "$0") --nifi-container my-migration-container
EOF
}

NIFI_CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
SOURCE_CONNECTION_STRING="${SOURCE_CONNECTION_STRING:-}"
TARGET_CONNECTION_STRING="${TARGET_CONNECTION_STRING:-}"
# Defaults here only matter if neither an override flag nor simple-migration.sh's saved state
# (recovered below via nifi_lib_load_state, which eval's SOURCE_CONNECTION_MODE/
# TARGET_CONNECTION_MODE back into scope alongside the connection strings, if that migration
# saved them) sets these - standalone is the same default every other script in this project uses.
SOURCE_CONNECTION_MODE="${SOURCE_CONNECTION_MODE:-standalone}"
TARGET_CONNECTION_MODE="${TARGET_CONNECTION_MODE:-standalone}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) NIFI_CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --source-connection-string) SOURCE_CONNECTION_STRING="$2"; shift 2 ;;
    --source-connection-mode) SOURCE_CONNECTION_MODE="$2"; shift 2 ;;
    --target-connection-string) TARGET_CONNECTION_STRING="$2"; shift 2 ;;
    --target-connection-mode) TARGET_CONNECTION_MODE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

nifi_lib_pick_runtime
nifi_lib_check_disk_space

echo "==> discovering NiFi credentials for '$NIFI_CONTAINER_NAME'"
if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
  # `|| true`: under set -e/pipefail, a no-match grep (e.g. credentials already rotated past the
  # container's log buffer) fails this assignment and would silently abort the script before the
  # "no NiFi credentials" error below can run.
  CREDS="$($RUNTIME logs "$NIFI_CONTAINER_NAME" 2>&1 | grep -A1 "Generated Username" | tail -2)" || true
  NIFI_USER="${NIFI_USER:-$(printf '%s\n' "$CREDS" | sed -n '1s/.*\[\(.*\)\]/\1/p')}"
  NIFI_PASS="${NIFI_PASS:-$(printf '%s\n' "$CREDS" | sed -n '2s/.*\[\(.*\)\]/\1/p')}"
  # The credentials line is eventually rotated out of the container's logs for good, so fall
  # back to the copy deploy-to-nifi.sh saved inside the container while it was still there.
  if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
    nifi_lib_load_state "$NIFI_CONTAINER_NAME" "$NIFI_CREDS_FILE_PATH"
  fi
fi
if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
  echo "error: no NiFi credentials given, and none could be read from '$NIFI_CONTAINER_NAME'" >&2
  echo "       logs (they're only printed once, on first container creation) or from the copy" >&2
  echo "       saved inside the container - pass --nifi-user/--nifi-password explicitly if" >&2
  echo "       you already have credentials." >&2
  exit 1
fi
echo "  authenticated as $NIFI_USER"

if [[ -z "$SOURCE_CONNECTION_STRING" || -z "$TARGET_CONNECTION_STRING" ]]; then
  echo "==> recovering source/target connection strings simple-migration.sh saved for this container"
  nifi_lib_load_state "$NIFI_CONTAINER_NAME" "$NIFI_STATE_FILE_PATH"
fi
if [[ -z "$SOURCE_CONNECTION_STRING" || -z "$TARGET_CONNECTION_STRING" ]]; then
  echo "  none found - direct source/target reachability checks will be skipped (pass" >&2
  echo "  --source-connection-string/--target-connection-string to enable them)" >&2
fi

DIAG_ARGS=(--nifi-container "$NIFI_CONTAINER_NAME" --nifi-user "$NIFI_USER" --nifi-password "$NIFI_PASS")
[[ -n "$SOURCE_CONNECTION_STRING" ]] && DIAG_ARGS+=(--source-connection-string "$SOURCE_CONNECTION_STRING" --source-connection-mode "$SOURCE_CONNECTION_MODE")
[[ -n "$TARGET_CONNECTION_STRING" ]] && DIAG_ARGS+=(--target-connection-string "$TARGET_CONNECTION_STRING" --target-connection-mode "$TARGET_CONNECTION_MODE")

echo
DIAG_OUT="$("$PROJECT_ROOT/scripts/diagnose-r2dfly.sh" "${DIAG_ARGS[@]}")"
printf '%s\n' "$DIAG_OUT"

BULLETIN_COUNT="$(printf '%s\n' "$DIAG_OUT" | grep -c '^  \[' || true)"
INVALID_SERVICES="$(printf '%s\n' "$DIAG_OUT" | grep 'validationStatus=' | grep -v 'validationStatus=VALID' || true)"
STUCK_QUEUES="$(printf '%s\n' "$DIAG_OUT" | awk -F'queued=' '/ -> .*queued=/ && ($2+0)>0')"

# The bulletin board is a historical log - a bulletin can be a transient, already-resolved blip
# (e.g. the built-in MapCacheServer/MapCacheClientService "Cursor State Cache" pair briefly
# reconnecting) rather than a live problem, and NiFi keeps recent bulletins around for a while
# even after whatever caused them clears up. Don't trust a single snapshot: if anything looked
# off, back off and recheck live health (controller-service validity + whether queues are still
# stuck) once before deciding it's worth flagging.
INVALID_SERVICES2=""
STUCK_QUEUES2=""
if [[ "$BULLETIN_COUNT" -gt 0 || -n "$INVALID_SERVICES" || -n "$STUCK_QUEUES" ]]; then
  echo
  echo "==> something looked off on the first pass - rechecking live health after a 15s backoff before flagging it"
  sleep 15
  DIAG_OUT2="$("$PROJECT_ROOT/scripts/diagnose-r2dfly.sh" "${DIAG_ARGS[@]}")" || true
  INVALID_SERVICES2="$(printf '%s\n' "$DIAG_OUT2" | grep 'validationStatus=' | grep -v 'validationStatus=VALID' || true)"
  STUCK_QUEUES2="$(printf '%s\n' "$DIAG_OUT2" | awk -F'queued=' '/ -> .*queued=/ && ($2+0)>0')"
fi

echo
echo "=================================================="
echo "Recommendation"
echo "=================================================="
if [[ -n "$INVALID_SERVICES2" ]]; then
  echo "  one or more controller services are still invalid after a recheck - see section 3 above"
  echo "  for the specific validation error(s); the flow can't move data correctly until those"
  echo "  are fixed."
elif [[ -n "$STUCK_QUEUES2" ]]; then
  echo "  these connections still have items queued and not draining after a recheck:"
  printf '%s\n' "$STUCK_QUEUES2" | sed 's/^/    /'
  echo "  the processor immediately downstream of each is likely stuck or failing silently -"
  echo "  check its activeThreadCount/tasksCompleted in section 2 above."
elif [[ "$BULLETIN_COUNT" -gt 0 ]]; then
  echo "  found $BULLETIN_COUNT bulletin(s) in the log (see section 1 above), but a recheck ~15s"
  echo "  later found controller services valid and no queues stuck - this looks like a transient,"
  echo "  already-resolved condition (e.g. a brief Cursor State Cache reconnect) rather than an"
  echo "  active failure. Re-run this script later if you still suspect a real problem."
else
  echo "  no NiFi-side errors or stuck queues detected. If the target's key count still fell"
  echo "  short of the source's:"
  echo "    - check whether --key-types was passed to the migration (an intentional, documented"
  echo "      gap in that case)"
  echo "    - confirm direct source/target reachability in section 4 above"
  echo "    - for a cluster source, confirm no master became unreachable or started a slot"
  echo "      migration mid-run - the topology pre-flight check only ran once, at migration"
  echo "      start, and wouldn't catch that"
fi

# extract_dbsize <source|target> - pulls the key count diagnose-r2dfly.sh's section 4 already
# computed (cluster-aware sum or plain DBSIZE, whichever applies) out of the captured $DIAG_OUT,
# rather than querying again here - reusing the one already-fetched value keeps this line
# consistent with section 4 above instead of risking a different-looking number from a second,
# separately-timed query against a target that may still be actively receiving keys. Scoped
# between this side's "  source (" / "  target (" header and the other side's, so it can't pick
# up the wrong side's dbsize line.
extract_dbsize() {
  local side="$1"
  printf '%s\n' "$DIAG_OUT" | awk -v side="$side" '
    $0 ~ "^  " side " \\(" { in_section=1; next }
    /^  (source|target) \(/ { in_section=0 }
    in_section && /dbsize/ { sub(/.*: /, ""); print; exit }
  '
}
SOURCE_KEYCOUNT="$(extract_dbsize source)"
TARGET_KEYCOUNT="$(extract_dbsize target)"
[[ -n "$SOURCE_KEYCOUNT" ]] || SOURCE_KEYCOUNT="unknown (reachability check skipped or failed - see section 4 above)"
[[ -n "$TARGET_KEYCOUNT" ]] || TARGET_KEYCOUNT="unknown (reachability check skipped or failed - see section 4 above)"

echo
echo "KEY COUNTS for source and target are currently: SOURCE: $SOURCE_KEYCOUNT TARGET: $TARGET_KEYCOUNT"

