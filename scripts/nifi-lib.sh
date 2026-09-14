#!/usr/bin/env bash
# Shared helpers for talking to the local NiFi dev container via its CLI/REST API.
# Sourced by deploy-to-nifi.sh and run-r2dfly.sh - not meant to be run directly.
#
# Callers must set CONTAINER_NAME before calling nifi_lib_init, and either
# NIFI_TOKEN, or both NIFI_USER and NIFI_PASS, for it to authenticate.

source "$PROJECT_ROOT/scripts/toolbox-lib.sh"

nifi_lib_pick_runtime() {
  if command -v docker >/dev/null 2>&1; then
    RUNTIME=docker
  elif command -v podman >/dev/null 2>&1; then
    RUNTIME=podman
  else
    echo "error: neither docker nor podman found on PATH" >&2
    return 1
  fi
}

# nifi_lib_check_disk_space [threshold-percent, default 75] - warns (does not abort) if the
# filesystem backing $RUNTIME's storage is at or above the given percent used. Checks the
# container runtime's own storage root (via `$RUNTIME info`), not blindly "/" - that's where
# NiFi's actual data (content/flowfile/provenance repositories, inside the container's own
# writable layer) and this project's build artifacts (pulled image layers, the Maven dependency
# cache volume) really accumulate - falling back to "/" if that lookup fails for any reason
# (e.g. an unsupported info format on some runtime version). Added after a real incident: a
# 7.6G test host filled completely from accumulated image layers/build cache/NiFi's own content
# repository across many repeated migration runs, which didn't just fail loudly - it silently
# corrupted NiFi's FlowFile repository (a failed swap-file write mid-run) before anyone noticed.
nifi_lib_check_disk_space() {
  local threshold="${1:-75}" storage_root df_fields used_pct avail_human
  storage_root="$("$RUNTIME" info --format '{{.Store.GraphRoot}}' 2>/dev/null || "$RUNTIME" info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
  [[ -d "$storage_root" ]] || storage_root="/"
  # -Ph gives both the percent (unaffected by -h) and a human-readable avail size in one call,
  # so the warning branch below never needs a second df.
  df_fields="$(df -Ph "$storage_root" 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5, $4}')"
  used_pct="${df_fields%% *}"
  avail_human="${df_fields#* }"
  [[ "$used_pct" =~ ^[0-9]+$ ]] || return 0
  if (( used_pct >= threshold )); then
    echo "warning: disk backing $RUNTIME's storage ($storage_root) is ${used_pct}% full (${avail_human:-?} free) - a migration run can fill the rest of it and corrupt NiFi's FlowFile repository (confirmed directly: a full disk silently failed a swap-file write mid-run, not just a clean out-of-space error). Consider freeing space now ($RUNTIME image prune, journalctl --vacuum-size, clearing old content_repository archives) or resizing the volume before continuing." >&2
  fi
}

# Wraps `$RUNTIME exec` with a retry for podman's own host-side race, not for
# genuine command failures: podman has been observed to time out waiting for
# conmon's exit-file ("Error: timed out waiting for file ...: internal libpod
# error") even though the exec'd command already completed successfully inside
# the container - confirmed by re-running reset-r2dfly-flow.sh, which succeeded
# on retry with no other change. Only that exact runtime error text triggers a
# retry; a real failure from the command itself is returned as-is.
runtime_exec() {
  local attempt out err rc
  for attempt in 1 2 3; do
    err="$(mktemp)"
    rc=0
    # `|| rc=$?` (one compound statement, not `out=$(...); rc=$?` as two) is load-bearing under
    # set -e: a failing assignment kills the whole function right there before `rc=$?` (as its
    # own separate statement) ever runs - which would silently defeat both the podman-race retry
    # below and the final `cat "$err"` error dump for every unguarded caller of this function
    # (the vast majority - nifi_cli/nifi_api_get etc. are rarely wrapped in `|| true` at their
    # call sites).
    out="$("$RUNTIME" exec "$@" 2>"$err")" || rc=$?
    if [[ $rc -eq 0 ]]; then
      printf '%s' "$out"
      rm -f "$err"
      return 0
    fi
    if [[ $attempt -lt 3 ]] && grep -qi "internal libpod error" "$err"; then
      echo "warning: podman exec hit a host-side exit-file race (attempt $attempt/3) - retrying" >&2
      rm -f "$err"
      sleep 2
      continue
    fi
    cat "$err" >&2
    rm -f "$err"
    return "$rc"
  done
}

# Resolves the container's real bind address and authenticates, leaving
# NIFI_CLI, NIFI_TRUSTSTORE, NIFI_INTERNAL_URL, NIFI_TS_PASS, and NIFI_TOKEN set.
nifi_lib_init() {
  NIFI_CLI="/opt/nifi/nifi-toolkit-current/bin/cli.sh"
  NIFI_TRUSTSTORE="/opt/nifi/nifi-current/conf/truststore.p12"

  NIFI_TS_PASS="$(runtime_exec "$CONTAINER_NAME" grep 'nifi.security.truststorePasswd' /opt/nifi/nifi-current/conf/nifi.properties | cut -d= -f2 || true)"
  if [[ -z "$NIFI_TS_PASS" ]]; then
    echo "error: could not read truststore password from container '$CONTAINER_NAME' - is it running?" >&2
    return 1
  fi

  local container_hostname
  container_hostname="$(runtime_exec "$CONTAINER_NAME" hostname || true)"
  if [[ -z "$container_hostname" ]]; then
    echo "error: could not read hostname from container '$CONTAINER_NAME' - is it running?" >&2
    return 1
  fi
  # NiFi binds its HTTPS listener to nifi.web.https.host (the container's own hostname), not
  # localhost - so commands run via `exec` inside the container's network namespace must target
  # that hostname rather than localhost:8443 (which only the host's published-port mapping serves).
  NIFI_INTERNAL_URL="https://${container_hostname}:8443"

  if [[ -n "${NIFI_TOKEN:-}" ]]; then
    return 0
  fi
  if [[ -z "${NIFI_USER:-}" || -z "${NIFI_PASS:-}" ]]; then
    echo "error: NiFi credentials required - set NIFI_TOKEN, or both NIFI_USER and NIFI_PASS" >&2
    return 1
  fi
  NIFI_TOKEN="$(runtime_exec "$CONTAINER_NAME" "$NIFI_CLI" nifi get-access-token \
    -u "$NIFI_INTERNAL_URL" -ts "$NIFI_TRUSTSTORE" -tst PKCS12 -tsp "$NIFI_TS_PASS" \
    -usr "$NIFI_USER" -pwd "$NIFI_PASS" || true)"
  if [[ -z "$NIFI_TOKEN" ]]; then
    echo "error: NiFi authentication failed" >&2
    return 1
  fi
}

# Where migration connection details get stashed inside the NiFi container (see
# nifi_lib_save_state/nifi_lib_load_state below) so a later, separate invocation - e.g.
# simple-troubleshoot.sh - can recover them without re-prompting the user. Lives inside the
# container rather than the project directory: it travels with the container regardless of
# where a script runs from, and doesn't add a file to the project workspace.
NIFI_STATE_FILE_PATH="/tmp/r2dfly-migration-state.env"

# Where the container's own auto-generated single-user credentials get stashed, by
# deploy-to-nifi.sh, the one script that sees them while they're still fresh in the container's
# logs (NiFi prints them exactly once, on first startup, and the log driver eventually rotates
# that line out entirely). A separate file from NIFI_STATE_FILE_PATH because the two have
# different lifecycles - credentials are fixed for the container's whole life, while migration
# state is fully overwritten by every new migration run - and nifi_lib_save_state replaces its
# entire target file on each call, so sharing one file would let a later state-save silently
# erase the saved credentials, or a credentials-save erase the migration state.
NIFI_CREDS_FILE_PATH="/tmp/r2dfly-nifi-credentials.env"

# nifi_lib_save_state <container> <file> <VAR>... - persists the CURRENT VALUE of each named
# shell variable into <container> at <file> (a container-side path, e.g. NIFI_STATE_FILE_PATH or
# NIFI_CREDS_FILE_PATH), as %q-shell-quoted VAR=value assignments nifi_lib_load_state can later
# `eval` back safely. Replaces <file> wholesale, so every call must pass the full set of
# variables that file is meant to hold. Needed because NiFi's own REST API never returns a
# `sensitive` property (e.g. connection-string) once set, and never returns the generated
# credentials at all - there is no other way to recover either from a running container.
nifi_lib_save_state() {
  local container="$1" state_file="$2"; shift 2
  local var content="" b64
  for var in "$@"; do
    content+="$var=$(printf '%q' "${!var}")"$'\n'
  done
  b64="$(printf '%s' "$content" | base64 | tr -d '\n')"
  # `||`: a failed save here would otherwise abort the whole calling script via set -e - this is
  # a best-effort convenience for a later, separate invocation (simple-troubleshoot.sh and
  # friends), not something the current run depends on, so a warning and continuing is the right
  # degradation, not a hard stop.
  runtime_exec "$container" sh -c "printf '%s' '$b64' | base64 -d > $state_file" >/dev/null ||
    echo "warning: could not save $state_file to '$container' for later runs" >&2
}

# nifi_lib_load_state <container> <file> - reads back state saved by nifi_lib_save_state to
# <file> in <container>, if any, and eval's it into the caller's shell. A no-op (nothing set) if
# <file> isn't there - e.g. it was never written, or the container was recreated since.
nifi_lib_load_state() {
  local container="$1" state_file="$2" content
  content="$(runtime_exec "$container" cat "$state_file" 2>/dev/null)" || return 0
  [[ -n "$content" ]] && eval "$content"
}

nifi_cli() {
  runtime_exec "$CONTAINER_NAME" "$NIFI_CLI" nifi "$@" \
    -u "$NIFI_INTERNAL_URL" -ts "$NIFI_TRUSTSTORE" -tst PKCS12 -tsp "$NIFI_TS_PASS" -btk "$NIFI_TOKEN"
}

nifi_api_get() {
  runtime_exec "$CONTAINER_NAME" curl -sk -H "Authorization: Bearer $NIFI_TOKEN" "$NIFI_INTERNAL_URL/nifi-api/$1"
}

nifi_api_put() {
  runtime_exec "$CONTAINER_NAME" curl -sk -X PUT -H "Authorization: Bearer $NIFI_TOKEN" \
    -H "Content-Type: application/json" -d "$2" "$NIFI_INTERNAL_URL/nifi-api/$1"
}

nifi_api_post() {
  runtime_exec "$CONTAINER_NAME" curl -sk -X POST -H "Authorization: Bearer $NIFI_TOKEN" \
    -H "Content-Type: application/json" -d "$2" "$NIFI_INTERNAL_URL/nifi-api/$1"
}

nifi_api_delete() {
  runtime_exec "$CONTAINER_NAME" curl -sk -X DELETE -H "Authorization: Bearer $NIFI_TOKEN" \
    "$NIFI_INTERNAL_URL/nifi-api/$1"
}

# nifi_current_version <rest-path, e.g. "controller-services/<id>">
nifi_current_version() {
  nifi_api_get "$1" | py3 "import json,sys; print(json.load(sys.stdin)['revision']['version'])"
}

# derive_heap_mb <docker/podman --memory value, e.g. "4g", "2048m", "512000000"> -> prints an
# integer MB heap size on stdout (~75% of the limit, leaving headroom for the JVM's own
# off-heap/metaspace overhead) and returns 0, or returns 1 with nothing printed if the value
# doesn't parse. Used to size NIFI_JVM_HEAP_MAX from a container memory limit - a bigger
# container is wasted on NiFi's small default heap unless the heap grows to match.
derive_heap_mb() {
  local mem="$1" num unit mb
  if [[ "$mem" =~ ^([0-9]+)([kKmMgG][bB]?)?$ ]]; then
    num="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2],,}"
    case "$unit" in
      k|kb) mb=$(( num / 1024 )) ;;
      m|mb) mb=$num ;;
      g|gb) mb=$(( num * 1024 )) ;;
      "") mb=$(( num / 1024 / 1024 )) ;;
      *) return 1 ;;
    esac
    (( mb > 0 )) || return 1
    echo $(( mb * 3 / 4 ))
    return 0
  fi
  return 1
}
