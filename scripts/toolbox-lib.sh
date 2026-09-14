#!/usr/bin/env bash
# Builds/runs a small container image (curl + python3 + redis-cli, see toolbox/Dockerfile)
# so scripts never need those tools installed on the host - only Docker/Podman itself.
# Sourced by redis-lib.sh and nifi-lib.sh - not meant to be run directly. Callers must have
# PROJECT_ROOT and RUNTIME set before calling any function here (both are already required
# by everything that sources this).

TOOLBOX_IMAGE="${TOOLBOX_IMAGE:-nifi-redis-migration-toolbox:latest}"

# Set by --verbose in the entry-point scripts (deploy-to-nifi.sh, simple-migration.sh) or the
# VERBOSE env var directly. Controls run_with_progress below - default is a quiet progress
# bar instead of raw pull/build/download output.
VERBOSE="${VERBOSE:-false}"

# run_with_progress <description> <command...> - runs a command with its own stdout/stderr
# captured to a temp file and a simple animated progress bar shown in its place, unless
# VERBOSE=true (then the command's real output streams through unmodified). Meant for slow
# pull/build/download steps (container image layers, Maven dependencies, apk packages) where
# per-file/per-layer detail is noise on a normal run. On failure, the captured output is
# dumped so the error is never silently swallowed.
run_with_progress() {
  local desc="$1"; shift
  if [[ "$VERBOSE" == "true" ]]; then
    echo "==> $desc" >&2
    "$@"
    return $?
  fi
  local logfile pid rc width=20 pos=0 dir=1 bar
  logfile="$(mktemp)"
  SECONDS=0
  "$@" >"$logfile" 2>&1 &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    bar="$(printf '%*s' "$pos" '' | tr ' ' '#')$(printf '%*s' $((width - pos)) '')"
    # Elapsed time is the point of this line, not the bar itself - a step that can silently
    # take a minute or more (a first-time image pull/Maven build with no cache yet) otherwise
    # looks identical to a hang, since nothing else prints while it runs (confirmed in
    # practice: a user mistook a slow-but-healthy first build for a stuck process and killed
    # it, since the bar alone gives no sense of whether time is actually passing).
    printf '\r==> %s [%s] %ds elapsed' "$desc" "$bar" "$SECONDS" >&2
    pos=$((pos + dir))
    if [[ $pos -ge $width || $pos -le 0 ]]; then dir=$((-dir)); fi
    sleep 0.15
  done
  # `|| rc=$?` (one compound statement) is load-bearing under set -e: `wait "$pid"; rc=$?` as
  # two separate statements dies on the first one, before `rc=$?` ever runs - which would defeat
  # this whole function's reason for existing (the failed-command log dump right below) for
  # every real build/pull failure, the routine case this function exists to handle gracefully.
  rc=0
  wait "$pid" || rc=$?
  if [[ $rc -eq 0 ]]; then
    printf '\r==> %s [%s] done (%ds).\n' "$desc" "$(printf '%*s' "$width" '' | tr ' ' '#')" "$SECONDS" >&2
  else
    printf '\r==> %s failed (exit %s) - full output below (pass --verbose to see this live next time):\n' "$desc" "$rc" >&2
    cat "$logfile" >&2
  fi
  rm -f "$logfile"
  return $rc
}

# Builds the toolbox image if a matching tag doesn't already exist. Docker/Podman's own
# layer cache makes a rebuild against an unchanged Dockerfile a no-op either way, but
# checking first avoids even that overhead on every single script invocation.
toolbox_lib_ensure_image() {
  $RUNTIME image inspect "$TOOLBOX_IMAGE" >/dev/null 2>&1 && return 0
  run_with_progress "building toolbox image ($TOOLBOX_IMAGE): curl/python3/redis-cli, so no host installs are needed" \
    $RUNTIME build -t "$TOOLBOX_IMAGE" "$PROJECT_ROOT/artifacts/toolbox"
}

# toolbox_run used to `$RUNTIME run --rm -i` a brand-new container for every single call - for
# a caller that invokes it many times in a row (a poll loop checking DBSIZE every few seconds
# for a whole migration, or a per-node/per-index loop in cluster mode), that means one full
# container create+start+teardown per call stacked directly on top of the real network round
# trip it's making. What follows instead starts ONE long-lived container the first time it's
# actually needed and execs into it repeatedly for the rest of the top-level script's lifetime.
#
# The tricky part: redis_cli's toolbox fallback (redis-lib.sh) - by far the hottest caller,
# since it's what every DBSIZE poll and per-node/per-key EVAL ultimately goes through for a
# real remote/cloud target - always invokes toolbox_run inside `$(...)` command substitution,
# which bash always runs in a SUBSHELL. A plain shell variable (or a trap registered) inside
# that subshell never survives back to the parent once the substitution completes - confirmed
# directly: an earlier version of this cache kept the container name in a plain variable and
# registered its cleanup trap lazily on first use, and measurably respun a fresh container on
# EVERY redis_cli call anyway, silently defeating the whole point. Fixed by tracking the
# container by a NAME DETERMINISTIC ACROSS THE WHOLE PROCESS ($$, which bash keeps stable
# through any depth of command-substitution subshell) and checking for it via `container
# inspect` - real state outside the shell - instead of a variable, so any subshell can
# independently discover "yes, this process's container is already up" no matter how deep it's
# nested. The one piece that must run in the TRUE top-level shell is the cleanup trap (a trap
# set inside a subshell only fires for that subshell's own exit, not the parent's) - so trap
# registration is a separate, explicit opt-in (toolbox_lib_enable_reuse) that each top-level
# script calls once, itself, at its own top level. A script that never calls it gets today's
# original behavior (toolbox_run falls back to one-shot `run --rm -i`) - safe by default, since
# an unbounded background container with nothing left alive to clean it up would be a real leak.
TOOLBOX_REUSE_ENABLED=""
TOOLBOX_CONTAINER_NAME=""

# toolbox_lib_enable_reuse - opts this process into the persistent-container optimization.
# MUST be called from the script's own top level (not from inside a function invoked via
# `$(...)`/`( )`), directly after sourcing redis-lib.sh/cluster-lib.sh, and before this script
# sets any EXIT trap of its own - it composes with a trap already in place when it runs, but
# (being an ordinary `trap` call) would itself be silently clobbered by one set afterward.
toolbox_lib_enable_reuse() {
  TOOLBOX_REUSE_ENABLED=true
  # Set eagerly (not lazily on first real toolbox_run call) specifically so the EXIT trap below
  # - which always runs in THIS true top-level shell, per this function's own doc comment - has
  # a correct name to clean up even though toolbox_run itself is normally invoked from deep
  # inside a subshell (redis_cli's `$(...)` fallback). A subshell's own assignment to this same
  # variable never survives back to this shell (confirmed directly - see the big comment above),
  # so if this line only ran lazily inside _toolbox_lib_ensure_container, the container a
  # subshell actually started would never get cleaned up here at all - it's deterministic ($$-
  # based) so there's no correctness cost to computing it before it's actually needed.
  TOOLBOX_CONTAINER_NAME="nifi-redis-migration-toolbox-run-$$"
  local existing
  existing="$(trap -p EXIT)"
  if [[ -n "$existing" ]]; then
    # `trap -p EXIT` prints exactly: trap -- 'the command' EXIT
    local prev="${existing#trap -- \'}"
    prev="${prev%\' EXIT}"
    trap "$prev; _toolbox_lib_stop_container" EXIT
  else
    trap '_toolbox_lib_stop_container' EXIT
  fi
}

_toolbox_lib_stop_container() {
  [[ -n "$TOOLBOX_CONTAINER_NAME" ]] || return 0
  # $RUNTIME may still be unset here: a run that enables reuse but exits (e.g. a validation
  # error) before redis_lib_pick_runtime ever gets to run never learns docker-vs-podman at all -
  # under this script's `set -u`, referencing $RUNTIME un-set would itself crash the exit trap.
  # Confirmed directly. Nothing to clean up in that case anyway: no runtime was ever picked, so
  # no container could have been started through it either.
  [[ -n "${RUNTIME:-}" ]] || return 0
  $RUNTIME stop -t 0 "$TOOLBOX_CONTAINER_NAME" >/dev/null 2>&1 || true
}

# _toolbox_lib_ensure_container - starts the persistent container if it isn't already running
# (checked via `container inspect` against the deterministic name - real, subshell-safe state,
# see the big comment above - not a variable). --entrypoint sleep + a plain non-"infinity"
# integer argument: the toolbox image is Alpine with BusyBox's sleep, which doesn't reliably
# accept GNU coreutils' "infinity" keyword - a huge plain second count (~68 years) is portable
# to both and just as good for "runs until we explicitly stop it". Tolerates a concurrent
# sibling call (a different subshell of this same process) winning the race to start it first -
# `run --name` failing because the name is already taken is treated as success, not an error,
# as long as the container is actually there afterward.
_toolbox_lib_ensure_container() {
  local name="$TOOLBOX_CONTAINER_NAME"
  $RUNTIME container inspect "$name" >/dev/null 2>&1 && return 0
  toolbox_lib_ensure_image
  # --network host: this exists for targets not reachable via `exec` into a named container (a
  # real remote/cloud instance, or a service bound to the host's own 127.0.0.1). Without host
  # networking, the toolbox container's 127.0.0.1 would resolve to itself rather than the host,
  # and a source like `redis://127.0.0.1:6379` would never connect.
  if $RUNTIME run -d --rm --network host --name "$name" --entrypoint sleep "$TOOLBOX_IMAGE" 2147483647 >/dev/null 2>&1; then
    return 0
  fi
  # Didn't start - either a genuine failure, or a concurrent sibling call already won the race
  # (same deterministic name, "already in use"). Only the latter is fine.
  $RUNTIME container inspect "$name" >/dev/null 2>&1
}

# toolbox_run <command...> - runs a command in the toolbox image; stdin is passed through, so
# e.g. a Lua script or JSON blob can still be piped/redirected in exactly as if the command
# were run locally. Uses the persistent container (with a one-retry-after-restart if the exec
# itself fails - e.g. the container died or was removed out from under us between calls, say by
# an operator running `podman container prune` mid-migration) when toolbox_lib_enable_reuse has
# been called; otherwise behaves exactly as before (`run --rm -i`, fresh container every time).
toolbox_run() {
  if [[ "$TOOLBOX_REUSE_ENABLED" != "true" ]]; then
    toolbox_lib_ensure_image
    $RUNTIME run --rm -i --network host "$TOOLBOX_IMAGE" "$@"
    return $?
  fi
  _toolbox_lib_ensure_container
  # `|| rc=$?` (not `if ...; then return 0; fi; local rc=$?`) is load-bearing: bash reports 0
  # for an if/fi whose condition was false and took no branch, regardless of the condition
  # command's own real exit status - `$?` read straight after the fi would always show 0 here,
  # making the warning below always claim "(exit 0)" no matter what actually failed.
  local rc=0
  $RUNTIME exec -i "$TOOLBOX_CONTAINER_NAME" "$@" || rc=$?
  [[ $rc -eq 0 ]] && return 0
  echo "warning: toolbox container '$TOOLBOX_CONTAINER_NAME' exec failed (exit $rc) - restarting it and retrying once" >&2
  $RUNTIME rm -f "$TOOLBOX_CONTAINER_NAME" >/dev/null 2>&1 || true
  _toolbox_lib_ensure_container
  $RUNTIME exec -i "$TOOLBOX_CONTAINER_NAME" "$@"
}

# py3 <script> - runs `python3 -c <script>` in the toolbox image, stdin passed through.
# Drop-in replacement for `... | python3 -c "$script"`.
py3() {
  toolbox_run python3 -c "$1"
}
