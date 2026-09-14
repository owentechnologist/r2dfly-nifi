#!/usr/bin/env bash
# Shared helpers for running redis-cli against either a local container (via
# docker/podman exec) or a directly-reachable connection string. Sourced by
# run-r2dfly.sh and populate-test-data.sh - not meant to be run directly.

source "$PROJECT_ROOT/scripts/toolbox-lib.sh"

redis_lib_pick_runtime() {
  if command -v docker >/dev/null 2>&1; then
    RUNTIME=docker
  elif command -v podman >/dev/null 2>&1; then
    RUNTIME=podman
  else
    echo "error: neither docker nor podman found on PATH" >&2
    return 1
  fi
}

# redis_lib_extract_host redis://[:password@]host:port[/db] -> host
# Handles comma-separated cluster-mode connection strings by taking the first node.
redis_lib_extract_host() {
  local s="$1"
  s="${s%%,*}"
  s="${s#*://}"
  s="${s#*@}"
  s="${s%%:*}"
  s="${s%%/*}"
  echo "$s"
}

# redis_lib_extract_port redis://[:password@]host:port[/db] -> port (default 6379 if omitted)
# Handles comma-separated cluster-mode connection strings by taking the first node.
redis_lib_extract_port() {
  local s="$1"
  s="${s%%,*}"
  s="${s#*://}"
  s="${s#*@}"
  s="${s%%/*}"
  if [[ "$s" == *:* ]]; then
    echo "${s#*:}"
  else
    echo "6379"
  fi
}

# redis_cli <container> <connection-string> <redis-cli args...>
# Tries `<runtime> exec -i <container> redis-cli ...` first; only falls back to
# `redis-cli` via the toolbox container (see toolbox-lib.sh) if the exec itself failed to
# reach the container (nonzero exit - e.g. no such container, not running - which is the
# normal case for any real remote/cloud instance rather than a local test container). A
# pre-check like `podman ps | grep` was tried first but proved flaky under many rapid calls
# (occasionally missing a container that was, in fact, running) - trying the real
# exec and only falling back on its actual failure is the reliable signal.
# Per-command Redis errors (e.g. unknown command) still exit 0 from redis-cli
# itself, so they pass through as text for the caller to inspect, not a fallback.
# Extra args are passed straight through to redis-cli (including none, for reading
# a batch of commands from stdin).
# The exec-failed warning is only printed once the toolbox fallback has ALSO been tried and
# failed - most invocations target a real remote/cloud instance, never a local container, so
# unconditionally warning about the exec attempt on every single call (there's no cheaper way
# to know in advance which one applies) would just be noise on the overwhelmingly common path
# where the fallback succeeds straight away.
redis_cli() {
  local container="$1" connstr="$2"; shift 2
  local out rc=0
  # The `|| rc=$?` is load-bearing under the callers' `set -e`: a bare `out=$(...)`
  # assignment propagates a failing command's exit status as its own, which would abort the
  # whole script right here instead of ever reaching the fallback below.
  out="$($RUNTIME exec -i "$container" redis-cli "$@" 2>&1)" || rc=$?
  if [[ $rc -eq 0 ]]; then
    printf '%s\n' "$out"
    return 0
  fi
  # redis-cli's -u only accepts a single URI - a cluster-mode connstr's comma-separated seed
  # list needs the same "take the first node" treatment as redis_lib_extract_host/port above,
  # or this fallback would pass an invalid -u value straight through.
  local fallback_connstr="${connstr%%,*}"
  local fallback_out fallback_rc=0
  # Only stdout is captured here (no 2>&1) - redis-cli's own client-side stderr chatter (e.g.
  # "Warning: Using a password with '-a' or '-u' ..." whenever a caller passes -u itself) must
  # stream straight to the terminal like normal, NOT get bundled into the value callers treat as
  # real command output. Getting this wrong doesn't just look noisy - cluster_lib_dbsize_sum
  # requires its captured DBSIZE to be a bare integer and silently treats anything else as 0, so
  # a stray warning line prepended to the real count broke cluster-mode key totals outright.
  fallback_out="$(toolbox_run redis-cli -u "$fallback_connstr" "$@")" || fallback_rc=$?
  if [[ $fallback_rc -eq 0 ]]; then
    printf '%s\n' "$fallback_out"
    return 0
  fi
  echo "warning: '$RUNTIME exec' into '$container' failed (exit $rc): $out - falling back to redis-cli via the toolbox container against $fallback_connstr" >&2
  echo "warning: toolbox fallback also failed (exit $fallback_rc) - see redis-cli's error above" >&2
  return "$fallback_rc"
}

# The five functions below are defined after redis_cli rather than next to the extract_host/
# extract_port string helpers above because they're all built on it - they query a live server.

# redis_lib_detect_product_kind <container> <connstr> -> "dragonfly:<version>", "valkey:<version>",
# "redis:<version>", or "unknown", read from INFO SERVER. Dragonfly reports a dragonfly_version
# field and Valkey a valkey_version one; both are checked BEFORE the plain redis_version field,
# because Valkey still reports a legacy-compat redis_version of its own (confirmed directly
# against real Redis 6/8, Redis Stack, Valkey and Dragonfly), so trying redis_version first would
# misreport Valkey as Redis. Used to label each side's product and to decide --dfly-to-dfly.
redis_lib_detect_product_kind() {
  local container="$1" connstr="$2" info version
  # `|| true`: without it, a connection blip's non-zero exit fails this assignment and aborts the
  # calling script under its set -e/pipefail, instead of degrading to the "unknown" default below.
  info="$(redis_cli "$container" "$connstr" INFO SERVER 2>/dev/null | tr -d '\r')" || true
  version="$(sed -n 's/^dragonfly_version://p' <<< "$info")"
  if [[ -n "$version" ]]; then
    echo "dragonfly:$version"; return 0
  fi
  version="$(sed -n 's/^valkey_version://p' <<< "$info")"
  if [[ -n "$version" ]]; then
    echo "valkey:$version"; return 0
  fi
  version="$(sed -n 's/^redis_version://p' <<< "$info")"
  if [[ -n "$version" ]]; then
    echo "redis:$version"; return 0
  fi
  echo "unknown"
}

# redis_lib_product_label <kind> -> display string for a redis_lib_detect_product_kind result,
# e.g. "dragonfly:1.34.1" -> "Dragonfly 1.34.1".
redis_lib_product_label() {
  local kind="$1"
  case "$kind" in
    dragonfly:*) echo "Dragonfly ${kind#dragonfly:}" ;;
    valkey:*)    echo "Valkey ${kind#valkey:}" ;;
    redis:*)     echo "Redis ${kind#redis:}" ;;
    *)           echo "unknown (could not read INFO SERVER)" ;;
  esac
}

# redis_lib_detect_capabilities <container> <connstr> -> space-separated subset of
# "json search topk bloom cms" that the server actually supports.
# COMMAND INFO <cmd> is the probe: a non-empty reply means the server recognizes the command, an
# empty one means it doesn't. That's product- and version-independent, so this never has to know
# whether a given side ships these as loadable modules (Redis Stack), built in (Redis 8,
# Dragonfly), or not at all (plain Redis 6) - confirmed directly against all four.
# A plain "name:probe-command" list rather than an associative array on purpose: this project has
# to keep working under macOS's stock bash 3.2 (which a restricted-PATH sudo still resolves to),
# and `declare -A` doesn't exist there.
redis_lib_detect_capabilities() {
  local container="$1" connstr="$2" pair name cmd reply caps=""
  for pair in "json:json.get" "search:ft.info" "topk:topk.info" "bloom:bf.exists" "cms:cms.info"; do
    name="${pair%%:*}"
    cmd="${pair#*:}"
    # `|| true`: see redis_lib_detect_product_kind above - an unreachable server degrades to "no
    # capabilities detected" rather than aborting the caller mid-probe.
    reply="$(redis_cli "$container" "$connstr" COMMAND INFO "$cmd" 2>/dev/null | tr -d '[:space:]')" || true
    [[ -n "$reply" ]] && caps="$caps $name"
  done
  echo "${caps# }"
}

# redis_lib_count_search_indexes <container> <connstr> <capabilities> -> how many search indexes
# that server has, or 0 if it has no search support at all.
# Only ever calls FT._LIST once <capabilities> (a redis_lib_detect_capabilities result) has already
# confirmed search support: on a server without it, redis-cli prints "ERR unknown command
# `FT._LIST`, with args beginning with:" to STDOUT, not stderr (command-reply errors always do in
# this project's non-interactive single-command invocation style), so an unconditional call would
# count that one error line as "1 index" on a server that can't do search at all. FT._LIST itself
# is cheap - it enumerates index names, it never scans the keyspace.
redis_lib_count_search_indexes() {
  local container="$1" connstr="$2" caps="$3" count
  if [[ " $caps " != *" search "* ]]; then
    echo 0
    return 0
  fi
  # `|| true` earns its place twice over here: grep -c exits 1 when it counts zero matches, and
  # redis_cli exits non-zero against an unreachable server - either would abort the caller.
  count="$(redis_cli "$container" "$connstr" FT._LIST 2>/dev/null | grep -c . || true)"
  count="$(tr -d '[:space:]' <<< "$count")"
  [[ "$count" =~ ^[0-9]+$ ]] || count=0
  echo "$count"
}

# redis_lib_warn_capability_gaps <source_caps> <target_caps> <dfly_to_dfly> <source_index_count>
# Warns (on stderr) about module/data types the source can hold that this migration can't
# actually land on the target. Which gap matters differs per type, because ModuleTypeHandler
# rebuilds each one differently (see docs/TUTORIAL.md's "Dragonfly-to-Dragonfly Optimizations"):
# JSON and TopK are reconstructed via JSON.*/TOPK.* commands against any target that has them, so
# the only gap is a target missing the command family outright. Bloom and CMS have no
# type-specific reconstruction path in that processor at all - they only ever move via the
# Dragonfly-to-Dragonfly DUMP/RESTORE fast path - so their gap is "this run isn't dfly-to-dfly",
# whatever the target's own BF.*/CMS.* support happens to look like. Search indexes are rebuilt
# with FT.CREATE, so what matters is whether the source actually holds any: an empty one has
# nothing to lose.
redis_lib_warn_capability_gaps() {
  local source_caps="$1" target_caps="$2" dfly_to_dfly="$3" index_count="$4" warned="false"
  if [[ " $source_caps " == *" json "* && " $target_caps " != *" json "* ]]; then
    echo "  warning: the source supports JSON (ReJSON-RL) keys but the target has no JSON.* commands - any JSON key found will fail to write and be routed to ModuleTypeHandler's 'failure' relationship" >&2
    warned="true"
  fi
  if [[ " $source_caps " == *" topk "* && " $target_caps " != *" topk "* ]]; then
    echo "  warning: the source supports TopK (TopK-TYPE) keys but the target has no TOPK.* commands - any TopK key found will fail to write and be routed to ModuleTypeHandler's 'failure' relationship" >&2
    warned="true"
  fi
  if [[ " $source_caps " == *" bloom "* && "$dfly_to_dfly" != "true" ]]; then
    echo "  warning: the source supports Bloom filter (MBbloom--) keys, but this tool can only migrate them via the Dragonfly-to-Dragonfly DUMP/RESTORE fast path (both sides Dragonfly, matching version), which this run is not using - any Bloom filter key found will fail to write and be routed to ModuleTypeHandler's 'failure' relationship" >&2
    warned="true"
  fi
  if [[ " $source_caps " == *" cms "* && "$dfly_to_dfly" != "true" ]]; then
    echo "  warning: the source supports Count-Min Sketch (CMSk-TYPE) keys, but this tool can only migrate them via the Dragonfly-to-Dragonfly DUMP/RESTORE fast path (both sides Dragonfly, matching version), which this run is not using - any CMS key found will fail to write and be routed to ModuleTypeHandler's 'failure' relationship" >&2
    warned="true"
  fi
  if [[ "$index_count" =~ ^[0-9]+$ && "$index_count" -gt 0 ]] && [[ " $target_caps " != *" search "* ]]; then
    echo "  warning: the source has $index_count search index(es) but the target has no FT.* commands - the indexed documents themselves still migrate, but SearchIndexRehydrator can't recreate those indexes with FT.CREATE on the target" >&2
    warned="true"
  fi
  if [[ "$warned" == "true" ]]; then
    echo "  note: the warnings above come from pre-flight command-support checks on each server, not from a scan of the actual keys - ignore any of them if the source doesn't really hold keys of that type" >&2
  fi
}
