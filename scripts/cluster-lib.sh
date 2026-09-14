#!/usr/bin/env bash
# Pre-flight topology health check + cluster-aware DBSIZE reconciliation for a Redis/Valkey/
# Dragonfly Cluster acting as a migration source or target. Sourced by simple-migration.sh and
# run-r2dfly.sh alongside redis-lib.sh - reuses redis_cli's existing container/toolbox
# fallback and the py3 helper (both from redis-lib.sh/toolbox-lib.sh), no new dependencies.
# Callers only invoke these when a side's connection-mode is "cluster" - a no-op otherwise.
#
# Deliberately does NOT attempt to provision or fix a cluster's slot layout - a target
# cluster's slots are assumed already assigned externally (DragonflyDB Cloud, or a manual
# DFLYCLUSTER CONFIG push). This only verifies that's already true before allowing a
# migration to start.

# cluster_lib_node_connstr <template-connstr> <host> <port> - builds a single-node connection
# string for <host>:<port>, preserving the scheme and [:password@] userinfo from the first
# node of <template-connstr> (a cluster's nodes share the same auth/TLS config).
cluster_lib_node_connstr() {
  local template="$1" host="$2" port="$3"
  local scheme rest first userinfo=""
  scheme="${template%%://*}"
  rest="${template#*://}"
  first="${rest%%,*}"
  if [[ "$first" == *@* ]]; then
    userinfo="${first%%@*}@"
  fi
  echo "${scheme}://${userinfo}${host}:${port}"
}

# _cluster_lib_topology <container> <connstr> - runs CLUSTER NODES against the first seed node
# (any one node reports the whole cluster's topology) and parses it into a simple tab-separated
# report:
#   STATUS\tOK                             - fully slot-covered, no failed/mid-migration nodes
#   STATUS\tFAIL\t<reason>                  - otherwise, with a specific reason
#   MASTER\t<host>\t<port>\t<slot-ranges>   - one line per healthy master (only ever present
#                                             alongside STATUS OK)
# Uses CLUSTER NODES (simple line-based text) rather than CLUSTER SHARDS, whose nested RESP3
# reply is awkward to parse from redis-cli's plain-text output.
_cluster_lib_topology() {
  local container="$1" connstr="$2" nodes
  # A separate statement is load-bearing: bash evaluates every value expression in a single
  # `local a=$1 b=$a` statement against the OUTER scope, not sequentially - `first` computed
  # inline here would silently see connstr as still empty/unset, not the value just assigned
  # on this same line (confirmed directly: `local a="$1" b="${a}_x"` gives b="_x", not
  # "${1}_x"). Broke only the toolbox fallback path (used whenever no locally-named container
  # matches, e.g. any real remote/cloud cluster) since the exec-into-local-container fast path
  # never actually uses $first/$connstr at all.
  local first="${connstr%%,*}"
  # `|| true`: under set -e/pipefail, a connection failure here would otherwise abort the whole
  # script silently, before the deliberate "returned nothing" check right below can run.
  nodes="$(redis_cli "$container" "$first" CLUSTER NODES 2>/dev/null | tr -d '\r')" || true
  if [[ -z "$nodes" ]]; then
    echo -e "STATUS\tFAIL\tCLUSTER NODES returned nothing - is this really a cluster-mode node?"
    return
  fi
  printf '%s\n' "$nodes" | py3 "
import sys

lines = [l for l in sys.stdin.read().splitlines() if l.strip()]
masters = []
bad = []
covered = [False] * 16384

for line in lines:
    fields = line.split(' ')
    if len(fields) < 8:
        continue
    addr, flags = fields[1], fields[2]
    flagset = set(flags.split(','))
    hostport = addr.split('@')[0]
    if ',' in hostport:
        hostport = hostport.split(',')[0]
    host, _, port = hostport.rpartition(':')
    if 'fail' in flagset or 'fail?' in flagset:
        bad.append(f'{host}:{port} flagged {flags}')
        continue
    if 'handshake' in flagset or 'noaddr' in flagset or 'master' not in flagset:
        continue
    ranges = []
    for tok in fields[8:]:
        if tok.startswith('['):
            bad.append(f'{host}:{port} has a slot mid-migration: {tok}')
            continue
        s, _, e = tok.partition('-')
        s = int(s)
        e = int(e) if e else s
        ranges.append((s, e))
        for slot in range(s, e + 1):
            covered[slot] = True
    masters.append((host, port, ranges))

gaps = []
start = None
for slot in range(16384):
    if not covered[slot]:
        if start is None:
            start = slot
    elif start is not None:
        gaps.append((start, slot - 1))
        start = None
if start is not None:
    gaps.append((start, 16383))

if bad:
    print('STATUS\tFAIL\t' + '; '.join(bad))
elif gaps:
    print('STATUS\tFAIL\tslot coverage gaps: ' + ', '.join(f'{a}-{b}' for a, b in gaps))
elif not masters:
    print('STATUS\tFAIL\tno healthy master nodes found')
else:
    print('STATUS\tOK')
    for host, port, ranges in masters:
        print(f'MASTER\t{host}\t{port}\t' + (','.join(f'{a}-{b}' for a, b in ranges) or '(none)'))
"
}

# cluster_lib_verify <label> <container> <connstr> - <container> is only used for the initial
# topology-discovery query against the first seed node; each subsequent per-master DBSIZE call
# uses that master's own hostname as its container name guess, since a cluster's masters live
# in distinct containers. Full pre-flight check: CLUSTER INFO must
# report cluster_state:ok, then _cluster_lib_topology must report STATUS OK (full slot
# coverage, no failed/mid-migration nodes). On success, prints a per-master node/slot/key
# summary to stderr and the total key count across all masters to stdout (for the caller to
# capture); on failure, prints the specific reason to stderr and returns 1 without printing
# anything to stdout - callers running under `set -e` should capture this with a plain
# `VAR="$(cluster_lib_verify ...)"`, which aborts the script on that nonzero exit.
cluster_lib_verify() {
  local label="$1" container="$2" connstr="$3"
  # See _cluster_lib_topology's comment above - `first` must be its own statement, not part
  # of the same `local` line that assigns `connstr`.
  local first="${connstr%%,*}"

  local info
  # `|| true`: see _cluster_lib_topology's nodes= lookup above - without it, a connection
  # failure would abort the script here instead of hitting the deliberate check below.
  info="$(redis_cli "$container" "$first" CLUSTER INFO 2>/dev/null | tr -d '\r')" || true
  if [[ "$info" != *"cluster_state:ok"* ]]; then
    echo "error: $label cluster_state is not 'ok' - refusing to proceed. CLUSTER INFO:" >&2
    echo "$info" >&2
    return 1
  fi

  local report topology_status
  report="$(_cluster_lib_topology "$container" "$connstr")"
  topology_status="$(echo "$report" | awk -F'\t' '$1=="STATUS"{print $2; exit}')"
  if [[ "$topology_status" != "OK" ]]; then
    echo "error: $label cluster topology check failed: $(echo "$report" | awk -F'\t' '$1=="STATUS"{print $3; exit}')" >&2
    return 1
  fi

  echo "  $label cluster: cluster_state=ok, slot coverage complete (0-16383)" >&2
  local total=0 tag host port ranges size node_connstr
  while IFS=$'\t' read -r tag host port ranges; do
    [[ "$tag" == "MASTER" ]] || continue
    node_connstr="$(cluster_lib_node_connstr "$first" "$host" "$port")"
    # Each node's own hostname is used as its container name guess for the exec fallback
    # (same convention as SOURCE_CONTAINER/TARGET_CONTAINER elsewhere) - the caller-supplied
    # $container only applies to the single topology-discovery query above, since a cluster's
    # masters live in distinct containers, not all under one name.
    # </dev/null is load-bearing: `podman/docker exec -i` passes through stdin, and without
    # this, it would inherit and drain the remaining lines of this loop's own here-string
    # (see the `done <<< ...` below) as its own stdin - starving the next `read` and silently
    # truncating the loop to one iteration. Found by testing against a real 3-master cluster.
    # `|| true`: a per-node connection blip would otherwise abort this whole pre-flight check
    # under set -e/pipefail - the very next line already falls back to size=0 for exactly this
    # case, it just needs to actually be reached.
    size="$(redis_cli "$host" "$node_connstr" DBSIZE 2>/dev/null </dev/null | tr -d '\r')" || true
    [[ "$size" =~ ^[0-9]+$ ]] || size=0
    echo "    $host:$port  slots=$ranges  keys=$size" >&2
    total=$((total + size))
  done <<< "$(grep '^MASTER' <<< "$report")"
  echo "  $label total keys across masters: $total" >&2
  echo "$total"
}

# cluster_lib_dbsize_sum_nodes <node-list> - sums DBSIZE across an already-known set of master
# nodes, in the "host\tport\tnode-connstr" format cluster_lib_each_master produces. Split out of
# cluster_lib_dbsize_sum so a caller that polls DBSIZE repeatedly (e.g. run-r2dfly.sh's
# poll_until_synced, once every few seconds for the whole migration) can fetch the node list via
# cluster_lib_each_master ONCE up front and reuse it, instead of re-running CLUSTER NODES (a
# full extra round trip, on top of the per-node DBSIZE calls) on every single tick for a
# topology that essentially never changes mid-migration.
cluster_lib_dbsize_sum_nodes() {
  local node_list="$1" total=0 host port node_connstr size
  while IFS=$'\t' read -r host port node_connstr; do
    [[ -n "$host" ]] || continue
    # </dev/null is load-bearing: `podman/docker exec -i` passes through stdin, and without
    # this, it would inherit and drain the remaining lines of this loop's own here-string
    # (see the `done <<< ...` below) as its own stdin - starving the next `read` and silently
    # truncating the loop to one iteration. Found by testing against a real 3-master cluster.
    # `|| true`: a per-node connection blip would otherwise abort this whole progress-poll under
    # set -e/pipefail instead of falling back to size=0 on the very next line, as intended.
    size="$(redis_cli "$host" "$node_connstr" DBSIZE 2>/dev/null </dev/null | tr -d '\r')" || true
    [[ "$size" =~ ^[0-9]+$ ]] || size=0
    total=$((total + size))
  done <<< "$node_list"
  echo "$total"
}

# cluster_lib_dbsize_sum <container> <connstr> - lightweight cluster-aware DBSIZE, for a one-off
# check (or a caller that can't cache the node list itself). Sums DBSIZE across all current
# master nodes; does not re-check topology health. A caller polling repeatedly should instead
# call cluster_lib_each_master once and pass its result to cluster_lib_dbsize_sum_nodes on each
# tick - see that function's comment for why.
cluster_lib_dbsize_sum() {
  local container="$1" connstr="$2"
  cluster_lib_dbsize_sum_nodes "$(cluster_lib_each_master "$container" "$connstr")"
}

# cluster_lib_each_master <container> <connstr> - prints one "host\tport\tnode-connstr" line
# per healthy master node (topology via _cluster_lib_topology). For any caller that must run a
# command directly against every shard individually rather than through the cluster's shared
# endpoint - most importantly SCAN (a plain SCAN, or one issued from inside an EVAL, only ever
# iterates whichever single node the connection happens to land on, never the whole cluster -
# see cluster_lib_dbsize_sum's own comment, discovered the same way). Does not re-verify
# topology health itself - callers that need that guarantee should call cluster_lib_verify
# first. `</dev/null` on the topology query below matters for the same reason it does in
# cluster_lib_verify/cluster_lib_dbsize_sum's own per-node loops: this function's own caller
# may itself be reading from a here-string in an enclosing loop, and CLUSTER NODES here runs
# through the same `exec -i` mechanism that would otherwise drain it.
cluster_lib_each_master() {
  local container="$1" connstr="$2" report
  local first="${connstr%%,*}"
  report="$(_cluster_lib_topology "$container" "$connstr")"
  local tag host port ranges
  while IFS=$'\t' read -r tag host port ranges; do
    [[ "$tag" == "MASTER" ]] || continue
    printf '%s\t%s\t%s\n' "$host" "$port" "$(cluster_lib_node_connstr "$first" "$host" "$port")"
  done <<< "$(grep '^MASTER' <<< "$report")"
}

