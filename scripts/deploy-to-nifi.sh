#!/usr/bin/env bash
# Builds the nifi-redis-migration-nar and deploys it into a local NiFi
# container's NAR auto-load directory, so NiFi picks up the RedisScanReader /
# RedisTypeDeserializer / RedisBatchWriter / etc. processors live, for manual
# testing of the Redis -> Dragonfly migration flow.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
ARTIFACTS_DIR="$PROJECT_ROOT/artifacts"
NAR_MODULE="nifi-redis-migration-nar"
PROCESSORS_MODULE="nifi-redis-migration-processors"

# NiFi version must match the NAR's target NiFi API version (see nifi.version in pom.xml).
NIFI_VERSION="$(grep -m1 '<nifi.version>' "$ARTIFACTS_DIR/pom.xml" | sed -E 's/.*<nifi.version>(.*)<\/nifi.version>.*/\1/')"

# Images are pinned to fully-qualified docker.io (Docker Hub) references - an unqualified
# name like "apache/nifi" can make podman pause with an interactive "which registry did you
# mean?" prompt if the host has more than one registry configured for short names. Qualifying
# it here means that never happens, and it's unambiguous which registry every image comes from.
CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_IMAGE="${NIFI_IMAGE:-docker.io/apache/nifi:${NIFI_VERSION}}"
NIFI_HTTPS_PORT="${NIFI_HTTPS_PORT:-8443}"
SKIP_TESTS="${SKIP_TESTS:-true}"
MAVEN_IMAGE="${MAVEN_IMAGE:-docker.io/library/maven:3.9-eclipse-temurin-21}"
M2_CACHE_VOLUME="${M2_CACHE_VOLUME:-nifi-redis-migration-m2-cache}"
# --verbose shows real pull/build/download output instead of a progress bar (see
# run_with_progress in toolbox-lib.sh). Provenance repository capping (see further down for
# why - it's the confirmed root cause of a real stuck migration on a small-disk host) is on by
# default; --no-disable-provenance restores NiFi's own default (10GB, full lineage/audit
# history) for anyone who actually wants that and has the disk for it. No other flags are
# accepted - every other setting here is env-var driven (see the block above and below).
VERBOSE="${VERBOSE:-false}"
DISABLE_PROVENANCE="${DISABLE_PROVENANCE:-true}"
for arg in "$@"; do
  case "$arg" in
    --verbose) VERBOSE=true ;;
    --disable-provenance) DISABLE_PROVENANCE=true ;;
    --no-disable-provenance) DISABLE_PROVENANCE=false ;;
  esac
done
# Container resource limits (only applied when the container is first created - see below).
# Default: unset, i.e. no cpu/memory limit and NiFi's own default JVM heap.
NIFI_CPUS="${NIFI_CPUS:-}"
NIFI_MEMORY="${NIFI_MEMORY:-}"
NIFI_JVM_HEAP_INIT="${NIFI_JVM_HEAP_INIT:-}"
NIFI_JVM_HEAP_MAX="${NIFI_JVM_HEAP_MAX:-}"

# --- pick a container runtime (docker or podman) ---
if command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
elif command -v podman >/dev/null 2>&1; then
  RUNTIME=podman
else
  echo "error: neither docker nor podman found on PATH" >&2
  exit 1
fi
echo "==> using container runtime: $RUNTIME"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"

# sha256_stdin - sha256sum isn't guaranteed present (e.g. a bare macOS host without coreutils),
# but one of it or shasum always is.
sha256_stdin() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | cut -d' ' -f1
  else
    shasum -a 256 | cut -d' ' -f1
  fi
}

# compute_source_hash - a content hash (not mtime-based - a fresh git checkout, or files
# just touched by a merge, must still hash the same as before if content didn't change) of
# everything that actually affects the built NAR: the reactor root pom (nifi.version etc.),
# the processors module (real Java source) and its pom, and the NAR module's own pom
# (packaging descriptor - it has no src/ of its own). target/ is deliberately excluded, it's
# build OUTPUT, not input - including it would make this hash never match twice.
compute_source_hash() {
  {
    cat "$ARTIFACTS_DIR/pom.xml"
    cat "$ARTIFACTS_DIR/$PROCESSORS_MODULE/pom.xml"
    find "$ARTIFACTS_DIR/$PROCESSORS_MODULE/src" -type f | sort | xargs cat
    cat "$ARTIFACTS_DIR/$NAR_MODULE/pom.xml"
  } | sha256_stdin
}

# --- build the NAR (via a Maven+JDK21 container, so neither is needed on the host) - skipped
#     entirely if the source hasn't changed since the last successful build, so an unmodified
#     re-run of simple-migration.sh/deploy-to-nifi.sh doesn't pay a Maven invocation (and,
#     further down, a container restart) for no reason. ---
SOURCE_HASH="$(compute_source_hash)"
BUILD_HASH_FILE="$ARTIFACTS_DIR/$NAR_MODULE/target/.source-hash"
# `|| true`: under set -e/pipefail, `find` on a target/ dir that doesn't exist yet (a brand-new
# checkout that's never been built here before) fails, and that failure would otherwise abort
# the whole script silently right here - NAR_FILE is meant to just come back empty in that case,
# per the `-n "$NAR_FILE"` check below, so the "else" branch below builds it for the first time.
NAR_FILE="$(find "$ARTIFACTS_DIR/$NAR_MODULE/target" -maxdepth 1 -name '*.nar' 2>/dev/null | head -n1)" || true
if [[ -n "$NAR_FILE" && -f "$BUILD_HASH_FILE" && "$(cat "$BUILD_HASH_FILE")" == "$SOURCE_HASH" ]]; then
  echo "==> source unchanged since last build (hash ${SOURCE_HASH:0:12}) - reusing $(basename "$NAR_FILE"), skipping Maven build"
else
  MVN_ARGS=(-f /workspace/pom.xml -pl "$NAR_MODULE" -am clean package)
  if [[ "$SKIP_TESTS" == "true" ]]; then
    MVN_ARGS+=(-DskipTests)
  fi
  run_with_progress "building $NAR_MODULE (nifi.version=$NIFI_VERSION) via $MAVEN_IMAGE - pulls the image and downloads Maven dependencies on first run" \
    $RUNTIME run --rm \
    -v "$ARTIFACTS_DIR":/workspace:Z \
    -v "$M2_CACHE_VOLUME":/root/.m2 \
    -w /workspace \
    "$MAVEN_IMAGE" mvn "${MVN_ARGS[@]}"

  # `|| true`: see the NAR_FILE lookup above - without it, a build that fails so badly it never
  # even creates target/ would abort silently right here instead of hitting the check below.
  NAR_FILE="$(find "$ARTIFACTS_DIR/$NAR_MODULE/target" -maxdepth 1 -name '*.nar' | head -n1)" || true
  if [[ -z "$NAR_FILE" ]]; then
    echo "error: no .nar file produced under $NAR_MODULE/target" >&2
    exit 1
  fi
  echo "==> built $(basename "$NAR_FILE")"
  echo "$SOURCE_HASH" > "$BUILD_HASH_FILE"
fi

# --- ensure the NiFi container exists and is running ---
CONTAINER_WAS_RUNNING=false
if $RUNTIME ps -a --format '{{.Names}}' | grep -Fqx "$CONTAINER_NAME"; then
  CONTAINER_WAS_RUNNING=true
  if [[ -n "$NIFI_CPUS" || -n "$NIFI_MEMORY" || -n "$NIFI_JVM_HEAP_INIT" || -n "$NIFI_JVM_HEAP_MAX" ]]; then
    echo "note: --cpus/--memory/JVM heap settings only apply when a container is first created; '$CONTAINER_NAME' already exists so its resource limits are unchanged (remove it, or use a different NIFI_CONTAINER_NAME, to apply new limits)" >&2
  fi
  if ! $RUNTIME ps --format '{{.Names}}' | grep -Fqx "$CONTAINER_NAME"; then
    echo "==> starting existing container $CONTAINER_NAME"
    $RUNTIME start "$CONTAINER_NAME"
  else
    echo "==> reusing running container $CONTAINER_NAME"
  fi
else
  echo "==> creating container $CONTAINER_NAME from $NIFI_IMAGE"
  # This image only serves HTTPS. NIFI_WEB_PROXY_HOST allowlists the Host
  # header we'll connect with through the published port; leaving the bind
  # host on its default (the container's own hostname) keeps Jetty listening
  # on the container's real interface so port publishing can reach it.
  # Both localhost and 127.0.0.1 are allowlisted since browsers may send
  # either as the Host header depending on which one you type in the URL.
  #
  # Also allowlist this *host* machine's own hostname/IPs: when NiFi runs on a remote box
  # (e.g. an EC2 instance) and a client maps that hostname to the box's IP in its own
  # /etc/hosts to reach the UI, Jetty rejects the connection with "Invalid SNI"/400 unless
  # that exact hostname is in NIFI_WEB_PROXY_HOST too - it isn't covered by localhost/
  # 127.0.0.1 above, and isn't knowable in advance since it depends on where this script
  # runs. Best-effort and portable: any value this host can't produce (e.g. `hostname -I`
  # on macOS) is silently skipped, never a hard failure.
  EXTRA_PROXY_HOSTS=()
  add_proxy_host() {
    [[ -n "$1" ]] && EXTRA_PROXY_HOSTS+=("$1:${NIFI_HTTPS_PORT}")
  }
  add_proxy_host "$(hostname 2>/dev/null || true)"
  add_proxy_host "$(hostname -f 2>/dev/null || true)"
  for ip in $(hostname -I 2>/dev/null || true); do
    add_proxy_host "$ip"
  done
  NIFI_WEB_PROXY_HOST="localhost:${NIFI_HTTPS_PORT},127.0.0.1:${NIFI_HTTPS_PORT}"
  if [[ ${#EXTRA_PROXY_HOSTS[@]} -gt 0 ]]; then
    # dedupe while preserving order, in case hostname/hostname -f/an IP coincide
    DEDUPED="$(printf '%s\n' "${EXTRA_PROXY_HOSTS[@]}" | awk '!seen[$0]++' | paste -sd, -)"
    NIFI_WEB_PROXY_HOST="${NIFI_WEB_PROXY_HOST},${DEDUPED}"
    echo "==> also allowlisting this host's own hostname/IP for the NiFi UI: $DEDUPED"
  fi
  RUN_ARGS=(-d --name "$CONTAINER_NAME"
    -p "${NIFI_HTTPS_PORT}:8443"
    -e NIFI_WEB_PROXY_HOST="$NIFI_WEB_PROXY_HOST")
  # host.docker.internal resolves out of the box on Docker Desktop, but Docker Engine (Linux)
  # only wires it up when asked - podman provides its own host.containers.internal alias
  # automatically, so this flag is docker-only. See simple-migration.sh's use of this alias.
  [[ "$RUNTIME" == "docker" ]] && RUN_ARGS+=(--add-host "host.docker.internal:host-gateway")
  [[ -n "$NIFI_CPUS" ]] && RUN_ARGS+=(--cpus "$NIFI_CPUS")
  if [[ -n "$NIFI_MEMORY" ]]; then
    RUN_ARGS+=(--memory "$NIFI_MEMORY")
    if [[ -z "$NIFI_JVM_HEAP_MAX" ]]; then
      DERIVED_HEAP_MB="$(derive_heap_mb "$NIFI_MEMORY")" || true
      if [[ -n "${DERIVED_HEAP_MB:-}" ]]; then
        NIFI_JVM_HEAP_MAX="${DERIVED_HEAP_MB}m"
        echo "==> deriving NiFi JVM max heap ${NIFI_JVM_HEAP_MAX} from --memory $NIFI_MEMORY (set NIFI_JVM_HEAP_MAX to override)"
      else
        echo "warning: could not parse NIFI_MEMORY='$NIFI_MEMORY' to derive a JVM heap size; leaving NiFi's default heap in place" >&2
      fi
    fi
  fi
  [[ -n "$NIFI_JVM_HEAP_INIT" ]] && RUN_ARGS+=(-e "NIFI_JVM_HEAP_INIT=$NIFI_JVM_HEAP_INIT")
  [[ -n "$NIFI_JVM_HEAP_MAX" ]] && RUN_ARGS+=(-e "NIFI_JVM_HEAP_MAX=$NIFI_JVM_HEAP_MAX")
  echo "==> container resources: cpus=${NIFI_CPUS:-<none>}  memory=${NIFI_MEMORY:-<none>}  jvm-heap-init=${NIFI_JVM_HEAP_INIT:-<image default>}  jvm-heap-max=${NIFI_JVM_HEAP_MAX:-<image default>}"
  run_with_progress "pulling $NIFI_IMAGE and starting the container" $RUNTIME run "${RUN_ARGS[@]}" "$NIFI_IMAGE"

  echo "==> waiting for initial NiFi startup - first-time startup can take a few minutes (JVM"
  echo "    cold start, generating a self-signed cert, extracting nars). Good time for a quick"
  echo "    round of desk yoga: reach for the sky, roll your shoulders, a gentle seated twist"
  echo "    each side - NiFi should be ready before you are."
  for i in $(seq 1 60); do
    if $RUNTIME logs "$CONTAINER_NAME" 2>&1 | grep -q "Generated Username"; then
      break
    fi
    sleep 5
  done
fi

# --- deploy the NAR via the NAR auto-load directory ---
# Skipped (copy AND restart) if this exact container already has this exact source hash
# deployed and running - checked by container ID, not just name, so a container that got
# removed and recreated under the same name (fresh nar_extensions/, nothing loaded yet) is
# never mistaken for one that already has it. Not container uptime/age: an old container can
# still be running yesterday's code if the source changed after it started, and a
# freshly-started container tells you nothing about whether ITS nar_extensions/ already
# matches current source (see the CONTAINER_WAS_RUNNING="true" - existed already, running or
# not - branch above) - only comparing actual deployed content answers that.
CONTAINER_ID="$($RUNTIME inspect -f '{{.Id}}' "$CONTAINER_NAME")"
DEPLOY_STATE_FILE="/tmp/r2dfly-nar-deployed.${CONTAINER_NAME}"
DEPLOY_NEEDED=true
if [[ "$CONTAINER_WAS_RUNNING" == "true" && -f "$DEPLOY_STATE_FILE" ]]; then
  read -r DEPLOYED_CONTAINER_ID DEPLOYED_SOURCE_HASH < "$DEPLOY_STATE_FILE"
  if [[ "$DEPLOYED_CONTAINER_ID" == "$CONTAINER_ID" && "$DEPLOYED_SOURCE_HASH" == "$SOURCE_HASH" ]]; then
    DEPLOY_NEEDED=false
    echo "==> $CONTAINER_NAME already has this exact NAR (source hash ${SOURCE_HASH:0:12}) loaded and running - skipping copy/restart"
  fi
fi

if [[ "$DEPLOY_NEEDED" == "true" ]]; then
  echo "==> copying NAR into container nar_extensions/"
  $RUNTIME cp "$NAR_FILE" "$CONTAINER_NAME:/opt/nifi/nifi-current/nar_extensions/"

  # NiFi's NAR Auto-Loader only hot-loads a bundle whose group:artifact:version coordinate it
  # hasn't seen before; a rebuilt NAR with the same 1.0.0-SNAPSHOT coordinate is logged as
  # "Found existing bundle with coordinate ..., will not load" and the old code just keeps
  # running (confirmed directly in nifi-app.log) - only a full restart re-extracts it. A
  # brand-new container has nothing loaded yet, so this only matters when reusing one.
  if [[ "$CONTAINER_WAS_RUNNING" == "true" ]]; then
    echo "==> restarting the container so the rebuilt NAR (same SNAPSHOT coordinate) is actually reloaded"
    $RUNTIME restart "$CONTAINER_NAME"
  fi
  echo "$CONTAINER_ID $SOURCE_HASH" > "$DEPLOY_STATE_FILE"
fi

# --- wait for NiFi's REST API to come up (any HTTP response, even 401, means it's serving) ---
# Checked via `exec curl` inside the container itself (against its own hostname - NiFi's web
# listener binds there, not to loopback, see nifi-lib.sh's nifi_lib_init) rather than the
# host-published port, so this needs neither a host curl nor a working port-publish path.
echo "==> waiting for NiFi to become available on https://localhost:${NIFI_HTTPS_PORT}/nifi"
CONTAINER_HOSTNAME="$(runtime_exec "$CONTAINER_NAME" hostname)"
NIFI_READY=false
for i in $(seq 1 60); do
  HTTP_CODE="$(runtime_exec "$CONTAINER_NAME" curl -sk -o /dev/null -w '%{http_code}' "https://${CONTAINER_HOSTNAME}:8443/nifi-api/system-diagnostics" 2>/dev/null || true)"
  if [[ "$HTTP_CODE" =~ ^[0-9]{3}$ ]]; then
    NIFI_READY=true
    break
  fi
  sleep 5
done

if [[ "$NIFI_READY" != "true" ]]; then
  echo "warning: NiFi did not report ready within 5 minutes; check '$RUNTIME logs $CONTAINER_NAME'" >&2
  exit 1
fi

echo "==> NiFi is up: https://localhost:${NIFI_HTTPS_PORT}/nifi"
# `|| true`: under set -e/pipefail, a no-match grep (no credentials block in the logs, e.g. an
# already-existing container whose log driver has since rotated the original lines out) fails
# this assignment and would silently abort the script here - CREDS is meant to just come back
# empty in that case, per the checks below.
CREDS="$($RUNTIME logs "$CONTAINER_NAME" 2>&1 | grep -A1 "Generated Username" | tail -2)" || true
NIFI_USER="$(printf '%s\n' "$CREDS" | sed -n '1s/.*\[\(.*\)\]/\1/p')"
NIFI_PASS="$(printf '%s\n' "$CREDS" | sed -n '2s/.*\[\(.*\)\]/\1/p')"
if [[ -n "$NIFI_USER" && -n "$NIFI_PASS" ]]; then
  echo "==> single-user login credentials (only shown on first container creation):"
  echo "$CREDS"
  # Saved inside the container itself, not this project's workspace - these are the container's
  # own generated secrets, not project state. Needed because the container's log driver
  # eventually rotates this line out of `$RUNTIME logs` entirely (confirmed directly: a
  # heavily-used migration container no longer had "Generated Username" anywhere in its logs
  # after enough log volume accumulated), after which every script that re-derives credentials
  # this same way fails outright, with no way to recover them short of recreating the container.
  nifi_lib_save_state "$CONTAINER_NAME" "$NIFI_CREDS_FILE_PATH" NIFI_USER NIFI_PASS
elif nifi_lib_load_state "$CONTAINER_NAME" "$NIFI_CREDS_FILE_PATH" && [[ -n "${NIFI_USER:-}" && -n "${NIFI_PASS:-}" ]]; then
  echo "==> recovered the single-user login credentials saved when this container was first created (no longer present in its logs)"
else
  echo "warning: could not find NiFi credentials - neither in the container's logs (only printed once, on first creation) nor previously saved for it" >&2
fi
echo "==> the NAR is placed in nar_extensions/ and reloaded (via restart, if the container was already running); check the container logs if the new processors don't appear in the Add Processor dialog yet."

# --- cap (default) or restore NiFi's provenance repository (--no-disable-provenance) ---
# NiFi's provenance repository logs fine-grained per-FlowFile lineage events (CREATE,
# ATTRIBUTES_MODIFIED, etc.) - its disk usage grows with FlowFile *count*, independent of
# whether the migration itself is making progress. Confirmed directly as the root cause of a
# real stuck migration: on a small (7.6GB) host, the default nifi.properties cap
# (nifi.provenance.repository.max.storage.size=10 GB - itself already bigger than that whole
# disk) let the provenance repository's Lucene index grow until the disk filled entirely, after
# which EVERY write path in NiFi (provenance indexing, FlowFile repo checkpointing, and
# eventually RedisBatchWriter's actual writes to the target) started failing with
# "IOException: No space left on device" in a loop - the target sat at 0 keys/0 bytes written
# the whole time, with no clearer error surfaced anywhere else. Capped by default for exactly
# this reason; pass --no-disable-provenance to restore full lineage/audit history if you
# actually want it and have the disk for it.
#
# Kept as the same WriteAheadProvenanceRepository implementation (not a NoOp one) with its own
# storage/rollover caps shrunk to a small fixed footprint, rather than switching implementation
# class - verified directly that this NiFi version's nifi-provenance-repository-nar only
# bundles WriteAheadProvenanceRepository, no NoOp alternative, so pointing nifi.properties at an
# unverified class name risks NiFi failing to start entirely instead of just capping disk use.
PROVENANCE_CAP_SIZE="8 MB"
PROVENANCE_ROLLOVER_SIZE="1 MB"
PROVENANCE_MAX_STORAGE_TIME="5 mins"
# NiFi's own shipped defaults (see nifi.properties) - restored by --no-disable-provenance on a
# container a previous default-on run already capped; otherwise this project would have no way
# back to full tracking short of recreating the container from scratch.
PROVENANCE_DEFAULT_SIZE="10 GB"
PROVENANCE_DEFAULT_ROLLOVER_SIZE="100 MB"
PROVENANCE_DEFAULT_MAX_STORAGE_TIME="30 days"

# apply_provenance_settings <label> <max-storage-size> <rollover-size> <max-storage-time> -
# writes the three properties and restarts+waits, shared by both the cap and restore paths
# below so that logic (and its 5-minute readiness timeout) exists exactly once.
apply_provenance_settings() {
  local label="$1" size="$2" rollover="$3" storage_time="$4"
  echo "==> $label: max.storage.size=$size, rollover.size=$rollover, max.storage.time=$storage_time"
  runtime_exec "$CONTAINER_NAME" sed -i \
    -e "s/^nifi.provenance.repository.max.storage.size=.*/nifi.provenance.repository.max.storage.size=$size/" \
    -e "s/^nifi.provenance.repository.rollover.size=.*/nifi.provenance.repository.rollover.size=$rollover/" \
    -e "s/^nifi.provenance.repository.max.storage.time=.*/nifi.provenance.repository.max.storage.time=$storage_time/" \
    /opt/nifi/nifi-current/conf/nifi.properties > /dev/null
  echo "==> restarting the container to apply the provenance repository change"
  $RUNTIME restart "$CONTAINER_NAME"
  echo "==> waiting for NiFi to come back up"
  NIFI_READY=false
  for i in $(seq 1 60); do
    HTTP_CODE="$(runtime_exec "$CONTAINER_NAME" curl -sk -o /dev/null -w '%{http_code}' "https://${CONTAINER_HOSTNAME}:8443/nifi-api/system-diagnostics" 2>/dev/null || true)"
    if [[ "$HTTP_CODE" =~ ^[0-9]{3}$ ]]; then
      NIFI_READY=true
      break
    fi
    sleep 5
  done
  if [[ "$NIFI_READY" != "true" ]]; then
    echo "warning: NiFi did not come back up within 5 minutes after the provenance-repository restart; check '$RUNTIME logs $CONTAINER_NAME'" >&2
    exit 1
  fi
  echo "==> NiFi is back up"
}

CURRENT_PROVENANCE_CAP="$(runtime_exec "$CONTAINER_NAME" sed -n 's/^nifi.provenance.repository.max.storage.size=//p' /opt/nifi/nifi-current/conf/nifi.properties)"
if [[ "$DISABLE_PROVENANCE" == "true" ]]; then
  if [[ "$CURRENT_PROVENANCE_CAP" == "$PROVENANCE_CAP_SIZE" ]]; then
    echo "==> provenance repository already capped at $PROVENANCE_CAP_SIZE - skipping"
  else
    apply_provenance_settings "capping NiFi's provenance repository (default on; pass --no-disable-provenance to restore full tracking)" \
      "$PROVENANCE_CAP_SIZE" "$PROVENANCE_ROLLOVER_SIZE" "$PROVENANCE_MAX_STORAGE_TIME"
  fi
else
  if [[ "$CURRENT_PROVENANCE_CAP" == "$PROVENANCE_DEFAULT_SIZE" ]]; then
    echo "==> provenance repository already at NiFi's own defaults - skipping"
  else
    apply_provenance_settings "--no-disable-provenance: restoring NiFi's own provenance repository defaults" \
      "$PROVENANCE_DEFAULT_SIZE" "$PROVENANCE_DEFAULT_ROLLOVER_SIZE" "$PROVENANCE_DEFAULT_MAX_STORAGE_TIME"
  fi
fi

# --- auto-import the r2dfly.json baseline flow via the NiFi CLI, so the user only has to
#     configure the two connection-pool controller services (source/target), never build the
#     flow by hand. Skipped if an R2Dfly Migration group already exists (re-running this script
#     must never clobber a user's in-progress configuration). ---
R2DFLY_TEMPLATE="$ARTIFACTS_DIR/r2dfly.json"

if [[ ! -f "$R2DFLY_TEMPLATE" ]]; then
  echo "==> no r2dfly.json found at $R2DFLY_TEMPLATE; skipping baseline flow import"
  exit 0
fi

echo "==> checking whether the R2Dfly Migration flow is already loaded"
# NIFI_USER/NIFI_PASS were already resolved above, from the container's logs or from the copy
# saved inside the container when they were still in its logs.
if [[ -z "${NIFI_USER:-}" || -z "${NIFI_PASS:-}" ]]; then
  echo "warning: no NiFi credentials available; skipping automatic flow import. Import $R2DFLY_TEMPLATE by hand via 'nifi pg-import -i r2dfly.json' once you have credentials." >&2
  exit 0
fi

nifi_lib_init

# `|| true`: under set -e/pipefail, a transient NiFi API hiccup would otherwise abort the script
# silently right here, before the deliberate error check below can run.
ROOT_ID="$(nifi_cli get-root-id -ot simple | tr -d '[:space:]')" || true
if [[ -z "$ROOT_ID" ]]; then
  echo "error: could not reach NiFi's REST API to determine the root process group id (check NiFi container health/logs)" >&2
  exit 1
fi
EXISTING="$(nifi_cli pg-list -pgid "$ROOT_ID" -ot simple 2>/dev/null | grep -c 'R2Dfly Migration' || true)"

if [[ "$EXISTING" -gt 0 ]]; then
  echo "==> R2Dfly Migration flow already present; leaving it untouched (re-run does not overwrite your configured properties)"
  exit 0
fi

echo "==> importing baseline flow from r2dfly.json"
$RUNTIME cp "$R2DFLY_TEMPLATE" "$CONTAINER_NAME:/tmp/r2dfly.json"
# `|| true` on both: under set -e/pipefail, a failure here (or a UUID-less pg-import output)
# would otherwise abort the script before the deliberate "-z NEW_PG_ID" warning below can run.
IMPORT_OUTPUT="$(nifi_cli pg-import -i /tmp/r2dfly.json -ot simple)" || true
echo "$IMPORT_OUTPUT"
NEW_PG_ID="$(echo "$IMPORT_OUTPUT" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)" || true

if [[ -z "$NEW_PG_ID" ]]; then
  echo "warning: could not determine the imported process group's id from pg-import output; enable its controller services by hand" >&2
  exit 0
fi

echo "==> enabling controller services in the imported flow"
# The two connection-pool services are expected to stay DISABLED/INVALID here - their
# Connection String is deliberately left blank in the template, so NiFi will always refuse to
# enable them until the user sets it. pg-enable-services reports that as an error even though
# the other two (cursor-cache) services enable fine, so don't let it abort the script.
nifi_cli pg-enable-services -pgid "$NEW_PG_ID" || true

echo "==> R2Dfly Migration flow imported."
echo "==> Next: open the canvas, set the 'Connection String' property on the 'Source Redis Pool' and"
echo "    'Target Dragonfly Pool' controller services (the only two required properties left blank),"
echo "    enable those two services, then start the flow."
