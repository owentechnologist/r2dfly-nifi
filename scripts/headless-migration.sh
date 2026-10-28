#!/usr/bin/env bash
# One-time copy with no NiFi server or UI: runs r2dfly's flow on NiFi's Stateless engine inside the
# apache/nifi image, and exits when every key has been copied. Settings come from the same TOML
# file simple-migration.sh reads, limited to the keys listed in --help.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/redis-lib.sh"

NIFI_VERSION="2.11.0"
NIFI_IMAGE="${NIFI_IMAGE:-docker.io/apache/nifi:$NIFI_VERSION}"
STATELESS_ZIP="nifi-stateless-assembly-$NIFI_VERSION-bin.zip"
STATELESS_URL="https://repo1.maven.org/maven2/org/apache/nifi/nifi-stateless-assembly/$NIFI_VERSION/$STATELESS_ZIP"
STATELESS_SHA256="b1fca93798d5e05218f41c0ebd16ada4aa0d776ef4846a9df8a435a077285a36"
CACHE_DIR="${R2DFLY_CACHE_DIR:-$HOME/.cache/r2dfly}"
STANDARD_NARS=(nifi-standard-services-api-nar nifi-standard-shared-nar nifi-distributed-cache-services-nar nifi-ssl-context-service-nar)

usage() {
  cat <<USAGE
Usage: $(basename "$0") --toml-file FILE [--work-dir DIR]
       $(basename "$0") --source-connection-string S --target-connection-string T [--work-dir DIR]

Copies the source into the target once, with no NiFi server: the flow runs on NiFi's Stateless
engine in the $NIFI_IMAGE image, and the script exits when the copy is complete. The first run
downloads the engine (about 40 MB, checksum-verified) into $CACHE_DIR.

TOML keys it takes (the same keys as simple-migration.sh; any other key is an error):
  [source] connection-string, connection-mode, require-tls
  [target] connection-string, connection-mode, require-tls
  [migration] key-types, disallow-key-types, topk-mode, prefix-only-list, prefix-deny-list, batch-size
  [transform] rename-prefix, ttl-strategy, ttl-reset-offset-ms
  [run] migration-id

--work-dir holds the run's flow, logs and dead-letter records (default: ./r2dfly-headless-<migration-id>).
A connection string with localhost means the container runtime's host (on macOS, its VM).
Exit status: 0 when the copy is complete; non-zero, with the log's last lines, otherwise.
USAGE
}

TOML_FILE="" WORK_DIR=""
SOURCE_CONNECTION_STRING="" TARGET_CONNECTION_STRING=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --toml-file) TOML_FILE="${2:?--toml-file needs a file}"; shift 2 ;;
    --source-connection-string) SOURCE_CONNECTION_STRING="${2:?}"; shift 2 ;;
    --target-connection-string) TARGET_CONNECTION_STRING="${2:?}"; shift 2 ;;
    --work-dir) WORK_DIR="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument '$1'" >&2; usage >&2; exit 1 ;;
  esac
done

redis_lib_pick_runtime

if [[ -n "$TOML_FILE" ]]; then
  [[ -f "$TOML_FILE" ]] || { echo "error: TOML file '$TOML_FILE' not found" >&2; exit 1; }
  ASSIGNMENTS="$(toolbox_run python3 -c '
import shlex, sys, tomllib
allowed = {
    ("source", "connection-string"): "SOURCE_CONNECTION_STRING", ("source", "connection-mode"): "SOURCE_CONNECTION_MODE",
    ("source", "require-tls"): "SOURCE_REQUIRE_TLS", ("target", "connection-string"): "TARGET_CONNECTION_STRING",
    ("target", "connection-mode"): "TARGET_CONNECTION_MODE", ("target", "require-tls"): "TARGET_REQUIRE_TLS",
    ("migration", "key-types"): "KEY_TYPES", ("migration", "disallow-key-types"): "DISALLOW_KEY_TYPES",
    ("migration", "topk-mode"): "TOPK_MODE", ("migration", "prefix-only-list"): "PREFIX_ONLY_LIST",
    ("migration", "prefix-deny-list"): "PREFIX_DENY_LIST", ("migration", "batch-size"): "BATCH_SIZE",
    ("transform", "rename-prefix"): "RENAME_PREFIX", ("transform", "ttl-strategy"): "TTL_STRATEGY",
    ("transform", "ttl-reset-offset-ms"): "TTL_RESET_OFFSET_MS", ("run", "migration-id"): "MIGRATION_ID",
}
data = tomllib.load(sys.stdin.buffer)
unsupported = []
for section, values in data.items():
    for key, value in (values.items() if isinstance(values, dict) else []):
        if (section, key) not in allowed:
            unsupported.append(f"[{section}] {key}")
            continue
        text = str(value).lower() if isinstance(value, bool) else str(value)
        print(f"{allowed[(section, key)]}={shlex.quote(text)}")
if unsupported:
    print("error: headless-migration.sh does not support " + ", ".join(unsupported) + "; use simple-migration.sh for those", file=sys.stderr)
    sys.exit(1)
' < "$TOML_FILE")" || exit 1
  eval "$ASSIGNMENTS"
fi

[[ -n "$SOURCE_CONNECTION_STRING" && -n "$TARGET_CONNECTION_STRING" ]] || {
  echo "error: a source and a target connection string are needed" >&2; usage >&2; exit 1; }
SOURCE_CONNECTION_MODE="$(tr '[:lower:]' '[:upper:]' <<< "${SOURCE_CONNECTION_MODE:-standalone}")"
TARGET_CONNECTION_MODE="$(tr '[:lower:]' '[:upper:]' <<< "${TARGET_CONNECTION_MODE:-standalone}")"
for mode in "$SOURCE_CONNECTION_MODE" "$TARGET_CONNECTION_MODE"; do
  case "$mode" in STANDALONE|SENTINEL|CLUSTER) ;; *) echo "error: connection-mode must be standalone, sentinel or cluster (got '$mode')" >&2; exit 1 ;; esac
done
[[ "$SOURCE_CONNECTION_STRING" == rediss://* ]] && SOURCE_REQUIRE_TLS=true
[[ "$TARGET_CONNECTION_STRING" == rediss://* ]] && TARGET_REQUIRE_TLS=true
SOURCE_REQUIRE_TLS="${SOURCE_REQUIRE_TLS:-false}" TARGET_REQUIRE_TLS="${TARGET_REQUIRE_TLS:-false}"
KEY_TYPES="${KEY_TYPES:-$DEFAULT_KEY_TYPES}" DISALLOW_KEY_TYPES="${DISALLOW_KEY_TYPES:-}"
TOPK_MODE="${TOPK_MODE:-exact}" DFLY_TO_DFLY=false
redis_lib_resolve_key_types || exit 1
TTL_STRATEGY="$(tr '[:lower:]' '[:upper:]' <<< "${TTL_STRATEGY:-preserve}")"
MIGRATION_ID="${MIGRATION_ID:-headless-$(date +%s)}"
WORK_DIR="${WORK_DIR:-$PWD/r2dfly-headless-$MIGRATION_ID}"
mkdir -p "$WORK_DIR/extensions" "$WORK_DIR/state" "$WORK_DIR/content" "$WORK_DIR/dead-letter"
WORK_DIR="$(cd "$WORK_DIR" && pwd)"

echo "==> the Stateless engine ($NIFI_VERSION)"
STATELESS_HOME="$CACHE_DIR/nifi-stateless-$NIFI_VERSION"
if [[ ! -d "$STATELESS_HOME/lib" ]]; then
  mkdir -p "$CACHE_DIR/download"
  curl -fsSL -o "$CACHE_DIR/download/$STATELESS_ZIP" "$STATELESS_URL"
  actual="$(shasum -a 256 "$CACHE_DIR/download/$STATELESS_ZIP" 2>/dev/null || sha256sum "$CACHE_DIR/download/$STATELESS_ZIP")"
  if [[ "${actual%% *}" != "$STATELESS_SHA256" ]]; then
    echo "error: $STATELESS_ZIP does not match its expected SHA-256; not using it" >&2; exit 1
  fi
  unzip -q -o "$CACHE_DIR/download/$STATELESS_ZIP" -d "$CACHE_DIR"
  rm -f "$CACHE_DIR/download/$STATELESS_ZIP"
fi

echo "==> NARs: r2dfly's, and NiFi's standard ones from $NIFI_IMAGE"
"$PROJECT_ROOT/scripts/deploy-to-nifi.sh" --build-only
cp "$PROJECT_ROOT"/artifacts/nifi-redis-migration-nar/target/nifi-redis-migration-nar-*.nar "$WORK_DIR/extensions/"
NAR_CACHE="$CACHE_DIR/nars-$NIFI_VERSION"
mkdir -p "$NAR_CACHE"
for nar in "${STANDARD_NARS[@]}"; do
  if [[ ! -f "$NAR_CACHE/$nar-$NIFI_VERSION.nar" ]]; then
    holder="$($RUNTIME create "$NIFI_IMAGE")"
    $RUNTIME cp "$holder:/opt/nifi/nifi-current/lib/$nar-$NIFI_VERSION.nar" "$NAR_CACHE/" || { $RUNTIME rm "$holder" >/dev/null; exit 1; }
    $RUNTIME rm "$holder" >/dev/null
  fi
  cp "$NAR_CACHE/$nar-$NIFI_VERSION.nar" "$WORK_DIR/extensions/"
done

echo "==> the flow: $PROJECT_ROOT/artifacts/r2dfly.json, without its search-index processors"
JAVA_HOME_IN_IMAGE="$($RUNTIME run --rm --entrypoint sh "$NIFI_IMAGE" -c 'echo $JAVA_HOME')"
CACHE_PORT=$(( 40000 + RANDOM % 20000 ))
toolbox_run python3 -c '
import json, sys
key_types, prefix_only, prefix_deny, batch_size, rename, ttl_strategy, ttl_offset, src_mode, tgt_mode, src_tls, tgt_tls, cache_port, cacerts = sys.argv[1:]
flow = json.load(sys.stdin)
fc = flow["flowContents"]
dropped = {p["identifier"] for p in fc["processors"] if p["type"].endswith(("SearchIndexExporter", "SearchIndexRehydrator"))}
fc["processors"] = [p for p in fc["processors"] if p["identifier"] not in dropped]
fc["connections"] = [c for c in fc["connections"] if c["source"]["id"] not in dropped and c["destination"]["id"] not in dropped]
flow["parameterContexts"] = {"r2dfly": {"name": "r2dfly", "inheritedParameterContexts": [], "parameters": [
    {"name": "source-url", "sensitive": True}, {"name": "target-url", "sensitive": True}, {"name": "migration-id", "sensitive": False}]}}
fc["parameterContextName"] = "r2dfly"

def opt(value):
    return value or None

ssl = None
if "true" in (src_tls, tgt_tls):
    template = next(s for s in fc["controllerServices"] if s["type"].endswith("MapCacheClientService"))
    ssl = dict(template, identifier="r2dfly-headless-ssl", instanceIdentifier="r2dfly-headless-ssl", name="Headless SSL Context",
               type="org.apache.nifi.ssl.StandardSSLContextService",
               bundle={"group": "org.apache.nifi", "artifact": "nifi-ssl-context-service-nar", "version": template["bundle"]["version"]},
               properties={"Truststore Filename": cacerts, "Truststore Password": "changeit", "Truststore Type": "PKCS12"},
               propertyDescriptors={})
    fc["controllerServices"].append(ssl)
for s in fc["controllerServices"]:
    kind = s["type"].rsplit(".", 1)[-1]
    if kind in ("StandardRedisConnectionPoolService", "StandardDragonflyConnectionPoolService"):
        source = kind == "StandardRedisConnectionPoolService"
        tls = src_tls if source else tgt_tls
        s["properties"].update({"connection-string": "#{source-url}" if source else "#{target-url}",
                                "connection-mode": src_mode if source else tgt_mode, "require-tls": tls,
                                "ssl-context-service": ssl["identifier"] if tls == "true" else None})
    elif kind == "MapCacheServer":
        s["properties"]["Port"] = cache_port
    elif kind == "MapCacheClientService":
        s["properties"].update({"Server Hostname": "localhost", "Server Port": cache_port})
for p in fc["processors"]:
    kind = p["type"].rsplit(".", 1)[-1]
    props = p["properties"]
    if kind == "RedisScanReader":
        props.update({"migration-id": "#{migration-id}", "key-type-filter": key_types, "partition-count": "1",
                      "prefix-only-list": opt(prefix_only), "prefix-deny-list": opt(prefix_deny)})
    elif kind in ("RedisBatchWriter", "ModuleTypeHandler"):
        props.update({"key-prefix-rename": opt(rename), "ttl-strategy": ttl_strategy})
        if ttl_offset:
            props["ttl-reset-offset-ms"] = ttl_offset
        if kind == "RedisBatchWriter" and batch_size:
            props["batch-size"] = batch_size
    elif kind == "RedisDeadLetterWriter":
        props["output-directory"] = "/work/dead-letter"
json.dump(flow, sys.stdout, indent=1)
' "$KEY_TYPES" "${PREFIX_ONLY_LIST:-}" "${PREFIX_DENY_LIST:-}" "${BATCH_SIZE:-}" "${RENAME_PREFIX:-}" "$TTL_STRATEGY" \
  "${TTL_RESET_OFFSET_MS:-}" "$SOURCE_CONNECTION_MODE" "$TARGET_CONNECTION_MODE" "$SOURCE_REQUIRE_TLS" "$TARGET_REQUIRE_TLS" \
  "$CACHE_PORT" "$JAVA_HOME_IN_IMAGE/lib/security/cacerts" \
  < "$PROJECT_ROOT/artifacts/r2dfly.json" > "$WORK_DIR/flow.json"

# Java properties: a backslash in a value has to be doubled.
prop() { printf '%s=%s\n' "$1" "${2//\\/\\\\}"; }
cat > "$WORK_DIR/engine.properties" <<PROPS
nifi.stateless.nar.directory=/stateless/lib
nifi.stateless.extensions.directory=/work/extensions
nifi.stateless.working.directory=/work/state
nifi.stateless.content.repository.directory=/work/content
nifi.stateless.sensitive.props.key=$(LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 32)
PROPS
( umask 077
  { prop nifi.stateless.flow.snapshot.file /work/flow.json
    prop nifi.stateless.flow.name r2dfly-headless
    prop nifi.stateless.parameters.r2dfly r2dfly
    prop nifi.stateless.parameters.r2dfly.source-url "$SOURCE_CONNECTION_STRING"
    prop nifi.stateless.parameters.r2dfly.target-url "$TARGET_CONNECTION_STRING"
    prop nifi.stateless.parameters.r2dfly.migration-id "$MIGRATION_ID"
  } > "$WORK_DIR/flow.properties" )

echo "==> copying (migration id $MIGRATION_ID, key types $KEY_TYPES); log: $WORK_DIR/headless.log"
rc=0
$RUNTIME run --rm --network host --user 0 \
  -v "$STATELESS_HOME":/stateless:ro -v "$PROJECT_ROOT/scripts/headless":/driver:ro -v "$WORK_DIR":/work \
  --entrypoint java "$NIFI_IMAGE" -Dlogback.configurationFile=/stateless/conf/stateless-logback.xml \
  -cp '/stateless/lib/*:/stateless/conf' /driver/HeadlessCopy.java /work/engine.properties /work/flow.properties 1 \
  > "$WORK_DIR/headless.log" 2>&1 || rc=$?
rm -f "$WORK_DIR/flow.properties"

grep '^headless:' "$WORK_DIR/headless.log" || true
DEAD_LETTERS="$(find "$WORK_DIR/dead-letter" -type f | wc -l | tr -d ' ')"
[[ "$DEAD_LETTERS" -gt 0 ]] && echo "  keys NOT copied: $DEAD_LETTERS - one JSON record each, with the reason, in $WORK_DIR/dead-letter"
if [[ "$rc" -ne 0 ]]; then
  echo "error: the headless copy failed (exit $rc); the log's last lines:" >&2
  tail -n 30 "$WORK_DIR/headless.log" >&2
  exit "$rc"
fi
echo "Done."
