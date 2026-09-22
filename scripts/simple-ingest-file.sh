#!/usr/bin/env bash
# Reusable file->Dragonfly ingestion: builds a NiFi flow via the REST API that reads a delimited
# file - from the local/container filesystem or an S3-compatible bucket - and writes each record
# as a Hash (or JSON document) into Dragonfly, keyed off one primary-key field. Generalizes
# ingest-file-demo.sh (which ingests one hardcoded generated dataset) into a config-driven tool
# for ingesting a real file: everything dataset-specific (source location/type, primary key,
# target type, key prefix) comes from a TOML config instead of being hardcoded. Self-contained
# and idempotent the same way ingest-file-demo.sh is: re-running finds and reuses whatever
# components it already created, and RedisBatchWriter's default OVERWRITE conflict strategy means
# a re-run just rewrites the same keys rather than piling up more.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"
source "$PROJECT_ROOT/scripts/redis-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") --toml-file FILE [options]

Builds a NiFi flow from scratch through the REST API that reads a delimited file - local/container
filesystem or an S3-compatible bucket - and writes each record into Dragonfly as a Hash or JSON
document. All dataset-specific settings (source location, primary key, target type, key prefix)
come from the TOML config; see scripts/config/simple-ingest-file-example.toml for the schema.

  --toml-file FILE              required. Looked for as given, else under scripts/config/

NiFi connection/auth (omit all three to auto-read the container's auto-generated single-user
credentials, the same fallback simple-migration.sh uses). Also settable via [nifi] in the TOML:
  --nifi-container NAME        container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER             NiFi single-user login username (env NIFI_USER)
  --nifi-password PASSWORD     NiFi single-user login password (env NIFI_PASS)
  --nifi-token TOKEN           use an existing bearer token instead of user/password (env NIFI_TOKEN)

Dragonfly connection. Also settable via [dragonfly] in the TOML:
  --dragonfly-container NAME   container to verify results against via redis-cli (env DRAGONFLY_CONTAINER,
                                default: target-dragonfly)
  --dragonfly-connection-string S
                                connection string the NiFi processors use, resolved from INSIDE the NiFi
                                container's own network (env DRAGONFLY_CONNECTION_STRING,
                                default: redis://target-dragonfly:6379). A rediss:// scheme auto-enables
                                TLS (an SSL Context Service is provisioned automatically, trusting the
                                NiFi container's own JVM default truststore)

  -h, --help                   this help
EOF
}

# require_value <flag> <remaining-args...> - clear error for a flag missing its value instead of
# a raw "$2: unbound variable" from set -u.
require_value() {
  if [[ $# -lt 2 ]]; then
    echo "error: $1 requires a value" >&2; usage; exit 1
  fi
}

# resolve_toml_path <file> - <file> as given first (relative to cwd, or absolute), else
# scripts/config/<file>, matching simple-migration.sh's own convention.
resolve_toml_path() {
  local f="$1"
  if [[ -f "$f" ]]; then echo "$f"; return 0; fi
  if [[ -f "$PROJECT_ROOT/scripts/config/$f" ]]; then echo "$PROJECT_ROOT/scripts/config/$f"; return 0; fi
  return 1
}

# load_toml_config <path> - parses the TOML via the toolbox image's python3 (tomllib built in,
# see simple-migration.sh's own load_toml_config for the verified base image details) and eval's
# the settings as this script's own variables. Unknown sections/keys are warned about, not
# silently ignored - almost certainly a typo.
load_toml_config() {
  local path="$1" out
  if ! out="$(toolbox_run python3 -c "$(cat <<'PYEOF'
import sys, tomllib, shlex

MAPPING = {
    ("source", "type"): "SOURCE_TYPE",
    ("source", "path"): "SOURCE_PATH",
    ("source", "bucket"): "SOURCE_BUCKET",
    ("source", "endpoint-override"): "SOURCE_ENDPOINT_OVERRIDE",
    ("source", "region"): "SOURCE_REGION",
    ("source", "access-key-id"): "SOURCE_ACCESS_KEY_ID",
    ("source", "secret-access-key"): "SOURCE_SECRET_ACCESS_KEY",
    ("source", "path-style-access"): "SOURCE_PATH_STYLE_ACCESS",
    ("source", "file-filter"): "SOURCE_FILE_FILTER",
    ("source", "file-type"): "SOURCE_FILE_TYPE",
    ("source", "delimiter"): "SOURCE_DELIMITER",
    ("record", "primary-key"): "RECORD_PRIMARY_KEY",
    ("record", "target-type"): "RECORD_TARGET_TYPE",
    ("record", "key-prefix"): "RECORD_KEY_PREFIX",
    ("dragonfly", "connection-string"): "DRAGONFLY_CONNECTION_STRING",
    ("dragonfly", "container"): "DRAGONFLY_CONTAINER",
    ("nifi", "container"): "NIFI_CONTAINER_NAME",
    ("nifi", "user"): "NIFI_USER",
    ("nifi", "password"): "NIFI_PASS",
}

try:
    data = tomllib.load(sys.stdin.buffer)
except tomllib.TOMLDecodeError as e:
    print(f"invalid TOML: {e}", file=sys.stderr)
    sys.exit(1)

for (section, key), varname in MAPPING.items():
    section_data = data.get(section)
    if not isinstance(section_data, dict) or key not in section_data:
        continue
    value = section_data[key]
    value = "true" if value is True else "false" if value is False else str(value)
    print(f"{varname}={shlex.quote(value)}")

known_keys_by_section = {}
for section, key in MAPPING:
    known_keys_by_section.setdefault(section, set()).add(key)
for section, contents in data.items():
    if section not in known_keys_by_section:
        print(f"warning: unrecognized TOML section [{section}] in config - ignored", file=sys.stderr)
        continue
    if not isinstance(contents, dict):
        continue
    for key in contents:
        if key not in known_keys_by_section[section]:
            print(f"warning: unrecognized TOML key '{key}' in [{section}] - ignored", file=sys.stderr)
PYEOF
)" < "$path")"; then
    echo "error: failed to parse TOML config '$path'" >&2
    exit 1
  fi
  eval "$out"
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"
DRAGONFLY_CONTAINER="${DRAGONFLY_CONTAINER:-target-dragonfly}"
DRAGONFLY_CONNECTION_STRING="${DRAGONFLY_CONNECTION_STRING:-redis://target-dragonfly:6379}"
SOURCE_TYPE=""
SOURCE_PATH=""
SOURCE_BUCKET=""
SOURCE_ENDPOINT_OVERRIDE=""
SOURCE_REGION="us-east-1"
SOURCE_ACCESS_KEY_ID=""
SOURCE_SECRET_ACCESS_KEY=""
SOURCE_PATH_STYLE_ACCESS="true"
SOURCE_FILE_FILTER=""
SOURCE_FILE_TYPE=""
SOURCE_DELIMITER=""
RECORD_PRIMARY_KEY=""
RECORD_TARGET_TYPE="hash"
RECORD_KEY_PREFIX=""
TOML_FILE=""

# --toml-file is handled as its own pre-pass, same as simple-migration.sh's --toml-file: its
# settings become this script's baseline BEFORE the normal flag loop, so any flag given on the
# command line overrides it regardless of ordering.
ARGS=("$@")
FILTERED_ARGS=()
i=0
while [[ $i -lt ${#ARGS[@]} ]]; do
  if [[ "${ARGS[$i]}" == "--toml-file" ]]; then
    if [[ $((i + 1)) -ge ${#ARGS[@]} ]]; then
      echo "error: --toml-file requires a value" >&2; usage; exit 1
    fi
    TOML_FILE="${ARGS[$((i + 1))]}"
    i=$((i + 2))
    continue
  fi
  if [[ "${ARGS[$i]}" == "-h" || "${ARGS[$i]}" == "--help" ]]; then
    usage; exit 0
  fi
  FILTERED_ARGS+=("${ARGS[$i]}")
  i=$((i + 1))
done
if [[ ${#FILTERED_ARGS[@]} -gt 0 ]]; then set -- "${FILTERED_ARGS[@]}"; else set --; fi

if [[ -z "$TOML_FILE" ]]; then
  echo "error: --toml-file is required" >&2; usage; exit 1
fi
TOML_PATH="$(resolve_toml_path "$TOML_FILE")" || {
  echo "error: TOML config '$TOML_FILE' not found (looked for it as given, and under $PROJECT_ROOT/scripts/config/)" >&2
  exit 1
}
echo "==> loading settings from $TOML_PATH"
nifi_lib_pick_runtime
load_toml_config "$TOML_PATH"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) require_value "$@"; CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) require_value "$@"; NIFI_USER="$2"; shift 2 ;;
    --nifi-password) require_value "$@"; NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) require_value "$@"; NIFI_TOKEN="$2"; shift 2 ;;
    --dragonfly-container) require_value "$@"; DRAGONFLY_CONTAINER="$2"; shift 2 ;;
    --dragonfly-connection-string) require_value "$@"; DRAGONFLY_CONNECTION_STRING="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

# --- Validate the dataset-specific settings the TOML was responsible for. ---
case "$SOURCE_TYPE" in
  local|s3) ;;
  *) echo "error: [source].type must be 'local' or 's3' (got '${SOURCE_TYPE:-<unset>}')" >&2; exit 1 ;;
esac
[[ -n "$SOURCE_PATH" ]] || { echo "error: [source].path is required" >&2; exit 1; }
case "$SOURCE_FILE_TYPE" in
  tsv|csv) ;;
  custom) [[ -n "$SOURCE_DELIMITER" ]] || { echo "error: [source].file-type = \"custom\" requires [source].delimiter" >&2; exit 1; } ;;
  *) echo "error: [source].file-type must be tsv, csv, or custom (got '${SOURCE_FILE_TYPE:-<unset>}')" >&2; exit 1 ;;
esac
if [[ "$SOURCE_TYPE" == "s3" ]]; then
  [[ -n "$SOURCE_BUCKET" ]] || { echo "error: [source].bucket is required when [source].type = \"s3\"" >&2; exit 1; }
  [[ -n "$SOURCE_ACCESS_KEY_ID" && -n "$SOURCE_SECRET_ACCESS_KEY" ]] || {
    echo "error: [source].access-key-id and [source].secret-access-key are required when [source].type = \"s3\"" >&2; exit 1; }
fi
[[ -n "$RECORD_PRIMARY_KEY" ]] || { echo "error: [record].primary-key is required" >&2; exit 1; }
case "$RECORD_TARGET_TYPE" in
  hash|json) ;;
  *) echo "error: [record].target-type must be 'hash' or 'json' (got '$RECORD_TARGET_TYPE')" >&2; exit 1 ;;
esac
[[ -n "$RECORD_KEY_PREFIX" ]] || { echo "error: [record].key-prefix is required" >&2; exit 1; }
if [[ "$SOURCE_TYPE" == "local" ]]; then
  [[ -e "$SOURCE_PATH" ]] || { echo "error: [source].path '$SOURCE_PATH' does not exist on the host" >&2; exit 1; }
fi

# TLS is auto-detected from the connection string's own scheme (rediss:// vs redis://) rather
# than a separate flag - the same signal Lettuce's RedisURI uses.
DRAGONFLY_REQUIRE_TLS="false"
[[ "$DRAGONFLY_CONNECTION_STRING" == rediss://* ]] && DRAGONFLY_REQUIRE_TLS="true"

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
RUN_LOG="$LOG_DIR/simple-ingest-file-$(date +%s).log"
exec > >(tee -a "$RUN_LOG") 2>&1
echo "==> logging this run to $RUN_LOG"

# Standard-component bundles are read from pom.xml (same lookup deploy-to-nifi.sh already does)
# rather than hardcoded, so a NiFi upgrade doesn't leave this script creating processors against a
# stale version string. The custom NAR's own bundle coordinate is fixed, same literal used
# throughout this project.
NIFI_VERSION="$(grep -m1 '<nifi.version>' "$PROJECT_ROOT/artifacts/pom.xml" | sed -E 's/.*<nifi.version>(.*)<\/nifi.version>.*/\1/')"
if [[ -z "$NIFI_VERSION" ]]; then
  echo "error: could not read <nifi.version> from artifacts/pom.xml" >&2; exit 1
fi
CUSTOM_BUNDLE='{"group":"io.dragonfly.nifi.redis","artifact":"nifi-redis-migration-nar","version":"1.0.0-SNAPSHOT"}'
STANDARD_BUNDLE='{"group":"org.apache.nifi","artifact":"nifi-standard-nar","version":"'"$NIFI_VERSION"'"}'
RECORD_SERDE_BUNDLE='{"group":"org.apache.nifi","artifact":"nifi-record-serialization-services-nar","version":"'"$NIFI_VERSION"'"}'
SSL_CONTEXT_BUNDLE='{"group":"org.apache.nifi","artifact":"nifi-ssl-context-service-nar","version":"'"$NIFI_VERSION"'"}'
AWS_BUNDLE='{"group":"org.apache.nifi","artifact":"nifi-aws-nar","version":"'"$NIFI_VERSION"'"}'

# --- JSON body builders. Pure string functions, same style used throughout this project. ---

create_body() {
  printf '{"revision":{"version":0},"component":{"type":"%s","bundle":%s,"name":"%s","position":{"x":%s,"y":%s}}}' \
    "$1" "$2" "$3" "$4" "$5"
}

proc_config_body() {
  printf '{"revision":{"version":%s},"component":{"id":"%s","config":{"properties":{%s},"autoTerminatedRelationships":[%s]}}}' \
    "$2" "$1" "$3" "$4"
}

svc_config_body() {
  printf '{"revision":{"version":%s},"component":{"id":"%s","properties":{%s}}}' "$2" "$1" "$3"
}

conn_create_body() {
  printf '{"revision":{"version":0},"component":{"source":{"id":"%s","groupId":"%s","type":"PROCESSOR"},"destination":{"id":"%s","groupId":"%s","type":"PROCESSOR"},"selectedRelationships":[%s],"flowFileExpiration":"0 sec","backPressureObjectThreshold":10000,"backPressureDataSizeThreshold":"1 GB"}}' \
    "$2" "$1" "$3" "$1" "$4"
}

pg_create_body() {
  printf '{"revision":{"version":0},"component":{"name":"%s","position":{"x":%s,"y":%s}}}' "$1" "$2" "$3"
}

run_status_body() {
  printf '{"revision":{"version":%s},"state":"%s","disconnectedNodeAcknowledged":false}' "$1" "$2"
}

nifi_lib_pick_runtime

# --- Stage the local source file into the NiFi container's own filesystem (GetFile reads from
# there, not the host - see nifi-quirks-skill.md #9/#10). S3 needs no staging: ListS3/FetchS3Object
# reach the bucket directly from inside the NiFi container. ---
CONTAINER_DATA_DIR="/opt/nifi/nifi-current/ingest-data"
if [[ "$SOURCE_TYPE" == "local" ]]; then
  runtime_exec "$CONTAINER_NAME" mkdir -p "$CONTAINER_DATA_DIR"
  if [[ -d "$SOURCE_PATH" ]]; then
    echo "==> copying the source directory into the NiFi container ($CONTAINER_NAME:$CONTAINER_DATA_DIR)"
    "$RUNTIME" cp "$SOURCE_PATH/." "$CONTAINER_NAME:$CONTAINER_DATA_DIR"
  else
    echo "==> copying the source file into the NiFi container ($CONTAINER_NAME:$CONTAINER_DATA_DIR)"
    "$RUNTIME" cp "$SOURCE_PATH" "$CONTAINER_NAME:$CONTAINER_DATA_DIR/$(basename "$SOURCE_PATH")"
  fi
fi

# File Filter (GetFile) / the RouteOnAttribute regex (S3) both default to an exact, anchored match
# of the source path's own final component, unless [source].file-filter overrides it - so pointing
# at one file "just works", while pointing at a directory/prefix plus an explicit file-filter picks
# among several.
if [[ -z "$SOURCE_FILE_FILTER" ]]; then
  BASE="${SOURCE_PATH##*/}"
  ESCAPED="$(printf '%s' "$BASE" | sed 's/[.[\*^$()+?{|]/\\&/g')"
  EFFECTIVE_FILE_FILTER="^${ESCAPED}\$"
else
  EFFECTIVE_FILE_FILTER="$SOURCE_FILE_FILTER"
fi

# Credentials are only ever printed once, in the container's own logs on first creation - same
# fallback ingest-file-demo.sh/simple-migration.sh use, so this script doesn't force a re-entry of
# credentials the user never had to type the first time.
if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  CREDS="$("$RUNTIME" logs "$CONTAINER_NAME" 2>&1 | grep -A1 "Generated Username" | tail -2)" || true
  NIFI_USER="$(sed -n '1s/.*\[\(.*\)\]/\1/p' <<< "$CREDS")"
  NIFI_PASS="$(sed -n '2s/.*\[\(.*\)\]/\1/p' <<< "$CREDS")"
  if [[ -z "$NIFI_USER" || -z "$NIFI_PASS" ]]; then
    nifi_lib_load_state "$CONTAINER_NAME" "$NIFI_CREDS_FILE_PATH"
  fi
fi
if [[ -z "$NIFI_TOKEN" && ( -z "$NIFI_USER" || -z "$NIFI_PASS" ) ]]; then
  echo "error: no NiFi credentials given, and none could be read from the container's logs (they're only" >&2
  echo "       printed once, on first container creation) or from the copy saved inside the container -" >&2
  echo "       pass --nifi-token, or both --nifi-user and --nifi-password, explicitly." >&2
  exit 1
fi

nifi_lib_init

# process group is found/created by a name derived from key-prefix, so re-running against the
# same key-prefix reuses the same flow instead of creating a duplicate one each time.
PG_NAME="Ingest: ${RECORD_KEY_PREFIX}"
echo "==> locating (or creating) the '$PG_NAME' process group"
ROOT_ID="$(nifi_cli get-root-id -ot simple | tr -d '[:space:]')" || true
if [[ -z "$ROOT_ID" ]]; then
  echo "error: could not reach NiFi's REST API to determine the root process group id (check NiFi container health/logs)" >&2
  exit 1
fi
PG_ID="$(nifi_api_get "flow/process-groups/$ROOT_ID" | py3 "
import json, sys
d = json.load(sys.stdin)
for g in d['processGroupFlow']['flow']['processGroups']:
    if g['component']['name'] == '$PG_NAME':
        print(g['component']['id'])
        break
")" || true
if [[ -z "$PG_ID" ]]; then
  RESP="$(nifi_api_post "process-groups/$ROOT_ID/process-groups" "$(pg_create_body "$PG_NAME" 0 0)")" || true
  PG_ID="$(py3 "import json,sys; print(json.load(sys.stdin).get('component',{}).get('id',''))" <<< "$RESP" 2>/dev/null)" || true
  if [[ -z "$PG_ID" ]]; then
    echo "error: could not create the '$PG_NAME' process group. NiFi's response: $RESP" >&2
    exit 1
  fi
  echo "  created process group: $PG_ID"
else
  echo "  found process group: $PG_ID"
fi

# nifi_api_put_checked <path> <body> - a non-JSON response (e.g. "Cannot modify configuration of
# ... because it is currently not disabled") is a hard failure, not something to discard - see
# nifi-quirks-skill.md #2.
nifi_api_put_checked() {
  local path="$1" body="$2" resp
  resp="$(nifi_api_put "$path" "$body")" || true
  if ! echo "$resp" | py3 "import json,sys; json.load(sys.stdin)" >/dev/null 2>&1; then
    echo "error: PUT $path was rejected: $resp" >&2
    exit 1
  fi
  echo "$resp"
}

# create_component <endpoint-segment: processors|controller-services> <type> <bundle-json> <name> <x> <y>
create_component() {
  local endpoint="$1" type="$2" bundle="$3" name="$4" x="$5" y="$6" resp id
  resp="$(nifi_api_post "process-groups/$PG_ID/$endpoint" "$(create_body "$type" "$bundle" "$name" "$x" "$y")")" || true
  id="$(py3 "import json,sys; print(json.load(sys.stdin).get('component',{}).get('id',''))" <<< "$resp" 2>/dev/null)" || true
  if [[ -z "$id" ]]; then
    echo "error: could not create $name ($type). NiFi's response: $resp" >&2
    return 1
  fi
  echo "$id"
}

configure_processor() {
  local id="$1" label="$2" ver
  ver="$(nifi_current_version "processors/$id")" || true
  [[ -n "$ver" ]] || { echo "error: could not read the current revision of $label" >&2; exit 1; }
  nifi_api_put_checked "processors/$id" "$(proc_config_body "$id" "$ver" "$3" "$4")" > /dev/null
}

configure_service() {
  local id="$1" label="$2" ver
  ver="$(nifi_current_version "controller-services/$id")" || true
  [[ -n "$ver" ]] || { echo "error: could not read the current revision of $label" >&2; exit 1; }
  nifi_api_put_checked "controller-services/$id" "$(svc_config_body "$id" "$ver" "$3")" > /dev/null
}

# ensure_connection <source-id> <destination-id> <label> <relationships-json-members>
ensure_connection() {
  local src="$1" dst="$2" label="$3" rels="$4" resp
  local pairs
  pairs="$(nifi_api_get "process-groups/$PG_ID/connections" | py3 "
import json, sys
d = json.load(sys.stdin)
for c in d['connections']:
    print(c['component']['source']['id'] + '>' + c['component']['destination']['id'])
")" || true
  if grep -Fqx -- "$src>$dst" <<< "$pairs"; then
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

# wait_for_state <kind: processors|controller-services> <id> <field> <expected> <timeout> - polls
# component state (processor runStatus, or controller-service state) since both the run-status and
# property-update PUTs return as soon as NiFi accepts the request, not once the transition has
# actually finished - see nifi-quirks-skill.md #3.
wait_for_state() {
  local kind="$1" id="$2" field="$3" expected="$4" timeout="${5:-30}" waited=0 actual
  while true; do
    actual="$(nifi_api_get "$kind/$id" | py3 "
import json, sys
d = json.load(sys.stdin)
c = d.get('component', {})
print(c.get('$field') or d.get('status', {}).get('$field', ''))
")" || actual=""
    [[ "$actual" == "$expected" ]] && return 0
    if [[ "$waited" -ge "$timeout" ]]; then
      echo "warning: $kind/$id did not reach $field=$expected within ${timeout}s (last seen: ${actual:-unknown})" >&2
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

# configure_and_enable_service <id> <label> <properties-json-members> - disables first if the
# service is already enabled from a previous run (PUTting properties to an enabled service is
# rejected, see nifi-quirks-skill.md #2), then re-enables it.
configure_and_enable_service() {
  local id="$1" label="$2" props="$3" ver state
  state="$(nifi_api_get "controller-services/$id" | py3 "import json,sys; print(json.load(sys.stdin)['component'].get('state',''))")" || state=""
  if [[ "$state" == "ENABLED" || "$state" == "ENABLING" ]]; then
    ver="$(nifi_current_version "controller-services/$id")" || true
    nifi_api_put "controller-services/$id/run-status" "$(run_status_body "$ver" "DISABLED")" > /dev/null
    wait_for_state controller-services "$id" state DISABLED 30
  fi
  configure_service "$id" "$label" "$props"
  ver="$(nifi_current_version "controller-services/$id")" || true
  nifi_api_put "controller-services/$id/run-status" "$(run_status_body "$ver" "ENABLED")" > /dev/null
  wait_for_state controller-services "$id" state ENABLED 30
}

# ensure_ssl_context_service - only called when DRAGONFLY_REQUIRE_TLS=true. Reuses an existing
# SSL Context Service in this process group if one's already there, else creates one pointed at
# the NiFi container's own JVM default truststore (found via $JAVA_HOME, not a hardcoded JDK
# version string, since that changes with image updates) - it already trusts publicly-CA-signed
# endpoints like Dragonfly Cloud's out of the box, no client certificate needed.
ensure_ssl_context_service() {
  local svc_id java_home cacerts_path
  svc_id="$(nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
for s in d['controllerServices']:
    for api in s['component'].get('controllerServiceApis', []):
        if 'SSLContextService' in api.get('type', ''):
            print(s['component']['id']); sys.exit()
")" || true
  if [[ -z "$svc_id" ]]; then
    svc_id="$(create_component controller-services "org.apache.nifi.ssl.StandardSSLContextService" "$SSL_CONTEXT_BUNDLE" "SSL Context Service" 200 150)" || exit 1
    echo "  created: $svc_id" >&2
  else
    echo "  found: $svc_id" >&2
  fi
  java_home="$(runtime_exec "$CONTAINER_NAME" sh -c 'echo $JAVA_HOME')" || true
  if [[ -z "$java_home" ]]; then
    echo "error: could not read \$JAVA_HOME from container '$CONTAINER_NAME'" >&2; exit 1
  fi
  cacerts_path="${java_home}/lib/security/cacerts"
  configure_and_enable_service "$svc_id" "SSL Context Service" \
    "\"Truststore Filename\":\"$cacerts_path\",\"Truststore Password\":\"changeit\",\"Truststore Type\":\"PKCS12\"" >&2
  echo "$svc_id"
}

echo "==> finding or creating the Record Reader"
SVC_CSV="$(nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
for s in d['controllerServices']:
    if s['component']['type'].endswith('.csv.CSVReader'):
        print(s['component']['id'])
        break
")" || true
if [[ -z "$SVC_CSV" ]]; then
  SVC_CSV="$(create_component controller-services "org.apache.nifi.csv.CSVReader" "$RECORD_SERDE_BUNDLE" "Record Reader" 0 0)" || exit 1
  echo "  created: $SVC_CSV"
else
  echo "  found: $SVC_CSV"
fi

echo "==> finding or creating the Dragonfly connection pool"
SVC_DFLY="$(nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
for s in d['controllerServices']:
    if s['component']['type'].endswith('StandardDragonflyConnectionPoolService'):
        print(s['component']['id'])
        break
")" || true
if [[ -z "$SVC_DFLY" ]]; then
  SVC_DFLY="$(create_component controller-services "io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService" "$CUSTOM_BUNDLE" "Dragonfly Connection Pool" 0 150)" || exit 1
  echo "  created: $SVC_DFLY"
else
  echo "  found: $SVC_DFLY"
fi

if [[ "$SOURCE_TYPE" == "s3" ]]; then
  echo "==> finding or creating the AWS credentials provider"
  SVC_AWS_CREDS="$(nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
for s in d['controllerServices']:
    if s['component']['type'].endswith('AWSCredentialsProviderControllerService'):
        print(s['component']['id'])
        break
")" || true
  if [[ -z "$SVC_AWS_CREDS" ]]; then
    SVC_AWS_CREDS="$(create_component controller-services "org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService" "$AWS_BUNDLE" "AWS Credentials Provider" -200 150)" || exit 1
    echo "  created: $SVC_AWS_CREDS"
  else
    echo "  found: $SVC_AWS_CREDS"
  fi
  configure_and_enable_service "$SVC_AWS_CREDS" "AWS Credentials Provider" \
    "\"Access Key ID\":\"$SOURCE_ACCESS_KEY_ID\",\"Secret Access Key\":\"$SOURCE_SECRET_ACCESS_KEY\""
fi

echo "==> configuring controller services"
case "$SOURCE_FILE_TYPE" in
  tsv) CSV_FORMAT_PROPS="\"CSV Format\":\"tdf\"" ;;
  csv) CSV_FORMAT_PROPS="\"CSV Format\":\"rfc4180\"" ;;
  custom) CSV_FORMAT_PROPS="\"CSV Format\":\"custom\",\"Value Separator\":\"$SOURCE_DELIMITER\"" ;;
esac
# schema-access-strategy=csv-header-derived makes "Treat First Line as Header" moot (the header
# is always required and never emitted as a record under this strategy - confirmed against the
# live NAR's own CSVUtils bytecode), so the source file must have a header row naming its fields.
configure_and_enable_service "$SVC_CSV" "Record Reader" \
  "\"schema-access-strategy\":\"csv-header-derived\",$CSV_FORMAT_PROPS"

# require-tls defaults to "true" on this controller service (deliberately - see the service's own
# javadoc); DRAGONFLY_REQUIRE_TLS was auto-detected above from the connection string's scheme.
if [[ "$DRAGONFLY_REQUIRE_TLS" == "true" ]]; then
  echo "==> rediss:// connection string detected; provisioning an SSL Context Service"
  SSL_SVC="$(ensure_ssl_context_service)" || exit 1
  configure_and_enable_service "$SVC_DFLY" "Dragonfly Connection Pool" \
    "\"connection-string\":\"$DRAGONFLY_CONNECTION_STRING\",\"require-tls\":\"true\",\"ssl-context-service\":\"$SSL_SVC\""
else
  configure_and_enable_service "$SVC_DFLY" "Dragonfly Connection Pool" \
    "\"connection-string\":\"$DRAGONFLY_CONNECTION_STRING\",\"require-tls\":\"false\""
fi

# s3_common_props - the Region/Endpoint Override/Use Path Style Access/credentials properties
# shared by ListS3 and FetchS3Object. Region="Custom" + a free-text Custom Region is required
# whenever an Endpoint Override URL is set (a non-AWS S3-compatible endpoint) - the AWS SDK's
# signer still needs *some* region value even though most S3-compatible providers ignore it, and
# NiFi's own Region property only accepts a fixed AWS-region enum unless "Custom" is selected.
s3_common_props() {
  local parts=("\"AWS Credentials Provider service\":\"$SVC_AWS_CREDS\"")
  if [[ -n "$SOURCE_ENDPOINT_OVERRIDE" ]]; then
    parts+=("\"Endpoint Override URL\":\"$SOURCE_ENDPOINT_OVERRIDE\"" "\"Region\":\"Custom\"" "\"Custom Region\":\"$SOURCE_REGION\"")
  else
    parts+=("\"Region\":\"$SOURCE_REGION\"")
  fi
  parts+=("\"Use Path Style Access\":\"$SOURCE_PATH_STYLE_ACCESS\"")
  local IFS=,
  echo "${parts[*]}"
}

echo "==> finding or creating the flow processors"
if [[ "$SOURCE_TYPE" == "local" ]]; then
  PROC_IDS="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
order = ['GetFile', 'RecordToKeyRecord', 'RedisBatchWriter']
ids = {name: '' for name in order}
for p in d['processors']:
    t = p['component']['type']
    for name in order:
        if not ids[name] and t.endswith(name):
            ids[name] = p['component']['id']
            break
print('|'.join(ids[name] for name in order))
")" || true
  IFS='|' read -r PROC_GETFILE PROC_RTKR PROC_WRITER <<< "$PROC_IDS"
  if [[ -z "$PROC_GETFILE" ]]; then
    PROC_GETFILE="$(create_component processors "org.apache.nifi.processors.standard.GetFile" "$STANDARD_BUNDLE" "Read Source File" 0 300)" || exit 1
    echo "  created GetFile: $PROC_GETFILE"
  else
    echo "  found GetFile: $PROC_GETFILE"
  fi
else
  PROC_IDS="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
order = ['ListS3', 'RouteOnAttribute', 'FetchS3Object', 'RecordToKeyRecord', 'RedisBatchWriter']
ids = {name: '' for name in order}
for p in d['processors']:
    t = p['component']['type']
    for name in order:
        if not ids[name] and t.endswith(name):
            ids[name] = p['component']['id']
            break
print('|'.join(ids[name] for name in order))
")" || true
  IFS='|' read -r PROC_LISTS3 PROC_ROUTE PROC_FETCH PROC_RTKR PROC_WRITER <<< "$PROC_IDS"
  if [[ -z "$PROC_LISTS3" ]]; then
    PROC_LISTS3="$(create_component processors "org.apache.nifi.processors.aws.s3.ListS3" "$AWS_BUNDLE" "List S3 Source Objects" 0 300)" || exit 1
    echo "  created ListS3: $PROC_LISTS3"
  else
    echo "  found ListS3: $PROC_LISTS3"
  fi
  if [[ -z "$PROC_ROUTE" ]]; then
    PROC_ROUTE="$(create_component processors "org.apache.nifi.processors.standard.RouteOnAttribute" "$STANDARD_BUNDLE" "Filter by File Filter" 300 300)" || exit 1
    echo "  created RouteOnAttribute: $PROC_ROUTE"
  else
    echo "  found RouteOnAttribute: $PROC_ROUTE"
  fi
  if [[ -z "$PROC_FETCH" ]]; then
    PROC_FETCH="$(create_component processors "org.apache.nifi.processors.aws.s3.FetchS3Object" "$AWS_BUNDLE" "Fetch S3 Source Object" 600 300)" || exit 1
    echo "  created FetchS3Object: $PROC_FETCH"
  else
    echo "  found FetchS3Object: $PROC_FETCH"
  fi
fi
if [[ -z "$PROC_RTKR" ]]; then
  PROC_RTKR="$(create_component processors "io.dragonfly.nifi.redis.processors.RecordToKeyRecord" "$CUSTOM_BUNDLE" "RecordToKeyRecord" 900 300)" || exit 1
  echo "  created RecordToKeyRecord: $PROC_RTKR"
else
  echo "  found RecordToKeyRecord: $PROC_RTKR"
fi
if [[ -z "$PROC_WRITER" ]]; then
  PROC_WRITER="$(create_component processors "io.dragonfly.nifi.redis.processors.RedisBatchWriter" "$CUSTOM_BUNDLE" "RedisBatchWriter" 1300 300)" || exit 1
  echo "  created RedisBatchWriter: $PROC_WRITER"
else
  echo "  found RedisBatchWriter: $PROC_WRITER"
fi

if [[ "$SOURCE_TYPE" == "local" ]]; then
  ALL_PROCS=("$PROC_GETFILE" "$PROC_RTKR" "$PROC_WRITER")
else
  ALL_PROCS=("$PROC_LISTS3" "$PROC_ROUTE" "$PROC_FETCH" "$PROC_RTKR" "$PROC_WRITER")
fi

echo "==> stopping the flow so NiFi will accept property/wiring changes"
for p in "${ALL_PROCS[@]}"; do
  ver="$(nifi_current_version "processors/$p")" || true
  nifi_api_put "processors/$p/run-status" "$(run_status_body "$ver" "STOPPED")" > /dev/null || true
done
for p in "${ALL_PROCS[@]}"; do
  wait_for_state processors "$p" runStatus Stopped 30 || true
done

if [[ "$SOURCE_TYPE" == "local" ]]; then
  echo "==> configuring GetFile to read the source from the container filesystem"
  # "Keep Source File" true: this processor never needs write/delete permission on
  # $CONTAINER_DATA_DIR, only read - it re-reads the same copy on every RUN_ONCE below.
  configure_processor "$PROC_GETFILE" "GetFile" \
    "\"Input Directory\":\"$CONTAINER_DATA_DIR\",\"Keep Source File\":\"true\",\"File Filter\":\"$EFFECTIVE_FILE_FILTER\"" \
    ''
else
  echo "==> configuring ListS3 / RouteOnAttribute / FetchS3Object"
  # Listing Strategy "No Tracking": a plain one-shot list on every RUN_ONCE, no state maintained
  # across runs - matches GetFile's own "Keep Source File" re-read-every-time semantics, and
  # avoids needing a DistributedMapCacheClient just to track what's already been listed.
  configure_processor "$PROC_LISTS3" "ListS3" \
    "$(s3_common_props),\"Bucket\":\"$SOURCE_BUCKET\",\"Prefix\":\"$SOURCE_PATH\",\"Listing Strategy\":\"No Tracking\"" \
    ''
  # ListS3 has no failure relationship - only 'success', which flows on to the filter below.
  configure_processor "$PROC_ROUTE" "RouteOnAttribute" \
    "\"Routing Strategy\":\"Route to 'match' if any matches\",\"file-filter-match\":\"\${filename:matches('$EFFECTIVE_FILE_FILTER')}\"" \
    '"unmatched"'
  # ListS3 sets the 's3.bucket' and standard 'filename' (the object key) attributes on every
  # FlowFile it lists - FetchS3Object reads those back via Expression Language.
  configure_processor "$PROC_FETCH" "FetchS3Object" \
    "$(s3_common_props),\"Bucket\":\"\${s3.bucket}\",\"Object Key\":\"\${filename}\"" \
    '"failure"'
fi

configure_processor "$PROC_RTKR" "RecordToKeyRecord" \
  "\"record-reader\":\"$SVC_CSV\",\"target-type\":\"$RECORD_TARGET_TYPE\",\"key-format\":\"${RECORD_KEY_PREFIX}:\${${RECORD_PRIMARY_KEY}}\",\"${RECORD_PRIMARY_KEY}\":\"/${RECORD_PRIMARY_KEY}\"" \
  '"failure"'

configure_processor "$PROC_WRITER" "RedisBatchWriter" \
  "\"dragonfly-connection-pool\":\"$SVC_DFLY\"" \
  '"success","skipped","failure","retry"'

echo "==> wiring the flow"
if [[ "$SOURCE_TYPE" == "local" ]]; then
  ensure_connection "$PROC_GETFILE" "$PROC_RTKR" "GetFile success -> RecordToKeyRecord" '"success"'
else
  ensure_connection "$PROC_LISTS3" "$PROC_ROUTE" "ListS3 success -> RouteOnAttribute" '"success"'
  ensure_connection "$PROC_ROUTE" "$PROC_FETCH" "RouteOnAttribute matched -> FetchS3Object" '"matched"'
  ensure_connection "$PROC_FETCH" "$PROC_RTKR" "FetchS3Object success -> RecordToKeyRecord" '"success"'
fi
ensure_connection "$PROC_RTKR" "$PROC_WRITER" "RecordToKeyRecord success -> RedisBatchWriter" '"success"'

echo "==> validating the flow"
# Validated by processor *type* suffix (stable across runs), not by id - simpler than threading
# the ALL_PROCS id list through to python.
if [[ "$SOURCE_TYPE" == "local" ]]; then
  TARGET_TYPES="GetFile,RecordToKeyRecord,RedisBatchWriter"
else
  TARGET_TYPES="ListS3,RouteOnAttribute,FetchS3Object,RecordToKeyRecord,RedisBatchWriter"
fi
VALID="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
targets = set('$TARGET_TYPES'.split(','))
d = json.load(sys.stdin)
ok = True
for p in d['processors']:
    c = p['component']
    for t in targets:
        if c['type'].endswith(t):
            status = c.get('validationStatus')
            print(f\"  {'PASS' if status == 'VALID' else 'FAIL'} {t}: {status}\")
            if status != 'VALID':
                ok = False
                for e in (c.get('validationErrors') or []):
                    print(f'    - {e}')
print('OK' if ok else 'INVALID')
")" || true
if ! grep -q '^OK$' <<< "$VALID"; then
  echo "$VALID"
  echo "error: the flow is not valid - see the validation errors above." >&2
  exit 1
fi
echo "$VALID" | grep '^  '

echo "==> starting the flow's continuously-running processors"
if [[ "$SOURCE_TYPE" == "local" ]]; then
  RUNNING_PROCS=("$PROC_RTKR" "$PROC_WRITER")
else
  RUNNING_PROCS=("$PROC_ROUTE" "$PROC_FETCH" "$PROC_RTKR" "$PROC_WRITER")
fi
for p in "${RUNNING_PROCS[@]}"; do
  ver="$(nifi_current_version "processors/$p")" || true
  nifi_api_put "processors/$p/run-status" "$(run_status_body "$ver" "RUNNING")" > /dev/null
done
for p in "${RUNNING_PROCS[@]}"; do
  wait_for_state processors "$p" runStatus Running 30
done

if [[ "$SOURCE_TYPE" == "local" ]]; then
  echo "==> running GetFile once (picks up the matching source file(s))"
  GETFILE_VER="$(nifi_current_version "processors/$PROC_GETFILE")" || true
  nifi_api_put "processors/$PROC_GETFILE/run-status" "$(run_status_body "$GETFILE_VER" "RUN_ONCE")" > /dev/null
else
  echo "==> running ListS3 once (lists the matching source object(s))"
  LISTS3_VER="$(nifi_current_version "processors/$PROC_LISTS3")" || true
  nifi_api_put "processors/$PROC_LISTS3/run-status" "$(run_status_body "$LISTS3_VER" "RUN_ONCE")" > /dev/null
fi

echo "==> waiting for the flow to finish processing (queue drains to empty)"
WAITED=0
TIMEOUT=120
while true; do
  STATS="$(nifi_api_get "flow/process-groups/$PG_ID/status" | py3 "
import json, sys
try:
    s = json.load(sys.stdin)['processGroupStatus']['aggregateSnapshot']
    print(f\"{s['queuedCount']}|{s['activeThreadCount']}\")
except Exception:
    print('?|?')
")" || STATS="?|?"
  IFS='|' read -r QUEUED ACTIVE <<< "$STATS"
  if [[ "$QUEUED" == "0" && "$ACTIVE" == "0" ]]; then
    break
  fi
  if [[ "$WAITED" -ge "$TIMEOUT" ]]; then
    echo "warning: flow still shows $QUEUED queued / $ACTIVE active threads after ${TIMEOUT}s - reporting whatever landed so far" >&2
    break
  fi
  sleep 2
  WAITED=$((WAITED + 2))
done

echo "==> checking the results in Dragonfly"
FOUND="$(redis_cli "$DRAGONFLY_CONTAINER" "$DRAGONFLY_CONNECTION_STRING" --scan --pattern "${RECORD_KEY_PREFIX}:*" 2>/dev/null | grep -c . || true)"
echo "  $FOUND key(s) now present under '${RECORD_KEY_PREFIX}:*'"
if [[ "$FOUND" -gt 0 ]]; then
  SAMPLE_KEY="$(redis_cli "$DRAGONFLY_CONTAINER" "$DRAGONFLY_CONNECTION_STRING" --scan --pattern "${RECORD_KEY_PREFIX}:*" 2>/dev/null | head -1)"
  echo "  sample record ($SAMPLE_KEY):"
  if [[ "$RECORD_TARGET_TYPE" == "json" ]]; then
    redis_cli "$DRAGONFLY_CONTAINER" "$DRAGONFLY_CONNECTION_STRING" JSON.GET "$SAMPLE_KEY" | sed 's/^/    /'
  else
    redis_cli "$DRAGONFLY_CONTAINER" "$DRAGONFLY_CONNECTION_STRING" HGETALL "$SAMPLE_KEY" | sed 's/^/    /'
  fi
else
  echo "warning: no keys found under '${RECORD_KEY_PREFIX}:*' - check $RUN_LOG and the flow's validation/processor status in NiFi" >&2
fi

echo "==> done"
echo "  process group: $PG_NAME ($PG_ID)"
echo "  log:            $RUN_LOG"
