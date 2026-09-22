#!/usr/bin/env bash
# Demo: writes a tab-separated file of classical-music records to disk, builds a small NiFi flow
# via the REST API - GetFile (reads that TSV from the NiFi container's own filesystem) ->
# RecordToKeyRecord (CSVReader, tab-delimited) -> RedisBatchWriter -> Dragonfly - runs it once,
# and verifies the records landed in Dragonfly as Hash objects. Self-contained and idempotent:
# re-running finds and reuses whatever it already created, regenerates the same 500 rows
# (deterministic seed), and RedisBatchWriter's default OVERWRITE conflict strategy plus the
# row-numbered keys mean a re-run just rewrites the same N hashes rather than piling up more.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"
source "$PROJECT_ROOT/scripts/redis-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Writes a demo dataset of classical-music records (piece, form, composer, likely date of
creation, country of origin) to $PROJECT_ROOT/classical-music-demo.tsv, then ingests it into
Dragonfly as Hash objects via a NiFi flow built from scratch through the REST API.

NiFi connection/auth (omit all three to auto-read the container's auto-generated single-user
credentials, the same fallback simple-migration.sh uses):
  --nifi-container NAME        container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER             NiFi single-user login username (env NIFI_USER)
  --nifi-password PASSWORD     NiFi single-user login password (env NIFI_PASS)
  --nifi-token TOKEN           use an existing bearer token instead of user/password (env NIFI_TOKEN)

Dragonfly connection:
  --dragonfly-container NAME   container to verify results against via redis-cli (env DRAGONFLY_CONTAINER,
                                default: target-dragonfly)
  --dragonfly-connection-string S
                                connection string the NiFi processors use, resolved from INSIDE the NiFi
                                container's own network (env DRAGONFLY_CONNECTION_STRING,
                                default: redis://target-dragonfly:6379). A rediss:// scheme auto-enables
                                TLS (an SSL Context Service is provisioned automatically, trusting the
                                NiFi container's own JVM default truststore - sufficient for a
                                publicly-CA-signed endpoint like Dragonfly Cloud, no client cert needed)

Dataset:
  --record-count N             number of rows to generate and ingest (env RECORD_COUNT, default: 500)
  --key-prefix PREFIX          Dragonfly key prefix, keys land at PREFIX:<row-id> (env KEY_PREFIX, default: music)

  -h, --help                   this help

Example:
  $(basename "$0")
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-}"
NIFI_PASS="${NIFI_PASS:-}"
NIFI_TOKEN="${NIFI_TOKEN:-}"
DRAGONFLY_CONTAINER="${DRAGONFLY_CONTAINER:-target-dragonfly}"
DRAGONFLY_CONNECTION_STRING="${DRAGONFLY_CONNECTION_STRING:-redis://target-dragonfly:6379}"
RECORD_COUNT="${RECORD_COUNT:-500}"
KEY_PREFIX="${KEY_PREFIX:-music}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    --nifi-token) NIFI_TOKEN="$2"; shift 2 ;;
    --dragonfly-container) DRAGONFLY_CONTAINER="$2"; shift 2 ;;
    --dragonfly-connection-string) DRAGONFLY_CONNECTION_STRING="$2"; shift 2 ;;
    --record-count) RECORD_COUNT="$2"; shift 2 ;;
    --key-prefix) KEY_PREFIX="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

# TLS is auto-detected from the connection string's own scheme (rediss:// vs redis://) rather
# than a separate flag - the same signal Lettuce's RedisURI uses, so there's nothing to keep in
# sync between a flag and the URI you actually pass.
DRAGONFLY_REQUIRE_TLS="false"
[[ "$DRAGONFLY_CONNECTION_STRING" == rediss://* ]] && DRAGONFLY_REQUIRE_TLS="true"

if ! [[ "$RECORD_COUNT" =~ ^[0-9]+$ && "$RECORD_COUNT" -gt 0 ]]; then
  echo "error: --record-count must be a positive integer, got '$RECORD_COUNT'" >&2; exit 1
fi

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
RUN_LOG="$LOG_DIR/ingest-file-demo-$(date +%s).log"
exec > >(tee -a "$RUN_LOG") 2>&1
echo "==> logging this run to $RUN_LOG"

# Standard-component bundles are read from pom.xml (same lookup deploy-to-nifi.sh already does)
# rather than hardcoded, so a NiFi upgrade doesn't leave this script creating processors against a
# stale version string. The custom NAR's own bundle coordinate is fixed, same literal
# build-live-phase-flow.sh uses.
NIFI_VERSION="$(grep -m1 '<nifi.version>' "$PROJECT_ROOT/artifacts/pom.xml" | sed -E 's/.*<nifi.version>(.*)<\/nifi.version>.*/\1/')"
if [[ -z "$NIFI_VERSION" ]]; then
  echo "error: could not read <nifi.version> from artifacts/pom.xml" >&2; exit 1
fi
CUSTOM_BUNDLE='{"group":"io.dragonfly.nifi.redis","artifact":"nifi-redis-migration-nar","version":"1.0.0-SNAPSHOT"}'
STANDARD_BUNDLE='{"group":"org.apache.nifi","artifact":"nifi-standard-nar","version":"'"$NIFI_VERSION"'"}'
RECORD_SERDE_BUNDLE='{"group":"org.apache.nifi","artifact":"nifi-record-serialization-services-nar","version":"'"$NIFI_VERSION"'"}'
SSL_CONTEXT_BUNDLE='{"group":"org.apache.nifi","artifact":"nifi-ssl-context-service-nar","version":"'"$NIFI_VERSION"'"}'

# --- JSON body builders. Pure string functions, same style as build-live-phase-flow.sh. ---

# create_body <type> <bundle-json> <name> <x> <y> - shape is identical for a processor or a
# controller service, so one builder serves both create_component() calls below.
create_body() {
  printf '{"revision":{"version":0},"component":{"type":"%s","bundle":%s,"name":"%s","position":{"x":%s,"y":%s}}}' \
    "$1" "$2" "$3" "$4" "$5"
}

# proc_config_body <id> <revision-version> <properties-json-members> <auto-terminated-json-members>
proc_config_body() {
  printf '{"revision":{"version":%s},"component":{"id":"%s","config":{"properties":{%s},"autoTerminatedRelationships":[%s]}}}' \
    "$2" "$1" "$3" "$4"
}

# svc_config_body <id> <revision-version> <properties-json-members>
svc_config_body() {
  printf '{"revision":{"version":%s},"component":{"id":"%s","properties":{%s}}}' "$2" "$1" "$3"
}

# conn_create_body <pg-id> <source-proc-id> <destination-proc-id> <relationships-json-members>
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

# --- Dataset generation. Pure bash, no external dependency - a composer table (real name,
# country of origin, birth/death year) crossed with a form/key/opus template, so every row is a
# plausible-looking classical-music record without inventing false facts about a real named work.
# A fixed RANDOM seed makes every run write byte-identical content, so the file is a stable
# on-disk artifact rather than something that churns on every re-run.
# ---

generate_dataset() {
  local out="$1" count="$2"
  RANDOM=20260921
  local composers=(
    "Johann Sebastian Bach|Germany|1685|1750" "George Frideric Handel|Germany|1685|1759"
    "Antonio Vivaldi|Italy|1678|1741" "Joseph Haydn|Austria|1732|1809"
    "Wolfgang Amadeus Mozart|Austria|1756|1791" "Ludwig van Beethoven|Germany|1770|1827"
    "Franz Schubert|Austria|1797|1828" "Felix Mendelssohn|Germany|1809|1847"
    "Frederic Chopin|Poland|1810|1849" "Robert Schumann|Germany|1810|1856"
    "Franz Liszt|Hungary|1811|1886" "Giuseppe Verdi|Italy|1813|1901"
    "Richard Wagner|Germany|1813|1883" "Johannes Brahms|Germany|1833|1897"
    "Georges Bizet|France|1838|1875" "Modest Mussorgsky|Russia|1839|1881"
    "Pyotr Ilyich Tchaikovsky|Russia|1840|1893" "Antonin Dvorak|Czechia|1841|1904"
    "Edvard Grieg|Norway|1843|1907" "Nikolai Rimsky-Korsakov|Russia|1844|1908"
    "Gabriel Faure|France|1845|1924" "Giacomo Puccini|Italy|1858|1924"
    "Edward Elgar|England|1857|1934" "Gustav Mahler|Austria|1860|1911"
    "Claude Debussy|France|1862|1918" "Richard Strauss|Germany|1864|1949"
    "Jean Sibelius|Finland|1865|1957" "Ralph Vaughan Williams|England|1872|1958"
    "Sergei Rachmaninoff|Russia|1873|1943" "Gustav Holst|England|1874|1934"
    "Maurice Ravel|France|1875|1937" "Bela Bartok|Hungary|1881|1945"
    "Igor Stravinsky|Russia|1882|1971" "Sergei Prokofiev|Russia|1891|1953"
    "Dmitri Shostakovich|Russia|1906|1975" "Aaron Copland|United States|1900|1990"
  )
  local forms=(
    Symphony "Piano Sonata" "Violin Sonata" "Cello Sonata" "String Quartet" "Piano Trio"
    "Piano Quartet" "Piano Concerto" "Violin Concerto" "Cello Concerto" Prelude Fugue Nocturne
    Etude Mazurka Waltz Impromptu Ballade Rhapsody Polonaise Serenade Divertimento Variations
    Toccata Suite Overture
  )
  local keys=(
    "C major" "C minor" "C-sharp minor" "D major" "D minor" "E-flat major" "E major" "E minor"
    "F major" "F minor" "F-sharp minor" "G major" "G minor" "A-flat major" "A major" "A minor"
    "B-flat major" "B-flat minor" "B major" "B minor"
  )

  {
    printf 'id\tpiece\tform\tcomposer\tdate\tcountry\n'
    local i c name country birth death form key num op span year piece
    for ((i = 1; i <= count; i++)); do
      c="${composers[$((RANDOM % ${#composers[@]}))]}"
      IFS='|' read -r name country birth death <<< "$c"
      form="${forms[$((RANDOM % ${#forms[@]}))]}"
      key="${keys[$((RANDOM % ${#keys[@]}))]}"
      num=$(((RANDOM % 12) + 1))
      op=$(((RANDOM % 130) + 1))
      # Composed sometime between the composer's 15th year and death - a rough but real
      # constraint, so no row claims a piece written before its composer was born.
      span=$((death - (birth + 15)))
      ((span < 1)) && span=1
      year=$((birth + 15 + (RANDOM % span)))
      piece="${form} No. ${num} in ${key}, Op. ${op}"
      printf '%d\t%s\t%s\t%s\t%d\t%s\n' "$i" "$piece" "$form" "$name" "$year" "$country"
    done
  } > "$out"
}

TSV_FILE="$PROJECT_ROOT/classical-music-demo.tsv"
TSV_BASENAME="$(basename "$TSV_FILE")"
echo "==> generating $RECORD_COUNT classical-music records"
generate_dataset "$TSV_FILE" "$RECORD_COUNT"
echo "  wrote $(wc -l < "$TSV_FILE" | tr -d ' ') lines (incl. header) to $TSV_FILE; sample:"
head -n 4 "$TSV_FILE" | sed 's/^/    /'

nifi_lib_pick_runtime

# GetFile reads from the NiFi container's OWN filesystem, not the host's - the host-side TSV
# above is only a source copy - see nifi-quirks-skill.md #9/#10 on container network/filesystem
# separation. "Keep Source File" stays enabled (set below) so this copy only needs read
# permission, matching the read-only cp deploy-to-nifi.sh already does for nar_extensions/.
CONTAINER_DATA_DIR="/opt/nifi/nifi-current/demo-data"
CONTAINER_TSV_PATH="$CONTAINER_DATA_DIR/$TSV_BASENAME"
echo "==> copying the dataset into the NiFi container ($CONTAINER_NAME:$CONTAINER_TSV_PATH)"
runtime_exec "$CONTAINER_NAME" mkdir -p "$CONTAINER_DATA_DIR"
"$RUNTIME" cp "$TSV_FILE" "$CONTAINER_NAME:$CONTAINER_TSV_PATH"

# Credentials are only ever printed once, in the container's own logs on first creation - same
# fallback simple-migration.sh uses, so this script doesn't force a re-entry of credentials the
# user never had to type the first time.
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

echo "==> locating (or creating) the Classical Music Ingest Demo process group"
ROOT_ID="$(nifi_cli get-root-id -ot simple | tr -d '[:space:]')" || true
if [[ -z "$ROOT_ID" ]]; then
  echo "error: could not reach NiFi's REST API to determine the root process group id (check NiFi container health/logs)" >&2
  exit 1
fi
PG_ID="$(nifi_api_get "flow/process-groups/$ROOT_ID" | py3 "
import json, sys
d = json.load(sys.stdin)
for g in d['processGroupFlow']['flow']['processGroups']:
    if g['component']['name'] == 'Classical Music Ingest Demo':
        print(g['component']['id'])
        break
")" || true
if [[ -z "$PG_ID" ]]; then
  RESP="$(nifi_api_post "process-groups/$ROOT_ID/process-groups" "$(pg_create_body "Classical Music Ingest Demo" 0 0)")" || true
  PG_ID="$(py3 "import json,sys; print(json.load(sys.stdin).get('component',{}).get('id',''))" <<< "$RESP" 2>/dev/null)" || true
  if [[ -z "$PG_ID" ]]; then
    echo "error: could not create the Classical Music Ingest Demo process group. NiFi's response: $RESP" >&2
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

echo "==> finding or creating the CSV reader (tab-delimited, schema from header)"
SVC_CSV="$(nifi_api_get "flow/process-groups/$PG_ID/controller-services" | py3 "
import json, sys
d = json.load(sys.stdin)
for s in d['controllerServices']:
    if s['component']['type'].endswith('.csv.CSVReader'):
        print(s['component']['id'])
        break
")" || true
if [[ -z "$SVC_CSV" ]]; then
  SVC_CSV="$(create_component controller-services "org.apache.nifi.csv.CSVReader" "$RECORD_SERDE_BUNDLE" "TSV Reader" 0 0)" || exit 1
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
# StandardSSLContextService requires *some* keystore or truststore populated (a bare instance
# with neither is invalid), so "just trust the system CAs" isn't an option here, and
# require-tls=true needs this per AbstractRedisConnectionPoolService's customValidate.
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

echo "==> configuring controller services"
# schema-access-strategy=csv-header-derived makes "Treat First Line as Header" moot (the header
# is always required and never emitted as a record under this strategy - confirmed against the
# live NAR's own CSVUtils bytecode), so no separate header-boolean property is needed here.
configure_and_enable_service "$SVC_CSV" "TSV Reader" \
  "\"schema-access-strategy\":\"csv-header-derived\",\"CSV Format\":\"tdf\""
# require-tls defaults to "true" on this controller service (deliberately - see the service's own
# javadoc); DRAGONFLY_REQUIRE_TLS was auto-detected above from the connection string's scheme.
# Leaving require-tls unset would pass validation but fail every write at runtime, and leaving it
# "true" against a plaintext dev target (target-dragonfly) would fail to connect at all.
if [[ "$DRAGONFLY_REQUIRE_TLS" == "true" ]]; then
  echo "==> rediss:// connection string detected; provisioning an SSL Context Service"
  SSL_SVC="$(ensure_ssl_context_service)" || exit 1
  configure_and_enable_service "$SVC_DFLY" "Dragonfly Connection Pool" \
    "\"connection-string\":\"$DRAGONFLY_CONNECTION_STRING\",\"require-tls\":\"true\",\"ssl-context-service\":\"$SSL_SVC\""
else
  configure_and_enable_service "$SVC_DFLY" "Dragonfly Connection Pool" \
    "\"connection-string\":\"$DRAGONFLY_CONNECTION_STRING\",\"require-tls\":\"false\""
fi

echo "==> finding or creating the three flow processors"
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
  PROC_GETFILE="$(create_component processors "org.apache.nifi.processors.standard.GetFile" "$STANDARD_BUNDLE" "Read Classical Music TSV" 0 300)" || exit 1
  echo "  created GetFile: $PROC_GETFILE"
else
  echo "  found GetFile: $PROC_GETFILE"
fi
if [[ -z "$PROC_RTKR" ]]; then
  PROC_RTKR="$(create_component processors "io.dragonfly.nifi.redis.processors.RecordToKeyRecord" "$CUSTOM_BUNDLE" "RecordToKeyRecord" 400 300)" || exit 1
  echo "  created RecordToKeyRecord: $PROC_RTKR"
else
  echo "  found RecordToKeyRecord: $PROC_RTKR"
fi
if [[ -z "$PROC_WRITER" ]]; then
  PROC_WRITER="$(create_component processors "io.dragonfly.nifi.redis.processors.RedisBatchWriter" "$CUSTOM_BUNDLE" "RedisBatchWriter" 800 300)" || exit 1
  echo "  created RedisBatchWriter: $PROC_WRITER"
else
  echo "  found RedisBatchWriter: $PROC_WRITER"
fi

echo "==> stopping the flow so NiFi will accept property/wiring changes"
for p in "$PROC_GETFILE" "$PROC_RTKR" "$PROC_WRITER"; do
  ver="$(nifi_current_version "processors/$p")" || true
  nifi_api_put "processors/$p/run-status" "$(run_status_body "$ver" "STOPPED")" > /dev/null || true
done
for p in "$PROC_GETFILE" "$PROC_RTKR" "$PROC_WRITER"; do
  wait_for_state processors "$p" runStatus Stopped 30 || true
done

echo "==> configuring GetFile to read the dataset from the container filesystem"
# "Keep Source File" true: this processor never needs write/delete permission on
# $CONTAINER_DATA_DIR, only read - it re-reads the same copy on every RUN_ONCE below rather than
# consuming it, which also means a re-run doesn't depend on delete having actually succeeded.
configure_processor "$PROC_GETFILE" "GetFile" \
  "\"Input Directory\":\"$CONTAINER_DATA_DIR\",\"Keep Source File\":\"true\"" \
  ''

configure_processor "$PROC_RTKR" "RecordToKeyRecord" \
  "\"record-reader\":\"$SVC_CSV\",\"target-type\":\"hash\",\"key-format\":\"${KEY_PREFIX}:\${id}\",\"id\":\"/id\"" \
  '"failure"'

configure_processor "$PROC_WRITER" "RedisBatchWriter" \
  "\"dragonfly-connection-pool\":\"$SVC_DFLY\"" \
  '"success","skipped","failure","retry"'

echo "==> wiring the flow"
ensure_connection "$PROC_GETFILE" "$PROC_RTKR" "GetFile success -> RecordToKeyRecord" '"success"'
ensure_connection "$PROC_RTKR" "$PROC_WRITER" "RecordToKeyRecord success -> RedisBatchWriter" '"success"'

echo "==> validating the flow"
VALID="$(nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
targets = {'GetFile', 'RecordToKeyRecord', 'RedisBatchWriter'}
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

echo "==> starting RecordToKeyRecord and RedisBatchWriter"
for p in "$PROC_RTKR" "$PROC_WRITER"; do
  ver="$(nifi_current_version "processors/$p")" || true
  nifi_api_put "processors/$p/run-status" "$(run_status_body "$ver" "RUNNING")" > /dev/null
done
wait_for_state processors "$PROC_RTKR" runStatus Running 30
wait_for_state processors "$PROC_WRITER" runStatus Running 30

echo "==> running GetFile once (picks up the one TSV file, emitting a single FlowFile carrying all $RECORD_COUNT rows)"
GETFILE_VER="$(nifi_current_version "processors/$PROC_GETFILE")" || true
nifi_api_put "processors/$PROC_GETFILE/run-status" "$(run_status_body "$GETFILE_VER" "RUN_ONCE")" > /dev/null

echo "==> waiting for $RECORD_COUNT Hash keys to land in Dragonfly under '${KEY_PREFIX}:*'"
count_dragonfly_keys() {
  redis_cli "$DRAGONFLY_CONTAINER" "$DRAGONFLY_CONNECTION_STRING" --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null | grep -c . || true
}
WAITED=0
TIMEOUT=90
FOUND=0
while true; do
  FOUND="$(count_dragonfly_keys)"
  FOUND="$(tr -d '[:space:]' <<< "$FOUND")"
  [[ "$FOUND" =~ ^[0-9]+$ ]] || FOUND=0
  [[ "$FOUND" -ge "$RECORD_COUNT" ]] && break
  if [[ "$WAITED" -ge "$TIMEOUT" ]]; then
    echo "warning: only $FOUND/$RECORD_COUNT keys present after ${TIMEOUT}s - checking processor status" >&2
    nifi_api_get "process-groups/$PG_ID/processors" | py3 "
import json, sys
d = json.load(sys.stdin)
for p in d['processors']:
    c = p['component']
    print(f\"  {c['name']}: {p.get('status', {}).get('runStatus')} validation={c.get('validationStatus')}\")
    for e in (c.get('validationErrors') or []):
        print(f'    - {e}')
" >&2 || true
    break
  fi
  sleep 2
  WAITED=$((WAITED + 2))
done

echo "==> stopping RecordToKeyRecord and RedisBatchWriter"
for p in "$PROC_RTKR" "$PROC_WRITER"; do
  ver="$(nifi_current_version "processors/$p")" || true
  nifi_api_put "processors/$p/run-status" "$(run_status_body "$ver" "STOPPED")" > /dev/null || true
done

echo
echo "=================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Ingest File Demo summary"
echo "=================================================="
echo "  process group:  $PG_ID (Classical Music Ingest Demo)"
echo "  source file:    $TSV_FILE"
echo "  Dragonfly keys under '${KEY_PREFIX}:*': $FOUND / $RECORD_COUNT"
if [[ "$FOUND" -ge "$RECORD_COUNT" ]]; then
  echo "  sample record (${KEY_PREFIX}:1):"
  redis_cli "$DRAGONFLY_CONTAINER" "$DRAGONFLY_CONNECTION_STRING" HGETALL "${KEY_PREFIX}:1" | sed 's/^/    /'
  echo
  echo "success: $RECORD_COUNT Hash objects are in Dragonfly."
  exit 0
else
  echo
  echo "error: only $FOUND/$RECORD_COUNT Hash objects made it to Dragonfly - see the warnings above." >&2
  exit 1
fi
