#!/usr/bin/env bash
# Builds a deployment-artifacts/nifi-redis-migration-<version>.zip from the project's
# artifacts/, docs/, and scripts/ directories (excluding build output under any target/ dir).
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<EOF
Usage: $(basename "$0") <version>

Example:
  $(basename "$0") r2dfly-A-007
EOF
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

VERSION="$1"
OUTPUT="$PROJECT_ROOT/deployment-artifacts/nifi-redis-migration-${VERSION}.zip"

cd "$PROJECT_ROOT"
rm -f "$OUTPUT"
zip -r "$OUTPUT" artifacts docs scripts -x "*/target/*"

echo "==> built $OUTPUT"
