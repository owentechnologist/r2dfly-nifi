#!/usr/bin/env bash
# Builds dist/<version>.zip and its .sha256, the files a GitHub Release carries. The zip holds the
# committed README, artifacts/, docs/ and scripts/ under one <version>/ folder, plus the built NAR
# and its .source-hash, so deploy-to-nifi.sh on the user's host reuses that NAR instead of running
# a Maven build. The version is R2DFLY_VERSION from scripts/version.sh.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
NAR_TARGET="artifacts/nifi-redis-migration-nar/target"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--publish]

Builds dist/$R2DFLY_VERSION.zip and dist/$R2DFLY_VERSION.zip.sha256 from the current commit.
  --publish   also create the GitHub Release $R2DFLY_VERSION (tagging the current commit) with
              both files attached; needs the gh CLI, logged in
EOF
}

PUBLISH=false
for arg in "$@"; do
  case "$arg" in
    --publish) PUBLISH=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument '$arg'" >&2; usage >&2; exit 1 ;;
  esac
done

cd "$PROJECT_ROOT"
# The zip is cut from the commit, and the NAR from the working tree: they match only when clean.
if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: uncommitted changes - commit them first, so the release matches a commit" >&2
  exit 1
fi

# A release NAR comes from a fresh build with the unit tests on, never a reused dev build.
rm -f "$NAR_TARGET/.source-hash"
SKIP_TESTS=false scripts/deploy-to-nifi.sh --build-only

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
git archive --prefix="$R2DFLY_VERSION/" HEAD README.md artifacts docs scripts | tar -x -C "$STAGE"
mkdir -p "$STAGE/$R2DFLY_VERSION/$NAR_TARGET"
cp "$NAR_TARGET"/*.nar "$NAR_TARGET/.source-hash" "$STAGE/$R2DFLY_VERSION/$NAR_TARGET/"

mkdir -p dist
rm -f "dist/$R2DFLY_VERSION.zip"
(cd "$STAGE" && zip -qr "$PROJECT_ROOT/dist/$R2DFLY_VERSION.zip" "$R2DFLY_VERSION")
if command -v sha256sum >/dev/null 2>&1; then
  (cd dist && sha256sum "$R2DFLY_VERSION.zip" > "$R2DFLY_VERSION.zip.sha256")
else
  (cd dist && shasum -a 256 "$R2DFLY_VERSION.zip" > "$R2DFLY_VERSION.zip.sha256")
fi
echo "==> built dist/$R2DFLY_VERSION.zip"

if [[ "$PUBLISH" == "true" ]]; then
  gh release create "$R2DFLY_VERSION" "dist/$R2DFLY_VERSION.zip" "dist/$R2DFLY_VERSION.zip.sha256" \
    --target "$(git rev-parse HEAD)" --title "$R2DFLY_VERSION" \
    --notes "Download $R2DFLY_VERSION.zip, unzip it, and follow README.md."
fi
