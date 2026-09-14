#!/usr/bin/env bash
# Rsyncs this project's files to the Linux server that actually runs podman/NiFi, so local
# edits (scripts, r2dfly.json, processor source) can be tested there. Only syncs source - not
# build output (nifi-redis-migration-nar/nifi-redis-migration-processors' target/ dirs), which
# gets rebuilt on the server itself by deploy-to-nifi.sh.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"

usage() {
  cat <<EOF
Usage: $(basename "$0") --host HOST [options]

  --host HOST        server to SSH to (env NIFI_SERVER_HOST) - required
  --user USER         SSH user (env NIFI_SERVER_USER, default: ubuntu)
  --remote-dir DIR     destination directory on the server (env NIFI_SERVER_DIR,
                       default: ~/nifi-redis-migration/nifi-redis-migration)
  --scripts-only        only transfer the scripts/ directory (the *.sh files)
  -i, --identity FILE  SSH private key to authenticate with (env NIFI_SERVER_IDENTITY)
  -n, --dry-run        show what would be transferred without changing anything
  -h, --help           this help

Example:
  $(basename "$0") --host 1.2.3.4
  $(basename "$0") --host 1.2.3.4 --scripts-only -i ~/.ssh/aws_key
EOF
}

HOST="${NIFI_SERVER_HOST:-}"
USER_NAME="${NIFI_SERVER_USER:-ubuntu}"
REMOTE_DIR="${NIFI_SERVER_DIR:-~/nifi-redis-migration/nifi-redis-migration}"
IDENTITY="${NIFI_SERVER_IDENTITY:-}"
DRY_RUN=false
SCRIPTS_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --user) USER_NAME="$2"; shift 2 ;;
    --remote-dir) REMOTE_DIR="$2"; shift 2 ;;
    --scripts-only) SCRIPTS_ONLY=true; shift ;;
    -i|--identity) IDENTITY="$2"; shift 2 ;;
    -n|--dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$HOST" ]]; then
  echo "error: --host (or NIFI_SERVER_HOST) is required" >&2
  usage
  exit 1
fi

SSH_ARGS=()
RSYNC_ARGS=(-avz)
if [[ -n "$IDENTITY" ]]; then
  SSH_ARGS+=(-i "$IDENTITY")
  RSYNC_ARGS+=(-e "ssh -i $IDENTITY")
fi
if [[ "$DRY_RUN" == "true" ]]; then
  RSYNC_ARGS+=(-n)
fi

echo "==> ensuring $REMOTE_DIR exists on $USER_NAME@$HOST"
ssh "${SSH_ARGS[@]}" "$USER_NAME@$HOST" "mkdir -p $REMOTE_DIR"

if [[ "$SCRIPTS_ONLY" == "true" ]]; then
  echo "==> syncing scripts/ to $USER_NAME@$HOST:$REMOTE_DIR/scripts/"
  rsync "${RSYNC_ARGS[@]}" "$PROJECT_ROOT/scripts/" "$USER_NAME@$HOST:$REMOTE_DIR/scripts/"
else
  echo "==> syncing $PROJECT_ROOT/ to $USER_NAME@$HOST:$REMOTE_DIR/"
  rsync "${RSYNC_ARGS[@]}" --exclude 'target/' "$PROJECT_ROOT/" "$USER_NAME@$HOST:$REMOTE_DIR/"
fi

echo "==> done."
