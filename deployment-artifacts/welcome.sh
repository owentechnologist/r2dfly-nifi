#!/usr/bin/env bash
# Unpacks the project deployment zip (expected alongside this script) and prints the
# simplest way to run a Redis -> Dragonfly migration. Ships external to the zip itself,
# so this is the first thing you run on a fresh server after copying both files over.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZIP_FILE="${1:-$SCRIPT_DIR/nifi-redis-migration.zip}"
DEST_DIR="${2:-$(pwd)}"
# Unpacks into its own subfolder (named after the zip) rather than dumping scripts/docs/
# artifacts straight into $DEST_DIR - keeps a re-run, or unpacking alongside other things
# already in $DEST_DIR, from scattering files across it.
SUBFOLDER_NAME="$(basename "$ZIP_FILE" .zip)"
TARGET_DIR="$DEST_DIR/$SUBFOLDER_NAME"

cat <<'ART'
                    +----+    +----+
                  /REDIS/|  /VALKEY/|
                 /     / | /      / |
                +-----+  |+------+  |
                |     |  ||      |  +
                |     | / |      | /
                |     |/  |      |/
                +-----+   +------+
                 \ MIGRATION /
                  \  TOOL   /
                   `.._....'
                       ||
                      \\//
                     .-@@-.
:+*=:             =@%@@@@@@%=            :+*+:
%@@@@@@%*=.       =@@@@@@@@@@-      .=#%@@@@@@#
@@@@@@@@@@@@#+-. .%@@@@@@@@#. .-+#@@@@@@@@@@@@%
-@@@@@@@@@@@@@@@@*:#@@%%%%@#:*@@@@@@@@@@@@@@@@-
  :+*********####-%@.DRAGON.@%-####********++.
 .%@@@@@@@@@@@@@%:@@ .FLY. @@:@@@@@@@@@@@@@@%
 .@@@@@@@@%*+-:   =@@@@@@@@=   :-+*%@@@@@@@%.
   =*+-:           ###**###           :-+*=
                  %@@%%%%%%@
                  *@@%%%%%*@
                  +@@%%%%%=@
                   :##%%%%#:
                   :@@%%%%%:
                    @@%O%%#
                    @@%%%%#
                     @%%%%@
                     @%%%%@
                      %T%
                      %%%
                      %%%
                       %
                       %
                      ...
ART

# `|| true`: read returns nonzero on EOF (e.g. no interactive stdin available) - unguarded,
# that would abort the script here under set -e instead of just skipping the pause.
read -r -p "Hit enter to continue... " _ || true

cat <<EXAMPLE

Here's what running a full migration looks like once this is unpacked - e.g. tuned for a
larger keyspace with some CPU/memory headroom and extra parallelism, source and target
pointed at their own instances:

  cd $TARGET_DIR/scripts
  ./simple-migration.sh \\
    --cpus 4 \\
    --memory 8g \\
    --parallelism 2 \\
    --writer-concurrency 5 \\
    --source-connection-string redis://192.168.1.22:6379 \\
    --target-connection-string rediss://default:password@my.dragonflydb.cloud:6385

EXAMPLE

echo "This will unpack $(basename "$ZIP_FILE") into its own subfolder: $TARGET_DIR"
echo "and show the basic steps for migrating from Redis/Valkey to Dragonfly."
echo
# `|| true`: see the earlier read above - EOF must not abort the script under set -e here either.
read -r -p "Continue? [Y/n] " REPLY || true
case "$REPLY" in
  [nN]*)
    echo "Aborted."
    exit 0
    ;;
esac

if ! command -v unzip >/dev/null 2>&1; then
  echo "error: 'unzip' not found - install it first (e.g. 'sudo apt-get install unzip' or 'sudo yum install unzip')" >&2
  exit 1
fi
if [[ ! -f "$ZIP_FILE" ]]; then
  echo "error: deployment zip not found at $ZIP_FILE" >&2
  echo "       usage: $(basename "$0") [path-to-zip] [dest-dir]" >&2
  exit 1
fi

echo "==> unpacking $(basename "$ZIP_FILE") into $TARGET_DIR"
mkdir -p "$TARGET_DIR"
unzip -q -o "$ZIP_FILE" -d "$TARGET_DIR"

cat <<EOF

Redis -> Dragonfly migration tool is ready in: $TARGET_DIR

Prerequisites: Docker or Podman, 16GB RAM / 8 CPU cores free. Nothing else needs to be
installed - the NAR is built with a containerized Maven+JDK, and redis-cli/python3/curl
run from a small helper image built automatically on first use.

Simplest usage - run a full migration in one command:

  cd $TARGET_DIR/scripts
  ./simple-migration.sh \\
    --source-connection-string redis://source-host:6379 \\
    --target-connection-string rediss://default:password@target-host:6385

A rediss:// connection string automatically enables TLS to that side - no separate flag
needed. This validates connectivity, builds and deploys the processor NAR, starts NiFi,
configures and starts the migration, and prints a summary of keys migrated.

Every run also writes a full settings/activity log to logs/<migration-id>.log (alongside
scripts/) - check there if you need a record of exactly what a past migration run did.

See docs/quickstart.md for options and examples, or docs/TUTORIAL.md for a full walkthrough.
EOF
