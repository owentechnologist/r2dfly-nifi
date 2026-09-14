#!/usr/bin/env bash
# Resets a NiFi container's single-user login to a new known value, in place, when the existing
# login has become unrecoverable. Both of the ways back have to be gone at once for that. The
# one-time "Generated Username"/"Generated Password" lines have rotated out of the container's
# logs (NiFi prints them exactly once, on first startup), AND there is no saved copy at
# /tmp/r2dfly-nifi-credentials.env inside the container - either the container predates this
# project's credential-persistence feature, or its copy was lost. Before this script the only way
# back was deleting the container and starting the migration over.
#
# The reset alone is not the point. This also saves the new credentials where the rest of the
# project looks for them, so simple-migration.sh, simple-troubleshoot.sh and reset-r2dfly-flow.sh
# recover them automatically on their next run.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/version.sh"
echo "==> r2dfly version $R2DFLY_VERSION"
source "$PROJECT_ROOT/scripts/nifi-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --nifi-container NAME     container running NiFi (env NIFI_CONTAINER_NAME, default: nifi-redis-migration)
  --nifi-user USER          new single-user login username (env NIFI_USER, default: admin)
  --nifi-password PASSWORD  new single-user login password (env NIFI_PASS, default: generated)
  -y, --yes                 don't prompt for confirmation before resetting
  -h, --help                this help
EOF
}

CONTAINER_NAME="${NIFI_CONTAINER_NAME:-nifi-redis-migration}"
NIFI_USER="${NIFI_USER:-admin}"
NIFI_PASS="${NIFI_PASS:-}"
ASSUME_YES=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nifi-container) CONTAINER_NAME="$2"; shift 2 ;;
    --nifi-user) NIFI_USER="$2"; shift 2 ;;
    --nifi-password) NIFI_PASS="$2"; shift 2 ;;
    -y|--yes) ASSUME_YES=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

nifi_lib_pick_runtime

# An `if !` guard rather than a bare grep, because `grep -Fqx` returns 1 on no-match, which under
# set -e would abort here with no output at all, so none of the error text below would ever print.
#
# The listing is captured first and matched from a here-string rather than piped into grep,
# because the piped spelling is unsound under `set -o pipefail`. `grep -q` exits on its first
# match, and a producer left holding an unfinished write at that moment takes SIGPIPE and returns
# 141; pipefail makes the whole pipeline 141, and the `!` inverts that into refusing to run
# against a container that IS running. Two independent things strand that write, both measured on
# this host at 20 runs each. Output past the pipe buffer (probed at 64KB) forces the producer to
# block mid-write, so it always happens: `cat` of a 262KB listing hit 141 20 of 20 times, of a
# 392-byte one 0 of 20. Under the buffer it instead turns on whether the producer process happens
# to still be alive, which varies by producer: a bash builtin hit it 20 of 20 even at 392 bytes,
# while `seq | sed` hit it 0 of 20 at 100 names and 20 of 20 at 5000. `podman ps` here is an
# exec'd binary emitting 4 names and 70 bytes, the shape that does not fail, and it measured 0 of
# 20 - so at this call site the piped form is a latent hazard rather than an observed failure, and
# it would start biting on a host with enough containers to clear 64KB of names. The here-string
# retires the whole class at no cost, because grep is handed a file with no live writer behind it.
# `|| true` would be the wrong fix, masking the genuine no-match 1 so this check could never fire.
RUNNING_CONTAINERS="$($RUNTIME ps --format '{{.Names}}')"
if ! grep -Fqx "$CONTAINER_NAME" <<< "$RUNNING_CONTAINERS"; then
  echo "error: no running container named '$CONTAINER_NAME'" >&2
  echo "       this script only resets the login on an existing running container - it never" >&2
  echo "       creates one. Start the container (or run ./deploy-to-nifi.sh) first." >&2
  exit 1
fi

if [[ -z "$NIFI_PASS" ]]; then
  # Alphanumeric only. Not for entropy - 24 random alphanumerics is already far more than enough
  # - but to remove any doubt about how NiFi's own config writer handles shell or XML
  # metacharacters on their way into login-identity-providers.xml.
  #
  # Both forms oversample (48 and 64 bytes) so the alnum-only filter never leaves fewer than the
  # 24 characters wanted. Two details in them are load-bearing, both measured directly on a macOS
  # host (bash 3.2, BSD userland):
  #   - LC_ALL=C on the tr. Without it, BSD/macOS tr aborts with "Illegal byte sequence" on the
  #     binary input from /dev/urandom. 5 of 5 runs failed without it.
  #   - The trailing `|| true` inside the command substitution. head closes the pipe on an
  #     infinite producer, so tr takes SIGPIPE and pipefail fails the whole pipeline with 141
  #     (PIPESTATUS=141 0 0). 200 of 200 runs failed without it, 0 of 200 with it. `|| true` does
  #     not touch the captured stdout, so the value still lands in the variable, and the length
  #     gate below is the real backstop if a generator ever did come back short.
  #
  # openssl is preferred when present because its producer is finite, so that form needs none of
  # the SIGPIPE reasoning above. No other script in this project calls openssl, so it is not an
  # established dependency here and the /dev/urandom form has to genuinely work on its own.
  if command -v openssl >/dev/null 2>&1; then
    NIFI_PASS="$(openssl rand -base64 48 | LC_ALL=C tr -dc 'A-Za-z0-9' | cut -c1-24 || true)"
  else
    NIFI_PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c64 | cut -c1-24 || true)"
  fi
  echo "==> generated a new password. It is shown once below and will not be repeated."
fi

# NiFi's own minimums for bin/nifi.sh set-single-user-credentials. One gate on whichever values we
# ended up with, flag-supplied or generated, so a short one gets a message naming the real minimum
# instead of a NiFi stack trace from inside the container.
if (( ${#NIFI_USER} < 4 )); then
  echo "error: username '$NIFI_USER' is ${#NIFI_USER} characters - NiFi requires at least 4" >&2
  exit 1
fi
if (( ${#NIFI_PASS} < 12 )); then
  echo "error: password is ${#NIFI_PASS} characters - NiFi requires at least 12" >&2
  exit 1
fi

if [[ "$ASSUME_YES" != "true" ]]; then
  echo "==> new username: $NIFI_USER"
  echo "==> new password: $NIFI_PASS"
  echo "This restarts container '$CONTAINER_NAME'. Anyone signed in on the old credentials is logged out."
  # Two deliberate differences from reset-r2dfly-flow.sh's confirmation.
  #
  # No `[[ -t 0 ]]` / "proceeding automatically" branch, so this always prompts. Auto-proceeding on
  # a credential reset that restarts the container is the wrong default, and a piped `n` has to
  # actually abort.
  #
  # The read is guarded, because unguarded it returns nonzero at EOF and set -e would exit right
  # here without ever printing "aborted" (confirmed directly). Guarded, no input means REPLY is
  # empty and falls into the abort branch below, which is the safe direction to fail in.
  # `|| REPLY=""` rather than `|| true`, because read leaves REPLY unset when stdin is closed
  # outright rather than merely at EOF (`0<&-`), and set -u then kills the script on an unbound
  # variable before the abort branch can run. Assigning it here makes both cases abort cleanly.
  read -r -p "Continue? [y/N] " REPLY || REPLY=""
  if [[ "$REPLY" != [yY]* ]]; then
    echo "aborted - nothing was changed."
    exit 1
  fi
fi

echo "==> writing the new single-user credentials"
# That absolute path is the one docs/TUTORIAL.md already documents. stdout is dropped because it is
# only NiFi's own bootstrap chatter, while stderr is left alone so a real failure both surfaces and
# aborts the script under set -e. Deliberately no `|| true` here, because a failed write must not
# be followed by a restart and a summary that both report success.
runtime_exec "$CONTAINER_NAME" /opt/nifi/nifi-current/bin/nifi.sh \
  set-single-user-credentials "$NIFI_USER" "$NIFI_PASS" > /dev/null

# set-single-user-credentials only rewrites conf/login-identity-providers.xml on disk. NiFi reads
# that file at startup, and the already-running process keeps the old credentials in memory, so the
# new login does not work at all until the container restarts.
echo "==> restarting the container so NiFi reads the new credentials"
$RUNTIME restart "$CONTAINER_NAME"

echo "==> waiting for NiFi to come back up"
# Same readiness poll deploy-to-nifi.sh uses. Checked via `exec curl` inside the container against
# its own hostname, because NiFi's web listener binds there rather than to loopback (see
# nifi-lib.sh's nifi_lib_init). Any three-digit HTTP code counts as ready. That matters
# particularly here, since an unauthenticated 401 is the expected answer once the login has just
# been reset, and it still proves NiFi is serving.
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
  echo "warning: NiFi did not come back up within 5 minutes after the credential reset; check '$RUNTIME logs $CONTAINER_NAME'" >&2
  exit 1
fi
echo "==> NiFi is back up"

# The step this script exists for. Running `nifi.sh set-single-user-credentials` by hand leaves you
# still locked out of every script here, because they all recover credentials from the container's
# logs or from this file, and the log lines are long gone by the time anyone needs this.
#
# Saved only now that NiFi is confirmed back up. Saving before the restart is confirmed would
# record credentials for a NiFi that never came back.
nifi_lib_save_state "$CONTAINER_NAME" "$NIFI_CREDS_FILE_PATH" NIFI_USER NIFI_PASS

echo "==> NiFi single-user login reset."
echo "==>   username: $NIFI_USER"
echo "==>   password: $NIFI_PASS"
echo "==> simple-migration.sh, simple-troubleshoot.sh and reset-r2dfly-flow.sh will now recover these automatically."
