#!/usr/bin/env bash
#
# The only thing the PC's pipeline key is allowed to do.
#
# Installed in ~/.ssh/authorized_keys as:
#
#   restrict,command="/home/kpennock/dashcam-db/scripts/dashcam_import_gate.sh" ssh-ed25519 AAAA... dashcam-pipeline
#
# "restrict" turns off port forwarding, agent forwarding, X11 and a pty, and the
# forced command means whatever the client asks for is ignored: only the three
# verbs below are possible, and anything else is refused. So a stolen key can
# deliver a batch of drives and nothing more -- no shell, no other commands, no
# reading files elsewhere on the server.
#
# Verbs (sent as the ssh command, read from SSH_ORIGINAL_COMMAND):
#
#   ping              -> "pong <host>", to check the key and the route work
#   status <uuid>     -> one word: waiting | loading | done | failed | unknown
#   receive <uuid>    -> reads a tar on stdin into ready/<uuid>, after checking it
#
set -euo pipefail

IMPORT_ROOT="${IMPORT_ROOT:-/srv/dashcam/import}"
INCOMING="$IMPORT_ROOT/incoming"
READY="$IMPORT_ROOT/ready"
DONE="$IMPORT_ROOT/done"
FAILED="$IMPORT_ROOT/failed"

refuse() {
  echo "refused: $*" >&2
  exit 1
}

# Split the requested command without letting the client run anything: no eval,
# no globbing, and only the first two words are ever looked at.
read -r -a argv <<< "${SSH_ORIGINAL_COMMAND:-}"
verb="${argv[0]:-}"
arg="${argv[1]:-}"

is_uuid() {
  [[ "$1" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]
}

case "$verb" in
  ping)
    echo "pong $(hostname)"
    exit 0
    ;;

  status)
    is_uuid "$arg" || refuse "status needs a batch id"
    if   [[ -f "$DONE/_batches/$arg.json" ]]; then echo done
    elif [[ -d "$READY/$arg" ]];               then echo waiting
    elif [[ -d "$INCOMING/$arg.partial" ]];    then echo loading
    elif compgen -G "$FAILED/*/$arg*" > /dev/null; then echo failed
    else echo unknown
    fi
    exit 0
    ;;

  receive)
    is_uuid "$arg" || refuse "receive needs a batch id"

    # Already here? Say so and read nothing, so a retry after a dropped
    # connection cannot load the same drives twice.
    if [[ -d "$READY/$arg" || -f "$DONE/_batches/$arg.json" ]]; then
      echo "already_received $arg"
      exit 0
    fi

    staging="$INCOMING/$arg.partial"
    rm -rf "$staging"
    mkdir -p "$staging"
    trap 'rm -rf "$staging"' ERR

    # --no-same-owner and --no-same-permissions: the tar comes from a Windows
    # machine and must not dictate ownership here. -C keeps extraction inside
    # the staging folder.
    tar -x --no-same-owner --no-same-permissions -C "$staging"

    if [[ ! -f "$staging/SHA256SUMS" ]]; then
      rm -rf "$staging"
      refuse "the delivery has no SHA256SUMS"
    fi
    if ! ( cd "$staging" && sha256sum --quiet -c SHA256SUMS ); then
      rm -rf "$staging"
      refuse "the delivery does not match its checksums"
    fi
    if [[ ! -f "$staging/batch.json" ]]; then
      rm -rf "$staging"
      refuse "the delivery has no batch.json"
    fi

    mkdir -p "$READY"
    mv "$staging" "$READY/$arg"
    trap - ERR
    echo "received $arg"
    exit 0
    ;;

  *)
    refuse "only ping, status <uuid> and receive <uuid> are allowed"
    ;;
esac
