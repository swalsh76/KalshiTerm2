#!/usr/bin/env bash
# Host-side mDNS advertisement of the KalshiTerm server. The server runs in a container behind
# Docker's VM and cannot announce itself on the LAN, so this runs on the host (as a LaunchDaemon
# on macOS, see com.kalshiterm.advertise.plist.template) and publishes
#
#   _kterm._tcp   port PORT   TXT: v=1  fp=<SHA-256 of the TLS certificate, 64 hex digits>
#
#   advertise.sh CERT_PEM PORT [--name NAME] [--print]
#
# The fingerprint is a CROSS-CHECK for `kterm server trust`, not a trust anchor: mDNS is
# unauthenticated, so anyone on the LAN can publish anything. The fingerprint the server's
# operator printed (`kterm-server cert show`) remains the authority.
#
# The certificate is re-read every minute, so `cert rotate` is re-advertised without a restart.
# --print shows what would be published and exits (used by the tests; publishes nothing).
set -u

SERVICE_TYPE="_kterm._tcp"
cert="${1:?usage: advertise.sh CERT_PEM PORT [--name NAME] [--print]}"
port="${2:?usage: advertise.sh CERT_PEM PORT [--name NAME] [--print]}"
shift 2
name="KalshiTerm $(hostname -s 2>/dev/null || hostname)"
print_only=false
while [ $# -gt 0 ]; do
  case "$1" in
    --name) name="${2:?--name needs a value}"; shift 2 ;;
    --print) print_only=true; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

case "$port" in
  '' | *[!0-9]*) echo "PORT must be a number" >&2; exit 2 ;;
esac

fingerprint() {
  openssl x509 -in "$cert" -noout -fingerprint -sha256 2>/dev/null \
    | sed -e 's/^.*=//' -e 's/://g' | tr 'A-F' 'a-f'
}

# The command that publishes the record; one argument per line so names with spaces survive.
publisher() {
  local fp="$1"
  if command -v dns-sd >/dev/null 2>&1; then
    printf '%s\n' dns-sd -R "$name" "$SERVICE_TYPE" local "$port" "v=1" "fp=$fp"
  elif command -v avahi-publish-service >/dev/null 2>&1; then
    printf '%s\n' avahi-publish-service "$name" "$SERVICE_TYPE" "$port" "v=1" "fp=$fp"
  else
    echo "need dns-sd (macOS) or avahi-publish-service (Linux, package avahi-utils)" >&2
    return 1
  fi
}

fp="$(fingerprint)"
if [ -z "$fp" ]; then
  echo "cannot read a certificate from $cert" >&2
  exit 1
fi

if $print_only; then
  publisher "$fp"
  exit $?
fi

child=""
stop() { [ -n "$child" ] && kill "$child" 2>/dev/null; exit 0; }
trap stop TERM INT

while true; do
  fp="$(fingerprint)"
  if [ -n "$fp" ]; then
    mapfile_args=()
    while IFS= read -r line; do mapfile_args+=("$line"); done < <(publisher "$fp") || exit 1
    "${mapfile_args[@]}" >/dev/null 2>&1 &
    child=$!
    published="$fp"
    # Stay while the record is current; leave to republish if the certificate changed or the
    # publisher died.
    while kill -0 "$child" 2>/dev/null && [ "$(fingerprint)" = "$published" ]; do
      sleep 60 &
      wait $!
    done
    kill "$child" 2>/dev/null
    wait "$child" 2>/dev/null
  else
    sleep 60 &
    wait $!
  fi
done
