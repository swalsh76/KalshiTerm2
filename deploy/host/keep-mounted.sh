#!/usr/bin/env bash
# Keeps an SMB share mounted. Run by launchd as a LaunchAgent, at login and every minute (see
# com.kalshiterm.keepmounted.plist.template), so the NAS share survives a login, a network change
# and a NAS restart without anyone typing a password.
#
#   keep-mounted.sh SHARE_URL MOUNT_POINT        e.g.  smb://stebe@navi/kdb  /Volumes/kdb
#
# - mounted and answering: does nothing (silently).
# - mounted but not answering (the stale mount left by a network change): forces it off, then
#   remounts it.
# - not mounted: mounts it with the password already saved in the login keychain. With no saved
#   password it does NOT try: the only result would be a password dialog on a screen nobody is
#   watching. It says so in the log and exits 2.
# Every wait is bounded, so a stuck mount cannot pile up jobs. The log is
# ~/Library/Logs/kalshiterm-keep-mounted.log (override: KTERM_KEEPMOUNTED_LOG).
set -u

url="${1:?usage: keep-mounted.sh SHARE_URL MOUNT_POINT}"
mp="${2:?usage: keep-mounted.sh SHARE_URL MOUNT_POINT}"
log="${KTERM_KEEPMOUNTED_LOG:-$HOME/Library/Logs/kalshiterm-keep-mounted.log}"
t_ls="${KTERM_LS_TIMEOUT:-8}"          # seconds to wait for the share to list its top level
t_mount="${KTERM_MOUNT_TIMEOUT:-40}"   # seconds to wait for a mount (or an unmount) to finish

mkdir -p "$(dirname "$log")"
say() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" >>"$log"; }
if [ "$(wc -c <"$log" 2>/dev/null || echo 0)" -gt 1000000 ]; then
  tail -n 200 "$log" >"$log.tmp" && mv "$log.tmp" "$log"
fi

within() { # within SECONDS COMMAND...: run it, and kill it if it takes longer
  local secs=$1
  shift
  perl -e 'alarm shift; exec @ARGV' "$secs" "$@"
}
is_mounted() { mount | grep -qF " on $mp (smbfs"; }
responds() { within "$t_ls" ls "$mp" >/dev/null 2>&1; }

if is_mounted; then
  responds && exit 0
  say "$mp is mounted but not answering; forcing it off"
  within "$t_mount" diskutil unmount force "$mp" >>"$log" 2>&1 \
    || within "$t_mount" umount -f "$mp" >>"$log" 2>&1
  if is_mounted; then
    say "could not unmount the stale share at $mp"
    exit 1
  fi
fi

host="$(printf '%s' "$url" | sed -E 's#^smb://([^@/]*@)?([^/:]+).*#\2#')"
if ! security find-internet-password -s "$host" >/dev/null 2>&1; then
  say "no saved password for $host in the keychain: not trying (it would only show a dialog)"
  exit 2
fi

say "mounting $url at $mp"
within "$t_mount" osascript -e "mount volume \"$url\"" >>"$log" 2>&1
if is_mounted && responds; then
  say "mounted at $mp"
else
  say "FAILED to mount $url"
  exit 1
fi
