#!/usr/bin/env bash
# Host-side check of the external data drive. The server runs in a container and cannot see
# whether macOS mounted the SSD, so this script (run every minute by launchd, see
# com.kalshiterm.hostcheck.plist.template) writes the answer to a JSON file the server reads.
#
#   check-data-drive.sh DRIVE_PATH OUTPUT_JSON [--allow-plain-directory]
#
# DRIVE_PATH must be a mount point (e.g. /Volumes/KalshiData); with --allow-plain-directory any
# directory passes the mount test (for a data folder on the boot disk, and for tests).
# Exit status is always 0 when the report was written: "drive missing" is a result, not a crash.
set -u

drive="${1:?usage: check-data-drive.sh DRIVE_PATH OUTPUT_JSON [--allow-plain-directory]}"
out="${2:?usage: check-data-drive.sh DRIVE_PATH OUTPUT_JSON [--allow-plain-directory]}"
plain=false
[ "${3:-}" = "--allow-plain-directory" ] && plain=true

mounted=false
writable=false
free_pct=null
error=""

device_of() { df -P "$1" 2>/dev/null | awk 'NR==2 {print $1}'; }

if [ -d "$drive" ]; then
  if $plain || [ "$(device_of "$drive")" != "$(device_of "$(dirname "$drive")")" ]; then
    mounted=true
  else
    error="$drive exists but is not a mount point (drive unplugged?)"
  fi
else
  error="$drive does not exist"
fi

if $mounted; then
  probe="$drive/.kterm-hostcheck.$$"
  if (echo ok >"$probe" && sync && rm -f "$probe") 2>/dev/null; then
    writable=true
  else
    rm -f "$probe" 2>/dev/null
    error="cannot write to $drive"
  fi
  free_pct="$(df -Pk "$drive" 2>/dev/null | awk 'NR==2 && $2 > 0 {printf "%.1f", $4 * 100 / $2}')"
  [ -n "$free_pct" ] || free_pct=null
fi

# Keep the JSON valid whatever the path or message contains.
clean() { printf '%s' "$1" | tr -d '"\\' | tr '\n\t' '  '; }

tmp="$out.tmp.$$"
printf '{"checked_at": %s, "path": "%s", "mounted": %s, "writable": %s, "free_pct": %s, "error": "%s"}\n' \
  "$(date +%s)" "$(clean "$drive")" "$mounted" "$writable" "$free_pct" "$(clean "$error")" >"$tmp" \
  && mv "$tmp" "$out"
