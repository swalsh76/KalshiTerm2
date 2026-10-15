#!/usr/bin/env bash
# Host-side installer for the server machine (the Mac Studio). It does the steps that the
# container-run `kterm-server init` cannot: it checks what the host must provide, asks which
# volume holds the data, and installs the LaunchDaemons for the drive check and the LAN
# advertiser. Runs as your normal user; it asks for sudo once, for the few commands that need root.
#
#   install.sh check     [--key-file PATH] [--drive PATH] [--allow-plain-directory]
#   install.sh install   [--drive PATH] [--advertise | --no-advertise] [--port N] [--yes]
#                        [--dry-run] [--allow-plain-directory]
#   install.sh verify
#   install.sh uninstall [--yes] [--dry-run]
#
# Order of a stand-up (docs/PLAN.md, section 8.2):  check -> init -> up -> install.
# `check` is read-only and runs BEFORE init. `install` runs AFTER init (it needs .env, state/ and
# the TLS certificate that init creates). Over SSH, use `ssh -t` so sudo can ask for a password.
# --dry-run changes nothing: it prints the plists and the commands it would run.
# `verify` waits for the next report of the installed drive check (up to a minute) and says
# whether it sees a mounted, writable drive; use it after fixing a permission problem.
# The macOS launchd steps only run on macOS; check and --dry-run work anywhere with bash.
#
# Written for bash 3.2 (the version macOS ships): no mapfile, no associative arrays.
set -u

here="$(cd "$(dirname "$0")" && pwd)"
deploy_dir="$(dirname "$here")"
launchd_dir=/Library/LaunchDaemons
volumes_dir="${KTERM_VOLUMES_DIR:-/Volumes}"
hc_label=com.kalshiterm.hostcheck
adv_label=com.kalshiterm.advertise
tmp="" # the scratch directory of an install; global because the EXIT trap outlives the function

usage() { sed -n '2,/^set -u/{/^set -u/!p;}' "$0" | sed 's/^# \{0,1\}//'; }
die() { echo "error: $*" >&2; exit 1; }
warn() { echo "  warning: $*" >&2; }
good() { echo "  ok: $*"; }
problems=0
bad() { echo "  PROBLEM: $*" >&2; problems=$((problems + 1)); }

# ------------------------------------------------------------------------------ small helpers

device_of() { df -P "$1" 2>/dev/null | awk 'NR==2 {print $1}'; }
is_mount_point() { [ -d "$1" ] && [ "$(device_of "$1")" != "$(device_of "$(dirname "$1")")" ]; }

# A value from deploy/.env (written by init); empty when absent.
env_value() { [ -f "$deploy_dir/.env" ] && sed -n "s/^$1=//p" "$deploy_dir/.env" | head -n 1; }

# Paths end up inside sed expressions and XML; refuse characters that would break either.
unsafe_path() {
  case "$1" in
    *[\&\<\>\"\'\|\\]*) return 0 ;;
  esac
  [ "$1" != "$(printf '%s' "$1" | tr -d '\n\r')" ]
}

file_mode() { stat -f %Lp "$1" 2>/dev/null || stat -c %a "$1" 2>/dev/null; }

ask() { # ask PROMPT -> reads one line from stdin into $answer; fails at end of input
  printf '%s ' "$1" >&2
  IFS= read -r answer || { echo >&2; die "no answer (end of input); pass the options instead, see: install.sh help"; }
}

confirm() { # confirm PROMPT -> succeeds on yes; --yes answers yes
  $assume_yes && return 0
  ask "$1 [y/N]"
  case "$answer" in y | Y | yes | YES) return 0 ;; *) return 1 ;; esac
}

# ------------------------------------------------------------------------------ the checks

# `ssh host 'command'` runs a non-interactive shell that skips the login profile, so Docker's
# CLI is often not on its PATH even though it works in a terminal. Look where Docker puts it.
docker_dirs="${KTERM_DOCKER_DIRS:-$HOME/.docker/bin:/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin}"

find_docker() { # succeeds when docker is usable from here; puts its directory on PATH if needed
  local d old_ifs="$IFS"
  command -v docker >/dev/null 2>&1 && return 0
  IFS=:
  for d in $docker_dirs; do
    if [ -x "$d/docker" ]; then
      IFS="$old_ifs"
      PATH="$d:$PATH"
      docker_note=" (found in $d; it is not on this shell's PATH)"
      return 0
    fi
  done
  IFS="$old_ifs"
  return 1
}

docker_note=""
check_docker() {
  if ! find_docker; then
    bad "docker not found on PATH or in: $docker_dirs. If Docker Desktop is installed, add its directory to PATH in ~/.zshenv so that 'ssh host command' sees it too"
  elif ! docker info >/dev/null 2>&1; then
    bad "docker is installed but not running (start Docker Desktop and wait for it to be ready)"
  else
    good "docker is running$docker_note"
  fi
}

check_key() {
  local k="$1" mode
  case "$k" in /*) ;; *) bad "key path must be absolute (launchd and Docker do not expand ~): $k"; return ;; esac
  if [ -d "$k" ]; then
    bad "$k is a DIRECTORY, not a file. This is what Docker leaves behind when it is asked to mount a path that does not exist. If it is empty, remove it ('rmdir $k'), then put the key file there"
  elif [ ! -e "$k" ]; then
    bad "$k does not exist. Create a read-only API key in Kalshi and save its private key there"
  elif [ ! -s "$k" ]; then
    bad "$k is empty"
  elif ! head -n 1 "$k" | grep -q 'PRIVATE KEY'; then
    bad "$k does not look like a PEM private key (first line should be -----BEGIN PRIVATE KEY-----)"
  elif grep -q 'ENCRYPTED' "$k"; then
    bad "$k is passphrase-protected; the server needs an unencrypted key"
  else
    good "$k is a PEM private key file"
    mode="$(file_mode "$k")"
    case "$mode" in
      *00) ;;
      *) warn "$k has mode $mode; run: chmod 600 $k" ;;
    esac
  fi
}

check_drive() {
  local d="$1" probe free budget
  case "$d" in /*) ;; *) bad "drive path must be absolute: $d"; return ;; esac
  if unsafe_path "$d"; then bad "drive path contains a character not allowed in it (& < > \" ' | backslash, newline): $d"; return; fi
  if [ ! -d "$d" ]; then bad "$d does not exist (is the drive plugged in and mounted? see: ls $volumes_dir)"; return; fi
  if ! $plain && ! is_mount_point "$d"; then
    bad "$d exists but is not a mount point, so it is not a separate drive. Pick the drive itself (ls $volumes_dir)"
    return
  fi
  probe="$d/.kterm-install-probe.$$"
  if (echo ok >"$probe" && rm -f "$probe") 2>/dev/null; then
    good "$d is writable"
  else
    rm -f "$probe" 2>/dev/null
    bad "cannot write to $d"
    return
  fi
  free="$(df -Pk "$d" 2>/dev/null | awk 'NR==2 {printf "%d", $4 / 1048576}')"
  budget="$(env_value KTERM_STORAGE_BUDGET_GB)"
  budget="${budget%%.*}"
  if [ -n "$free" ] && [ -n "$budget" ] && [ "$free" -lt "$budget" ] 2>/dev/null; then
    warn "$d has about $free GB free, below the storage budget of $budget GB"
  elif [ -n "$free" ]; then
    good "$d has about $free GB free"
  fi
}

check_scripts_executable() {
  local f
  for f in check-data-drive.sh advertise.sh; do
    [ -x "$here/$f" ] || bad "$here/$f is not executable (chmod +x it)"
  done
}

# ------------------------------------------------------------------------------ arguments

cmd="${1:-help}"
[ $# -gt 0 ] && shift
key_file="" drive="" port="" advertise="ask" assume_yes=false dry_run=false plain=false
while [ $# -gt 0 ]; do
  case "$1" in
    --key-file) key_file="${2:?--key-file needs a path}"; shift 2 ;;
    --drive) drive="${2:?--drive needs a path}"; shift 2 ;;
    --port) port="${2:?--port needs a number}"; shift 2 ;;
    --advertise) advertise=yes; shift ;;
    --no-advertise) advertise=no; shift ;;
    --yes) assume_yes=true; shift ;;
    --dry-run) dry_run=true; shift ;;
    --allow-plain-directory) plain=true; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[ -n "$drive" ] && [ "$drive" != "/" ] && drive="${drive%/}"

# ------------------------------------------------------------------------------ check

do_check() {
  echo "Checking this host:"
  check_docker
  [ -n "$key_file" ] && check_key "$key_file"
  [ -n "$drive" ] && check_drive "$drive"
  check_scripts_executable
  if [ "$problems" -gt 0 ]; then
    echo >&2
    echo "$problems problem(s) found; fix them and run check again." >&2
    exit 1
  fi
  local name ip
  name="$(scutil --get LocalHostName 2>/dev/null)"
  ip="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null)"
  echo
  echo "All good. Next, from $deploy_dir:"
  echo "  KEY_FILE=${key_file:-/absolute/path/to/server_key.pem} docker compose -f docker-compose.setup.yml run --build --rm \\"
  echo "    init --key-id <key id> --host ${name:-<name>}.local --ip ${ip:-<LAN address>}"
  echo "then: docker compose up -d --build   and   host/install.sh install${drive:+ --drive $drive}"
}

# ------------------------------------------------------------------------------ install

choose_drive() { # sets $drive from a numbered list of the mounted volumes
  local entries="" n=0 v i
  for v in "$volumes_dir"/*; do
    [ -d "$v" ] || continue
    if $plain || { is_mount_point "$v" && [ "$(device_of "$v")" != "$(device_of /)" ]; }; then
      n=$((n + 1))
      entries="$entries$v
"
      echo "  $n) $v" >&2
    fi
  done
  [ "$n" -gt 0 ] || die "no mounted volume found under $volumes_dir. Plug in and mount the data drive, then run again (or give --drive PATH)"
  ask "Which volume holds the server's data? [1-$n, or type a path]:"
  case "$answer" in
    '' | *[!0-9]*) drive="${answer%/}" ;;
    *)
      [ "$answer" -ge 1 ] && [ "$answer" -le "$n" ] || die "choose a number from 1 to $n"
      i=0
      while IFS= read -r v; do
        i=$((i + 1))
        [ "$i" -eq "$answer" ] && drive="$v"
      done <<EOF
$entries
EOF
      ;;
  esac
}

render() { # render TEMPLATE OUT: strip the instruction comment, fill the placeholders
  sed -e '/^<!--$/,/^-->$/d' \
    -e "s|__DEPLOY_DIR__|$deploy_dir|g" -e "s|__DRIVE_PATH__|$drive|g" -e "s|__PORT__|$port|g" \
    "$1" >"$2"
}

run() { # run a root command (or show it, in a dry run)
  if $dry_run; then echo "  would run: $*"; else "$@"; fi
}

install_daemon() { # install_daemon LABEL RENDERED_FILE
  local label="$1" file="$2" dest="$launchd_dir/$1.plist"
  if $dry_run; then
    echo "----- BEGIN $label.plist (would be written to $dest) -----"
    cat "$file"
    echo "----- END $label.plist -----"
  fi
  run sudo launchctl bootout "system/$label" 2>/dev/null
  run sudo install -m 644 -o root -g wheel "$file" "$dest"
  run sudo launchctl bootstrap system "$dest" || die "launchctl bootstrap failed for $label (see the troubleshooting notes in host/$label.plist.template)"
}

privacy_help() {
  cat >&2 <<'MSG'

The drive is mounted but the background job cannot WRITE to it, while you (and root, run by
hand) can. On macOS this is the privacy protection for external volumes: it applies to
background jobs, not to your login session. To fix it:
  1. On the Mac itself (or over Screen Sharing; it cannot be done over ssh):
     System Settings > Privacy & Security > Full Disk Access > "+" > press Cmd-Shift-G >
     type /bin/bash > Add, and make sure its switch is on.
  2. Check again:  host/install.sh verify
To confirm that this is the cause first:
  sudo log show --last 15m --predicate 'process == "tccd"' | grep -i removable
The daemons stay installed meanwhile; nothing else needs to be redone.
MSG
}

wait_for_report() { # wait for a host.json written at or after $1 that shows a mounted, writable drive
  local state="$deploy_dir/state/host.json" start="$1" t mounted="" writable="" limit="${KTERM_VERIFY_TIMEOUT:-90}"
  echo "Waiting for a fresh drive report (up to $limit s)..."
  while [ $(($(date +%s) - start)) -lt "$limit" ]; do
    if [ -f "$state" ]; then
      t="$(sed -n 's/.*"checked_at": *\([0-9][0-9]*\).*/\1/p' "$state")"
      if [ -n "$t" ] && [ "$t" -ge "$start" ]; then
        mounted="$(sed -n 's/.*"mounted": *\([a-z]*\).*/\1/p' "$state")"
        writable="$(sed -n 's/.*"writable": *\([a-z]*\).*/\1/p' "$state")"
        [ "$mounted" = true ] && [ "$writable" = true ] && break
      fi
    fi
    sleep 2
  done
  [ -n "$mounted" ] || die "no fresh $state appeared; is the job running? (sudo launchctl print system/$hc_label) Or run host/check-data-drive.sh by hand to see why"
  echo "  host drive report: mounted=$mounted writable=$writable"
  [ "$mounted" = true ] && [ "$writable" = true ] && return 0
  if [ "$mounted" = true ]; then privacy_help; fi
  die "the drive check does not see a mounted, writable drive: $(cat "$state")"
}

do_verify() { # after a fix: wait for the next report from the installed drive check
  [ -d "$deploy_dir/state" ] || die "$deploy_dir/state not found: run init first"
  wait_for_report "$(date +%s)"
  echo "The drive check works."
}

do_install() {
  local start
  [ -f "$deploy_dir/.env" ] || die "$deploy_dir/.env not found: run init first (docs/PLAN.md, section 8.2; 'install.sh check' prints the command)"
  [ -d "$deploy_dir/state" ] || die "$deploy_dir/state not found: run init first"
  check_scripts_executable
  [ "$problems" -eq 0 ] || exit 1
  if unsafe_path "$deploy_dir"; then die "the deploy directory path contains a character not allowed in it: $deploy_dir"; fi

  if ! $dry_run; then
    [ "$(uname -s)" = Darwin ] || die "this installer drives macOS launchd; on another system install the daemons by hand (see the notes in host/*.plist.template)"
    [ "$(id -u)" -ne 0 ] || die "run this as your normal user, not root (it uses sudo where needed)"
    command -v sudo >/dev/null 2>&1 || die "sudo not found"
  fi

  if [ -z "$drive" ]; then
    $assume_yes && die "--yes needs --drive PATH (it will not guess which volume holds the data)"
    echo "Volumes on this host:" >&2
    choose_drive
  fi
  if $plain; then warn "using a plain folder, not a separate drive: the mount check is skipped, and a missing SSD will not be noticed"; fi
  echo "Checking the data drive:"
  check_drive "$drive"
  [ "$problems" -eq 0 ] || exit 1

  [ -n "$port" ] || port="$(env_value KTERM_API_PORT)"
  [ -n "$port" ] || port=8700
  case "$port" in '' | *[!0-9]*) die "the port must be a number" ;; esac

  if [ "$advertise" = ask ]; then
    if $assume_yes; then advertise=yes; else
      ask "Announce the server on the LAN so clients can find it ('kterm server discover')? [Y/n]"
      case "$answer" in n | N | no | NO) advertise=no ;; *) advertise=yes ;; esac
    fi
  fi
  if [ "$advertise" = yes ] && [ ! -f "$deploy_dir/secrets/tls_cert.pem" ]; then
    die "$deploy_dir/secrets/tls_cert.pem not found (init creates it), so the advertiser has nothing to announce; use --no-advertise or run init"
  fi

  tmp="$(mktemp -d)" || die "cannot create a temporary directory"
  trap 'rm -rf "$tmp"' EXIT
  render "$here/$hc_label.plist.template" "$tmp/$hc_label.plist"
  [ "$advertise" = yes ] && render "$here/$adv_label.plist.template" "$tmp/$adv_label.plist"
  if command -v plutil >/dev/null 2>&1; then
    plutil -lint "$tmp"/*.plist >/dev/null || die "a rendered plist is not valid (this is a bug; the files are in $tmp)"
  fi

  echo
  echo "This will install, as root:"
  echo "  $launchd_dir/$hc_label.plist   drive check of $drive every 60 s -> $deploy_dir/state/host.json"
  [ "$advertise" = yes ] && echo "  $launchd_dir/$adv_label.plist   LAN announcement of port $port"
  if ! $dry_run; then
    confirm "Go ahead?" || { echo "Nothing changed."; exit 1; }
    sudo -v || die "sudo failed (over SSH, connect with 'ssh -t' so it can ask for a password)"
  fi

  start="$(date +%s)"
  install_daemon "$hc_label" "$tmp/$hc_label.plist"
  [ "$advertise" = yes ] && install_daemon "$adv_label" "$tmp/$adv_label.plist"
  if $dry_run; then echo; echo "Dry run: nothing was changed."; exit 0; fi

  wait_for_report "$start"
  echo
  echo "Installed. Now:"
  echo "  docker compose exec ingest kterm-server status     (the host-drive item should be gone)"
  echo "  Reminder: Docker Desktop's disk image should live on $drive"
  echo "            (Settings > Resources > Disk image location). This script cannot check it."
}

# ------------------------------------------------------------------------------ uninstall

do_uninstall() {
  local label
  if ! $dry_run; then
    [ "$(uname -s)" = Darwin ] || die "this installer drives macOS launchd"
    confirm "Remove the KalshiTerm LaunchDaemons (drive check and advertiser)?" || { echo "Nothing changed."; exit 1; }
    sudo -v || die "sudo failed (over SSH, connect with 'ssh -t')"
  fi
  for label in "$hc_label" "$adv_label"; do
    run sudo launchctl bootout "system/$label" 2>/dev/null
    run sudo rm -f "$launchd_dir/$label.plist"
  done
  echo "Removed. state/host.json stays; without the drive check, 'status' will report the host report as stale."
}

case "$cmd" in
  check) do_check ;;
  install) do_install ;;
  verify) do_verify ;;
  uninstall) do_uninstall ;;
  help | -h | --help) usage ;;
  *) echo "unknown command: $cmd (try: install.sh help)" >&2; exit 2 ;;
esac
