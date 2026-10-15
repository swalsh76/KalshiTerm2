# Build log

The slice-by-slice record of how KalshiTerm was built: what each slice delivered, how it was
verified (rehearsals, measurements, live checks) and what it found. The design and the decisions
are in [PLAN.md](PLAN.md). This file keeps the numbering of what used to be PLAN section 10, so a
reference written "BUILD-LOG §10.6" is section 10.6 below, and a reference written "PLAN §8.2"
points into PLAN.md.

- [10.1 Phase 2 slices](#101-phase-2-slices)
- [10.2 Deferred work to loop back to (2.10) — not a blocker](#102-deferred-work-to-loop-back-to-210--not-a-blocker)
- [10.3 Phase 3 slices (server API)](#103-phase-3-slices-server-api)
- [10.4 Phase 3b slices (LAN features)](#104-phase-3b-slices-lan-features)
- [10.5 Phase 4 slices (analytics)](#105-phase-4-slices-analytics)
- [10.6 Phase 5 slices (client data side)](#106-phase-5-slices-client-data-side)
- [10.7 Phase 6 slices (client trading)](#107-phase-6-slices-client-trading)
- [10.8 Phase 7 slices (terminal UI)](#108-phase-7-slices-terminal-ui)
- [10.9 Cleanup C1: reuse and maintainability (decided 2026-10-14, by the user)](#109-cleanup-c1-reuse-and-maintainability-decided-2026-10-14-by-the-user)
- [10.10 Cleanup C2: first run, reproducible builds, CI hygiene, recovery-path tests (decided 2026-10-15, by the user)](#1010-cleanup-c2-first-run-reproducible-builds-ci-hygiene-recovery-path-tests-decided-2026-10-15-by-the-user)
- [10.11 Host: keep the NAS share mounted (decided 2026-10-15, by the user)](#1011-host-keep-the-nas-share-mounted-decided-2026-10-15-by-the-user)

## 10.1 Phase 2 slices

Each slice is its own branch, merged when CI is green. Database tests use a real TimescaleDB
container and run on the Linux CI job only (macOS/Windows runners have no Docker).

| # | Slice |
|---|---|
| 2.1 | Dev database stack: Compose + pinned TimescaleDB, Alembic scaffold, DB test harness |
| 2.2 | Reference data: `series`, `events`, `markets` + discovery poller (≈121k open ordinary markets, full refresh ≈14 s; combo markets arrive via lifecycle events, not polling) |
| 2.3 | Streaming core: batched `COPY` writer, `tickers` / `trades` / lifecycle tables, exchange + receipt timestamps, ingestion-lag metric, behaviour when the database is down |
| 2.4 | Combo-market storage (decision 10) |
| 2.5 (a: live add/remove on the feed — done; b: orderbook storage — done; c: watchlist controller — done) | Orderbook storage for the watchlist: snapshots + deltas, config-file watchlist + automatic top-N by volume (per-user watchlists arrive with the Phase 3 API). Semantics (decisions 12–13): **adding** a market subscribes it live (`add_markets`, fresh snapshot), copies its last 30 days of raw trades into `trades_watchlist`, and opens a `watchlist_periods` row; **while watched**, snapshots/deltas are stored and trades go to both trade tables; **removing** stops orderbook capture (`delete_markets`) and closes the period but deletes nothing — orderbook data ages out under normal retention, `trades_watchlist` is kept forever. Capture covers the union of all users' watchlists (reference-counted). Auto top-N entries stay ≥12 h once added; manual entries are never auto-removed. Needs a `kalshi-core` extension (live add/remove on `OrderBookFeed`) and a live probe of Kalshi's undocumented per-subscription market limit |
| 2.6 — done | Gap handling: gap log; trade backfill via REST on a `reconnected` event and on startup (tickers cannot be backfilled, so those gaps are recorded) |
| 2.7 (a: compression — done; b: candles + raw-data retention — done; c: settled-market slimming/tombstoning + `pins` — done) | Split because raw-data retention must not be switched on before the candles that replace it exist. **a:** columnar compression after 1 day. **b:** 1-minute and 1-hour continuous aggregates (trades OHLC/volume, ticker aggregates), then retention (trades 30 d, tickers 14 d, orderbook deltas 14 d, snapshots 90 d + 5-min downsample, `combo_large_trades` 365 d). **c:** decision 15 (full 30 d / slim to 90 d / delete unless watched or pinned) as a scheduled job, plus the `pins` table |
| 2.8 — done | Storage governor + `kterm-server status` (includes the data-drive checks: free space, missing/unwritable drive) |
| 2.9 — done (rehearsed on the MacBook; the Mac Studio itself waits for the SSD) | `kterm-server init`, production Compose, health checks, Mac Studio deployment (TLS certificate generation moves to Phase 3 with the API). Also built because a deployed server needs it: the periodic discovery loop (`ingest --discover-every`), `kterm-server health`, the host-side drive check, SIGTERM-driven graceful stop |
| 2.10 (a: volume calibration — **not started, not a blocker**; b: hardware qualification — **waiting for the SSD, not a blocker**) | Split because only half of it needs the new hardware (decision 11 puts the volume run on the MacBook). **a:** 48-hour run against production data, then retune retention defaults and settle the open questions. **b:** disk tests and recovery tests with the data on the external SSD, then switch the Mac Studio on as the production host. Full protocol and checklist: §10.2 |
| 2.11 (host installer) | Added 2026-10-14 by the user after the first stand-up on the Mac Studio: a host-side `deploy/host/install.sh` (preflight, data-volume prompt, LaunchDaemons for the drive check and the advertiser), plus a compose guard and a clearer `init` error for a wrong key path. Spec: §10.2 |

The server uses its own read-only Kalshi key (created at deployment time), separate from the
dev key.

## 10.2 Deferred work to loop back to (2.10) — not a blocker

Phase 2 is **code-complete at 2.9**: the whole server stack has been rehearsed on the MacBook
(PLAN §8.2). What remains is measurement and hardware qualification, which can happen whenever the
conditions are met. **Later phases must not wait for it and must not assume calibrated
numbers**; anything that depends on the results is listed below so it is not forgotten.

**2.10a — volume calibration: dropped (2026-10-09, by the user).** The 48-hour run is no
longer wanted. The decisions that waited on it (retention windows, tombstones, crypto 15-minute
books, compression layout re-check, Postgres tuning, the 500 GB budget and `auto_top_n`) stay
open and are decided from the live Mac Studio's own `status` history instead.

**2.10b — hardware qualification.** Condition: the 1 TB Thunderbolt 4 SSD is attached to the
Mac Studio. Checklist, in order (show the commands before running any of them on that host):
1. Format and mount the drive at a stable path; move Docker Desktop's disk image onto it.
2. Disk tests inside the database container, internal disk versus the SSD: `pg_test_fsync`
   and a write-heavy `pgbench`; record fsync latency. Check the drive's endurance rating.
3. Create the server's own read-only Kalshi key; `host/install.sh check`; `init`; `up` (PLAN §8.2);
   `host/install.sh install` (the host check LaunchDaemon, and the advertiser if wanted) and
   confirm `status` shows the drive. (2.11 builds the installer; until it is merged, install by
   hand from the notes in `deploy/host/*.plist.template`.)
4. Recovery tests while ingest runs: force-unmount or unplug the drive; confirm Postgres
   crash-recovers without corruption (data checksums are on), the gap log and backfill repair
   the trades, and `status` reports the outage and the return.
5. Reboot and power-loss behaviour: automatic restart after power failure, the drive mounted
   before Docker starts, ingest resumes by itself, UPS in place.
6. Record the results here, set the production budget, and make the Mac Studio the host.

**Interim results (2026-10-09, MacBook Pro, 1 TB Sabrent SSD over USB 3.1 at 10 Gb/s, which
is a slower stand-in for the Thunderbolt 4 drive).** The disk arrived as NTFS (Windows system
disk, mounted read-only, first seen on a USB 2 port at 480 Mb/s); it was erased and formatted
APFS. Steps 1 and 2 are done; the drive's own endurance rating is not checked.
- Host raw: sequential write 987 MB/s and read 900 MB/s (internal: 3.9 GB/s write); a durable
  8 KB write (`F_FULLFSYNC`) takes 4.4 ms (internal 3.9 ms), i.e. latency is close to internal
  while bulk throughput is capped by the link.
- Docker Desktop's disk image moved by setting `"DataFolder": "/Volumes/KalshiData/docker"` in
  `~/Library/Group Containers/group.com.docker/settings-store.json` with Docker quit (a fresh
  VM is created there: images and volumes start empty, so back up first). Dev database restored
  from a `backup` file onto it, counts matching the manifest.
- Inside the container: `pgbench -s 50` 1,674 TPS (1 client), 6,610 TPS (8 clients), durable
  commits; a 5M-row insert took 4.2 s plus 1.2 s checkpoint. Caveat: commits at 0.6 ms are
  faster than the host's 4.4 ms full flush, so the Docker VM does not appear to push every
  flush through to the flash; `pg_test_fsync` has not been run.
- Live production ingest (read-only) for 8 minutes: tickers about 645 rows/s and trades about
  182 rows/s, newest rows 0-1 s old, no errors, 22 jobs healthy. A dump takes 6.4 s (52.6 MB)
  and a restore of the 952 MB database was quick.
- **Step 4, drive-loss test (2026-10-09, 20:05 UTC, ingest running):** `diskutil unmount force`
  under load, 10 minutes later remount. Postgres PANICked at once on `fdatasync ... I/O error`
  (the right response: it does not carry on after a failed flush) and the Docker VM kept a
  stale handle to the image, so after remounting Postgres stayed at "rejecting connections"
  with I/O errors until **Docker Desktop was restarted**. Then crash recovery took 0.73 s
  (redo from 1/84E84710), the server accepted connections 2 s later, and a full scan of the large
  tables showed 0 checksum failures and 0 duplicate trades. Ingest, restarted, opened gap #36
  and backfilled 86,137 trades; trades are continuous minute by minute across the outage.
  **Lost for good: about 9.5 minutes of tickers (20:05:47 to 20:15:xx)** and, had a watchlist
  been running, orderbook deltas, because Kalshi has no history for them; the gap log only
  tracks trades. So: a lost drive needs an operator (remount, restart Docker), `status` and the
  host check must make that visible, and recovery itself is safe.
- **Sleep is a drive-loss event (2026-10-10, unplanned).** With Docker running from the USB
  SSD, closing the MacBook's lid ("Clamshell Sleep", 13:53:20) made Postgres PANIC on an
  `fdatasync` I/O error in the same second and left the Docker VM's filesystem read-only; the
  drive and link were fine afterwards. Same recovery as the unmount test (restart Docker
  Desktop; WAL replay, 0 checksum failures). **The Mac Studio must never sleep**: set
  `sudo pmset -a sleep 0 disksleep 0` and "Start up automatically after a power failure", and
  keep it on mains. The host drive check does not see sleep; it would show up as the drive
  check going stale and then as an unmount.
- Not yet done: steps 3, 5 and 6 (real server key, host LaunchDaemon, reboot and power-loss
  behaviour, power-fail restart) and a Thunderbolt link. The test above was a software
  unmount, not a physical unplug or power cut.

Exit criteria: measured numbers recorded in PLAN §9 and PLAN §11, retention defaults retuned or
explicitly confirmed, the open questions above closed, the Mac Studio running and healthy.

**2.11 — host installer (decided 2026-10-14, by the user).** The first stand-up on the Mac
Studio showed that the host-side steps are where a deployment silently goes wrong: a missing key
file made Docker mount an empty *directory* (`init` then failed with "'/input/key.pem' is a
directory", and the stray directory stayed behind), and the drive check and advertiser needed
hand-edited `sed` lines and root commands with a drive path nobody had recorded. `init` cannot fix
this (it runs in a container with no network and cannot see host mounts or run `launchctl`), so
the fix is a host-side script plus two small guards:

- `deploy/host/install.sh` (bash 3.2, which is what macOS ships; macOS/launchd only, other systems
  get a clear "install by hand" message). Run as the normal user: it asks for `sudo` once, for the
  few commands that need root. Subcommands:
  - `check [--key-file PATH] [--drive PATH]`: read-only preflight, runs **before** `init`. Docker
    reachable; the key is a regular, non-empty, unencrypted PEM file (a directory there gets the
    "Docker created this" explanation; a mode wider than 600 is a warning); the drive is a mount
    point, writable, with room for the budget. It never prints key contents. It ends by printing
    the `init` command for this host.
  - `install [--drive PATH] [--advertise|--no-advertise] [--port N] [--yes] [--dry-run]`: runs
    **after** `init`. Asks which volume holds the data (numbered list of `/Volumes`; a plain
    folder on the boot disk only with the explicit `--allow-plain-directory`), validates it,
    renders both templates (paths checked for characters that would break the plist), lints
    them, shows the plan, installs the `hostcheck` LaunchDaemon (and `advertise` unless
    declined), waits for the first fresh `state/host.json` and prints `mounted`/`writable`.
    Re-running is safe (boot out, then bootstrap). `--dry-run` changes nothing and prints the
    plists and commands.
  - `verify`: waits for the next report of the installed drive check and says whether it sees
    a mounted, writable drive (use it after fixing a permission problem); the same wait ends
    `install`, which polls for a good report instead of judging the first one.
  - `uninstall`: boots out and removes both daemons; leaves `state/` alone.
- `docker-compose.setup.yml` mounts the key with `bind.create_host_path: false`, so a wrong
  `KEY_FILE` fails at once ("bind source path does not exist") instead of creating a directory;
  and `init`'s `--key-file` error names that cause.
- **Found on the Mac Studio (2026-10-14): a launchd job cannot write to an external drive until
  macOS privacy (TCC, `kTCCServiceSystemPolicyRemovableVolumes`) allows it,** even though the
  same user, and root run from ssh, can. The drive check reported `mounted: true, writable:
  false`; `tccd` logged the denial. The script's `#!/usr/bin/env bash` line made `/usr/bin/env`
  the responsible program (a grant to `env` would be far too broad), so the hostcheck plist now
  starts `/bin/bash` explicitly and the operator grants Full Disk Access to `/bin/bash` (Mac's
  screen or Screen Sharing; not possible over ssh). `install` and `verify` print these steps
  when they see mounted-but-not-writable. Not yet decided: whether the drive check should keep
  a write probe at all, since the process that really writes the data is Docker Desktop (an
  app with its own permission); parked below until the Mac Studio run shows how it behaves.
- Reminded, not automated: Docker Desktop's disk-image location is an app setting the script
  cannot read reliably; `install` prints the reminder.

Tests: dry-run output (placeholders filled, valid plists, comment block stripped), refusals
(unsafe or relative paths, a non-mount-point, no `init` yet, no volume without a terminal), the
key checks, the volume menu, the compose guard and the `init` message; the tests run the script
with `/bin/bash` on macOS so the CI macOS jobs prove bash 3.2 compatibility. A real install
needs the Mac Studio, so that run is the user's (2.10b step 3) and is recorded there.
Exit criteria: the stand-up is `check` -> `init` -> `up` -> `install` with no hand-edited command.

## 10.3 Phase 3 slices (server API)

Same rules as Phase 2: one branch per slice, merged when CI is green, behaviour shown on real
data. Phase 3b (zeroconf, client profiles, TOFU pinning on the client side, `cert rotate`
prompts, backup/restore) follows and is not broken down yet.

Decisions taken for Phase 3 (2026-10-08; revisit any of them by saying so):
- **JSON carries prices and counts as decimal strings** (`"0.560000"`, `"18.50"`), like Kalshi
  and consistent with decision 14: no float ever touches a stored fact. Timestamps are ISO-8601
  UTC with microseconds.
- **The API is its own container** (`api`), beside `ingest`: either can restart without
  interrupting the other, and the API never holds the Kalshi key.
- **Nothing is served in plaintext off the machine**: the `api` command refuses a non-loopback
  bind without a TLS certificate. `/healthz` and `/readyz` are open and say nothing about the
  data; everything else needs a token. Interactive docs are off by default.
- Routes live under `/v1/`; list endpoints use keyset cursors (no offsets), with a hard page
  limit.

| # | Slice |
|---|---|
| 3.1 — done | API skeleton: FastAPI app factory, `/healthz`, `/readyz` (distinguishes database unreachable from not migrated; reveals nothing else), `kterm-server api` (uvicorn; `check_bind` refuses plaintext on any non-loopback address, tested against the real server), settings `KTERM_API_HOST/PORT/DOCS`, graceful SIGTERM. Not yet in Compose. Known: Starlette's test client warns that it will want `httpx2`; harmless today |
| 3.2 — done | Users and tokens: `users`, `api_tokens`, roles `read` / `admin`, `kterm-server user add/list/remove` and `token create/list/revoke` (the token goes alone to standard output, once), bearer-auth dependency, `/v1/me`, admin-only `/v1/status` (the `status` report), failed-auth throttling. Design: token = `kt_<id>_<256-bit secret>`; only the SHA-256 of the secret is stored; one row is fetched by id and compared in constant time; **every failure (missing, malformed, unknown, wrong, revoked, expired, removed user) returns the identical 401**; 10 failures in 60 s from one address locks it out with 429 + `Retry-After` (valid tokens included; in memory, bounded; configurable via `KTERM_AUTH_FAILURE_LIMIT/WINDOW_SECONDS`); `last_used_at` is written at most once a minute; tokens never appear in logs or `repr`s (tested). Found by the tests: Python's `\d` accepts non-ASCII digits, so token ids are now ASCII-only. Not yet built: token rotation, per-token scopes beyond the two roles, throttling by token (add if abuse appears) |
| 3.3 — done | TLS and deployment: `init --host/--ip` generates the self-signed certificate (ECDSA P-256, 365 days, not a CA, server-auth only; names = given hosts + `<host>.local` + localhost/loopback), `cert show` / `cert rotate`, `api` service in Compose (the only published port, 8700; least-privilege secret mounts), tested with a real TLS handshake: untrusted by default, trusted when pinned, hostname mismatch refused, TLS 1.1 refused, old pin fails after rotation; rehearsed on the MacBook over the LAN address |
| 3.4 — done | Reference and candle endpoints (all need a token). `GET /v1/markets` (filters `status`, `event`, `series`, `q` = case-insensitive substring of ticker or event title with `%`/`_` taken literally; ticker-ordered keyset paging via an opaque cursor, 100 per page, max 500), `GET /v1/markets/{ticker}` (adds rules and creation times), `GET /v1/events/{event_ticker}` (with its markets), `GET /v1/markets/{ticker}/candles` (`interval` 1m/1h, `source` trades/ticker, `start`/`end` with required time zones, `limit` up to 5,000, forward paging by cursor; without `start` the most recent bars, oldest first). Every market carries `stage` = full / slim / tombstone so empty rules are not mistaken for missing data. **All prices and counts are decimal strings built from the stored integers** (`"0.560000"`, `"18.50"`; tested at the bigint maximum), timestamps ISO-8601 UTC with microseconds, errors uniform (`{"error": ...}`, parameters named but never echoed). Bars exist only for minutes/hours with activity (no fill-forward); the newest bar can lag by the aggregate's refresh interval (1 min / 15 min). **Measured on 816,558 real markets:** pages 7-11 ms, filters 5-45 ms, worst-case substring search (a full scan) 0.1-0.2 s. Bugs found by the tests: Postgres `sum()` returns `numeric`, which the formatter did not accept (every candle volume would have failed; now exact for whole values and an error, never a rounding, otherwise); Python's lenient base64 decoder accepted `!!!` as a cursor (now strict) |
| 3.5 — done | Raw-data endpoints (token needed): `GET /v1/markets/{ticker}/trades` (the 30-day table plus the permanent copy, each trade once; keyset paging by time and trade id; most recent N without `start`), `.../ticks` (ticker history; the cursor counts rows already sent at a shared timestamp so ties page exactly), `.../orderbook?at=&depth=` (latest snapshot at or before `at` plus the net of the changes after it **in sequence**; `complete` is false for a REST-built snapshot, a recorded gap since the snapshot, or a change that removes more than existed; best bids/asks derived as 1 - the other side), `GET /v1/gaps`. Found by validating the orderbook against the exchange (see the corrections below) |
| 3.6 — done | Per-user watchlists: `user_watchlists` (removed with the user), `GET /v1/watchlist`, `PUT` / `DELETE /v1/watchlist/{ticker}` (idempotent; any role manages **its own** list, even admins see only theirs). **Limits (settings, defaults 50 per user and 200 distinct markets server-wide, `KTERM_WATCHLIST_MAX_PER_USER/TOTAL`)** are enforced inside one transaction under an advisory lock (without it three simultaneous users admitted 11 markets against a cap of 6; tested); a market someone already wants costs the server-wide cap nothing. Refused: combos, settled markets, unknown tickers. **The ingest controller watches the union** of the file, the auto top-N and every user's list; a market leaves only when nobody wants it and never within 12 hours of being added (decision 13 extended to users, so add/remove cycling cannot churn the feed); the best claim holds a market (file > user > auto); periods opened for users have source `user`. A light poll (15 s) reconciles early when any list changed, so a new entry is captured in seconds rather than at the 5-minute full cycle; `status` shows watched markets by source and how many users have lists. Live: 3 markets captured at startup, a 4th added while ingest ran was capturing and serving a real book (98 changes applied, bid 0.65 / ask 0.66) 12 s later; a removed market stayed captured (dwell) |
| 3.7 — done | Live push: `ingest_seq` write-order cursor (migration 0016), `/v1/stream` WebSocket. See "Live push" below |
| deferred | Prometheus `/metrics` (optional in PLAN §5.4) goes to the parking lot unless wanted; analytics and alert endpoints arrive with Phase 4 |

## 10.4 Phase 3b slices (LAN features)

**3b.2 as built.** `kterm` (entry point of `kalshiterm-client`, Typer) with `kterm config
add|list|show|use|token|remove` and a global `--profile/-p` (or `KTERM_PROFILE`). Profiles are
`profiles.json` in the platform config directory (`platformdirs`; override with
`KTERM_CONFIG_DIR`), written atomically with mode 0600, holding only name and address; the
first profile is the default. Addresses must be `https://` (plain `http://` only for loopback,
so a token never crosses the LAN in the clear). **Tokens live only in the OS keyring** (macOS
Keychain, Windows Credential Locker, Secret Service), entered with a hidden prompt or
`--token-stdin`, format-checked, never echoed or written to a file; without a keyring
(headless) `KTERM_TOKEN` supplies one, and there is no plaintext fallback. Everything is
validated before anything is written. Pinned certificate fingerprints join the profile in
3b.3. Checked against the real macOS Keychain (add, read, delete).

**3b.3 as built.** `kterm server trust` fetches the certificate the server presents (unverified,
for display only), shows its SHA-256 fingerprint, names and validity, and pins it after an
explicit yes (default No); `--fingerprint` pins without asking only if the fetched certificate
matches exactly (the scripting path; there is no blanket `--yes`). The pin is the certificate
file `certs/<profile>.pem` in the config directory plus its fingerprint in the profile; a
missing or altered file is refused. Connections use a TLS context that trusts **only** that
certificate (so name and expiry are still checked), and an `https` profile with no pin never
connects, so **the token is never sent to an untrusted or changed server**. A different
certificate is reported as "THE CERTIFICATE HAS CHANGED" with both fingerprints and refused;
an expired certificate or an uncovered name is reported as that, not as a change. Re-trusting
shows a warning and defaults to No. `config add --replace` with another address, and `config
remove`, drop the pin. `kterm server status [--json]` summarises `GET /v1/status` (admin token;
exit 1 when the report has problems) and explains 401/403/429. Verified against the real
`kterm-server api` over TLS, including a rotated certificate. Not done here (3b.4): mDNS
discovery and cross-checking the fingerprint a server advertises.

| # | Slice |
|---|---|
| 3b.1 — done | Backup and restore (server): measure a real dump and restore first; `kterm-server backup` / `restore`, retention (7 daily + 4 weekly), a scheduled `backup` service in Compose, loud failure and a mount check, status integration, and a restore drill on a real TimescaleDB container (compressed chunks, continuous aggregates, policy jobs) |
| 3b.2 — done | Client foundations: the `kterm` CLI, connection profiles in the user config directory, tokens in the OS keyring (`kterm config`, `--profile`) |
| 3b.3 — done | Trust-on-first-use pinning (the client shows the fingerprint, asks, pins the certificate; a changed certificate is refused until explicitly re-trusted) and `kterm server status` |
| 3b.4 — done | Discovery: a host-side mDNS advertiser (macOS `dns-sd`, Linux `avahi-publish`; the container cannot advertise through the Docker VM) carrying the certificate fingerprint in its TXT record; `kterm server discover` and a fingerprint cross-check on first connect |
| deferred | `kterm server logs` (mentioned in PLAN §8.6): parking lot |

## 10.5 Phase 4 slices (analytics)

**4.1 as built.** Migration 0018: `analysis_results` (hypertable, 1-day chunks, compressed after 1
day, dropped after 30; the CHECK allows exactly one of `market_id` / `event_ticker`) and
`analyzer_runs`. `kalshiterm_server.analytics`: `base` (the `Analyzer` protocol: `name`,
`interval` 10 s-1 day, `timeout`, `async run(ctx) -> list[Result]`; `Result` carries `value_e6`,
an `int`, and floats, bools and out-of-range values are rejected), `registry` (entry-point group
`kalshiterm.analyzers`; an entry point may name an instance, a class or a factory, and its name
must equal the analyzer's; a plugin that fails to import or breaks the rules is reported and
skipped, never fatal) and `runner` (one loop per analyzer; a restart does not re-run before the
interval has passed; a failed run retries after `min(interval, 5 min)`). A run records itself in
`analyzer_runs`, and its results, the `NOTIFY kterm_analysis` (`<analyzer>:<count>`) and its final
status are one transaction, so a rejected result means nothing is stored. A per-analyzer
advisory lock keeps two processes from running the same analyzer; a run left `running` by a
dead process is marked interrupted. Commands: `kterm-server analyze list|run NAME|serve`
(settings `KTERM_ANALYZERS_DISABLED`, `KTERM_ANALYZE_MAX_RESULTS`, `KTERM_ANALYZE_RETRY_MINUTES`);
the `analyze` Compose service has no route to the internet and no Kalshi key. `status` lists each
analyzer and raises a problem for a failed or timed-out last run, a plugin that did not load, or
no success within three intervals (at least 10 minutes); no analyzers at all is not a problem.
Verified with a throwaway plugin installed only through an entry point, against stored tickers.
**Trust:** plugins run in the analytics process with its database credentials, so installing one
is trusting it. Getting a plugin into the container is a custom image layer (`pip install` on top
of `kalshiterm-server`); sandboxing stays parked. No built-in analyzers yet: 4.2 adds the first.

Measured first (dev database, 2026-10-10): tickers carry `yes_bid`, `yes_ask` and their sizes,
so spread and top-of-book liquidity need no orderbook. Of 6,205 mutually exclusive events,
2,337 had every active market quoted in the last hour. Summing the latest quotes: 27 events had
YES **bids** summing over 100¢ (none over 105¢) and 70 had YES **asks** under 100¢; the largest
"opportunities" were stale or very wide (bids 114 / asks 189), so a usable check must require
simultaneous quotes, sizes and fees. The two directions are not equally valid: selling every
YES leg when bids sum over 100¢ is a real arbitrage for any mutually exclusive event (profit =
sum of bids - 100, before fees), but buying every YES leg when asks sum under 100¢ only works
if the event is also *exhaustive*, which Kalshi does not flag (most of the 70 are partial lists).

**Decisions (recommended defaults; say if you disagree).**
1. **Results are one narrow table** (`analysis_results`: analyzer, subject kind and id, metric,
   ts, `value_e6 bigint`), a compressed hypertable with retention. This keeps decision 14
   (no float, no JSON numbers); ratios such as volatility are stored as scaled integers.
2. **Arbitrage is bid-side only** (true for mutually exclusive events). The ask-side check is
   parked until there is a reliable "exhaustive" signal.
3. **The storage governor stays in the ingest process** (it works and is tested there), instead
   of moving into the analytics worker as PLAN §5.3 says. `kterm-server analyze` runs only analyzers.
4. **Fees are a configurable model**, default zero in code and set from Kalshi's published
   schedule when the arbitrage slice is built (it must be checked then, not assumed now).

**4.2 as built.** Three built-in analyzers in `analytics/builtin/market_metrics.py`, registered
through the same entry points as plugins: `market_quotes` (60 s), `market_moves` (60 s) and
`market_volatility` (300 s). **Universe: the watch union** (every open watchlist period plus pins,
`analytics/universe.py`). *Measured 2026-10-12:* 17,892 markets ticked in 10 minutes and 34,798 in an
hour, so analyzing every active market at one-minute cadence would store ~250 million rows a day;
the watch union reached 149 markets within an hour at `auto_top_n = 60` (each entry stays 12 h),
which is ~14 rows per market per minute, **~3 million rows a day**. Compressed (2.88 M rows of
10 metrics for 200 markets: 504 MB -> 22 MB) that is ~7.7 B/row on smooth data, so budget 10-15
B/row, **~1 GB for the 30-day retention**; the first day is uncompressed (~175 B/row with indexes,
~0.5 GB). Compute is not the constraint: 60-150 markets take well under a second per analyzer.
*Units:* every `value_e6` is millionths of the natural unit: prices, spreads, changes and volatility in
dollars (one cent = 10000), sizes and volumes in contracts, ages in seconds. Source facts are exact
(sizes are hundredths of a contract x 10000); derived numbers (mid rounded half up, averages, the
square root) are rounded to a millionth. *Metrics:* `bid ask mid spread spread_avg_15m quote_age
bid_size ask_size` (latest two-sided quote within 15 minutes), `last change_15m change_1h change_24h
volume_1h volume_24h` (baseline = the last tick at or before the window start), `volatility_1h` (the
per-minute root-mean-square change of the last price, a step across silent minutes scaled as a
random walk, at least 20 steps). A metric that cannot be computed honestly is absent; nothing is
carried over or invented. *Read from raw `tickers`, not `ticker_1m`:* the aggregate is
materialized-only and measured ~3 minutes behind the newest tick.
**Recording gaps (found while checking the design):** a quiet market and a market we were not
recording look the same in `tickers`, so a "24-hour change" across an ingest outage would be wrong.
A window metric is therefore reported only if the whole window lies after recording last resumed
(newest `ended_at` in `ingest_gaps`, ignoring `delta_shed`; never `started_at`, which is truncated
to the backfill window for long outages). Consequence: after any ingest restart, `change_24h` and
`volume_24h` return 24 hours later, 1-hour metrics after an hour, and volatility uses only the
trustworthy part of its hour. **Verified:** 301 values (quotes, 15-minute changes, volatility)
recomputed independently in Python from raw ticks on frozen data matched exactly; on live data
the first comparison showed 18 differences, all ticks that arrived between the analyzer's query
and the check. Runs are snapshots "as of the run's start": a tick stamped before that moment can
still be written a few seconds later.

**4.3 as built.** `arbitrage_bid` (60 s) in `analytics/builtin/arbitrage.py`; fee arithmetic in
`analytics/fees.py`. For each mutually exclusive event, the legs are the active markets whose
newest ticker has a positive bid and size, is newer than `max_quote_age` (default 600 s) and is
after `covered_since`; a leg without such a quote is left out, which can only lower the sum
(selling YES on any subset S is a worst case of `sum(bids in S) - 1`). Results per event (subject =
event ticker), only when `edge > min_edge`: `bid_sum`, `edge` (per set, before fees), `size` (the
smallest top-of-book size among the used legs), `fees`, `profit` = `size x edge - fees` (negative
when fees win), `legs`, `legs_total`, `oldest_quote_age`. It scans every market that ticked in the
window, not only watched ones (about 125 ms), and stores only positive-edge events.
*Fee model, from Kalshi's own documentation (API "Fee Rounding" and series `fee_type`, read
2026-10-12; the fee-schedule PDF itself rate-limited the fetch):* taker fee =
`ceil_6dp(M x 0.07 x C x P x (1 - P))` per fill, then Kalshi aligns the balance to $0.0001 (direct
members) or $0.01 (non-direct members) and rebates the overpayment across an order's fills. The
analyzer rounds each leg's order up to `KTERM_ARBITRAGE_FEE_ROUND_E6` (default 10000 = $0.01, the
conservative reading). `quadratic`, `quadratic_with_maker_fees` and
`quadratic_with_combo_maker_fees` all charge the taker formula to orders that take liquidity
(maker coefficients 0.25 / 0.5 only apply to resting orders); `flat` and the margin-program type
are not modelled, and their events produce no result. The multiplier comes from the stored
`series.fee_multiplier_e6` (the data holds 1.0, 0.5 and 0.0). Kalshi's worked example (1 contract
at $0.055 -> model fee $0.00363825, trade fee $0.003639) is a test. Not verified: whether
settlement itself is fee-free (assumed, as in the fee documentation's trade-only wording).
*Measured on live data (2026-10-12):* a stored-vs-live check of 400 markets whose Kalshi quote
and ours did not change across the read: 362 matched exactly (bid, ask, size), 14 matched in price,
24 differed, and for the differing ones Kalshi's order book sided with our ticker 14 times to 5 for
Kalshi's `/markets` object (the rest had moved). So the stored quote is the better view, but not
exact: **the output is an indication; the live book must be re-read before any trade** (Phase 6).
Ticker messages are never partial (0 nulls in 145,444 ticks). The 4 events checked by hand
(8 legs) matched Kalshi's order book exactly in price and size. **Result: 14 events had a positive
gross edge (bids summing to 100.1-102c), and every one lost money after fees** (for example 198
contracts, $1.98 gross, $6.93 of fees), i.e. no arbitrage in a liquid market after Kalshi's fees.
**4.4 as built.** Migration 0019: `alert_rules` and `alerts`. A rule is "when `<analyzer>.<metric>`
of this market or event is `gt` / `lt` / `abs_gt` a threshold" (`abs_gt` = further from zero
either way, for price moves). It is checked against the **latest analyzer results**, nothing is
recomputed. `analytics/catalog.py` lists the alertable metrics (price level, 15 m / 1 h / 24 h
change, 1 h / 24 h volume, spread, top-of-book sizes, volatility, arbitrage `profit` and `edge`),
with unit, allowed comparisons and freshness (three of the analyzer's intervals; volatility 15
minutes); plugin metrics are not alertable yet (parking lot), so a typo cannot create a rule that
never fires. **A market rule requires the market on the caller's own watchlist** (`not_watched`
otherwise), because analytics only cover watched markets; event rules are for arbitrage and need a
mutually exclusive event. Thresholds are decimal strings with at most six places, exactly.
*Firing:* the `alert_evaluator` analyzer (every 30 s, so it has the runner's lock, timeout and
`status` line) fires a rule when its condition **becomes** true (edge-triggered), at most once per
`cooldown_seconds` (default 900; the server minimum, `KTERM_ALERT_MIN_COOLDOWN_SECONDS`, is 300);
a missing or stale result counts as "not met" and re-arms the rule, which is what makes arbitrage
rules work (their result only exists while an edge does). A condition that returns inside the
cooldown fires when it ends, if still true. An alert is written with the rule update and a
`NOTIFY kterm_alerts` (`<user_id>:<alert_id>`) in one transaction, guarded so overlapping runs
cannot double-fire; it keeps a copy of what fired it, so it survives the rule being edited or
deleted. Alerts are pruned after 90 days. *Limits:* 50 rules per user (checked under a per-user
advisory lock; `KTERM_ALERT_RULES_MAX_PER_USER`). *REST (all per-user; another user's rule or
alert is a 404):* `GET /v1/alert-metrics`; `POST|GET /v1/alert-rules`,
`GET|PATCH|DELETE /v1/alert-rules/{id}` (a new threshold or re-enabling re-arms); `GET /v1/alerts`
(newest first, `unacknowledged`, `limit`, `next_cursor`); `POST /v1/alerts/{id}/acknowledge`
(idempotent). Pushing alerts live over `/v1/stream` is 4.5. Verified end to end through the real API
over TLS (rule refused until the market was watched, fired once at the stored price of $0.997, not
again on a second evaluation, acknowledged).

**4.5 as built.** *Alerts over `/v1/stream`:* add `"alerts"` to `channels` in `subscribe` (alone or
beside market channels) to receive your own alerts as `alert` events. They have **their own cursor
space**, `alerts.id`, named `alert_cursor` on events and `alerts_cursor` in `subscribed`,
`heartbeat` and `alerts_caught_up`, so it can never be mistaken for a market `cursor`; resume with
`"alerts_since": "<id>"` (replays your missed alerts in id order, then `alerts_caught_up`, then
live). Without it only alerts raised after the subscription are sent, so the recommended client
flow is `GET /v1/alerts?unacknowledged=true` for the backlog, then subscribe with `alerts_since`
set to the highest id held. *Why this is exactly-once:* the evaluator is the only writer of
`alerts`; it holds a per-analyzer advisory lock for its whole run and commits before releasing it,
so ids are assigned in commit order (the single-writer invariant of 3.7, again); a live
subscription starts after the highest **committed** id, so a transaction still open at that moment
is delivered when it commits (a test holds one open). Delivery is pulled page by page like the
market streams (`stream_page`), a slow client is dropped (1013) and resumes with `alerts_since`,
and the hub listens to `kterm_alerts` (with the 1-second poll as the fallback). Bad cursors are
rejected without changing the subscription (`alerts_since` without the channel, non-numeric, in
the future; `since` on an alerts-only subscription). Alerts older than 90 days are gone.
*Analytics REST (shared by all users, read-only):* `GET /v1/analytics/markets/{ticker}` (newest
value of every metric within `max_age_seconds`, default 1 hour, at most 24), `.../history?metric=
analyzer.metric` (newest first, `since` / `until` / `limit` / `next_cursor`),
`GET /v1/analytics/events/{event_ticker}`, and `GET /v1/analytics/arbitrage` (current
opportunities, best profit first, `min_profit`, with `oldest_quote_age_seconds`; indications only,
re-read the live book before trading). *Measured first:* on 2.6 million result rows (a day of 150
markets x 12 metrics) the latest-per-metric, per-pair and history queries all took ~4 ms, so
`DISTINCT ON` with the existing indexes is enough. `/v1/status` already carried the analyzer
state (4.1) and is now covered by an API test. Verified over TLS: a `wss://` client was pushed an
alert created by the real analyzer and evaluator running in separate processes.

**Phase 4 is complete.** Open items it leaves (parking lot): plugin metrics in alerts, wildcard and
relative-volume rules, alert delivery outside the client, ask-side arbitrage, plugin sandboxing.

| # | Slice |
|---|---|
| 4.1 — done | Framework: migration (`analysis_results`, `analyzer_runs`), the `Analyzer` interface and registry (entry points `kalshiterm.analyzers`; built-ins are registered the same way), `kterm-server analyze` runner (per-analyzer interval, per-run timeout, failures isolated and recorded), `NOTIFY` on new results, an `analyze` Compose service, `status` shows each analyzer's last run and problems; tested with a fake third-party plugin installed through an entry point |
| 4.2 — done | Market metrics: spread and top-of-book liquidity, realized volatility, price and volume change over windows, for markets that ticked recently; results stored per 4.1 |
| 4.3 — done | Cross-market arbitrage (bid side): simultaneous-quote window, sizes, executable size, fee model, one result per event; the fee schedule is checked against Kalshi's published one at the start of the slice |
| 4.4 — done | Alerts: per-user rules (price move, volume spike, spread, arbitrage profit) with per-user limits and cooldown, evaluator in `analyze`, `alerts` table, REST to create/list/acknowledge |
| 4.5 — done | API and push: `/v1/analytics/...` result queries, alerts pushed to their owner over `/v1/stream` with the existing cursor and resume rules, analyzer state in `/v1/status` |
| deferred | Alert delivery outside the client (email, mobile push), the ask-side arbitrage check, plugin sandboxing: parking lot |

## 10.6 Phase 5 slices (client data side)

**What exists:** the server's API is complete (reference data, candles, raw trades / ticks /
orderbook, gaps, watchlist, analytics, alerts, `/v1/stream`). The client has profiles, keyring
tokens, certificate pinning, discovery and one GET-only helper (`connection.request`: a new
connection per call, no retries, no streaming). **Why the library comes before the CLI:** Phase 6
(trading needs market data beside it) and Phase 7 (the local backend that serves the browser) are
both consumers of the same typed client, so the CLI is built as a thin consumer of it, not the
other way round.

**Decisions (recommended defaults; say if you disagree).**
1. **Async-first typed client** (`httpx` + `websockets`), response models in `pydantic` like
   `kalshi-core`, prices and counts as `Decimal` parsed from the server's decimal strings (never
   `float`). The CLI wraps it with `asyncio.run`.
2. **The client does not import the server package** (separate wheels; a workstation never has the
   server installed), so the response models are written for the client. **Drift is prevented by
   contract tests**: client tests run the real server app over a seeded database and require every
   endpoint's JSON to validate against the client's models (the workspace has both packages).
3. **Only idempotent GETs are retried** (network errors and 5xx, bounded backoff, honouring
   `Retry-After` on 429). Writes (watchlist, rules, acknowledge) are never retried silently.
4. **Live stream state lives in memory** (cursors for market data and for alerts); a dropped
   connection resumes with `since` / `alerts_since` and the library hands the caller each event
   exactly once. Persisting cursors across CLI runs is not needed (parking lot).
5. **CLI output:** `rich` tables for people, `--json` on every command for scripts; no CSV yet.

**5.1 as built.** `kalshiterm_client.client.ServerClient` (async; one `httpx` connection pool per
client) with `for_profile(profile)` (token from the keyring or `KTERM_TOKEN`; https needs a pinned
certificate and trusts only that one, so the token never reaches an untrusted or changed server;
loopback `http` needs no pin). Methods: `me`, `status`, `markets_page` / `iter_markets`, `market`,
`event`, `candles_page` / `iter_candles`, `trades_page` / `iter_trades`, `ticks_page` /
`iter_ticks`, `orderbook`, `gaps`; the iterators follow `next_cursor`, and the "latest N" forms
(no `start`) are a single page. `models.py` holds the pydantic models (`Decimal` prices and
counts parsed from the server's strings, aware datetimes, unknown fields ignored); `errors.py` the
taxonomy (`ConnectionProblem`, `CertificateChanged`, `ApiProblem` with `NotFound` / `Unauthorized` /
`Forbidden` / `InvalidRequest` / `RateLimited` / `ServerError`, each carrying the server's `code`
and extra detail). **Retries:** GETs only, on network errors and 5xx, after 0.5 / 1 / 2 s; a 429 is
waited out only if `Retry-After` is at most 5 s, otherwise reported at once; a certificate failure
is never retried; writes go through `_request(..., retry=False)` and are never retried. An answer
that is not JSON, or does not fit the model, becomes a `ConnectionProblem` that names the fields
but never echoes values. Naive datetimes are refused before any request. `kterm server status`
now runs on this client with its messages unchanged, and the old synchronous `connection.py` is
gone. **Contract tests** (`test_client_contract.py`, in the server suite because it needs the
database): the real app over real TLS with a pinned certificate and seeded data; every endpoint's
JSON must validate against the client's models and a walker fails on any server field the models
do not have (checked by removing a field: the test names it, e.g. `markets.yes_label`).
Verified on the live dev server: markets paged, a market's detail, ticks, an hourly bar and the
gap log fetched through a pinned connection.

**5.2 as built.** `ServerClient` gains: `watchlist()`, `watch(ticker)` / `unwatch(ticker)` (True if the
list changed); analytics `market_results`, `market_history_page` / `iter_market_history`,
`event_results`, `arbitrage(min_profit=...)`; alerts `alert_metrics`, `create_alert_rule`,
`alert_rules`, `alert_rule`, `update_alert_rule`, `delete_alert_rule`, `alerts_page` /
`iter_alerts`, `acknowledge_alert`. Models: `Watchlist`, `AnalyticsValue`, `MarketResults`,
`EventResults`, `MetricHistory`, `Opportunity`, `AlertMetric`, `AlertRule`, `Alert`, `AlertPage`.
**Writes are never retried** (watch, unwatch, create / update / delete rule, acknowledge go
through `_request(..., retry=False)`; a test fails each of the six against a 500, a 503 and a
dropped connection and requires exactly one request): if an answer is lost the caller looks and
decides, because a retried create could make the rule twice. **Numbers are never floats:**
thresholds and `min_profit` take a `Decimal` or a string; a `float` (or a bool, int or None) is a
`TypeError`, more than six decimal places or NaN / infinity a `ValueError`, both before any request;
numbers are sent in plain notation (`Decimal("1E+2")` goes out as `100`). `update_alert_rule` sends
only the fields given (so `enabled=False` is sent, `None` is not). The server's reason for a
refusal is kept: `InvalidRequest.code` / `.extra` (`not_watched` with the ticker,
`cooldown_too_short` with the minimum). Contract tests grew to 16 (real server, real TLS): the
walker now covers watchlist, analytics, catalog, rules and alerts (each new model checked by removing
a field and seeing the test name it), plus the full lifecycles: watchlist, rule create / change /
delete with the server's refusals, alert paging and acknowledgement scoped to the owner,
analytics values exact. Verified on the live dev server: a rule refused until the market was
watched, then created; the real analyzers and evaluator fired it; the library read the analytics,
the alert (value 0.997), acknowledged it twice, raised the threshold (re-armed) and cleaned up.

**5.3 as built.** `StreamClient` (`stream.py`; `for_profile` or explicit URL / token / TLS context):
`async for event in stream` yields **each market event and each alert exactly once, in order,
across reconnects**. *Two cursors, never mixed:* `cursor` (market data) and `alert_cursor`
(alerts); they advance when an event is handed to the caller and on the server's `heartbeat` and
`caught_up` / `alerts_caught_up` messages (sent only when everything up to that point was delivered
or filtered). A reconnect resubscribes with `since` / `alerts_since` set to them, so the server
replays exactly what was missed; as a safety net any event whose cursor is not strictly greater
than the last delivered is dropped and counted (`stats.duplicates_suppressed`), except orderbook
`replay` events (no cursor). *Backpressure:* the socket is read only when the caller asks for the
next event. *Resync:* `gap` (`cursor_too_old`) or a cursor from the future (a restored server)
yields `Resync` (state built from the stream, i.e. orderbooks, is stale) and resubscribes from now
so fresh books follow; the alerts cursor is kept, so no alert is lost; trades and ticks in the
gap must be fetched over REST if they matter; a server that keeps refusing a cursor is given up
on. *Reconnects:* dropped connections, silence for `idle_timeout` (default 45 s; the server's
heartbeat is 15 s by default, but its pump wakes once a second, so the real period is at least
1 s), close codes 1013 / 4429 / "too many connections" and failed connects retry after
`min(cap, 0.5 x 2^n)` s with jitter (50-100%), restarting after a connection that subscribed;
`Disconnected` says why and when. *Fatal, never looped:* unauthorized (4401 or an error frame),
a refused subscription (`InvalidRequest` with the server's code), a client the server cannot parse
(4400 / 1009), a message that does not fit its model (names fields, never values), and a
certificate problem (`CertificateChanged` / the same explanations as REST; the TLS diagnosis is now
shared). *Close wait:* a clean close waits for the server's reply, which a dead network never
sends; websockets' default of 10 s would delay noticing an outage by that much, so
`close_timeout` is 2 s (found by the blackhole test). Events: `TradeEvent`, `TickerEvent`,
`BookSnapshotEvent`, `BookDeltaEvent`, `AlertEvent`, and the control events `Connected`
(`resumed`), `CaughtUp`, `Resync`, `Disconnected`, `WatchlistChanged`, `UnknownEvent` (a newer
server's message type is kept, not dropped). `BookTracker` (`book.py`) keeps exact `Decimal`
orderbooks from snapshots and deltas (ignores deltas already in the snapshot by `sid` / `seq`,
flags a removal larger than the book as `inconsistent`; `reset()` on `Resync`). *Tests:* 32 unit
tests on a scripted socket (every protocol path), 7 for `BookTracker`, and 8 integration tests on
the real server over real TLS through a TCP proxy that can sever, refuse or blackhole: market
data, alerts, and both together survive repeated cuts exactly once and in order with zero
suppressed duplicates (server-side exactness), an orderbook rebuilt from the stream equals the
server's REST book, a replay-horizon overrun yields one `Resync` and a fresh book, wrong token and
changed certificate raise without retrying, and heartbeats keep a quiet connection alive while a
blackholed one is noticed. Sabotaging the resume (never sending `since`) fails all three
exactly-once tests. *Verified live:* ingest running, three busy markets streamed, the API process
killed and restarted: 7 retries over 6.8 s, `resumed=True`, **119 events delivered = 119 rows in
the database for those markets over the cursor range, 57 of them committed while the API was
down**, no repeats.

**5.4 as built.** Seven top-level commands, thin consumers of `ServerClient`, each with `--json`:
`kterm markets [QUERY] [--status --event --series --limit]` (follows pages up to `--limit`, at most
5,000: the server holds ~800,000 markets and the CLI never tries to fetch them all),
`market TICKER [--rules]` (detail, last quote and analytics; the optional pieces may fail without
failing the command), `candles TICKER [--interval --source --limit --start --end]`, `trades TICKER`,
`book TICKER [--at --depth]` (warns when the server says the book may be incomplete, with the reasons),
`analytics [TICKER | --event E] [--metric analyzer.metric]`, `arbitrage [--min-profit]`. Code:
`cli_common.py` (profile, `run_with_api`, console, JSON), `views.py` (pure model -> `rich` functions),
`data_cli.py` (commands). **Rules that matter:** numbers are rendered from `Decimal`, never rounded
(`0.560000` shows as `0.56`, `0.123456` stays whole; prices keep at least two decimals); every string
from the server goes through `rich.text.Text`, so a title such as `Will [bold]X[/bold] win?` cannot
change the display (tested); times are UTC and labelled, and a time typed without an offset is read
as UTC; the console is 120 columns wide when piped (rich assumes 80), so tickers are never cut;
`--json` is the typed models with decimals as exact strings (a test compares it with the server's
REST answer field by field). Bad options are refused before any request (exit 2), server and
network problems are one line (exit 1), and the token never appears in output. *Tests:* 25 on the
formatting and rendering, 32 on the commands against a scripted HTTPS server (including the
requests each command sends), 6 on the real server over TLS. Removing the stop-at-limit check makes
the list command loop forever on an endless server, i.e. the guard is exercised; reading a naive
time as local time fails two tests. *Verified live on the dev server:* the real `kterm` printed a
market list, a market's detail and last quote, hourly candles, trades, a 5-level book (with the
server's incompleteness warning, correct because ingest had restarted after the snapshot),
analytics, arbitrage and a not-found error.

**5.5 as built.** *Watchlist:* `kterm watchlist [list | add T... | remove T...]` (the listing shows
whether the server is capturing each orderbook yet, and how many of the 50 places are used).
*Alerts:* `kterm alerts [list [-u] | ack IDS | ack --all | rules | metrics | add METRIC SUBJECT OP
THRESHOLD | set ID | rm IDS | watch]`. `alerts add` asks the server's catalog whether the metric is
about a market or an event, so one subject argument is enough; it refuses an unknown metric or a
bad number before any request, and turns the server's refusals into what to do next
(`not_watched` -> "kterm watchlist add T", `invalid_comparison` -> the allowed ones,
`cooldown_too_short` -> the minimum, `rule_limit` -> "remove one"). Commands that take several
tickers or ids report each one, carry on after a failure, and exit 1 if any failed; every write
goes through the never-retried path. *Live:* `kterm watch TICKER... | --watchlist [--book]
[--no-trades] [--no-ticker] [--count N] [--json]` prints one line per trade and ticker update
(millisecond UTC times), or one JSON object per line; `--book` keeps a `BookTracker` live and shows
it on a terminal (redrawn at most 4 times a second), or prints it once at the end when piped.
`kterm alerts watch [--no-backlog] [--count N] [--json]` follows the 4.5 recommended flow: read
the newest alert id, list the unacknowledged backlog oldest first, then stream with
`alerts_since` set to that id (new `StreamClient(alerts_since=...)` parameter), skipping any id
already printed. Human notes (connecting, "disconnected (...); retrying in Ns", "reconnected
(resumed, nothing missed)", resyncs) go to stderr, so stdout stays pipeable; Ctrl-C stops quietly.
`--count` counts data events. *Design fix found by the tests:* `run_with_api` turned every
`ProfileError` (which includes the server's refusals) into a plain one-liner, so the helpful
messages above never ran; it now takes an `explain` function. *Tests:* 20 on the REST commands
against a scripted server (verbs, paths and exact JSON bodies; the fake server now handles
PUT / POST / PATCH / DELETE), 8 more view tests, 2 for `alerts_since`, and 9 on the real server
with a producer thread committing data while the commands run: watch output, JSON in cursor
order, a cut connection mid-command with every committed row between the first and last delivered
cursor shown exactly once (trades and alerts), the final book, backlog ordering (reversing it
fails the test), and a wrong token. *Verified live (Phase 5 acceptance):* with real ingest,
`watchlist add` of the busiest market, `alerts add market_moves.last`, the real analyzers and
evaluator firing it (value 0.21), `alerts watch` printing the backlog at once, `ack --all`, live
trades at millisecond times, a live 5-level book, and **a real API kill and restart during `kterm
watch`: five retries over ~6 s with growing waits, "reconnected (resumed, nothing missed)", 2,500
trades delivered = 2,500 in the database for that market and cursor range**.

**Phase 5 is complete.** The library (`ServerClient`, `StreamClient`, `BookTracker`) is what Phase 6
(market data beside trading) and Phase 7 (the local backend behind the browser) build on. Left in the
parking lot: CSV export, a local cache / offline mode, persisted stream cursors, shell completion.

| # | Slice |
|---|---|
| 5.1 — done | API client foundation: `ServerClient` (pinned TLS, token, connection reuse, timeouts, retries for GETs, the error taxonomy of `connection.py`, pagination iterators) and typed models + methods for `me`, `status`, markets (list / search / get), events, candles, trades, ticks, orderbook and gaps; contract tests against the real server; `kterm server status` moves onto it |
| 5.2 — done | The rest of the API in the client: watchlist, analytics (market latest / history, event, arbitrage) and alerts (metrics, rules create / list / change / delete, alerts list / acknowledge), with contract tests |
| 5.3 — done | Live stream client: `StreamClient` (pinned `wss`, header auth, reconnect with backoff, heartbeat timeout detection, `since` / `alerts_since` resume with exactly-once delivery, `gap` handling), a `BookTracker` that keeps an orderbook from `book_snapshot` / `book_delta` events; tested against the real server over a real socket with forced disconnects (no duplicates, no gaps) |
| 5.4 — done | CLI data views: `kterm markets`, `market`, `candles`, `trades`, `book`, `analytics`, `arbitrage` (tables and `--json`) |
| 5.5 — done | CLI account and live views: `watchlist`, `alerts` (rules, list, acknowledge), `watch` (live tickers / trades / book) and `alerts watch`; an end-to-end check against a live server |
| deferred | CSV export, a local response cache or offline mode, persisted stream cursors, shell completion: parking lot. Trading is Phase 6 and the browser UI Phase 7 |

## 10.7 Phase 6 slices (client trading)

**What exists:** the typed data client (Phase 5), `kalshi-core` with signed REST, public WebSocket
channels and a read-only transport guard (`ReadOnlyViolation`), a read token bucket only, and a key
loader that accepts only unencrypted Ed25519 PEM. **No write code exists.** Trading goes client ->
Kalshi directly, works with the server offline, defaults to demo in code, and production writes need explicit opt-in (decision 6).

**Facts from Kalshi's docs (2026-10-13; slice 6.1 re-verifies each against the demo before it is
relied on).** Create Order V2 takes `ticker`, `side` (`bid` / `ask`, not yes / no), `count` and
`price` as fixed-point strings, `time_in_force` (`fill_or_kill`, `good_till_canceled`,
`immediate_or_cancel`), `self_trade_prevention_type` (`taker_at_cross` / `maker`), and optionally
`client_order_id` (idempotency), `expiration_time`, `post_only`, `reduce_only` (requires IOC),
`cancel_order_on_pause`, `subaccount`, `order_group_id`; it costs 10 tokens. Also V2: batch create,
cancel, batch cancel, amend, decrease, queue positions. `DELETE /portfolio/events/orders` cancels
every resting order (2 tokens) and **"newly placed orders may also be cancelled during the minute
after the request"**. Positions: `position_fp`, `market_exposure_dollars`, `realized_pnl_dollars`,
`fees_paid_dollars`. Balance: `balance` (cents) and `balance_dollars`, `portfolio_value`.

**Decisions (recommended defaults; say if you disagree).**
1. **Risk layer first, as a pure component**: `RiskPolicy` + `check(order, snapshot)` with no I/O,
   fully tested before any order code exists. Every write path takes an `ApprovedOrder` that only
   the risk layer can construct, so an order cannot bypass it by construction.
2. **Fail closed**: if the position or balance snapshot is missing, stale (default 10 s) or the
   private WebSocket is down, orders are refused (cancels and the kill switch are never refused).
3. **Limits (all in a local `risk.toml`, conservative defaults)**: max contracts per order, max
   contracts per market, max notional per order (price x count incl. worst-case fee), max daily
   loss (realized P&L today plus open-order exposure), max open orders; a limit can only be
   loosened by editing the file, never by a CLI flag.
4. **Money is `Decimal` / fixed-point end to end**; prices and counts are sent as Kalshi's strings,
   never floats (the Phase 5 rule). Fees come from `kalshi-core`'s fee model for the worst case.
5. **Write transport**: the GET-only guard is replaced, not deleted, by an explicit
   `TradingCapability` object: only `kalshiterm-client`'s order service can mint one, a plain
   `KalshiRestClient` stays GET-only, and a test fails any write without the capability. A
   separate **write token bucket** (default 100 tokens/s x 0.9 margin, costs per endpoint) is
   added. Writes are never retried blindly: a lost answer is resolved by looking up the
   `client_order_id`, which is always set (UUID) so a retry cannot double-order.
6. **Environment gating (revised 2026-10-13: live account, no demo).** Decided by you: every
   step is verified on the **live** account (about $10), not the demo. Code and config still
   default to demo, and production writes stay explicit opt-in: `KALSHI_ENV=production` **and**
   `trading.production_enabled = true` in `risk.toml` (which you set; there is no code constant
   refusing it) **and** a per-order typed confirmation (ticker + side + count + price). Because the
   account is small and real, the shipped `risk.toml` defaults are tiny (order notional <= $1, open
   exposure <= $3, daily loss <= $2, open orders <= 3) and the live checks never loosen them. Each
   live write check is run by me only after I show you the exact order and you say go.
7. **Trading key**: a separate key from the data key, never on the server, stored outside the repo
   (`~/.kalshiterm/kalshi_trading_key.pem`, mode 600, checked on load). The unencrypted-PEM-only
   loader stays: encrypted-key + keyring-passphrase support (PLAN §6.2) is deferred to the
   parking lot unless you want it now.
8. **Dry-run** (`--dry-run`, and a profile default) runs the full pipeline (risk, rounding,
   signing-free payload build) and prints the order that would be sent; it makes no request.
9. **Kill switch** (`kterm kill`): cancel-all then verify by listing resting orders and re-cancel
   until none remain for the "next minute" window the docs warn about; also tripped automatically
   when the daily-loss limit is hit.
10. **Private channels** (`fill`, `user_orders`, `market_positions`) feed an in-memory
    `AccountTracker` (orders, positions, balance) seeded from REST and reconciled on every
    reconnect, with the same exactly-once-by-id discipline as `StreamClient`.
11. **Interface**: Typer CLI only (`kterm buy|sell|cancel|amend|orders|positions|balance|fills|kill`);
    the browser UI is Phase 7. Order commands print the risk verdict before sending.
12. **Testing**: scripted fake Kalshi (REST + private WS) for logic and the offline test; a
    **server-offline test** (no server process, profile unreachable) places, amends and cancels
    on the fake; live checks are opt-in (`KALSHI_INTEGRATION=1`), use the trading key, and are
    never run in CI. They cost real (cent-level) money and are approved order by order.
13. **Multi-shard (decided 2026-10-13).** Orders may go to any shard. Measured: of 8,000 open
    non-combo markets, 6,551 are on shard 0, 1,115 on shard 3 (tennis / baseball / basketball) and
    334 on shard 2 (crypto & commodities); shard 3 carried about as much 24 h volume as shard 0
    (694k vs 761k), so single-shard would have excluded half the activity. Every market has
    `exchange_index`; combos are shard 1. The risk layer therefore tracks balance, exposure and
    open orders **per shard**, with the daily-loss limit and kill switch **account-wide**; an order
    is **funded automatically** (decision 14), so the user never thinks about shards. Tests run the
    fake exchange with all four shards, including one where a shard has `trading_active` false.
14. **Automatic shard funding (decided 2026-10-13).** Kalshi's own web / app does this; the API
    does not ("programmatic traders must preallocate collateral on a given exchange shard"), so the
    client does it. Mechanism: `POST /portfolio/intra_exchange_instance_transfer` (`amount` in
    centicents, `source_exchange_shard`, `destination_exchange_shard`, `client_transfer_id` UUID
    for idempotency). **Not** Kalshi's target-allocation setting: it is a persistent account
    setting that also changes web behaviour and rebalances on a timer. Rules:
    a. The risk layer's pure `check` returns, besides approve / refuse, a **funding plan**: the
       smallest transfer(s) from shards with surplus (largest free balance first, never from a
       shard that has resting orders needing it) so the target shard covers the order's worst-case
       cost plus fee. No plan, no order: if total free balance cannot cover it the order is
       refused with the shortfall.
    b. The order service executes the plan, then places the order. A transfer never exceeds what
       the order needs (plus nothing extra), is shown in the risk verdict and the confirmation
       prompt ("will move $2.0000 from shard 0 to shard 3, then buy ..."), and is covered by the
       same per-order confirmation; there is no separate prompt.
    c. Money never leaves the account, but a transfer still counts as a write: it goes through
       the risk layer, the write bucket, the audit log, and is refused when the order itself would
       be. A transfer cap per order and per day (`risk.toml`) bounds a bug.
    d. Failure handling: Kalshi documents cross-index transfers as non-atomic ("up to three
       steps; completed steps are not undone") and, **measured live in 6.7, does not deduplicate on
       `client_transfer_id`** (the identical request sent twice moved the money twice), so a
       transfer is sent at most once; after an unclear answer it stays recorded as pending and the
       balance is read before anything else. Never retried blindly. If the order then fails, funds simply stay on that shard; the next order
       pulls them wherever needed. No sweep-back is built (the web app does none either).
    e. Dry-run prints the plan and makes no request. The kill switch never needs funding.
    f. Slice 6.7 checks on the live account that an unfunded shard rejects an order (how the
       rejection looks is undocumented) and that a $0.01-scale transfer round-trips.

| # | Slice |
|---|---|
| 6.1 — done | Measure first: a read-only look at the demo's order, position, balance and fill shapes (with the existing dev key, GET only) plus a re-check of the docs facts above; write the as-built models and the open questions into this section. No write code |
| 6.2 — done | Risk layer: `RiskPolicy` (per-shard balances, funding plan, transfer caps), `risk.toml` loading and validation, `check` as a pure function, `ApprovedOrder`, fail-closed rules, worst-case fee/notional math; exhaustive tests including property tests (no order that exceeds a limit is ever approved) |
| 6.3 — done | Account read side: `kalshi-core` portfolio models + GET methods (balance, positions, orders, fills, settlements, queue positions), trading key loading and permission check; production stays read-only |
| 6.4 — done | Write transport: `KalshiTradingClient` (separate, route-allowlisted), write token bucket, create / cancel / batch cancel / amend / decrease with idempotent `client_order_id` recovery, and the shard transfer call with `client_transfer_id` recovery (decision 14); the guard test is rewritten to prove writes need the capability and production is refused |
| 6.5 — done | Private WebSocket channels and `AccountTracker` (fill, user_orders, market_positions; reconnect, reconcile, exactly-once) and the order service tying risk, snapshot and transport together |
| 6.6 — done | CLI: `buy`, `sell` (both fund the shard automatically; no manual transfer command), `cancel`, `amend`, `orders`, `positions`, `balance`, `fills`, `kill`, `--dry-run`, risk verdict output, production confirmation prompt |
| 6.7 — done | Live end to end on the real account under the tiny limits: place far-from-market resting orders, amend, cancel, kill switch (no fills needed), then one deliberate minimal fill to check fills / positions / fees; the **server-offline trading test**; Phase 6 as-built notes. Needs the production trading key (see below) |
| 6.8 — done | Close-out: review of what the live checks showed, risk defaults revisited with you, docs. (Replaces the earlier separate sign-off slice; the sign-off is per order, in 6.7) |

**6.1 as built (measured 2026-10-13 with the read-only production dev key, GET only through the
guarded client; no code added to the repo, probe scripts were throwaway).** Account: balance
`$10.3490`, no open positions, 18 historical orders (all `executed`, `limit`, `taker_at_cross`), fills and
settlements present. Shapes:
- **Balance**: `balance` (int cents) *and* `balance_dollars` ("10.3490"), `portfolio_value` (int
  cents), `updated_ts` (unix seconds), `balance_breakdown` = one row per **exchange shard**
  (`exchange_index` 0-3, balance as a 4-decimal string). `/exchange/status` lists the same four
  shards (Default, Combos, Crypto & Commodities, Tennis / Baseball / Basketball), each with its own
  `trading_active`. **Consequence:** the risk layer's balance and the kill switch must be
  shard-aware; a market lives on one shard and funds are per shard.
- **Positions**: `market_positions` and `event_positions` lists plus `cursor`; empty here, so the
  populated shape rests on the docs (`position_fp`, `market_exposure_dollars`, ...) until the demo.
- **Orders** (read side) carry `order_id`, `client_order_id`, `ticker`, `status`, `type`, `action`
  (buy / sell), `side` (always `yes` in all 18), `book_side` (bid / ask), `outcome_side` (yes / no),
  `yes_price_dollars` / `no_price_dollars` (4 dp), `initial_count_fp` / `fill_count_fp` /
  `remaining_count_fp` (2 dp), `taker_` / `maker_` `fees_dollars` and `fill_cost_dollars` (6 dp),
  `created_time` / `last_update_time` (ISO), `exchange_index`, `subaccount_number`,
  `self_trade_prevention_type`, `user_id`. **A "sell NO at 0.99" appears as `action=sell`,
  `outcome_side=no`, `book_side=ask`, `side=yes`, `yes_price=0.0100`**: Kalshi stores everything on
  the YES book, so the order model must keep `outcome_side` + `action` as the user's intent and
  derive the book side, never trust `side` alone.
- **Fills**: `fill_id` = `trade_id`, `order_id`, `ticker` (and duplicate `market_ticker`),
  `count_fp`, `yes_price_dollars` / `no_price_dollars`, `fee_cost` (6 dp string), `is_taker`,
  `ts` (unix s) and `created_time`, plus the same side fields. **Settlements**: `yes_count_fp`,
  `no_count_fp`, `*_total_cost_dollars`, `fee_cost`, `market_result`, `revenue` / `value` as int cents.
- Money mixes int cents and 4 / 6-decimal strings across endpoints; the models parse every one into
  fixed-point (`*_e6`) without rounding, per the one-representation rule.

**6.2 as built.** `kalshiterm_client.risk` (pure, no I/O): `check(intent, snapshot, policy, now=,
production=)` returns a `Verdict` (all refusal reasons at once, each with a `Code` and a message
naming the numbers; the worst-case fee and cost; the funding plan; `trip_kill`). Approval yields an
`ApprovedOrder` that raises `TypeError` if constructed anywhere else. `kalshiterm_client.risk_config`
loads `risk.toml` (config directory): money as strings of dollars, counts as strings or ints,
floats / negatives / unknown keys refused with the key named; no file means the tiny defaults
(5 contracts and $1.00 per order, 10 contracts per market, $3.00 exposure, $2.00 daily loss, 3 open
orders, $2.00 per transfer, $5.00 transfers per day, snapshots at most 10 s old, production off).
The fee model moved from the server package to `kalshi_core.fees` (the server re-exports it).
Behaviour worth knowing:
- **Cost** = collateral + worst-case fee. A buy locks `count x limit`; a sell first closes what is
  held and the rest is a short (the other side at `1 - limit`). The fee is taken at the worst fill
  price the limit allows (it peaks at 0.50) and rounded up to a cent, so a 1-contract order at 0.50
  costs $0.52 in the check although the fee is $0.0175. Conservative on purpose.
- **Netting is not credited**: buying NO against a held YES still counts the full cash cost (Kalshi's
  real behaviour is checked in 6.7), but the *position* limit does net (it uses the resulting
  position, resting orders included, on the side the order pushes).
- **Fail closed** on: no snapshot, snapshot older than the limit or dated in the future, private stream
  down, unknown or non-trading shard, unknown fee multiplier. Production is refused unless
  `production_enabled`.
- **Daily loss** refuses all new orders once realized loss reaches the limit and sets `trip_kill`
  (closing orders are refused too; use the kill switch or Kalshi's own app to flatten).
- **Funding** takes from the shards with the most free balance first, in whole centicents, only from
  and to shards with transfers active, bounded per order and per day.
Tests: 46 (30 for the checks, 16 for the file) including a 20,000-case seeded fuzz that re-derives every
limit independently for each approved order, and nine deliberate sabotages of `risk.py` (position
off by one, fee at the limit price, open-order off by one, future-dated snapshots, funding order,
production switch, stream check, centicent rounding, short-sale collateral) each caught by a test.

**6.3 as built.** `kalshi_core.portfolio` holds the account models (`Balance` with a per-shard
`balance_breakdown` and `shard(i)`, `MarketPosition` / `EventPosition` / `PositionsPage`, `Order`,
`Fill`, `Settlement`, `QueuePosition`; `Decimal` throughout) and `KalshiRestClient` gained the GETs
`balance`, `positions_page` / `iter_positions`, `orders_page` / `iter_orders`, `order`, `fills_page` /
`iter_fills`, `settlements_page` / `iter_settlements`, `queue_positions`. The GET-only guard is
untouched: there is still no write code. **Trading key:** `KalshiSettings` gained
`trading_key_id` (`KALSHI_TRADING_KEY_ID`) and `trading_private_key_path` (default
`~/.kalshiterm/kalshi_trading_key.pem`); `load_trading_key` refuses a file that group or others can
read (POSIX; the message says `chmod 600 <file>`), and `KalshiSigner.for_trading` refuses a key id or
a file equal to the data key's, so the two keys cannot be confused. Verified live (read-only key,
2026-10-13): 18 orders, 18 fills, 5 settlements and the 4-shard balance parsed, every field Kalshi
sent is declared by a model (a live test fails on any new one), and an independent cross-check
agreed: order fees $0.041 = fill fees $0.041, 18 filled contracts on both sides, every order's fill
count equal to the sum of its fills. **Finding:** the integer-cents `balance` is the dollar balance
*truncated* ($10.3490 is reported as 1034 cents), so only `balance_dollars` / the shard breakdown
is used. **Still unmeasured** (no positions exist on the account): the populated positions shape
and whether the balance already excludes resting-order collateral; both are checked in 6.7.
Tests: 20 new plus one opt-in live (field coverage for every model with a deliberate-drift check, cursor paging, the
user-side price of a NO order, queue positions, five key-file modes, key separation) and four
sabotages caught (mode mask, key-id equality, same-file check, order price side).

**6.4 as built.** `kalshi_core.trading.KalshiTradingClient` is a **separate client** from
`KalshiRestClient`, which is unchanged and still GET-only with its guard and test. (This replaces the
planned `TradingCapability` object with something simpler that does the same job; PLAN decision 5's
intent is kept.) Layers: it cannot be built without a signer from `KalshiSigner.for_trading` (the
signer carries `purpose="trading"`); it refuses the production environment unless the caller passes
`allow_production=True`; a request hook allows only the eight routes in `ROUTES` (create, cancel one /
batch / all, amend, decrease, shard transfer, and the order listing used to find a lost order), so
no other endpoint (legacy create, withdrawals, upgrade, subaccounts) can be sent however the code
changes later. Methods: `create_order`, `cancel_order`, `cancel_orders`, `cancel_all`, `amend_order`,
`decrease_order`, `transfer`, `find_order`. Prices and counts are `Decimal` only and go out as
fixed-point strings (`float` is a `TypeError`; finer than 4 / 2 decimals is a `ValueError`, never
rounded). **Outcome handling:** a create is never sent twice. After a timeout, a dropped
connection or a 500 / 502 / 503 / 504 it lists orders since 5 s before the attempt and matches the
always-present `client_order_id` (4 looks over about 3.5 s); found means the order is returned
with `recovered=True`, not found raises `WriteOutcomeUnknown` carrying the id to reconcile by (an
id that must never be reused). A 409 is looked up too; a 400 is final. Amend and decrease are never
resent (`WriteOutcomeUnknown`). Cancels and transfers are retried (a cancel's end state is the same,
a 404 after an ambiguous try means "already gone"; a transfer carries `client_transfer_id`). A 429
means Kalshi did not process it, so every write retries it up to three times (0.5 / 1 / 2 s).
**Write budget** (measured from the live `GET /account/limits` and `/account/endpoint_costs`,
2026-10-13): Basic tier write bucket capacity 100, refill 100 per second (read: 600 and 200); cost 10
per request by default and **2 for every cancel form (per order in a batch)**; the client bucket is
`write_bucket` (100 x 0.9 margin, new setting `KALSHI_WRITE_TOKENS_PER_SECOND`). Request shapes are
from the V2 docs: `POST /portfolio/events/orders` (`side` bid / ask, `count`, `price`, `time_in_force`,
`self_trade_prevention_type`, `exchange_index`, `client_order_id`), `DELETE
/portfolio/events/orders/{id}?exchange_index=&market_ticker=`, `DELETE .../batched`, `DELETE
/portfolio/events/orders`, `POST .../{id}/amend`, `POST .../{id}/decrease`, `POST
/portfolio/intra_exchange_instance_transfer` (amount in centicents).
**Not yet known, checked in 6.7 on the live account:** (1) **how a V2 `bid` / `ask` maps to yes / no.**
V2 has no outcome field, so it must be the YES book (buy YES = bid, buy NO = ask at 1 - price), but the
orders Kalshi reports for a "sell NO" show `book_side=ask` beside `outcome_side=no`, so the mapping
function in 6.5 stays one pure, separately tested function and is proved with a one-cent resting
order read back from Kalshi before any other order is sent; (2) what Kalshi answers to a duplicate
`client_order_id` (undocumented); (3) how it rejects an order on an unfunded shard.
Tests: 48 against a scripted fake exchange (route allowlist rejects 10 other routes, exact request
bodies and signatures, every lost-answer path including a stateful exchange that drops the answer
and ends with exactly one order, 429 bounds, cancel / batch / amend / decrease / transfer, token
costs against a fake clock) and twelve sabotages of `trading.py`, each caught (retry a create, remove
the route guard, the trading-signer check, the production check, centicent conversion, batch
cost, the lookback window, 429 retry, the already-gone rule, retry an amend, the 409 lookup, the
lookup loop).

**6.5 as built.** *Core:* WebSocket models `FillMsg` (`fill`), `UserOrderMsg` (`user_order`),
`MarketPositionMsg` (`market_position`); `KalshiRestClient.market / event / series`;
`ExchangeIndexStatus.intra_exchange_transfers_active` (absent means "not allowed"). *Client:*
- `trading_state.py` (`trading_state.json` in the config directory, mode 600, atomic writes): the
  day's (UTC) realized-P&L baseline, the day's transferred total, the halt, unresolved
  `client_order_id`s and pending transfers. A transfer is counted **before** the request is sent, so a
  crash cannot hide it; a damaged file is an error, never a reset (a reset would erase the halt).
- `account.py` `AccountTracker`: the private channels have **no snapshot and no sequence numbers**
  (Kalshi docs), so it re-reads balance, shard status, positions (`settlement_status=all`) and
  resting orders over REST after subscribing and after **every** reconnect, and reports the stream as
  not OK until that finishes; messages are idempotent (orders replace and older ones are ignored,
  fills deduplicated by `trade_id`, positions replace). There is **no balance channel**: the balance
  is polled every 5 s and at once after a fill, and the snapshot's age is the age of the last good
  balance, so a failing poll makes the risk layer refuse. Which way a resting order pushes a position
  is `(book_side == bid) == (outcome_side == yes)`, true both for Kalshi's reported legacy orders and a
  YES-book-only order. `AccountSnapshot` gained `halted`; `risk` gained `HALTED` and
  `MARKET_NOT_TRADABLE`.
- `orders.py` `OrderService`: `prepare` (read-only: market shard, status and fee straight from Kalshi
  via market -> event -> series, balance refresh, risk check, and the wire body is built so a price
  Kalshi cannot take fails **before** any money moves), `execute` (fresh re-check; `PlanChanged` if the
  cost or funding differs from what was shown; shard transfers one by one, each recorded first, a
  refused one taken back out of the day's total, an unclear one left pending; the balance re-read and
  the risk check repeated; only then the order with the `ApprovedOrder`), `describe` (the confirmation
  text, funding included), `cancel`, `kill` and a daily-loss guard (`guard_once`, `start_guard`).
  `to_v2` expresses everything on the YES book (buy YES = bid, buy NO at q = ask at 1 - q), the
  documented reading, **proved in 6.7**. `kill` saves the halt first, calls cancel-all, then lists
  resting orders and batch-cancels until **two** consecutive looks (a second apart) are empty, at most
  10 looks; it reports what remained and any error honestly. The guard (and any order attempt over the
  limit) fires it when today's realized loss reaches the limit.
Verified live, read-only (production data key, 2026-10-13): all three private channels subscribed
(the read-only key may), the tracker synced against the real account (4 shards with their trading /
transfer flags, balance $10.3490 on shard 0, 5 settled positions), and its realized-P&L baseline $0.69
equals the raw sum of the positions endpoint exactly. A crude "settlement-derived P&L" cross-check
disagreed and was discarded: its formula was wrong (cost fields cover both sides), not the tracker.
Tests: 16 for the state file, 18 for the tracker, 33 for the service, on shared scripted fakes
(`tests/trading_fakes.py`); nineteen sabotages of `orders.py` / `account.py` / `trading_state.py` /
`risk.py` were run and each is caught (one survivor revealed an untested "two empty looks" rule, now
tested; another was redundant code, now removed).
**Known limits (accepted, written down):** "today" is the UTC day and the baseline is taken the first
time this client runs that day, so a loss realized earlier the same UTC day, before the first run, is
not counted; exposure from a position message is the position's cost basis until the next REST read;
the balance was assumed net of resting-order collateral: **wrong, corrected in 6.7** (it is gross cash).

**6.6 as built.** `kterm buy | sell | amend | cancel | kill | resume | balance | positions | orders |
fills` (`trade_cli.py`, rendering in `trade_views.py`, wiring in `trade_runtime.py`). Trading talks to
Kalshi directly and needs no profile or server. Three runtime shapes: `read` (REST only; signs with the
data key if configured, else the trading key), `exit` (REST + trading client, tracker built but **not
started**, used by `cancel` and `kill` so getting out never waits for a WebSocket or a full account
read) and `trade` (tracker started and fully read before the command runs).
- `buy / sell TICKER COUNT PRICE [--side yes|no] [--ioc] [--post-only] [--dry-run] [--yes] [--json]`:
  prints the verdict first (what would be bought, worst-case cost with fee, any shard transfer, every
  refusal); refusal exits 1; `--dry-run` stops there. **Demo:** `y/N` (`--yes` skips). **Production:**
  the order must be typed out (`TICKER buy|sell yes|no COUNT PRICE`, numbers compared as numbers);
  `--yes` does not skip it and an absent input means nothing is sent.
- `amend ORDER_ID [--price] [--count]`: `OrderService.prepare_amend` checks the replacement as a new
  order with the old one left out of the snapshot (`AccountTracker.snapshot(exclude_order=)`), funds
  the shard if needed, then `execute_amend`. `--count` is the new **total** (what Kalshi's amend takes);
  the risk check uses what would still be unfilled. Whether Kalshi's count includes already-filled
  contracts is unverified (a fresh resting order has no fills, so 6.7 is unambiguous). The check counts
  the replacement's full cost against the balance, conservative if Kalshi already nets the old order.
- `cancel ORDER_ID...` uses Kalshi's own record of each order (shard and market), reports each id, exits
  1 if any could not be cancelled, works while halted. `kill` saves the halt, cancels everything and
  looks until two empty looks; exit 1 and the ids if anything is left. `resume` clears the halt after
  `y/N`. `balance` shows each shard and a `TRADING HALTED` line when halted.
- Exit status: 0 done; 1 refused or failed; 2 a usage mistake or, with an `UNKNOWN:` message, a write
  whose outcome is unknown (the message names the id to look for in `kterm orders --status all` /
  `kterm fills`). Every Kalshi / network / key problem is one clear line, never a traceback.
- Tables never cut an order id or a ticker (found by looking at the real account: ids were shown
  truncated, useless for `cancel`): columns have minimum widths and a piped console is widened to the
  table. `--json` on every command; the environment banner goes to stderr so stdout is clean.
Verified live on the real account, read-only: `balance` ($10.349 on shard 0), `positions --all` (5 flat
settled markets), `orders --status all` and `fills` (the 18 historical orders, full ids), and
`buy ... --dry-run` without a trading key gave the one-line "KALSHI_TRADING_KEY_ID is not set". A
throwaway script (data key standing in as the signer, a write-trap in place of the trading client)
prepared real orders without sending anything: a shard-0 market resolved to fee multiplier 1.0, status
active, worst-case cost $0.3200 for 1 contract at 0.30; a shard-3 market showed "first moves $0.0200
from shard 0 to shard 3"; zero write calls were attempted.
Tests: 30 CLI tests (every command against the scripted fakes, including the production typed
confirmation with eight wrong phrasings, exit codes, JSON, the wide-table cases), 7 more for amend and
cancel-by-id in `test_orders.py`; about 17 sabotages of the CLI and views each caught (one survivor, the
NO-side fill price, and the narrow-terminal width, now tested).

**6.6b fix (found when the real trading key was first used, 2026-10-13).** A dry run in production
could not start until production trading was enabled, because the trading client refused to be built.
A dry run should work before anything is enabled and should be unable to send at all. Now
`--dry-run` opens a **`preview`** runtime: everything is read (tracker started, real balances and
markets) but the trading client is a `PreviewTrading` that raises on every write. `exit` mode (cancel,
kill) always may act on production, so getting out never depends on the switch; `trade` mode still
needs `production_enabled` (`production_allowed(mode, policy)`, tested). Tests: dry runs open
`preview`, real orders `trade`; each preview write call refused; the permission rule by mode; four
sabotages caught. Live with the new trading key (Key ID registered 2026-10-13, public key supplied by
us, Ed25519, private key generated locally in `~/.kalshiterm/kalshi_trading_key.pem`, mode 600, never
displayed): a read signed with it returned the $10.3490 balance, and `kterm buy ... --dry-run` on a
shard-0 and a shard-3 market printed the real worst-case cost and the refusal "production_enabled =
false"; a deliberately oversized order listed six refusals (size, notional, position, exposure, shard
funds). The tracker wrote `trading_state.json` (day and realized-P&L baseline) in the config directory.

**6.7 as built: live verification on the real account (2026-10-13).** Set-up: a separate Ed25519
trading key (private half generated locally into `~/.kalshiterm/kalshi_trading_key.pem`, mode 600,
never displayed; public half pasted into Kalshi; full read / write scope chosen by the user; Key ID
only in the gitignored `.env`), and the approved `risk.toml` (the shipped tiny limits written out,
`production_enabled = true`). Every order was one contract at cent scale, shown before it was sent.
Net cost of the whole run: **$0.0121** (the one-cent spread of the deliberate fill round trip plus
$0.0021 in fees); the balance ended at $10.3369, all on shard 0.
| # | Test | Result |
|---|---|---|
| 1 | order mapping | A YES bid and a "buy NO" were both read back and checked against Kalshi's **orderbook**: a bid at 0.01 sits as a YES bid; a buy of NO at 0.01 (sent as `ask` at 0.99) sits as a NO bid at 0.01. The V2 `bid` / `ask` is the YES book, as `to_v2` assumed. |
| 2 | amend, cancel | price 1c -> 2c and count 1 -> 2 amended (remaining 2.00); cancel by id works. |
| 3 | kill switch, resume | halt saved, cancel-all, two empty follow-up looks, new orders refused with the halt reason, `balance` banner, `resume` clears it. |
| 4 | shard transfer, unfunded shard | An order on an unfunded shard is rejected: **HTTP 404 `insufficient_shard_balance` ("Exchange user not found")**; the CLI funds the shard ($0.02 moved 0 -> 3) and then places the order, which rests on shard 3. |
| 5 | one deliberate fill | BUY 1 YES at 0.02 IOC filled at $0.02, fee $0.0014 (the model said "up to $0.01", conservative); balance fell by exactly $0.0214; position 1 YES, exposure $0.02. SELL 1 YES at 0.01 IOC flattened it: realized -$0.01, fees $0.0021 in total. |
| 6 | server offline | with no KalshiTerm server running and a config whose only profile points at an unreachable server, `balance`, a real order and its cancel all worked. Also an automated test that makes the server client, the profile store and `socket.connect` raise if trading touches them. |
Findings, each fixed and tested (the live run found five real defects):
1. **Kalshi's `action` / `outcome_side` labels cannot be trusted** (a buy of NO is held as "sell, no,
   ask, yes 0.99"; a sale of YES at 1c appears in the fills as "sell, no, 0.99"). Risk, amend and the
   displays now use only the YES book: `book_side` (bid / ask) and the YES price. The defect it
   exposed: `kterm amend` on a buy of NO would have rebuilt it from the labels and sent a **bid at
   0.99** (crossing the book). Fixed; `orders` and `fills` show `BID` / `ASK` with the YES and NO
   prices; `amend --price` takes the YES price. `Order.price_dollars` (same wrong assumption) removed.
2. **The balance is gross cash**: a resting order does not reduce it (measured). The tracker now
   subtracts the collateral resting orders may need, per shard, to get what is free. (This corrects
   the 6.5 assumption "the balance is net of resting collateral".)
3. **Kalshi does not deduplicate transfers on `client_transfer_id`** (identical request twice = two
   transfers). Transfers are sent at most once; unclear -> `WriteOutcomeUnknown`, recorded pending.
   Decision 14d revised. Not so for orders: a repeated `client_order_id` gets **HTTP 409** and
   the client's lookup returns the original order (`recovered=True`), exactly one order exists.
4. **Eventual consistency**: the balance read right after a transfer, and the single-order read right
   after a cancel, lag the list endpoints by a second or two. The order service now waits up to ~10 s
   for a transfer to show in the balance (and refuses at once for any other reason) instead of
   failing "the shard does not hold what the order needs" with the money already moved.
5. **Dry runs** could not start in production until it was enabled (found when the real key was first
   used); fixed earlier as 6.6b (`preview` runtime that cannot send).
Also measured: after an amend `initial_count_fp` is left unchanged (a total of filled + remaining is
used); whether the amend `count` includes already-filled contracts could not be tested without a
partial fill, so **amend refuses to change the count of a partly filled order** (price only, or
cancel and re-place). Real fees match the model: $0.0014 at 0.02 and $0.0007 at 0.01 (Kalshi aligns
to $0.0001); the check's cent-rounded worst case is deliberately above them.

**6.8 close-out (2026-10-13).** Decisions with the user: the risk limits stay at the shipped tiny
defaults ($1.00 and 5 contracts per order, 10 contracts per market, $3.00 exposure, $2.00 daily loss,
3 open orders, $2.00 per transfer and $5.00 transfers per day); the amend-count question is parked
(PLAN §13); Phase 7 follows as planned. **Phase 6 is complete.** State left on the user's machine: the
trading key and `KALSHI_TRADING_KEY_ID`, `risk.toml` with `production_enabled = true`, and
`trading_state.json`; every production order still needs the typed confirmation and passes the risk
layer, and `kterm kill` / `cancel` work regardless. Summary of Phase 6: pure risk layer
(`risk.py`, `risk_config.py`), account reads and trading-key loading (`portfolio.py`, `auth.py`),
write transport with a route allowlist (`trading.py`), live account tracker and order service
(`account.py`, `orders.py`, `trading_state.py`), and the CLI (`trade_cli.py`, `trade_views.py`,
`trade_runtime.py`); 1,323 tests in the suite, live-verified for $0.0121.

**Decided after 6.1 (2026-10-13).** (1) Live account, not demo, for every verification step
(decision 6). **You will need to create a production API key with trade scope** (Kalshi account
settings, API keys; save the private key to `~/.kalshiterm/kalshi_trading_key.pem`, mode 600, and
put only the Key ID in `.env` as `KALSHI_TRADING_KEY_ID`); I will tell you when, which is the start
of 6.7 (6.2-6.6 run on the fake exchange and need nothing). It must be a separate key from the
read-only data key. (2) Multi-shard (decision 13).

Slices 6.1-6.6 send no real order. Parked, not planned: order groups, subaccounts, RFQ / communications
channel, encrypted trading keys, trading from the browser (Phase 7), strategies or automation.

## 10.8 Phase 7 slices (terminal UI)

**What exists:** everything the UI needs is already a typed Python library: `ServerClient` (data,
analytics, alerts, watchlist), `StreamClient` + `BookTracker` (live, exactly-once), and for trading
`OrderService` / `AccountTracker` / the risk layer, all verified live. Phase 7 is therefore a **thin
local backend** that exposes those to a browser, plus the front end. No business logic is duplicated
in either.

**Measured before designing (2026-10-13).** (1) **Node and npm are not installed on this machine**; the
front end needs them to develop and build (not to run: built assets ship in the wheel, Phase 8). Vite 8
requires Node `^20.19 || >=22.12`. (2) The client package has no web-server dependency yet (the server
package already uses FastAPI + uvicorn). (3) **The server has no layouts endpoint**, although PLAN §8.3 and
CLAUDE.md say layouts are per-user server data. (4) Library weight (npm unpacked, not bundled):
dockview 1.7 MB (MIT); Lightweight Charts 3.1 MB (Apache-2.0); Perspective 7.4 MB + datagrid 1.2 MB + a
viewer + WASM (Apache-2.0); AG Grid Community 21.6 MB (MIT). Perspective is by far the heaviest piece
and PLAN §7.3 chose it before any measurement, so it is decided by a spike (decision 5).

**Decisions (recommended defaults; say if you disagree).**
1. **Backend**: FastAPI + uvicorn inside `kalshiterm-client` (`kalshiterm_client/ui/`), started by
   `kterm ui`. One process on `127.0.0.1` (random free port unless `--port`), serving the built front
   end and `/api` (REST) + `/api/ws` (one multiplexed WebSocket: live market data, alerts, and, in
   trading mode, account events). The browser never sees the Kalshi keys or the server token.
2. **Security (from PLAN §7.3, made concrete)**: bind loopback only; `Host` header allow-list (stops DNS
   rebinding); `Origin` check on every non-GET request and WebSocket upgrade; a 256-bit per-session
   token printed in the URL **fragment** (never sent to any server, not logged), kept in page memory and sent
   as `Authorization: Bearer` (WebSocket: first message); **no cookies, no CORS**, so there is nothing
   for another tab to ride on. Tests for each (wrong Host, wrong Origin, missing / wrong token, WebSocket
   without a token).
3. **Trading is switched on and off inside the app (changed 2026-10-13 at the user's request;
   replaces "`kterm ui --trading`").** `kterm ui` always starts **read-only**: in that mode the
   backend has not loaded the trading key, has no trading client and no private WebSocket, and every
   order endpoint answers 403. A mode control in the header switches to **trading** (which, on this
   setup, is production):
   a. *Permission vs session.* `risk.toml` keeps granting **permission** (`production_enabled`); the
      app only controls the **session mode**. The page can never write `risk.toml` (limits and the
      permission change only by editing that file, as since slice 6.2), so a hijacked page or a stolen
      session token can ask for trading but cannot grant itself the right to. If the permission is
      missing the app says exactly what to edit.
   b. *Switching on* needs a human act the page cannot do silently: the user types a phrase in a
      dialog (`ENABLE PRODUCTION TRADING`, or `ENABLE DEMO TRADING` when the environment is demo), the
      backend checks the phrase, the permission and the key, then builds the trading runtime (loads the
      key, starts the account tracker) and answers with the new mode. The dialog shows the limits from
      `risk.toml` and today's loss and transfer totals.
   c. *Switching off* is one click, no phrase, instant: the trading runtime is stopped and the key
      dropped from memory. Resting orders stay unless the user also ticks "cancel all open orders".
   d. *It lapses on its own*: back to read-only after 30 minutes without any trading activity or
      when the last browser connection has been gone for 60 seconds (both overridable in
      `risk.toml`), and always at backend shutdown. The kill switch works in either mode.
   e. A persistent banner shows the mode (production = red); the header control always shows it.
   f. An order is still a **two-step flow**: `prepare` (the verdict, the funding plan and a one-time
      id) then `execute(id)`; the id expires in 60 s, is single use, and is bound to exactly what was
      shown (`PlanChanged` rules as in the CLI). The production typed order phrase is checked by the
      backend, not the page. Switching on does not weaken any of this.
4. **Types cannot drift**: the backend's OpenAPI schema generates the TypeScript API types
   (`openapi-typescript`); a CI check fails if the committed types are out of date. Prices and counts
   cross the wire as decimal strings and are never parsed to JS `number` for arithmetic (display
   formatting only).
5. **Front end**: React + TypeScript + Vite, dockview (panels), Lightweight Charts (charts), TanStack
   Query (REST) and one WebSocket store. **The grid library is decided by a measured spike in slice
   7.5** between **Perspective**, **AG Grid Community** and **TanStack Table + virtual scrolling**,
   because every grid panel (7.6-7.8) depends on it and swapping later means rewriting them. Pass
   marks, fixed before measuring: 5,000 rows x 10 columns taking 200 cell updates / s with changed
   cells flashing, 95th-percentile frame time <= ~17 ms; re-sorting on a live-updating column responsive
   (< ~50 ms per interaction); then the gzipped JS + WASM added to the initial load, and the glue code
   needed for sort, flash, keyboard navigation and column resize. Among candidates that pass, the
   lightest wins, then the least custom code. The result (numbers, not impressions) is written here.
   *Expectation recorded beforehand, unmeasured:* AG Grid Community balances best for this app, TanStack
   is lightest but costs feature work, Perspective's strengths (client-side pivot / huge datasets) are
   not needed here.
6. **Layouts are per-user server data** (as PLAN §8.3 says): a new `ui_layouts` table (migration 0020),
   endpoints to list / save / delete by name (size-capped), and `ServerClient` methods; the browser keeps
   the last layout in `localStorage` so the UI still opens when the server is down.
7. **Testing**: pytest for the backend (fakes from the Phase 5 / 6 tests), Vitest for front-end units,
   Playwright for end-to-end journeys against a backend started with the same fakes (deterministic).
   CI gets a Node job (install, lint, type-check, test, build, e2e); sources live in
   `packages/kalshiterm-client/frontend/`, the build goes to `ui_assets/` (git-ignored; bundled into the
   wheel in Phase 8). `kterm ui` without built assets says so and shows the dev command.
8. **Interaction**: a global command bar (Ctrl+K) with mnemonics (`M <ticker>`, `CH`, `BK`, `OR`, `POS`,
   `BUY <ticker> <n> <price>`, `KILL`), panels linked by colour channels (pick a ticker in one, linked
   panels follow), light and dark themes. **The kill switch is always visible in trading mode; a
   persistent red banner marks production.**
9. **Out of scope** (parking lot if raised): mobile layouts, multi-user login screens, chart drawing
   tools and saved studies, backtesting, alert delivery outside the app, themes beyond light / dark.

| # | Slice |
|---|---|
| 7.1 — done | Backend skeleton: `kterm ui`, FastAPI + uvicorn, the whole security layer of decision 2 with its tests, a placeholder page, the new dependencies. No data yet |
| 7.2 — done | Backend data API: server data through `ServerClient` (markets, market, candles, trades, book, analytics, arbitrage, watchlist, alerts) and the live stream relay through `StreamClient` / `BookTracker` (resume, exactly-once); OpenAPI -> generated types check; contract tests |
| 7.3 — done | Layouts on the server: migration 0020, endpoints, `ServerClient` methods, per-user scoping tests, relayed by the backend |
| 7.4 — done | Backend trading API: the mode switch of decision 3 (lazy trading runtime, typed phrase, permission check, lapse timers, switching off), account snapshot + live account events, prepare / execute / cancel / amend / kill / resume with the one-time ids and server-side typed confirmation; tests on the Phase 6 fakes (every order endpoint 403 in read-only; wrong phrase, missing permission, expiry); a live read-only check |
| 7.5 — done | Front-end scaffold: Node set up, Vite / React / TS, lint, type-check, Vitest, generated types, dockview shell, command bar skeleton, token bootstrap, themes, the CI Node job, and **the grid spike with its measured result** |
| 7.6 | Market panels: market list / search grid, market detail, watchlist, linked-ticker groups; candlestick chart (bars + live tick); order-book ladder and depth chart from `BookTracker` |
| 7.7 | Live panels: trades tape, alerts (rules and live alerts), analytics and arbitrage panels |
| 7.8 | Trading panels: the header mode control and its enable dialog, balance by shard, positions, orders, fills; the order ticket (prepare -> verdict -> confirm -> execute), amend / cancel from the orders grid, the always-visible kill switch, the production banner |
| 7.9 | Layouts and windows (save / load / delete named layouts, reset, popout windows for multi-monitor as far as dockview supports it), Playwright journeys (browse -> chart -> order ticket -> kill), performance measurements, README, PLAN as-built; **Phase 7 complete** |

**7.1 as built.** `kterm ui [--port N] [--no-browser]` starts the backend (`kalshiterm_client/ui/`:
`security.py`, `app.py`, `run.py`, plus `ui_cli.py`); FastAPI and uvicorn are now client dependencies.
It binds a socket on **127.0.0.1 only** (there is no option for another address; port 0 picks a free
one), creates a 256-bit session token and opens/prints `http://127.0.0.1:PORT/#token=...` (fragment).
`SecurityMiddleware` (pure ASGI, so it covers HTTP, WebSocket and static files alike): the `Host` header
must be `127.0.0.1:PORT` or `localhost:PORT` for this exact port (DNS rebinding); `Origin` must be
`http://127.0.0.1:PORT` or `http://localhost:PORT` on every non-GET request and every WebSocket upgrade,
and **a missing Origin is refused too**; every `/api` request needs `Authorization: Bearer <token>`
(constant-time compare; a token in a query string or cookie does not count; an unknown `/api` path is
401, not 404, until authenticated); the WebSocket (`/api/ws`) authenticates in its **first message**
within 5 s or closes with 4401 and gets no data (messages are capped at 64 KB). No cookies and no CORS
headers at all (a preflight gets 403); responses carry `X-Frame-Options: DENY`, `nosniff`,
`Referrer-Policy: no-referrer`, a same-origin Content-Security-Policy (with `'wasm-unsafe-eval'` for the
7.5 grid candidates, to be tightened once chosen), `Cache-Control: no-store` on `/api`; no `Server` or
`Date` header, no request log, no docs/OpenAPI endpoints (the schema for the generated types comes from
`create_app(...).openapi()`). Routes so far: `GET /api/health`, `GET /api/session` (`mode` is always
`read-only`), `POST /api/session/touch` (the keep-alive the later idle timeout will use), the
WebSocket, and the page (the built front end from `ui_assets/` if present, else a short placeholder).
There are **no order endpoints yet**. Tests: 73 (every Host and Origin variant for HTTP and WebSocket,
every token variant, no CORS / cookies, headers, static path traversal, real-TCP server and WebSocket,
the socket is loopback-only, no token in any log), and 18 sabotages of the security layer each caught
(one apparent survivor, `access_log`, is an equivalent mutant at uvicorn's `warning` level; it is
pinned by asserting the server configuration). **Verified live** against the real process with curl:
right token 200; no token, token in the query, hostile Host (right token), hostile Origin, no Origin,
and a CORS preflight are all refused; headers as above; a request to the machine's LAN address is
refused; the WebSocket authenticated, answered a ping, closed a wrong token with 4401 and refused a
hostile Origin with 403. The permission design of decision 3 (`risk.toml` grants, the app switches
the session) was **confirmed by the user** on 2026-10-13.

**7.2 as built.** *REST* (`ui/data_routes.py`): an explicit list of 24 routes, each one `ServerClient`
call returning its typed model (no generic proxy, so the browser cannot ask the backend to fetch an
arbitrary server path): `GET /api/server` (configured? reachable? who am I; never fails), markets,
market, event, candles, trades, ticks, orderbook, gaps, watchlist (+ PUT / DELETE per ticker),
analytics (market, history, event, arbitrage) and alerts (metrics, rules create / read / patch /
delete, alerts, acknowledge). Parameters are validated before the server is asked (limits, intervals,
patterns; **times must carry a time zone**, rule thresholds are decimal *strings*, never JSON
numbers); the server token never appears in an answer. The client library's errors become clear
statuses (`not_found` 404, `rejected_by_server` 422 with the server's code, `forbidden_by_server`
403, `server_rejected_token` / `server_unreachable` / `server_certificate_changed` 502,
`server_not_configured` 503). **With no profile or no token the UI still starts** and the data routes
say why (`kterm ui` prints `server: ... (configured | the reason)`); trading does not depend on the
server. Writes pass the Origin rule of 7.1. *Live stream* (`ui/stream_hub.py`, `ui/messages.py`): one
`StreamClient` per active union of what every browser connection asks for, restarted (debounced) when
the union changes; the backend keeps the order books and sends the page **whole books**, best price
first, at most 25 levels a side and at most ~10 per second per market; a page that subscribes to a
market already streaming gets its current book and last ticker at once; `link` messages say up / down
(retrying in N s) / failed; a `resync` clears books. **Bounded**: 100 markets per page, 200 across the
UI, a 2,000-message queue per page. A page that stops reading is **dropped with close code 1013 even
though its send is blocked** (a design gap found while testing: the first version only noticed in the
writer, which a stalled socket blocks; an overflow now tears the connection down directly). A fatal
stream error is reported once and **never retried on its own; a fresh subscribe retries once**
(found by a sabotage survivor: reloading the page after a failure would otherwise never retry).
*Contract*: `kalshiterm_client.ui.schema` writes `frontend/src/api/openapi.json` (24 paths, 28
operations) and `ws.schema.json` (10 server and 5 browser message types), committed; a test fails when
they differ from the code, and another that **no money field is a JSON number** (only `retry_in` is);
the TypeScript is generated from them in 7.5. *Tests*: 78 new (34 data routes against the real
`ServerClient` and a scripted server, 34 hub / WebSocket, 6 schema, 4 startup wiring), about 30
sabotages each caught after four test additions prompted by survivors. *Verified live*, against the
**real KalshiTerm server and real ingested data** (throwaway user, token and watchlist, all deleted
afterwards): `/api/server`, markets, detail, hourly candles, trades with exact decimal strings, error
statuses (naive time 400, unknown market 404, bad limit 422); the WebSocket relayed **1,050 trades and 20
ticker updates in 20 s** for a 15-minute crypto market (about 52 trades a second), reported the link
going down then up, and handled an unknown ticker (the server's own answer), a bad channel and non-JSON
without dropping the connection; and the book for the Bitcoin $200k market, delivered 0.3 s after
subscribing, **matched both the server's stored book and Kalshi's own book exactly** (yes 0.01 x
1,139,101.57; no 0.97 x 198,376.21, 0.96 x 1,446.95, 0.95 x 14,169.67). *Finding about the server
(not the UI)*: `kterm-server ingest` only creates the order-book feed and the watchlist controller when
started with `--watchlist FILE` or `--watch TICKER`; without either, API watchlists are silently never
captured (`capturing: false`). The deployment passes `--watchlist`, so it is a dev sharp edge (parked,
PLAN §13). The controller does a full reconcile every 300 s (TOML file, top-N refresh) and a cheap poll every 15 s for
changes to users' watchlists; capture began 6 s after the corrected start.

**7.3 as built.** *Server*: migration **0020** `ui_layouts (user_id, name, data jsonb, size_bytes,
created_at, updated_at)`, primary key (user, name), `ON DELETE CASCADE` with the user, CHECKs as a
backstop (name 1-64 chars, `data` an object, size bounded). `kalshiterm_server/layouts.py` + `api/layouts.py`:
`GET /v1/layouts` (names, sizes, times, plus `limit` 50 and `max_bytes` 262,144), `GET|PUT|DELETE
/v1/layouts/{name}`. `PUT` takes exactly `{"data": {...}}` and answers 201 (new) / 200 (replaced).
Names: letters, digits, space, `_` `-` `.`, starting with a letter or digit and not ending in a space (a
name can never traverse or break a URL). The server stores the object **without interpreting it** (the
front end defines and versions it) and enforces: size in **bytes of compact JSON** (exact boundary
tested; 413 `layout_too_large` with the true cap), depth <= 64, **no NUL anywhere** (jsonb cannot hold
one and would answer 500), no NaN / Infinity (`parse_constant` refuses them, 400), 50 layouts per user
(422 `layout_limit`; replacing never counts), the count checked under a **per-user advisory lock**
(12 concurrent saves at a cap of 3 give exactly 3). The body reader stops at the cap instead of
buffering (a lying or missing Content-Length helps no one). Layouts are strictly the caller's own:
every query is scoped by user, and an admin sees only their own too. *Client*: `Layout`,
`LayoutList`, `LayoutSummary`, `LayoutSaved`; `layouts()`, `layout(name)`, `save_layout(name, data)`,
`delete_layout(name)`; a bad name is a `ValueError` before any request; saves and deletes are never
retried. *UI backend*: `GET /api/layouts`, `GET|PUT|DELETE /api/layouts/{name}` (a layout is a `data`
object only; the server's reasons pass through), and a **general 1 MiB request-body cap** in the
security layer (declared length refused at once, undeclared counted as it arrives and answered 413
immediately, before the framework can turn the overflow into a 400); the OpenAPI schema was regenerated
and committed. *Tests*: 97 new (49 server API incl. isolation, every limit, the race and the table
CHECKs; 2 contract; 28 client; 18 UI backend) and about 35 sabotages each caught. Two real defects
came from testing, not review: an oversized upload reported the **read cutoff** (262,144 + 4,096) as
its limit instead of the real cap (a client would have shown a wrong number; found by the contract
test), and my first sabotages of the SQL failed for the wrong reason (asyncpg refused the mutated
statement) so they were redone with valid SQL before they counted. *Verified live* on the real stack
(real API, the dev database migrated to 0020, two throwaway users, all removed afterwards): a nested
layout with accents, emoji, a 20-digit integer and price strings came back **identical**; replace; user
B's list was empty and B could not read A's layout, and B saving the same name left A's data intact;
a 300 KB layout was refused with the real cap (262,144); a 1.2 MB body got 413 from the backend; the 51st
layout was refused (`layout_limit`, 50) while replacing at the cap still worked; bad names were
refused; delete worked; **removing the two users removed their 50 layout rows**.

**7.4 as built.** *Mode switch* (`ui/trading.py`, `TradingSession`): the session starts read-only with no
key loaded. `POST /api/mode/enable` checks `risk.toml` permission for the environment, then the exact
typed phrase (`ENABLE PRODUCTION TRADING` / `ENABLE DEMO TRADING`), then opens the Phase 6 `trade`
runtime (account tracker, guard); `POST /api/mode/disable` closes it (optionally cancelling open orders
first). Trading lapses after `ui_idle_minutes` (default 30; only deliberate actions count, not account
polling) or `ui_disconnect_seconds` (default 60) with no browser, and on shutdown; both are new
`[trading]` keys in `risk.toml`, strictly validated. *Orders*: `prepare` (and `/orders/{id}/amend`)
return the verdict, funding plan and a **one-time id valid 60 s**; `execute` takes that id (a wrong
production order phrase is refused without burning the id); `DELETE /orders/{id}`; `kill` works in either
mode (an ad hoc exit runtime when trading is off); `resume` needs `confirm: true`. Every `/api/trading/*`
route except kill answers 403 `trading_off` in read-only. Errors map to stable codes (`order_refused`
422 with the refusals, `plan_changed`/`funding_short` 409, `outcome_unknown` 502 with a reference,
`kalshi_error` with Kalshi's status). Request models are strict (no coercion, no unknown fields, amounts
as decimal strings). *Live account*: `AccountTracker` got an `on_change` hook; the WebSocket gained
`{"type":"account","enabled":bool}` (client) and `mode` / `account` messages (server) so the page sees
balances, positions, orders and fills as they change. `OrderService.cancel_everything()` was split out
of `kill()`. OpenAPI (35 paths) and WS schemas regenerated and committed. *Tests*: 37 session tests, the
HTTP / WS route tests, plus config, orders and account tests; about 40 sabotages each caught. Sabotage
testing also caught my own harness hanging on a mutated WS handler, so those tests now use a ping
sentinel and a hard timeout. *Verified live, read-only* on the real account via `kterm ui`: read-only
start (account route 403); wrong phrase 422; correct phrase enabled trading; the account snapshot
matched `kterm balance` ($10.3369 on shard 0, nothing elsewhere, no orders or positions); a dry-run
`prepare` of a 1-contract 1-cent buy was approved with a $0.02 funding plan and a one-time id (never
executed); a 50-contract order was refused with five reasons; disabling returned to read-only and the
account route to 403. **No order, transfer or kill was sent**; the balance and transferred-today were
unchanged afterwards.

**7.5 as built.** *Toolchain*: Node 22.23 LTS from Homebrew (`node@22` is keg-only: put
`/opt/homebrew/opt/node@22/bin` on `PATH`), pinned by `frontend/.nvmrc` and `engines`; exact versions in
`package.json` + `package-lock.json` (React 19.3, Vite 8.3, TypeScript 5.9 (7.0 exists but
typescript-eslint is not ready for it), Vitest 5, ESLint 10, `dockview-react` 8.5.1 (the `dockview`
package no longer exports the React component), Lightweight Charts 5.2 and TanStack Query, both
installed for 7.6). *App*: `src/lib/token.ts` takes the token from the URL fragment into memory and erases
it from the address bar (tested); `api.ts` (Bearer fetch, `ApiError`), `ws.ts` (first-message auth),
`theme.ts` (light / dark, `localStorage` optional), `commands.ts` + `CommandBar.tsx` (Ctrl+K, `HELP`,
`THEME`; the other mnemonics arrive with their panels), a dockview shell with a Welcome panel, a header
with mode / environment pills, connection dot and theme switch, and the red production banner.
`src/api/schema.d.ts` is generated from the committed `openapi.json` (`npm run gen:types`); CI regenerates
and fails on any diff. `vite build` writes `kalshiterm_client/ui_assets/` (git-ignored), which `kterm ui`
then serves; initial load 166 KB gzipped JS + 11 KB CSS. Dev: `kterm ui --port 8765 --no-browser`, then
`KTERM_UI_PORT=8765 npm run dev` (the proxy rewrites Host and Origin for the backend's checks). *CI*: a new
`frontend` job (npm ci, lint, type-check, Vitest, generated-types check, build). *Verified live* in a real
headless Chromium against the real `kterm ui` on the production environment (read-only): the page loaded
with no console errors or warnings under the backend's Content-Security-Policy, the token was gone from
the address bar, the WebSocket connected (`hello` set the pills `read-only` / `production` and the red
banner), Ctrl+K opened the command bar, `HELP` and `THEME` worked.

**The grid spike (decision 5), measured 2026-10-14.** One benchmark, identical for all three: 5,000 rows x 10
columns, a feed of 200 cell updates per second (10 cells per 50 ms) with changed cells flashing, frame
times from `requestAnimationFrame` over 8 s, then the same feed while the grid is re-sorted on a
changing column every 0.6 s (12 sorts: call until two frames have painted), then a 2,000 updates / s
stress run. Production builds, headless Chromium 148 on this Mac (the display ran at 120 Hz: an idle
two-frame wait is about 15 ms, which is the floor of every sort figure below).

| | TanStack Table 8 + react-virtual | AG Grid Community 36 | Perspective 3.8 (datagrid) |
|---|---|---|---|
| Added to the initial load (gzipped) | **about 45 KB** (page 89 KB incl. React) | 310 KB | 85 KB JS + CSS **plus 3.16 MB WASM** |
| Frame p95 / p99 at 200 updates / s | 9.1 / 9.3 ms | 9.2 / 9.3 ms | 9.1 / 9.3 ms |
| Frame p95 at 2,000 updates / s | 8.7 ms | 9.2 ms | 9.0 ms |
| Frames over 33 ms, 200 / s while re-sorting | **0** | 3 | 10 |
| Re-sort time, median / max | **16.6 / 22.3 ms** | 29 / 34 ms | 33 / 42 ms |
| Mount (5,000 rows) | 54 ms | 45 ms | 146 ms |
| Flash on change | about 8 lines (keyed span + CSS animation) | built in | not built in |
| Keyboard navigation, column resize | **to write** | built in | built in |

All three pass the marks fixed beforehand (p95 frame <= ~17 ms, re-sort < ~50 ms). **Decision: TanStack
Table + `@tanstack/react-virtual`**, by the stated rule (lightest among those that pass, then least custom
code): it is the lightest by a wide margin and the smoothest under re-sorting. My recorded expectation was
AG Grid; the numbers say otherwise. **Known cost and fallback:** keyboard navigation and column resize were
not part of the measurement and must be written in 7.6; if that proves disproportionate, AG Grid
Community also passed and is the fallback (a swap before 7.6's panels exist is cheap). Caveat: one
machine, headless, synthetic data; the real panels re-measure in 7.9. The spike code was discarded after
measuring; only `@tanstack/react-table` and `react-virtual` stay in `package.json`.

**Decided with the user (2026-10-13):** Node is approved (install **Node 22 LTS via Homebrew** at the
start of 7.5, pinned in the repo and in CI; slices 7.1-7.4 are Python only); layouts are
**server-stored per user** (decision 6); `kterm ui` is **read-only by default** with trading as an
opt-in (decision 3, changed the same day: switched on and off inside the app, with `risk.toml` still granting permission); the grid is **decided by the measured spike** (decision 5).

## 10.9 Cleanup C1: reuse and maintainability (decided 2026-10-14, by the user)

A review of the code base (layering, duplication, function size, documentation, test weight)
found the structure sound and the tests not overbuilt, with three maintainability gaps. This
slice closes them and nothing else. **No behaviour changes**: every existing test keeps passing
(none is deleted or weakened), and each step is its own commit so it can be reviewed alone.

| Step | Gap | Work |
|---|---|---|
| C1.1 | Money scaling is copied across all three packages: the scale constants are defined ten times and `1_000_000` appears inline thirteen more; the server and the client each have their own `to_e6` with different edge behaviour | One `kalshi_core.fixedpoint` (the server's exact, range-checked conversion moves there with its tests); the server module and the scattered constants go away; the client keeps a thin wrapper only where its user-input error message differs |
| C1.2 | Oversized functions on important paths: `risk.check` (141 lines, the order-safety layer) and the CLI `register()` functions (up to 272 lines, every command a closure); `server/cli.py` is 744 lines | Split into module-level functions that are readable and testable one at a time, with a docstring on each of the ~10 largest undocumented functions. Rules and messages stay the same |
| C1.3 | Small copies: `for_profile` is identical in `client.py` and `stream.py`; `money` and `aware` are each defined twice | Merge the identical ones; leave the ones whose behaviour genuinely differs |

Done (merged 2026-10-14, CI green): C1.1, C1.2 (the `register()` hoist, the `risk.check` split and
the eleven docstrings) and C1.3. (C1.2 also named `server/cli.py`; that part was not done and is C1.6.) The review's
other findings, scheduled by the user the same day:

| Step | Work | Approach and check |
|---|---|---|
| C1.4 | The slow pinning tests (~20 tests x 1.5 s) | The cost is not certificate generation (the test certificates are elliptic-curve and take milliseconds). `kterm server trust` listens for a LAN announcement for 1.5 s (PLAN section 8.5), and the tests do not need it: pass `--no-mdns` everywhere except the tests about discovery. Saves ~30 s of a 157 s run; no production code changes |
| C1.5 | The five remaining public functions of 40+ lines without a docstring (`cli.run` and `live_cli.work`, nested closures; `api/stream.event_for`, `market_metrics.run`, `ui/trading.enable`) | Docstrings only. Check as before: the syntax tree of every touched file is identical once docstrings are ignored |
| C1.6 | `server/cli.py` is 744 lines, and `ingest_command` is 130 lines (complexity 16) | Split `ingest_command` into small functions and move the backup, analyze and token command groups into their own modules, with the same flat `register()` pattern as the client; `cli.py` keeps the wiring. No option, help text or behaviour changes: `kterm-server --help` and every group's `--help` byte-identical before and after, all tests pass |
| C1.7 | The root `README.md` is five lines | A quickstart of about 100 lines for a public repository: what it is and its status, requirements, running the tests, standing up the server (pointing at `deploy/host/install.sh` and PLAN section 8), pointing the client at it (`kterm config add`, `kterm server trust`, the UI), and the safety defaults (demo environment, trading limits, trades never go through the server). Links to the design instead of repeating it |
| C1.8 | PLAN.md is 2,214 lines and 57% of it is section 10, a dated build journal (rehearsals, measurements, "as built" notes) that buries the durable design | Move section 10 verbatim to `docs/BUILD-LOG.md`, **keeping its `10.x` numbering**; PLAN.md keeps the phase table and a pointer. Update the 23 references in code, tests and docs (mostly `PLAN section 10.x`) and the "Start here" line in CLAUDE.md. Last, because the other steps add references. Check: every reference resolves to a heading in one of the two files (a one-off script, not a test) **Done 2026-10-15:** PLAN.md went from 2,249 to 983 lines and this file holds the other 1,288. The moved text is unchanged apart from heading depth and a `PLAN ` prefix on references to the design (checked line by line: all 1,270 lines match). The 19 references outside the docs (code comments, docstrings, `CLAUDE.md`, the README, a Compose comment) now say `BUILD-LOG §10.x`; all 48 `PLAN` and `BUILD-LOG` references in the repository resolve to a real heading, and no Python code changed (the syntax trees are identical once docstrings are ignored) |
| C1.9 (added and done 2026-10-15) | CI went red once: on a degraded Windows runner (setup steps 5 to 10 times slower than its sibling jobs) four healthy `test_orderbook_feed.py` tests failed their 5-second `asyncio.timeout(5)` guard and the job took 18 minutes instead of 3; the rerun passed in under 3 | The guards only turn a hang into a failure, so they should be generous. One named `HANG_GUARD = 30` s (`kalshi-core/tests/hang_guard.py`) now serves all 43 guards in the kalshi-core websocket tests, plus the one 5 s guard in the server's alert test. Timeouts that are the behaviour under test (`snapshot_timeout=0.1`, "nothing arrives within 0.2 s") stay short. Left alone: the client UI test's 3 s `wait_for` (it also asserts the stream ends promptly) and the guards of 10 s or more elsewhere. Check: with the constant set to 0.5 ms, tests fail in all five files, so none was missed |
| C1.10 (added and done 2026-10-15) | `CHANGELOG.md` had one line ("Phase 0") and `SECURITY.md` still said "This is a private project" although the repository has been public since 2026-10-08 | `CHANGELOG.md` is rewritten by phase from the merge history (no versions exist, so it says so). `SECURITY.md` says what is true: GitHub private vulnerability reporting is **not** enabled (checked), so it asks reporters to contact the maintainer privately, and it states what the design relies on. Enabling GitHub's private reporting, secret scanning and push protection (all off today) is a repository setting for the maintainer to decide, not a change to the code |

All of C1.4 to C1.10 were done on 2026-10-14 and 2026-10-15, in the order C1.4, C1.5, C1.6, C1.9, C1.7 with C1.10, then C1.8.

Out of scope on purpose (reviewed and rejected): blanket docstring coverage, deleting or merging
tests, splitting large test files, a docstring lint rule. Reviewed and left alone because they only
share a name: the two `money()` functions (a `Decimal` with a dash for "missing" vs integer
millionths) and the two `aware()` functions (the client UI raises HTTP 400, the server its own 422).

## 10.10 Cleanup C2: first run, reproducible builds, CI hygiene, recovery-path tests (decided 2026-10-15, by the user)

A second review, made before starting Phase 7, looked at what the first one did not: a clean
clone, supply-chain and CI hygiene, a secrets sweep of the public repository, dead code and the
coverage of the money-handling code. The sweep found no secrets in the tree or the history, no dead
code and no stale skipped tests; four things were worth fixing. Same rules as C1: no behaviour
changes beyond what each step says, no test deleted or weakened, one commit per step.

| Step | Finding | Work |
|---|---|---|
| C2.1 | `kterm ui` on a clean clone serves only a placeholder, because the front end is built by `npm run build` into the git-ignored `ui_assets/` and the only place that runs it is a CI check. The placeholder says "not built yet" without saying how, and the README's `kterm ui` line implied it just works | The README says to build the front end once (Node 22) and why; the placeholder page names the command. Phase 8 (packaging) will bundle the build into the client wheel and make this unnecessary |
| C2.2 | The server image copies `uv:latest`, and the Mac Studio rebuilds on every deploy, so a new uv release can change or break the build | Pin the uv image to the version in use (0.12.23, checked to exist). Python dependencies were already pinned by `uv.lock` and `--frozen` |
| C2.3 | CI warns on every run that `checkout@v4`, `setup-uv@v5` and `setup-node@v4` run on a deprecated Node, and nothing keeps them current | Move to checkout v7, setup-node v7 and setup-uv v10.3.0 (setup-uv publishes no moving major tag, so the exact version is pinned), and add a Dependabot file for the actions, the Docker image, npm and uv, monthly and capped at three open pull requests so a solo maintainer is not buried |
| C2.4 | The 25 lines the tests never reach in the money code are the recovery paths: the write client's retry after a transport error, a 429 and an ambiguous status, the kill switch's retry and error reporting, and the cancel error cases. Phase 7 puts a browser order ticket on the same service | Focused tests for exactly those branches, written to the code's documented contract (never to what a line happens to do). Measured before and after with branch coverage |

Reviewed and left alone: ten functions with complexity of 13 to 17 (none worse; moderate, and not
on the Phase 7 path); the repository settings that are off today (GitHub private vulnerability
reporting, secret scanning and push protection), which are the maintainer's decision and not code.

**Done 2026-10-15.**
- **C2.1:** the placeholder page and the README now name the build command.
- **C2.2:** the image was built and smoke-tested with the pinned uv (it reports 0.12.23 and has the
  PostgreSQL 18 tools; the CLI runs).
- **C2.3:** the workflow uses checkout v7, setup-node v7 and setup-uv v10.3.0, and
  `.github/dependabot.yml` covers the actions, the Docker image, npm and uv. A workflow cannot be run
  locally, so it was run by CI on a pull request before it reached `main`.
- **C2.4:** 19 new test cases. Branch coverage of the seven money-handling modules went from 97% to
  99% (the write client, the risk layer and `fixedpoint` and `fees` are at 100%; the two lines left
  are an `if None` in a conversion wrapper and the trimming of an in-memory fills list). Twelve
  deliberate breakages of the source (a 400 treated as retryable, one retry pause too few, an
  idempotent 5xx not retried, a kill switch that hides a failed listing or forgets what still
  rests, `cancel_by_ids` letting an unknown outcome escape, pruning that also drops resting
  orders, a spent order counted against the limits, an unreadable `risk.toml` treated as the
  defaults, non-table sections accepted) were each caught by a new test, and by no older one for
  the behaviours that had no test before.
- **Noted, not changed:** when the order listing never works, the kill switch's report is correctly
  not clean and lists every listing error, but its last line says "orders were still resting after
  the last pass", which overstates what is known. Changing a message is a behaviour change, so it
  is left for the maintainer.

## 10.11 Host: keep the NAS share mounted (decided 2026-10-15, by the user)

The hard-power-cut test (see 10.2, step 5) showed the database cannot be trusted to survive an
unplanned power loss on this host, so backups to the NAS (the share `kdb` on `navi`) stopped being
optional. The share was mounted by hand through Finder, and it disappeared the moment the Mac's
network changed (Wi-Fi to Ethernet): the mount died, and the copy to it died with `Socket is not
connected`. A mount that needs a person is not a place to put backups.

`deploy/host/keep-mounted.sh SHARE_URL MOUNT_POINT`, run by a launchd **LaunchAgent** at login and
every 60 seconds (`com.kalshiterm.keepmounted.plist.template`), keeps the share mounted at the same
path (`/Volumes/kdb`):
- mounted and answering: it does nothing;
- mounted but not answering (the stale mount after a network change): it forces it off, then remounts;
- not mounted: it mounts it with the credential already saved in the login keychain. If there is no
  saved credential it does **not** try, because the only result would be a password dialog on a
  screen nobody is watching; it logs that and exits;
- every wait is bounded, so a stuck mount cannot pile up jobs.

It runs in the user's session, so after a reboot it starts once FileVault is unlocked, which is the
accepted trade-off (the stack already waits for that unlock). The log is
`~/Library/Logs/kalshiterm-keep-mounted.log`.

Out of scope here, for the backup work that follows: the backup container must fail loudly when the
share is missing (the same bind-mount trap as the key file: Docker creates an empty directory for a
missing host path), and a NAS account limited to the one share is better than a personal login.
