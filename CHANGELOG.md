# Changelog

Nothing is released or versioned yet (the packages are `0.x` and never published), so this lists
what was built, newest first, grouped by build phase. Dates are when the work was merged. The
design and the slice-by-slice detail are in [docs/PLAN.md](docs/PLAN.md).

## Unreleased

### Host: the NAS share stays mounted (2026-10-15)
- **Added:** `deploy/host/keep-mounted.sh` and a launchd LaunchAgent template that keep an SMB share
  mounted at login and every minute: nothing when it answers, a forced unmount and remount when it
  has gone stale (a network change), and no attempt (only a log line) when no password is saved.
  Needed because backups will go to the NAS and a hand-made mount did not survive a network change.

### Dependency updates (2026-10-15)
- **Changed:** pydantic 2.14, pydantic-core 2.50, platformdirs 4.13 and ruff (Dependabot's first
  grouped Python update). pydantic 2.14 stopped putting the numeric pattern on `Decimal` fields in
  the JSON schema, so the models now say it themselves (`document_decimals`): the published API
  schema and the types generated from it are byte-for-byte unchanged.

### Cleanup C2: before Phase 7 (2026-10-15)
- **Fixed:** on a clean clone `kterm ui` showed a page saying the front end was not built, without
  saying how. The page and the README now name the one-time build command (Node 22), until Phase 8
  packages it.
- **Changed:** the server image pins the uv version it builds with, so a new uv release cannot change
  a deploy; CI uses current action versions; Dependabot (monthly, grouped, at most three open
  pull requests) keeps the actions, the Docker image, npm and uv current.
- Tests: 19 new cases for the order write client's retry rules, the kill switch, cancel-by-id, the
  order tracker and the risk config reader; branch coverage of the money-handling code 97% to 99%.

### Cleanup C1: reuse and maintainability (2026-10-14 to 2026-10-15)
- **Changed:** money scaling lives in one place, `kalshi_core.fixedpoint`, instead of ten copies of
  the constants and two differing `to_e6`. The old `kalshiterm_server.fixedpoint` and
  `kalshiterm_server.analytics.fees` modules are gone; import from `kalshi_core`.
- **Changed:** `server/cli.py` is split into command-group modules (`cli_ingest`, `cli_access`,
  `cli_backup`, `cli_analyze`); `kterm-server ... --help` is unchanged. The client's nested
  command definitions are ordinary functions; `risk.check` is one function per rule group.
- **Changed:** `kterm-server init` reports a key path that is a directory, or missing, with an
  explanation and exit code 1 (it was the bare option error, exit code 2).
- Documentation: docstrings on the large undocumented functions; a real README quickstart; PLAN.md
  now holds the design and `docs/BUILD-LOG.md` the build history (section numbers 10.1 to 10.9 kept).
- Tests: the pinning tests skip the LAN-announcement wait (42 s to 11 s for that file); the
  websocket tests use one generous hang guard so a slow CI runner cannot fail healthy tests; a
  time-of-day flake in two status tests is fixed.

### Deploying the server (2026-10-14)
- **Added:** `deploy/host/install.sh` (`check`, `install`, `verify`, `uninstall`): preflight, a
  prompt for the data volume, and the LaunchDaemons for the drive check and the LAN announcement.
- **Fixed:** the drive check runs as `/bin/bash` so macOS privacy permissions can be granted to
  a narrow program; a wrong `KEY_FILE` fails immediately instead of creating a directory.

### Phase 7: terminal UI (in progress, 2026-10-13 to 2026-10-14)
- **Added:** the local web UI backend (`kterm ui`), its data and trading APIs, per-user layouts,
  and the front-end scaffold (Vite, React, TypeScript, a dockable layout, the command bar).

### Phase 6: client trading (2026-10-13)
- **Added:** the risk layer with tiny default limits, the kill switch, a separate trading client
  that can reach only a fixed list of order routes, shard funding, account tracking and the
  `kterm buy | sell | amend | cancel | kill | resume | balance | positions | orders | fills`
  commands. Checked on the live account.

### Phase 5: client data side (2026-10-12 to 2026-10-13)
- **Added:** the typed API and stream clients, and the data, account and live commands (`markets`,
  `market`, `candles`, `trades`, `book`, `analytics`, `arbitrage`, `watch`, `watchlist`, `alerts`).

### Phase 4: analytics (2026-10-10 to 2026-10-12)
- **Added:** the analyzer framework (built in and plugins), market metrics, arbitrage detection,
  per-user alerts, and their REST endpoints and push.

### Phase 3b: LAN features (2026-10-09 to 2026-10-10)
- **Added:** database backup and restore, client profiles with tokens in the system keyring,
  trust-on-first-use certificate pinning, and mDNS discovery.

### Phase 3: server API (2026-10-08)
- **Added:** the HTTPS API with users and per-user tokens, a self-signed certificate, reference
  data, candles, raw data, per-user watchlists and a live websocket push.

### Phase 2: server storage and ingestion (2026-10-06 to 2026-10-08)
- **Added:** the TimescaleDB stack, reference data, the streaming ingestor, combo (multivariate)
  market handling, orderbook storage for watched markets, gap backfill, compression and retention,
  candles, market slimming, the storage governor, `kterm-server status`, and the Docker Compose
  deployment with health checks.

### Phase 1: `kalshi-core` (2026-10-06)
- **Added:** configuration (demo by default), request signing (Ed25519 only), rate limiting, a
  GET-only REST client, the websocket with reconnect, the orderbook, multivariate markets, clock
  skew handling, and opt-in live tests.

### Phase 0: workspace scaffolding (2026-10-06)
- **Added:** the uv workspace with three packages, linting, type checking and CI.
