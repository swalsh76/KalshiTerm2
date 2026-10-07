# KalshiTerm — Master Plan

> Status: **Pre-implementation; all design decisions resolved.** This
> document is the source of truth for the architecture and build order. Items marked
> **[OPEN]** are undecided; see [§12 Open Decisions](#12-open-decisions).

## Contents

1. [Goals](#1-goals)
2. [System Architecture](#2-system-architecture)
3. [Repository & Packaging](#3-repository--packaging)
4. [kalshi-core (SDK)](#4-kalshi-core-sdk)
5. [kalshiterm-server](#5-kalshiterm-server)
6. [kalshiterm-client](#6-kalshiterm-client)
7. [Client Interface (Bloomberg-style)](#7-client-interface-bloomberg-style)
8. [Remote LAN Deployment](#8-remote-lan-deployment)
9. [Storage Budget & Retention (100 GB)](#9-storage-budget--retention-100-gb)
10. [Build Phases](#10-build-phases)
11. [Risks & Notes](#11-risks--notes)
12. [Open Decisions](#12-open-decisions)

---

## 1. Goals

- Ingest real-time market data from Kalshi into **PostgreSQL**.
- Run a **server-side analytics component** over the stored data.
- Provide a **client** that (1) reads data and analytics from the server and
  (2) executes trades on Kalshi.
- Deliver a **Bloomberg-terminal-style** interface: dense, linked, keyboard-driven, live.
- Run the server stack (Postgres + ingestion + analytics + API) on a **separate machine
  on the same LAN** as the client.
- Written in **Python**, structured as standard installable packages. The project is
  **private**: no PyPI publishing; distribution is via the private git repo.

## 2. System Architecture

```
 ┌──────────── LAN SERVER (Docker host) ──────────────────────────┐
 │  docker compose:                                               │
 │   timescaledb ◄── ingestor ◄──── Kalshi WS/REST (data-only key)│
 │        ▲     ◄── analytics worker (+ storage governor)         │
 │        └──────── api :8700 (HTTPS + WS, token auth) ───────────┼──┐
 │   (Postgres port NOT exposed by default)                       │  │ LAN
 └────────────────────────────────────────────────────────────────┘  │
 ┌──────────── WORKSTATION ───────────────────────────────────────┐  │
 │  kterm local backend (127.0.0.1) ◄─────────────────────────────┼──┘
 │     ├── trading key ──► Kalshi REST/WS (orders)                │
 │     └── UI (browser or desktop)                                │
 └────────────────────────────────────────────────────────────────┘
```

**Key principle: trades go client → Kalshi directly, never through the server.**

- The trading private key never leaves the workstation.
- A compromised server cannot place orders.
- Trading keeps working if the server is down (data panels go stale; trading and the
  kill switch still function).
- One fewer network hop on order latency.

The server uses its **own, separate Kalshi API key** for data ingestion (Kalshi's
WebSocket requires an authenticated connection even for public channels).

## 3. Repository & Packaging

A **uv workspace** monorepo with **three packages** (private; never published to PyPI),
so each install pulls in only what it needs.

| Package | Purpose | Key dependencies |
|---|---|---|
| `kalshi-core` | Reusable Kalshi SDK: auth/signing, async REST, WS with reconnect, typed models, rate limiter, env config | `httpx`, `websockets`, `pydantic` v2, `cryptography` |
| `kalshiterm-server` | Ingestor, analytics worker, API, DB schema/migrations, ops CLI | `kalshi-core`, `sqlalchemy` 2.0, `asyncpg`, `alembic`, `fastapi`, `uvicorn`, `zeroconf` |
| `kalshiterm-client` | Data access, trading, risk layer, CLI, UI | `kalshi-core`, `httpx`, `typer`, `keyring`, UI stack (see §7) |

> Package names are internal to the workspace; PyPI availability is irrelevant.

```
KalshiTerm2/
├── pyproject.toml                  # workspace root; ruff/mypy/pytest config
├── packages/
│   ├── kalshi-core/
│   │   └── src/kalshi_core/{auth,rest,ws,models,ratelimit,config}.py
│   ├── kalshiterm-server/
│   │   └── src/kalshiterm_server/{ingest/,storage/,analytics/,api/,ops/,migrations/}
│   └── kalshiterm-client/
│       └── src/kalshiterm_client/{data/,trading/,risk/,cli/,ui/}
├── frontend/                       # only if web UI is chosen (§7)
├── deploy/
│   ├── docker-compose.yml
│   └── Dockerfile
├── docs/                           # Markdown docs (MkDocs optional)
└── .github/workflows/              # CI
```

**Standards**

- `src/` layout; PEP 621 `pyproject.toml`; **hatchling** build backend; version from git
  tags (`hatch-vcs`) or a static version; `py.typed` markers.
- Python **3.11+**.
- Tooling: `ruff` (lint + format), `mypy` (strict), `pytest` + `pytest-asyncio`, `respx`
  (HTTP mocks), `testcontainers` (Postgres), `pre-commit`.
- CI: GitHub Actions test matrix (3.11–3.13; Linux for all packages, plus Windows and
  macOS for client & core, since the workstation may be either); server Docker image
  built in CI to verify it. No registry push or PyPI publish required.
- Repo hygiene: `LICENSE` (**MIT**), `CHANGELOG.md`, `SECURITY.md`.
- All configuration via **pydantic-settings** (env vars and/or TOML). No hard-coded
  `localhost` anywhere.

## 4. kalshi-core (SDK)

**Endpoints** (verified against docs.kalshi.com, Oct 2026)

| | URL |
|---|---|
| REST (prod) | `https://external-api.kalshi.com/trade-api/v2` |
| REST (demo) | `https://external-api.demo.kalshi.co/trade-api/v2` |
| WebSocket (prod) | `wss://external-api-ws.kalshi.com/trade-api/ws/v2` |
| WebSocket (demo) | `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2` |

**Read-only safeguards (production data key in use during dev)**

1. **Key level:** a dedicated read-only Kalshi key (scope `read`), stored outside the repo
   (`~/.kalshiterm/`, mode 600). Only the Key ID goes in the gitignored `.env`.
2. **No write code exists** before Phase 6: no order, cancel, amend or transfer endpoints.
3. **GET-only transport guard** in the HTTP client (added with the REST client), with a
   test (implemented in slice 4: `ReadOnlyViolation`); Phase 6 must remove it deliberately.
4. **Explicit production opt-in** via `KALSHI_ENV=production`; demo stays the default;
   a clear banner is logged when running against production.
5. **Opt-in live tests:** integration tests against production run only when explicitly
   enabled and use a few cheap read calls (rate-limit friendly).

**Authentication**

- Headers: `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (ms), `KALSHI-ACCESS-SIGNATURE`.
- Signature over `timestamp + METHOD + path` (path excludes query string), base64-encoded.
- **Ed25519 keys only.** RSA-PSS is deliberately not supported (decided 2026-10-06); an RSA
  key file is rejected with a clear error.
- WS auth headers are sent during the handshake; the signed string is
  `timestamp + "GET" + "/trade-api/ws/v2"`.

**WebSocket channels**

- Public: `ticker`, `trade`, `orderbook_delta`, `market_lifecycle_v2`,
  `multivariate_market_lifecycle` (+ benchmark/oracle value feeds).
- Private: `fill`, `user_orders`, `market_positions`, `order_group_updates`,
  `communications`.

**Features**

- Async REST client with typed request/response models and pagination helpers.
- WS client: auto-reconnect with backoff, automatic resubscription,
  **sequence-gap detection on `orderbook_delta` → automatic recovery**:
  an in-stream `get_snapshot` (primary: its reply carries the subscription's next `seq`, so it
  aligns exactly with the delta stream) with a REST snapshot as fallback (no `seq`, so the book
  is flagged `approximate`). Observed live: `seq` is one counter per subscription shared by all
  its markets, so a gap invalidates every book in it; deltas are signed quantity changes.
- Client-side rate limiter mirroring Kalshi's **token-bucket** model (docs, Oct 2026):
  Basic tier = 200 read / 100 write tokens per second, most requests cost 10 tokens,
  buckets hold one second of budget, read and write are independent, and overage is a bare
  HTTP 429 (no `Retry-After`). We default to **90% of the budget** (180 read tokens/s ≈ 18
  requests/s on Basic) via `KALSHI_RATE_LIMIT_MARGIN`; a higher tier is a one-value change
  (`KALSHI_READ_TOKENS_PER_SECOND`). The write bucket is added in Phase 6. 429 backoff
  lives in the REST client; handles WS command rate-limit errors.
- **Demo environment is the default everywhere**; production requires explicit opt-in
  (`KALSHI_ENV=production`).
- **Read-only until Phase 6** (see safeguards below).
- `kterm doctor`-style clock-skew check (signed requests fail on skewed clocks). Kalshi has no
  time endpoint and documents no timestamp tolerance, so `KalshiRestClient.clock_skew()` infers
  the offset from the HTTP `Date` header (1 s resolution): each sample gives an interval,
  several spaced samples are intersected, and the result reports its uncertainty. The 2 s
  pass/fail threshold is our own, configurable choice, not Kalshi's.

## 5. kalshiterm-server

One Docker image, three service roles: `kterm-server ingest`, `kterm-server analyze`,
`kterm-server api`.

### 5.1 Ingestor

- **Discovery poller** (REST): series, events, markets — periodic refresh. Market outcomes
  (`result`, `settlement_value_dollars`, `settlement_ts`) come from the market object itself:
  Kalshi's `/portfolio/settlements` endpoint is account-scoped (your own positions), not
  market-wide. Markets older than the historical cutoff live under `/historical/*` (to
  evaluate for backfill in Phase 2).
- **Stream subscriber** (WS): `ticker`, `trade`, `market_lifecycle_v2` for all ingested
  markets; `orderbook_delta` for the **watchlist only**.
- **Gap handling:** Kalshi does not replay messages missed during a disconnect, and `sid`
  and `seq` restart on every reconnect (observed live: both came back as 1). The WS client
  yields a `reconnected` event ahead of any post-reconnect data; on it the ingestor backfills
  trades (and re-snapshots watched orderbooks) via REST for the outage window.
- Batched writes to Postgres via `COPY` (asyncpg).
- Each row stores both **exchange timestamp** and **receipt timestamp** (UTC) to measure
  ingestion lag.

### 5.2 Storage

- **PostgreSQL + TimescaleDB** (pinned official image).
- Reference tables: `series`, `events`, `markets` (outcomes stored on the market row).
- Hypertables: `tickers`, `trades`, `orderbook_snapshots`, `orderbook_deltas`.
- Continuous aggregates: 1-minute and 1-hour candles.
- Results: `analysis_results`, `alerts`.
- Migrations via **Alembic**, applied automatically on API startup.

### 5.3 Analytics worker

- **Plugin interface** via entry points (`kalshiterm.analyzers`) so third parties can add
  analyzers without forking.
- Built-in analyzers:
  - OHLC candles / implied-probability history
  - Spread & liquidity metrics
  - Realized volatility
  - **Cross-market arbitrage check** — mutually exclusive outcomes in an event whose YES
    prices don't sum to ~100¢
  - Price / volume move alerts
- Publishes new results via Postgres `NOTIFY` for live push to clients.
- Hosts the **storage governor** (§9).

### 5.4 API

- **FastAPI** over HTTPS (served directly by uvicorn with the self-signed cert).
- REST: markets, history, candles, analytics, alerts.
- WebSocket: live push fed by `LISTEN/NOTIFY`, with **catch-up on reconnect**
  (`since` cursor).
- Health: `/healthz`, `/readyz`; admin-only `/status` (ingestion lag, WS state, DB size per
  table, analyzer last-run, storage projection); optional Prometheus `/metrics`.
- Token auth (§8.3).

## 6. kalshiterm-client

### 6.1 Data side

- Typed client for the server API (REST + live WS stream) with reconnect and catch-up.
- Connection profiles (`kterm config`, `--profile lan|local`).

### 6.2 Trading side (direct to Kalshi)

- Place, amend, cancel orders; positions, balance, fills.
- Private WS channels: `fill`, `user_orders`, `market_positions`.
- Trading key stored locally (file path + OS keyring for passphrase).

### 6.3 Risk layer (every order passes through it)

- Max order size; max position per market; max daily loss.
- Confirmation prompt in production; demo is default.
- Dry-run mode.
- **Kill switch**: cancel all open orders.
- Risk checks use Kalshi's own position/balance data — **no dependency on the server**.

### 6.4 Interfaces

- **Typer CLI** for scripting (`kterm markets`, `kterm buy …`, `kterm server status`).
- Primary UI per §7.

## 7. Client Interface (Bloomberg-style)

**Requirements:** dense multi-panel layout (dock/link/tear-off, multi-monitor), command
bar with mnemonic navigation, keyboard-first, high-frequency streaming grids, quality
charts (candles, depth), integrated order entry.

### 7.1 Options compared

| | A. Textual TUI | B. Qt desktop (PySide6 + pyqtgraph) | C. Python-only web (NiceGUI/Dash/Panel) | D. Web: Python backend + TS frontend |
|---|---|---|---|---|
| Language | Pure Python | Pure Python | Pure Python | Python + TypeScript |
| Multi-panel density | Good, single window | **Excellent** (QDockWidget, multi-monitor) | Weak | **Excellent** (dockview) |
| Streaming grids | OK (tens of rows) | **Excellent** (QTableView) | **Poor** (server round-trip per update) | **Excellent** (Perspective / AG Grid) |
| Charts | **Poor** (terminal-resolution) | Very good, utilitarian | Good (Plotly), slow streaming | **Best** (Lightweight Charts) |
| Command bar / keyboard | Excellent | Good | Weak | Good (built by hand) |
| Remote access | Via SSH | No | Yes | Yes |
| `pip install` | Trivial | Easy, ~100 MB+ wheel | Easy | Easy — prebuilt assets in wheel |
| Toolchain burden | None | None | None | Node build step |
| UI testability | Good | Hard | Medium | Good (Playwright) |
| Effort | Low | Medium | Low–Med | **High** |

### 7.2 Assessment

- **C is eliminated** — server-round-trip frameworks cannot keep up with live order books.
- **A** is a fine companion tool but its charts disqualify it as the primary terminal.
- **B** is the strongest pure-Python option; costs are dated styling, heavy dependency,
  harder testing, and asyncio integration via `qasync`.
- **D** has the highest ceiling and is what modern trading front ends use.

### 7.3 Decision: D (web) + Typer CLI

- `kalshiterm-client` runs a **local FastAPI backend on 127.0.0.1** that holds trading
  keys, talks to Kalshi and to the LAN server. The browser never sees keys.
- Frontend: **React + TypeScript + Vite**, **dockview** (panels), **Perspective** (live
  grids), **TradingView Lightweight Charts** (charts), global command bar.
- Built frontend assets ship **inside the wheel** (built by a release script/CI), so
  `uv tool install` of the client → `kterm ui` needs no Node on the workstation.
- The client runs **natively** on the workstation (Windows or macOS), **not in Docker**:
  it needs the OS keychain (`keyring`), a trading-key file, and a browser reaching
  127.0.0.1. Docker Desktop's VM would break the keychain, complicate the loopback
  binding, and put the trading key in a container for no gain. Docker is for the server.
- Local backend hardening: bind 127.0.0.1 only, Origin checks, per-session token
  (prevents CSRF from other browser tabs placing orders).
- **Fallback:** if pure Python is a hard requirement, choose **B (Qt)**.

## 8. Remote LAN Deployment

### 8.1 Host requirements

**Target host: Mac Studio (M4, arm64) running Docker on macOS**, 100 GB data budget.
(Any Linux/Windows-WSL2 host also works; the image builds on the host for any arch.)

- Always-on: disable sleep, enable "start up automatically after a power failure".
  Docker Desktop starts at login, not boot, and FileVault disables auto-login — so either
  accept auto-login or use a launchd-managed runtime (OrbStack/Colima).
- Postgres uses a **named Docker volume**, not a bind mount. Set the Docker VM disk cap
  to ~100 GB plus margin for images; it does not shrink on its own.
- mDNS cannot be advertised from inside the container through the Docker VM: use a
  manual host/IP or run the zeroconf advertiser on the host.

- **Docker Engine + Compose v2** (any Linux, Windows with WSL2, or Docker-capable NAS).
- Docker is the **only** supported server deployment path.
- Image is built on the host from the repo (any arch); no registry required.
- Accurate clock (NTP / chrony) — required for Kalshi request signing.

### 8.2 Bootstrap

```bash
git clone <private-repo> && cd KalshiTerm2/deploy
docker compose run --rm api kterm-server init   # writes .env, secrets, TLS cert
docker compose up -d --build
docker compose exec api kterm-server user add <name>
docker compose exec api kterm-server token create --user <name> --role read
```

- Data directory is configurable (`KTERM_DATA_DIR`); a dedicated disk/partition is
  recommended.
- Upgrades: `git pull && docker compose up -d --build` (migrations run on startup).

### 8.3 Network exposure & security (LAN treated as untrusted)

- Only the **API port (default 8700)** is exposed to the LAN.
- Postgres stays on the internal Docker network by default.
- **Optional `--expose-db`** for ad-hoc analysis (Jupyter, DBeaver): read-only role,
  `pg_hba` restricted to the LAN subnet, `scram-sha-256`, TLS.
- **Multi-user**: the server has named users (`kterm-server user add <name>`). Each user
  has one or more API tokens; per-user data (watchlists, alert rules, UI layouts) is
  scoped by user, while market data and analytics results are shared.
- **API tokens**: `kterm-server token create --user <name> --role read|admin`; stored
  hashed server-side; stored in the OS keychain (`keyring`) client-side.
- Users never share trading credentials: trading keys stay on each user's own
  workstation (§2), so multi-user affects only the data side.
- Watchlist membership drives orderbook capture, so the ingested watchlist is the union
  of all users' watchlists, and the storage governor (§9.3) still applies globally.

### 8.4 TLS (self-signed)

- `kterm-server init` generates a self-signed certificate (via `cryptography`) with SANs
  for the hostname, `<host>.local`, and the LAN IP.
- uvicorn terminates TLS directly — no extra proxy container.
- Client uses **trust-on-first-use**: shows the fingerprint on first connect, then pins it.
- The fingerprint is also published in the mDNS TXT record for cross-checking.
- `kterm-server cert rotate` regenerates; clients get an explicit re-confirm prompt, never
  silent acceptance.

### 8.5 Discovery

- Server advertises `_kterm._tcp.local` via **zeroconf/mDNS**.
- `kterm server discover` lists servers; manual host/IP always supported.

### 8.6 Operations & resilience

- `kterm server status` / `kterm server logs` from the workstation; health panel in the UI.
- Client reconnects with backoff and catches up via `since` cursor.
- Server outage degrades data panels to "stale"; **trading is unaffected**.

### 8.7 Testing

- CI runs the server via Compose and the client in a **separate container on a separate
  Docker network**, exercising real networking, TLS pinning, and token auth.

### 8.8 Development environment

Development happens on a **MacBook Pro M4**; the production server host is a separate
**Mac Studio M4**. Same arch (arm64) and same Docker Desktop on macOS, so images tested on
the laptop match production.

- The dev stack runs locally in Docker Desktop on the MacBook; the client runs natively on
  the same machine via a `local` connection profile (the address lives in profile config,
  never hard-coded).
- Dev uses a **read-only production data key** (decision 2026-10-06), under the safeguards
  in §4; the default environment in code remains demo. Production data is also used for
  the Phase 2 48-hour calibration run, which runs on the Mac Studio.
- Dev storage budget is small (`KTERM_STORAGE_BUDGET_GB=10`–`20`) so the governor's
  thresholds are exercised; set the Docker VM disk cap to match.
- Backups: `KTERM_BACKUP_TARGET` unset or a local folder; the NAS is production only.
- Laptop sleep/battery cause ingestion gaps in dev; that is acceptable. Gap handling
  (`since` catch-up, WS reconnect) is worth testing by sleeping the laptop on purpose.
- Dev and production use distinct `.env` files and Compose project names so config and
  volumes never mix.

## 9. Storage Budget & Retention (100 GB)

Docker cannot cap a Postgres volume, so the server governs its own footprint.

### 9.1 Budget allocation

| Use | Allocation |
|---|---|
| Postgres overhead (WAL, temp, compression rewrites) + free-space headroom | 15 GB |
| Backups | 0 GB (stored on the NAS, §9.5) |
| Reference data, analytics results, continuous aggregates | 8 GB |
| Hot uncompressed chunks (last 1–2 days) | 10 GB |
| Compressed history | ~67 GB |
| **Total** | **100 GB** |

The budget is per host (`KTERM_STORAGE_BUDGET_GB`); the Mac Studio target is 100 GB.
A larger host can raise it and scale compressed history accordingly.

### 9.2 Retention defaults (all configurable)

| Data | Raw retention | Then |
|---|---|---|
| Trades (30 days raw; watchlist markets: forever) | Compressed after 1 day | 1-min / 1-hour candles forever |
| Tickers (ordinary markets) | 14 days, compressed after 1 day | 1-min / 1-hour aggregates kept forever |
| Multivariate (combo) markets | One compact row per market (legs as an array, plus outcome), 30 days; raw combo tickers/trades 3 days | No candles for combo markets |
| Orderbook deltas (watchlist only) | 14 days | Dropped |
| Orderbook snapshots (watchlist, every 60 s) | 90 days | 5-min downsample kept 1 year |
| Markets / events (incl. outcomes) | Forever | — |

TimescaleDB native compression expected to yield ~10–20× on aged chunks (unmeasured). Measured
volumes (2026-10-06) put raw ingestion at roughly 10 GB/day (~0.5–1 GB/day compressed), which
is why the original "trades forever" and "7-day uncompressed window" defaults were dropped.

### 9.3 Storage governor

- Runs inside the analytics worker (in Phase 2, before that worker exists, inside the ingestor
  process; same code); budget set by `KTERM_STORAGE_BUDGET_GB=100`.
- Tracks per-table size, daily growth rate, and **projected days to full**; exposed on
  `/status`, `kterm server status`, and the UI.
- **80% of budget:** alert + tighten raw retention windows.
- **90% of budget:** stop orderbook-delta capture (snapshots only). Trades and tickers are
  never shed.

### 9.4 Calibration

Volume figures above are estimates. Phase 2 includes a **48-hour calibration run against
production data** to measure rows/bytes per day; defaults will be tuned from measurements.

### 9.5 Backups

- Scheduled `pg_dump` streamed to `KTERM_BACKUP_TARGET`, an SMB/NFS share on the NAS.
  Backups consume none of the local budget, and being off-host they survive disk failure.
- The NAS has ample space, so retention is generous and configurable (default: 7 daily +
  4 weekly). No local staging copy is kept.
- If the NAS is unreachable the backup job fails loudly (alert + `/status`), never
  silently; the mount must be present after reboots, so check it before each run.
- `kterm-server backup` / `kterm-server restore`.

## 10. Build Phases

Each phase ends with passing tests and CI green.

| Phase | Deliverable |
|---|---|
| **0. Scaffolding** | uv workspace, three package skeletons, ruff/mypy/pytest, pre-commit, CI matrix, server image build, docs skeleton, MIT license & repo hygiene files |
| **1. kalshi-core** | Signing (Ed25519), REST client & models, WS client with reconnect/resubscribe/seq-gap recovery, rate limiter, **multivariate market support** (`/events/multivariate`, MVE market fields `mve_collection_ticker` / `mve_selected_legs`, WS `multivariate_market_lifecycle` channel); integration-tested read-only (production data key; demo where applicable) |
| **2. Server storage & ingestion** | Schema + Alembic, Timescale hypertables, compression/retention policies, continuous aggregates, ingestor, Compose stack, `kterm-server init`, storage governor, **48-h calibration run** — broken into slices 2.1–2.10 in §10.1 |
| **3. Server API** | REST + WS push with catch-up, token auth, TLS, health/status endpoints |
| **3b. LAN features** | zeroconf discovery, client profiles, TOFU cert pinning, `cert rotate`, `kterm server status`, backup/restore |
| **4. Analytics** | Plugin framework + built-in analyzers + alerts |
| **5. Client data side** | Server API client, CLI data views |
| **6. Client trading** | Risk layer first, order management, private WS channels; **demo-only until sign-off**; test proving trading works with server offline |
| **7. Terminal UI** | Bloomberg-style UI per §7 decision (local backend, panels, grids, charts, command bar, order entry) |
| **8. Packaging** | Frontend build bundled into client wheel, install docs, tagged v0.1.0 |

### 10.1 Phase 2 slices

Each slice is its own branch, merged when CI is green. Database tests use a real TimescaleDB
container and run on the Linux CI job only (macOS/Windows runners have no Docker).

| # | Slice |
|---|---|
| 2.1 | Dev database stack: Compose + pinned TimescaleDB, Alembic scaffold, DB test harness |
| 2.2 | Reference data: `series`, `events`, `markets` + discovery poller (≈121k open ordinary markets, full refresh ≈14 s; combo markets arrive via lifecycle events, not polling) |
| 2.3 | Streaming core: batched `COPY` writer, `tickers` / `trades` / lifecycle tables, exchange + receipt timestamps, ingestion-lag metric, behaviour when the database is down |
| 2.4 | Combo-market storage (decision 10) |
| 2.5 | Orderbook storage for the watchlist: snapshots + deltas, config-file watchlist + automatic top-N by volume (per-user watchlists arrive with the Phase 3 API) |
| 2.6 | Gap handling: gap log; trade backfill via REST on a `reconnected` event (tickers cannot be backfilled, so those gaps are recorded) |
| 2.7 | Compression, retention policies, continuous aggregates (1-minute, 1-hour) |
| 2.8 | Storage governor + `kterm-server status` |
| 2.9 | `kterm-server init`, production Compose, health checks, Mac Studio deployment (TLS certificate generation moves to Phase 3 with the API) |
| 2.10 | 48-hour calibration run (decision 11), then retune retention defaults |

The server uses its own read-only Kalshi key (created at deployment time), separate from the
dev key.

## 11. Risks & Notes

- **Client throughput (offline benchmark, `packages/kalshi-core/bench/throughput.py`,
  2026-10-06, M4 MacBook Pro, one core):** the WebSocket client reads and dispatches
  ~120–220k msgs/s (4–8 µs each; the low end with `permessage-deflate`, which Kalshi
  negotiates), and the orderbook feed applies ~130–145k deltas/s across 50 markets — roughly
  50× the live peak seen so far, so parsing is not the bottleneck. **The real risk is the
  unbounded receive queue:** with a slow consumer each queued message costs ~1.8 KB, so at
  ~700 msgs/s a stalled database writer grows memory ~1.3 MB/s (~4.7 GB/h) with no warning.
  **Resolved (slice 10, decided 2026-10-06):** the receive queue is bounded
  (`max_queue_messages`, default 500,000 ≈ 0.9 GB ≈ 5 min of stall at the mean rate). On
  overflow the client stops accepting data, drops the connection and reconnects; queued
  messages are still delivered in order, followed by a `reconnected` event with
  `reason="overflow"` and `dropped=N`, on which the ingestor backfills. The reconnect waits
  until the consumer has drained to half the limit, so a stalled consumer causes no reconnect
  storm (a dead one just leaves us disconnected with bounded memory). The orderbook feed
  applies backpressure so the backlog stays in the bounded queue; `stats()` exposes depth,
  limit, high-water mark, overflows and drops for the server's `/status`.
- **Live soak (`packages/kalshi-core/bench/soak.py`, 10 min, Tue 2026-10-06 ~21:30 ET, read-only):**
  all trades + all tickers + both lifecycle channels + orderbooks for the 50 busiest ordinary
  markets: 931k messages, mean 1,552/s, p95 2,022/s, peak 3,322/s; **zero** sequence gaps,
  reconnects or parse failures; memory flat at ~71 MB; event-loop lag max 13.5 ms. By channel
  (mean/s, avg payload): `ticker` 755 (433 B) — the largest by far —, multivariate lifecycle
  396 (246 B), `trade` 233 (305 B), orderbook deltas 108 for 50 markets, `event_lifecycle` 57,
  ordinary lifecycle 2.6. Extrapolated to a day (one sample; peaks vary): ~65 M ticker rows,
  ~34 M MVE lifecycle, ~20 M trades, ~5 M event lifecycle. Raw payload JSON is ~0.5 MB/s
  (~45 GB/day verbatim; ticker alone ~65%), so ticker retention and storage format are the
  main storage levers for the 100 GB budget — to be settled by the Phase 2 calibration run.
- **Multivariate volume (measured 2026-10-06, Tuesday evening, ~10–45 s samples):** the
  `multivariate_market_lifecycle` channel delivers ~400–500 messages/s (~100 market creations/s,
  plus ~100/s each of `determined` and `settled`), versus ~2/s for ordinary markets. Unfiltered
  `/markets` is dominated by `KXMVE…` combo markets (1000 of the first 1000 open), and each
  carries 2–58 legs (median ~5). At ~1 KB per creation message this alone is on the order of
  10 GB/day uncompressed. Only the 48-hour calibration run can say what is sustainable;
  expect MVE to need compact storage and short raw retention. Also measured: the unfiltered
  `trade` channel delivers ~180 trades/s across all markets (1,472 in 8 s), and Kalshi lists
  ~14,700 series.
- **Data volume** — multivariate (`KXMVE…`) markets are numerous and busy and are now kept, so
  the Phase 2 calibration run must measure them separately; the storage governor still
  applies, and MVE retention may need to be tighter than for ordinary markets. Full-universe
  orderbook streaming is very large; orderbooks are
  watchlist-only by design, and the storage governor enforces the budget.
- **Kalshi API terms** — if the server ever serves data to people other than the owner,
  review Kalshi's terms on data redistribution.
- **API drift** — Kalshi endpoints have changed hosts before; all URLs live in
  `kalshi-core` config, not scattered constants.
- **Clock skew** — breaks request signing; `kterm doctor` checks it.
- **Scope creep** — the predecessor project (KalshiTerm 1) failed through feature creep
  and shifting direction. Work proceeds phase by phase; out-of-plan ideas go to §13,
  and changes of direction are made in this document first, with approval.
- **Real money / live key** — demo is the default; dev uses a read-only production data
  key under the §4 safeguards; production trading requires explicit opt-in and
  passes the risk layer on every order.

## 12. Open Decisions

| # | Decision | Recommendation | Status |
|---|---|---|---|
| 1 | Client UI: D (web) vs B (Qt) | D (web), with Typer CLI | **Decided** — D (web) + Typer CLI |
| 2 | Trading mode: manual only vs automated strategies | Manual first; strategy interface in a later phase | **Decided** — manual first |
| 3 | Ingestion scope: all markets vs watchlist | Tickers/trades for all; orderbooks watchlist-only | **Decided** — as recommended |
| 4 | Server users: single vs multi-user | — | **Decided** — multi-user (see §8.3) |
| 5a | License | MIT or Apache-2.0 | **Decided** — MIT |
| 5b | Package names | Names as in §3 | **Decided** — keep as-is; private, no PyPI |
| 6 | Distribution | — | **Decided** — private git repo, no PyPI; client native on Windows/macOS, server in Docker |
| — | Server host runs Docker | Required | **Decided** |
| — | Server disk budget | 100 GB (Mac Studio host); backups on NAS | **Decided** |
| 8 | Key types | Ed25519 only; RSA-PSS not supported | **Decided** |
| 7 | Multivariate (combo) markets | Keep: ingest and store them (decided 2026-10-06; they dominate the live trade stream) | **Decided** |
| — | Dev credentials | Read-only production Kalshi key (decided 2026-10-06), layered safeguards in §4; demo stays default | **Decided** |
| 9 | Trade retention | 30 days raw (watchlist markets forever), 1-min/1-hour candles forever | **Decided** |
| 10 | Combo-market storage | Compact: one row per market (legs as array + outcome), 30 days; raw combo tickers/trades 3 days; no candles | **Decided** |
| 11 | Calibration host | MacBook with sleep disabled (before the Mac Studio is the host) | **Decided** |
| — | Server host | Mac Studio M4, Docker on macOS (supersedes Pi/Windows ideas) | **Decided** |
| — | TLS approach | Self-signed + TOFU pinning | **Decided** |
| — | Server location | Remote host on same LAN | **Decided** |

## 13. Parking Lot

Ideas raised but **not** in scope. Not to be built until promoted into a phase.

- Automated trading strategies (decision #2: manual first; strategy interface later).
