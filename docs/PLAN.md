# KalshiTerm — Master Plan

> Status: **Pre-implementation; design decisions resolved except package names.** This
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
9. [Storage Budget & Retention (150 GB)](#9-storage-budget--retention-150-gb)
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
- Written in **Python**, structured as standard packages suitable for **public
  distribution on PyPI**.

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

A **uv workspace** monorepo publishing **three PyPI distributions**, so each install
pulls in only what it needs.

| Package | Purpose | Key dependencies |
|---|---|---|
| `kalshi-core` | Reusable Kalshi SDK: auth/signing, async REST, WS with reconnect, typed models, rate limiter, env config | `httpx`, `websockets`, `pydantic` v2, `cryptography` |
| `kalshiterm-server` | Ingestor, analytics worker, API, DB schema/migrations, ops CLI | `kalshi-core`, `sqlalchemy` 2.0, `asyncpg`, `alembic`, `fastapi`, `uvicorn`, `zeroconf` |
| `kalshiterm-client` | Data access, trading, risk layer, CLI, UI | `kalshi-core`, `httpx`, `typer`, `keyring`, UI stack (see §7) |

> Package names are provisional — all three were unclaimed on PyPI as of 2026-10-06
> but not yet reserved. **[OPEN]**

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
├── docs/                           # MkDocs-Material
└── .github/workflows/              # CI, image build, PyPI publish
```

**Standards**

- `src/` layout; PEP 621 `pyproject.toml`; **hatchling** build backend; version from git
  tags (`hatch-vcs`); `py.typed` markers.
- Python **3.11+**.
- Tooling: `ruff` (lint + format), `mypy` (strict), `pytest` + `pytest-asyncio`, `respx`
  (HTTP mocks), `testcontainers` (Postgres), `pre-commit`.
- CI: GitHub Actions test matrix (3.11–3.13, Linux/Windows/macOS for client & core);
  multi-arch (amd64/arm64) Docker image to GHCR; **PyPI Trusted Publishing** on tags.
- Repo hygiene: `LICENSE` (**MIT**), `CHANGELOG.md`, `SECURITY.md`,
  `CONTRIBUTING.md`.
- All configuration via **pydantic-settings** (env vars and/or TOML). No hard-coded
  `localhost` anywhere.

## 4. kalshi-core (SDK)

**Endpoints** (verified against docs.kalshi.com, Oct 2026)

| | URL |
|---|---|
| REST (prod) | `https://external-api.kalshi.com/trade-api/v2` |
| REST (demo) | `https://external-api.demo.kalshi.co/trade-api/v2` |
| WebSocket (prod) | `wss://external-api-ws.kalshi.com/` |

**Authentication**

- Headers: `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (ms), `KALSHI-ACCESS-SIGNATURE`.
- Signature over `timestamp + METHOD + path` (path excludes query string), base64-encoded.
- Supports **RSA-PSS/SHA-256** and **Ed25519** keys.
- WS auth headers are sent during the handshake.

**WebSocket channels**

- Public: `ticker`, `trade`, `orderbook_delta`, `market_lifecycle_v2`,
  `multivariate_market_lifecycle` (+ benchmark/oracle value feeds).
- Private: `fill`, `user_orders`, `market_positions`, `order_group_updates`,
  `communications`.

**Features**

- Async REST client with typed request/response models and pagination helpers.
- WS client: auto-reconnect with backoff, automatic resubscription,
  **sequence-gap detection on `orderbook_delta` → automatic REST re-snapshot**.
- Client-side rate limiter; handles WS command rate-limit errors.
- **Demo environment is the default everywhere**; production requires explicit opt-in.
- `kterm doctor`-style clock-skew check (signed requests fail on skewed clocks).

## 5. kalshiterm-server

One Docker image, three service roles: `kterm-server ingest`, `kterm-server analyze`,
`kterm-server api`.

### 5.1 Ingestor

- **Discovery poller** (REST): series, events, markets, settlements — periodic refresh.
- **Stream subscriber** (WS): `ticker`, `trade`, `market_lifecycle_v2` for all ingested
  markets; `orderbook_delta` for the **watchlist only**.
- Batched writes to Postgres via `COPY` (asyncpg).
- Each row stores both **exchange timestamp** and **receipt timestamp** (UTC) to measure
  ingestion lag.

### 5.2 Storage

- **PostgreSQL + TimescaleDB** (pinned official image).
- Reference tables: `series`, `events`, `markets`, `settlements`.
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
- Built frontend assets ship **inside the wheel**: `pip install kalshiterm-client` →
  `kterm ui`. No Node needed by end users.
- Local backend hardening: bind 127.0.0.1 only, Origin checks, per-session token
  (prevents CSRF from other browser tabs placing orders).
- **Fallback:** if pure Python is a hard requirement, choose **B (Qt)**.

## 8. Remote LAN Deployment

### 8.1 Host requirements

- **Docker Engine + Compose v2** (any Linux, Windows with WSL2, or Docker-capable NAS).
- Docker is the **only** supported server deployment path.
- Multi-arch images (amd64 / arm64).
- Accurate clock (NTP / chrony) — required for Kalshi request signing.

### 8.2 Bootstrap

```bash
pip install kalshiterm-server
kterm-server init          # writes docker-compose.yml, .env, secrets, TLS cert
docker compose up -d
kterm-server token create --role read   # credential for a client
```

- Data directory is configurable (`KTERM_DATA_DIR`); a dedicated disk/partition is
  recommended.
- Upgrades: `docker compose pull && docker compose up -d` (migrations run on startup).

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

## 9. Storage Budget & Retention (150 GB)

Docker cannot cap a Postgres volume, so the server governs its own footprint.

### 9.1 Budget allocation

| Use | Allocation |
|---|---|
| Postgres overhead (WAL, temp, compression rewrites) + free-space headroom | 25 GB |
| Local backups (2 rolling compressed `pg_dump`s) | 15 GB |
| Reference data, analytics results, continuous aggregates | 10 GB |
| Hot uncompressed chunks (last 7 days) | 15 GB |
| Compressed history | ~85 GB |
| **Total** | **150 GB** |

### 9.2 Retention defaults (all configurable)

| Data | Raw retention | Then |
|---|---|---|
| Trades | Forever (compressed after 7 days) | — |
| Tickers | 30 days | 1-min / 1-hour aggregates kept forever |
| Orderbook deltas (watchlist only) | 14 days | Dropped |
| Orderbook snapshots (watchlist, every 60 s) | 90 days | 5-min downsample kept 1 year |
| Markets / events / settlements | Forever | — |

TimescaleDB native compression expected to yield ~10–20× on aged chunks.

### 9.3 Storage governor

- Runs inside the analytics worker; budget set by `KTERM_STORAGE_BUDGET_GB=150`.
- Tracks per-table size, daily growth rate, and **projected days to full**; exposed on
  `/status`, `kterm server status`, and the UI.
- **80% of budget:** alert + tighten raw retention windows.
- **90% of budget:** stop orderbook-delta capture (snapshots only). Trades and tickers are
  never shed.

### 9.4 Calibration

Volume figures above are estimates. Phase 2 includes a **48-hour calibration run against
production data** to measure rows/bytes per day; defaults will be tuned from measurements.

### 9.5 Backups

- Scheduled `pg_dump`, two rolling copies (counted in the budget).
- `KTERM_BACKUP_TARGET` may point to an SMB/NFS share — recommended, since a same-disk
  backup does not protect against disk failure.
- `kterm-server backup` / `kterm-server restore`.

## 10. Build Phases

Each phase ends with passing tests and CI green.

| Phase | Deliverable |
|---|---|
| **0. Scaffolding** | uv workspace, three package skeletons, ruff/mypy/pytest, pre-commit, CI matrix, multi-arch image build, MkDocs skeleton, license & repo hygiene files |
| **1. kalshi-core** | Signing (RSA-PSS + Ed25519), REST client & models, WS client with reconnect/resubscribe/seq-gap recovery, rate limiter; integration-tested against demo |
| **2. Server storage & ingestion** | Schema + Alembic, Timescale hypertables, compression/retention policies, continuous aggregates, ingestor, Compose stack, `kterm-server init`, storage governor, **48-h calibration run** |
| **3. Server API** | REST + WS push with catch-up, token auth, TLS, health/status endpoints |
| **3b. LAN features** | zeroconf discovery, client profiles, TOFU cert pinning, `cert rotate`, `kterm server status`, backup/restore |
| **4. Analytics** | Plugin framework + built-in analyzers + alerts |
| **5. Client data side** | Server API client, CLI data views |
| **6. Client trading** | Risk layer first, order management, private WS channels; **demo-only until sign-off**; test proving trading works with server offline |
| **7. Terminal UI** | Bloomberg-style UI per §7 decision (local backend, panels, grids, charts, command bar, order entry) |
| **8. Release** | Docs site, PyPI Trusted Publishing, GHCR images, v0.1.0 |

## 11. Risks & Notes

- **Data volume** — full-universe orderbook streaming is very large; orderbooks are
  watchlist-only by design, and the storage governor enforces the budget.
- **Kalshi API terms** — if the server ever serves data to people other than the owner,
  review Kalshi's terms on data redistribution.
- **API drift** — Kalshi endpoints have changed hosts before; all URLs live in
  `kalshi-core` config, not scattered constants.
- **Clock skew** — breaks request signing; `kterm doctor` checks it.
- **Real money** — demo is the default; production trading requires explicit opt-in and
  passes the risk layer on every order.

## 12. Open Decisions

| # | Decision | Recommendation | Status |
|---|---|---|---|
| 1 | Client UI: D (web) vs B (Qt) | D (web), with Typer CLI | **Decided** — D (web) + Typer CLI |
| 2 | Trading mode: manual only vs automated strategies | Manual first; strategy interface in a later phase | **Decided** — manual first |
| 3 | Ingestion scope: all markets vs watchlist | Tickers/trades for all; orderbooks watchlist-only | **Decided** — as recommended |
| 4 | Server users: single vs multi-user | — | **Decided** — multi-user (see §8.3) |
| 5a | License | MIT or Apache-2.0 | **Decided** — MIT |
| 5b | Package names | Names as in §3 | **[OPEN]** — all three unclaimed on PyPI as of 2026-10-06; confirm, then claim early |
| — | Server host runs Docker | Required | **Decided** |
| — | Server disk budget | 150 GB | **Decided** |
| — | TLS approach | Self-signed + TOFU pinning | **Decided** |
| — | Server location | Remote host on same LAN | **Decided** |
