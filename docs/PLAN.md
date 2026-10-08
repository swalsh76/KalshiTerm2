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
9. [Storage Budget & Retention (500 GB)](#9-storage-budget--retention-500-gb)
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
- **Live add/remove of markets (slice 2.5a, probed 2026-10-07):** one orderbook subscription
  accepted 3,000 markets without error. `update_subscription` `add_markets` / `delete_markets`
  reply `ok` with the *full resulting market set* (used to keep the reconnect registry correct),
  and **the reply consumes a number of the subscription's sequence** — so the WebSocket client
  passes a `control` marker through the stream in order and the tracker counts it, otherwise
  every live change would look like a gap. Added markets: quiet ones get no snapshot (so the
  feed always requests one via `get_snapshot`), busy ones sometimes get one by themselves
  (duplicates are harmless). Removing the last market unsubscribes rather than sending an empty
  list, which Kalshi might read as "all markets"; late messages for removed markets are
  counted in sequence and ignored.
- **Orderbook storage (slice 2.5b, measured 2026-10-07, 50 top-volume markets):**
  `orderbook_snapshots` (full book as parallel arrays, best-first; `approximate` when it came
  from the REST fallback) and `orderbook_deltas` (signed quantity change per level), both with
  the stream's `seq`, 1-day chunks. A book is rebuilt by replaying deltas on the latest
  snapshot, so snapshots are only checkpoints: the periodic snapshot interval is **300 s,
  configurable** (`--snapshot-interval`; the earlier 60 s was a deviation we dropped).
  Measured uncompressed: snapshot ≈ 950 B/row (≈31 levels), delta ≈ 135 B/row; deltas ran
  ≈ 150/s in total, dominated by a few weather markets (one at ~70/s), and deep crypto books
  run ~10x higher. Live check: replayed book equals Kalshi's REST book for 44/50 markets; the
  6 differing ones were the busiest, consistent with a few seconds' gap between the two reads.
  Bug found by the replay round-trip test: SNAPSHOT events must carry a *copy* of the book
  (the tracker mutates its book in place), so they do; DELTA events reference the live book
  and must be consumed immediately.
- **Watchlist controller (slice 2.5c, verified live 2026-10-07):** `kterm-server ingest
  --watchlist FILE` (TOML, re-read every cycle, default 300 s): `[watchlist] markets = [...]`
  (manual), `auto_top_n` and `auto_window_minutes` (default 60). "Top" = most contracts traded
  in the window, from our own `trades` table (ordinary markets only; combos and settled markets
  excluded), so a fresh database has no auto picks until a few minutes of trades exist.
  Manual markets leave the moment they are dropped from the file; auto markets stay >= 12 h
  once added, so the watched set can briefly exceed `auto_top_n`. `--watch` still works as extra
  manual tickers. Adding a market = feed `add_markets`, then the ingestor opens a
  `watchlist_periods` row and copies the market's raw trades into `trades_watchlist` **inside
  its next write transaction**, and from that same batch on writes its trades to both tables
  (no gap, no duplicate; a re-added market is filled in from `trades`, exact duplicates
  skipped). Verified live: 11,403 trades copied, 0 duplicates, all matching `trades`. On
  startup, periods left open by a previous run are closed (they did not cover the downtime) and
  re-opened as the list is applied. `watchlist_periods.source` is how a period was opened.
  Two bugs found on the way: an empty initial market list would have subscribed to *every*
  market (the client omits an empty `market_tickers`), so the feed now subscribes only when it
  has markets; and when a new subscription's first messages overtook the `begin_subscription`
  call, the tracker reset its sequence position and reported a false gap, so a late declaration
  now keeps what the early messages established.
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
- **Discovery modes (measured 2026-10-06):** a *full* refresh (first run, then at most daily, or
  `kterm-server discover --full`) reads ≈780k markets in 70–280 s at the rate limit; an
  *incremental* cycle asks only for events/markets with `min_updated_ts` since the bookmark
  (valid together with `mve_filter=exclude`; it covers new markets, status changes and
  settlements) and takes ≈2.5 s. Bookmarks advance only after a cycle fully succeeds, with a
  2-minute overlap. Rows are upserted only when a value changed, so refreshes leave no dead
  tuples. A value too precise for its fixed-point scale rejects that one row (logged, counted),
  never the cycle.
- **Stream subscriber** (WS): `ticker`, `trade`, `market_lifecycle_v2` for all ingested
  markets; `orderbook_delta` for the **watchlist only**.
- **Gap handling:** Kalshi does not replay messages missed during a disconnect, and `sid`
  and `seq` restart on every reconnect (observed live: both came back as 1). The WS client
  yields a `reconnected` event ahead of any post-reconnect data; on it the ingestor backfills
  trades (and re-snapshots watched orderbooks) via REST for the outage window.
- **Gap log and backfill (slice 2.6, verified live 2026-10-07):** every outage — a reconnect, a
  receive-queue overflow, or a restart (gap since the newest stored trade) — becomes an
  `ingest_gaps` row (window, reason, dropped, status, trades found/added/duplicates, combo
  counts, note). A background worker fetches the window from REST (`GET /markets/trades`,
  all markets, ~1,000 trades per 0.08 s; padded 10 s each side, capped at 6 h, truncation
  noted) and feeds each trade through the normal write path, so watched markets also reach
  `trades_watchlist`. Duplicates are impossible by construction: each trade id is checked
  against the database for the window and the ingestor's memory of the last 200k live ids, so
  re-running a window is safe (that is also the retry strategy: 3 attempts, backoff, then
  `failed`). Rows left `running` by a crashed run become `interrupted`. **Only trades are
  recoverable**: tickers, lifecycle events and orderbook deltas are not replayed (the gap row
  is the record), and combo trades go only to the large-trade log because their per-minute
  counters cannot be de-duplicated (those counters undercount during a gap). Live check: after
  ~100 s offline, the startup backfill left the database with exactly Kalshi's 11,348 ordinary
  trades for the window (0 missing, 0 extra) plus 37 large combo trades. The reconnect path is
  covered by tests with a faked socket; it was not provoked live.
- **Streaming writer (slice 2.3, measured 2026-10-07):** messages are timestamped when read off
  the socket, filtered (combo `KXMVE…` tickers/trades are skipped and counted until slice 2.4),
  buffered, and written in atomic batches via binary `COPY` every second or 5,000 rows. A failed
  batch is retried with backoff, in order, with no partial data; buffered + in-flight rows are
  capped so a stalled database pauses intake and the WebSocket client's bounded queue takes
  over; the shutdown drain gives up after 3 attempts rather than hang. Markets seen by the
  stream before discovery get a placeholder row (`status='unknown'`, same id kept) that every
  discovery cycle resolves by direct `tickers=` lookup. Capacity ≈100k msgs/s against a live
  mean of ~1.5k and peak ~3.3k. Live 90 s: 46k tickers + 7k trades written, 0 rejected, 0
  retries, ~25 ms per ~500-row flush; exchange→received lag p50 ≈ 0.2 s / p95 ≈ 0.85 s (includes
  the ~0.1–0.2 s local clock offset, so ~14 % of rows show a slightly negative lag), plus ≤1 s
  of deliberate batching before the write. Uncompressed rows ≈160 B (ticker) and ≈132 B
  (trade) including indexes → about 10 GB/day hot for ordinary markets, so compression after
  1 day (slice 2.7) is required, not optional.
- **Combo markets (slice 2.4b, decision 17):** every combo message is counted per minute and ticker family at intake (no network, no per-combo state); only individual combo trades with ≥ $500 taker dollars (contracts × price paid by the taker) are stored, by ticker text. Combos are identified by the `KXMVE` prefix in the stream (the stream carries no collection name). Taker dollars are capital deployed, not risk (a taker paying $0.9996 for a near-certain outcome counts fully).
- Batched writes to Postgres via `COPY` (asyncpg).
- Each row stores both **exchange timestamp** and **receipt timestamp** (UTC) to measure
  ingestion lag.

### 5.2 Storage

- **PostgreSQL 18 + TimescaleDB** (pinned official image; decided 2026-10-06 over 17: five
  years of support from the start, data checksums on by default, smoother future upgrades).
  The 18 image's volume is `/var/lib/postgresql` (PGDATA is versioned inside it).
- Reference tables: `series`, `events`, `markets` (outcomes stored on the market row).
- Hypertables: `tickers`, `trades`, `orderbook_snapshots`, `orderbook_deltas`, plus
  `trades_watchlist` (raw trades of every market that has ever been watched; no retention).
  `watchlist_periods(market, added_at, removed_at)` records orderbook coverage windows.
- Combo (multivariate) markets: `combo_stats_1m` (per-minute universe counters) and
  `combo_large_trades` (decision 17).
- Continuous aggregates: 1-minute and 1-hour candles.
- Results: `analysis_results`, `alerts`.
- Migrations via **Alembic**, applied automatically on API startup.

**Numeric convention (decision 14, 2026-10-06): one form, `bigint` fixed-point, everywhere.**
No `numeric`, no `float`, no `int4` in the schema. Two scales, carried in column names:
dollar amounts and strike levels in millionths (`*_e6`, e.g. `price_e6`, `floor_strike_e6`),
contract counts in hundredths (`*_e2`, e.g. `size_e2`, `volume_e2`). One Python module
(`kalshiterm_server.fixedpoint`) converts at the edge and **raises** on any value finer than
the scale — never rounds silently. SQL views expose readable decimals. Measured (2 M synthetic
ticker rows, compressed): `bigint` 10⁻⁴/10⁻² = 11.7 B/row, with 10⁻⁶ dollars = 16.1 B/row,
`numeric` 33.5, `float8` 54.0; declared integer width does not matter after compression, but
extra scale digits do (a universal 10⁻⁶ scale for counts was 25.2 B/row), hence two scales.

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

**Target host: Mac Studio (M4, arm64) running Docker on macOS**, data on a dedicated **1 TB
external Thunderbolt 4 SSD**, 500 GB data budget (decided 2026-10-08; was 100 GB on the
internal disk).
(Any Linux/Windows-WSL2 host also works; the image builds on the host for any arch.)

- Always-on: disable sleep, enable "start up automatically after a power failure".
  Docker Desktop starts at login, not boot, and FileVault disables auto-login — so either
  accept auto-login or use a launchd-managed runtime (OrbStack/Colima).
- Postgres uses a **named Docker volume**, not a bind mount. Set the Docker VM disk cap
  to ~700 GB (budget plus headroom and images); it does not shrink on its own.
- **External data drive (Thunderbolt 4, 1 TB, dedicated):**
  - Put Docker Desktop's *disk image* on the drive (Settings -> Resources -> Disk image
    location; verify on the host). Never bind-mount the Postgres data directory from macOS:
    it goes through VirtioFS, which is slow and unsuitable for fsync-heavy databases. The VM
    keeps a normal Linux filesystem and the named volume stays as is.
  - Thunderbolt 4 is 40 Gb/s, about 3 GB/s usable for NVMe; our write load is a few MB/s, so
    bandwidth is irrelevant. What matters is fsync latency and durability (measured in 2.10).
  - Consumer enclosures have no power-loss protection: put the Mac Studio and the drive on a
    UPS, use the supplied certified cable, never unplug while Docker runs, and disable disk
    sleep (`pmset`). Data checksums and crash-recovery tests already cover a hard stop.
  - The drive must be mounted before Docker starts and must not be shared with other uses
    (the budget assumes it is dedicated). Check the model's endurance rating: expected writes
    are 20-30 TB a year (estimate), so choose a drive rated well above that.
  - If the drive disappears, Postgres stops and the ingestor retries; on return the gap log
    and trade backfill recover trades (not tickers or orderbooks). `status` reports it.
- mDNS cannot be advertised from inside the container through the Docker VM: use a
  manual host/IP or run the zeroconf advertiser on the host.

- **Docker Engine + Compose v2** (any Linux, Windows with WSL2, or Docker-capable NAS).
- Docker is the **only** supported server deployment path.
- Image is built on the host from the repo (any arch); no registry required.
- Accurate clock (NTP / chrony) — required for Kalshi request signing.

### 8.2 Bootstrap

Phase 2 deploys **database + ingest only** (no API yet, so nothing is published to the LAN at
all). Two compose files in `deploy/`: `docker-compose.setup.yml` (the one-off `init`, with no
network) and `docker-compose.yml` (the stack; refuses to load until `.env` exists).

```bash
git clone <private-repo> && cd KalshiTerm2/deploy
# 1. the server's OWN read-only Kalshi key (separate from the dev key); init validates it is
#    an unencrypted Ed25519 PEM, copies it to secrets/ (mode 600) and prints nothing secret
KEY_FILE=~/.kalshiterm/server_key.pem \
  docker compose -f docker-compose.setup.yml run --rm init --key-id <key id> [--budget-gb 500]
# 2. start (migrations run first; discovery runs every 15 min inside ingest)
docker compose up -d --build
# 3. look
docker compose exec ingest kterm-server status
```

`init` writes `.env` (generated database password, settings), `secrets/kalshi_key.pem`,
`config/watchlist.toml` (starter: `auto_top_n = 20`; never overwritten) and `state/`. A second
run refuses unless `--force`, because a new password would lock the server out of its existing
database volume. All of these are git-ignored. On Linux add `--user "$(id -u):$(id -g)"`.

Host-side drive check (Mac Studio; closes the "container cannot see the host mount" gap):
`host/check-data-drive.sh DRIVE_PATH state/host.json` writes whether the drive is a mount
point, writable, and its free space; install it as a LaunchDaemon from
`host/com.kalshiterm.hostcheck.plist.template` (every minute, from boot, no login needed). The
server reads the file (`KTERM_HOST_STATE_FILE`); `status` raises a problem if the drive is not
mounted or writable, is nearly full, or the report is more than 5 minutes old or missing.

Design of `docker-compose.yml`: the database is on an `internal: true` network (no route to the
internet); only `ingest` also joins the default network to reach Kalshi; no `ports:`
anywhere; both services have health checks (`kterm-server health`: database reachable, at
head, fresh tickers and trades), `restart: unless-stopped`, rotated logs (20 MB x 5, so logs
cannot fill the Docker VM disk), and clean-stop grace periods. Key and config are mounted
read-only. The database image tag equals the dev compose file's (a test enforces it) and
telemetry is off.

**Rehearsed on the MacBook (2026-10-08, real Docker, real read-only key):** init -> up ->
healthy in ~20 s; database container cannot resolve outside hosts; key readable at mode 600;
full discovery 293 s; a 75 s ingest outage was backfilled (12,702 trades found, 9,344 added,
1,408 duplicates skipped). The rehearsal found and fixed four real problems that unit tests
could not: compose loads every service's required variables up front (so `init` had to move to
its own file); `docker compose run` replaces `command` but appends to `entrypoint` (so the
fixed init arguments live in the entrypoint); a Python process running as the container's
main process **ignores SIGTERM**, so `docker stop` waited the full 30 s and then killed it
without the final flush (now handled: stop takes under a second and drains); and the first
orderbooks only appear ~5 minutes after a fresh start, because the first watchlist cycle runs
before any trades exist to rank (the next cycle adds them).

- Upgrades: `git pull && docker compose up -d --build` (migrations run on startup).
- Phase 3 adds the `api` service (the only published port), users and tokens, TLS and the
  user/token commands.

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
  the Phase 2 48-hour volume calibration (decision 11: on the MacBook, sleep disabled); only the
  disk-speed and recovery tests need the Mac Studio's SSD (§10.2).
- Dev storage budget is small (`KTERM_STORAGE_BUDGET_GB=10`–`20`) so the governor's
  thresholds are exercised; set the Docker VM disk cap to match.
- Backups: `KTERM_BACKUP_TARGET` unset or a local folder; the NAS is production only.
- Laptop sleep/battery cause ingestion gaps in dev; that is acceptable. Gap handling
  (`since` catch-up, WS reconnect) is worth testing by sleeping the laptop on purpose.
- Dev and production use distinct `.env` files and Compose project names so config and
  volumes never mix.

## 9. Storage Budget & Retention (500 GB)

Docker cannot cap a Postgres volume, so the server governs its own footprint.

### 9.1 Budget allocation

| Use | Allocation |
|---|---|
| Postgres overhead (WAL, temp, compression rewrites) + free-space headroom | 100 GB |
| Backups | 0 GB (stored on the NAS, §9.5) |
| Reference data, analytics results, continuous aggregates (tombstones add ~7 GB/yr, §11) | 40 GB |
| Hot uncompressed chunks (last 1–2 days) | 20 GB |
| Compressed history | ~340 GB |
| **Total** | **500 GB** |

The budget is per host (`KTERM_STORAGE_BUDGET_GB`); the Mac Studio target is 500 GB of a
1 TB external SSD (decided 2026-10-08). The other ~430 GB is deliberate: the Docker VM disk
image does not shrink, compression and `VACUUM FULL` need scratch space, and SSDs slow down
when nearly full. **Retention defaults (§9.2) were not enlarged**: they were sized from
measurements and, at current rates, are expected to use well under this budget (a rough
estimate: ~100 GB). The surplus is headroom until the calibration run (2.10) says which
windows are worth lengthening; raising any of them is a plan change.

### 9.2 Retention defaults (all configurable)

| Data | Raw retention | Then |
|---|---|---|
| Trades (30 days raw; markets ever watched: forever, in `trades_watchlist`) | Compressed after 1 day | 1-min / 1-hour candles forever |
| Tickers (ordinary markets) | 14 days, compressed after 1 day | 1-hour aggregates (`ticker_1h`) kept forever; **1-minute aggregates (`ticker_1m`) 30 days** (decided 2026-10-07: ~7,400 rows/min, ~10.7 M/day, too big to keep forever) |
| Multivariate (combo) markets — decision 17 | **No per-combo rows.** Per-minute universe counters (`combo_stats_1m`: creations, determinations, settlements, tickers, trades, contracts, taker dollars; kept forever, ~1 row per minute per family) and a log of individual combo trades with ≥ $500 taker dollars (`combo_large_trades`, ≈15 k rows/day, 365 days). | No candles for combo markets |
| Orderbook deltas (watchlist only) | 14 days | Dropped |
| Orderbook snapshots (watchlist, every 300 s by default) | **365 days** (at the default interval they already are the 5-minute downsample; a shorter interval would need a real downsample step) | Dropped |
| Markets / events (incl. outcomes) — decision 15 | Age counts from `settlement_ts`; unsettled rows never expire. **0–30 days: full row. 30–90 days: slimmed to an outcome-only row** (rules text and sub-titles dropped; ticker, event, status, result, settlement, times and strikes kept). **After 90 days: deleted, except markets that are watched (ever, see decision 12) or pinned.** Events follow their markets; series are kept (≈15k rows). | — |

**Settled-market lifecycle (slice 2.7c, 2026-10-08):** a daily Timescale job
(`expire_markets`, windows in its config: 30 / 90 days) implements decision 15 with one
change decided 2026-10-08: **at day 90 a market is reduced to a tombstone, not deleted** (id,
ticker, event, type, status, result, settlement value and time are kept), because candles and
aggregates refer to markets by id and are kept forever; deleting the row would orphan them
and let a reappearing ticker get a second id. Events are deleted once every one of their
markets is expired; unsettled markets never expire; watched (any period) and pinned markets
are slimmed at 30 d but never tombstoned. The job is predicate-driven, so a row that discovery
rewrites in full is simply reduced again. `pins(market_id, pinned_at, note)` exists; the way to
pin arrives with the Phase 3 API. **Measured on Kalshi's real market list (829,072 markets,
2026-10-08):** 585,141 markets settled in the last 7 days, i.e. **~84,000 a day**. Average row:
full 824 B, slim 217 B, tombstone 148 B, plus ~85 B of index entries per row. The job
processed 341k tombstones and 168k slim rows in ~7 s; the table shrank 781 MB -> 411 MB.
**Consequence:** tombstones accumulate at ~84k/day x ~230 B = ~19 MB/day, **~7 GB a year**,
which will overrun the 8 GB reference line in §9.1 within about a year. Likely mitigation, to
decide with the calibration run (2.10), which will show what fraction of markets ever
tick/trade: keep a tombstone only for markets that have candle/aggregate/lifecycle data, and
delete the rest outright (as decision 15 originally read). The storage governor (2.8) must
watch `markets` size.

**Candles and retention (slice 2.7b, 2026-10-07):** continuous aggregates `candles_1m` /
`candles_1h` (trades: open, high, low, close, volume, count; forever) and `ticker_1m` (30 days) /
`ticker_1h` (forever) (price OHLC without null prices, last bid/ask, volume, open interest, tick
count), each with a `*_v` view in dollars. Refresh policies look back only 1-2 days, so dropping
old raw chunks never touches history; this is tested (raw deleted, candle unchanged).
Aggregates compress after 3 days (segmented by market). Raw retention is on: trades 30 d,
tickers 14 d, deltas 14 d, snapshots 365 d, `combo_large_trades` 365 d. Live check on 5 minutes of
real data: 2,617 of 2,617 minute candles; high/low/volume/count exact for all; total volume
equal to the trade table. **Known limit:** Kalshi stamps several trades with the same
microsecond, so open/close are ambiguous in those minutes (≈5% of minute candles; 1,716 such
instants in the sample). Retention periods are constants in migration 0010 until the
configuration work in 2.8/2.9. Aggregates created `WITH NO DATA`: a database that already holds
more than ~2 days of history before this migration would need a manual refresh.

**Compression, measured (slice 2.7a, 2026-10-07, 8 minutes of real data, 62 watched markets):**
layout = batches ordered by `market_id, ts DESC`, no `segmentby`, compressed after 1 day.
Segmenting by market was worse on the ticker table (few dozen rows per market per batch).

| Table | Raw B/row | Compressed B/row | Ratio |
|---|---|---|---|
| tickers | 159 | 23 | 6.8× (2.5× if segmented by market) |
| trades | 130 | 31 | 4.1× |
| trades_watchlist | 132 | 30 | 4.4× |
| orderbook_deltas | 126 | 14 | 8.9× |
| orderbook_snapshots | 2,325 | 1,218 | 1.9× (148 rows only) |

Single-market queries stay under 1 ms on both layouts at this size. Sample is small: re-measure
on a full day in the calibration run. **Delta volume depends on watchlist composition:** the
top-50-by-volume list ran ~1,800 deltas/s, of which five crypto 15-minute books made 58% (one
BTC book alone 615/s, 34%) — about 2 GB/day compressed, so 14 days of deltas could be ~30 GB.
The governor (2.8) must cover this; the auto top-N could also exclude fast crypto books.
`market_lifecycle` and `combo_large_trades` (168 kB for 540 rows) are not compressed.

TimescaleDB native compression: earlier plan figure **4–9×** until the calibration run (synthetic ticker
data measured 8.8× for integers, 3.5× for `numeric`; real data should compress better). Measured
volumes (2026-10-06) put raw ingestion at roughly 10 GB/day (~0.5–1 GB/day compressed), which
is why the original "trades forever" and "7-day uncompressed window" defaults were dropped.

### 9.3 Storage governor

- Runs inside the analytics worker (in Phase 2, before that worker exists, inside the ingestor
  process; same code); budget set by `KTERM_STORAGE_BUDGET_GB=500`.
- Tracks per-table size, daily growth rate, and **projected days to full**; exposed on
  `/status`, `kterm server status`, and the UI.
- **80% of budget:** alert + tighten raw retention windows.
- **90% of budget:** stop orderbook-delta capture (snapshots only). Trades and tickers are
  never shed.
- **The drive itself** is watched separately from the budget: alert when free space on the
  data drive falls below 15%, and `status` shows loudly if the drive is missing or unwritable.

**As built (slice 2.8, verified live 2026-10-08):** `kalshiterm_server/governor.py` runs as a
task inside `ingest` (every 10 minutes; `--governor-every`). Each cycle measures data + WAL,
stores a sample (`storage_samples`, kept 30 days), projects growth by least squares over the
last 24 h (needs 3 samples spanning an hour; compression makes sizes saw-tooth, so one number
is never trusted), and applies three modes with hysteresis: **tightened** from 80% (halves the
`tickers`, `orderbook_deltas` and `trades` raw windows, never below 3 / 3 / 7 days; snapshots
and watchlist trades are never touched; restored exactly under 70%), **shedding** from 90%
(orderbook deltas dropped at intake, snapshots/trades/tickers never; resumes under 85%; the
period is a `delta_shed` row in `ingest_gaps`, extended each cycle so an open period reads "up
to at least now"). State survives restarts (`governor_state`); every action is logged to
`governor_events`. The drive is probed each cycle (free space below 15%, and a real
write+fsync of a temp file). Settings: `KTERM_STORAGE_BUDGET_GB` (default 100, 500 in
production) and `KTERM_DISK_CHECK_PATH`.
**Limits to know:** (1) inside the server container the probe sees the *Docker VM's* disk,
which is where Postgres lives; whether the host mounted the external SSD is a host-level
fact the container cannot see, so 2.9 needs a small host-side check (launchd) and 2.10
tests it for real. (2) WAL counts toward the budget and has a floor of ~80-110 MB, which
dominates tiny dev budgets (irrelevant at 500 GB). (3) Tightening only shortens windows; it
takes effect when the retention job next runs (the governor requests an immediate run).
(4) Beyond 100% nothing more is shed: Trades and tickers are never shed by design.

`kterm-server status [--json]` (data model reusable by the Phase 3 `/status` API) prints
what needs attention first, then storage (largest tables, growth, days to full, drive), stream
freshness and rates, recent gaps, unhealthy background jobs, discovery age, watchlist size;
exit code 1 when anything needs attention. Timescale's built-in telemetry job is ignored and
**telemetry is switched off in the database config** (`timescaledb.telemetry_level=off`; the
production compose file written by `init` in 2.9 must do the same).

### 9.4 Calibration

Volume figures above are estimates. Phase 2 includes a **48-hour calibration run against
production data** to measure rows/bytes per day; defaults will be tuned from measurements.
It is tracked, with its protocol, in §10.2 and does not block later phases.

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
| **2. Server storage & ingestion** | Schema + Alembic, Timescale hypertables, compression/retention policies, continuous aggregates, ingestor, Compose stack, `kterm-server init`, storage governor — broken into slices 2.1–2.10 in §10.1. **Code-complete at 2.9; the calibration and hardware qualification (2.10) are tracked in §10.2 and do not gate Phase 3** |
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
| 2.5 (a: live add/remove on the feed — done; b: orderbook storage — done; c: watchlist controller — done) | Orderbook storage for the watchlist: snapshots + deltas, config-file watchlist + automatic top-N by volume (per-user watchlists arrive with the Phase 3 API). Semantics (decisions 12–13): **adding** a market subscribes it live (`add_markets`, fresh snapshot), copies its last 30 days of raw trades into `trades_watchlist`, and opens a `watchlist_periods` row; **while watched**, snapshots/deltas are stored and trades go to both trade tables; **removing** stops orderbook capture (`delete_markets`) and closes the period but deletes nothing — orderbook data ages out under normal retention, `trades_watchlist` is kept forever. Capture covers the union of all users' watchlists (reference-counted). Auto top-N entries stay ≥12 h once added; manual entries are never auto-removed. Needs a `kalshi-core` extension (live add/remove on `OrderBookFeed`) and a live probe of Kalshi's undocumented per-subscription market limit |
| 2.6 — done | Gap handling: gap log; trade backfill via REST on a `reconnected` event and on startup (tickers cannot be backfilled, so those gaps are recorded) |
| 2.7 (a: compression — done; b: candles + raw-data retention — done; c: settled-market slimming/tombstoning + `pins` — done) | Split because raw-data retention must not be switched on before the candles that replace it exist. **a:** columnar compression after 1 day. **b:** 1-minute and 1-hour continuous aggregates (trades OHLC/volume, ticker aggregates), then retention (trades 30 d, tickers 14 d, orderbook deltas 14 d, snapshots 90 d + 5-min downsample, `combo_large_trades` 365 d). **c:** decision 15 (full 30 d / slim to 90 d / delete unless watched or pinned) as a scheduled job, plus the `pins` table |
| 2.8 — done | Storage governor + `kterm-server status` (includes the data-drive checks: free space, missing/unwritable drive) |
| 2.9 — done (rehearsed on the MacBook; the Mac Studio itself waits for the SSD) | `kterm-server init`, production Compose, health checks, Mac Studio deployment (TLS certificate generation moves to Phase 3 with the API). Also built because a deployed server needs it: the periodic discovery loop (`ingest --discover-every`), `kterm-server health`, the host-side drive check, SIGTERM-driven graceful stop |
| 2.10 (a: volume calibration — **not started, not a blocker**; b: hardware qualification — **waiting for the SSD, not a blocker**) | Split because only half of it needs the new hardware (decision 11 puts the volume run on the MacBook). **a:** 48-hour run against production data, then retune retention defaults and settle the open questions. **b:** disk tests and recovery tests with the data on the external SSD, then switch the Mac Studio on as the production host. Full protocol and checklist: §10.2 |

The server uses its own read-only Kalshi key (created at deployment time), separate from the
dev key.

### 10.2 Deferred work to loop back to (2.10) — not a blocker

Phase 2 is **code-complete at 2.9**: the whole server stack has been rehearsed on the MacBook
(§8.2). What remains is measurement and hardware qualification, which can happen whenever the
conditions are met. **Later phases must not wait for it and must not assume calibrated
numbers**; anything that depends on the results is listed below so it is not forgotten.

**2.10a — volume calibration.** Condition: none (decision 11: the MacBook, sleep disabled).
Protocol: run the `deploy/` stack (rehearsed in §8.2) with `caffeinate -dimsu` for 48 hours
against production data, a large budget (so the governor does not interfere), and the real
default watchlist; save `kterm-server status --json` hourly. Report (in `docs/`) at 24 h and
48 h: rows and compressed bytes per day for every table; the **fraction of markets that ever
tick or trade** (decides the tombstone question); orderbook-delta share by market; WAL size;
the gap log (sleep gaps are expected on a laptop and a good test of backfill); job health.
Decisions waiting on it:
- retention windows (§9.2) and whether any should be lengthened given the 500 GB budget;
- tombstones for all settled markets vs only those with data (parking lot, ~7 GB/year);
- excluding fast crypto 15-minute books from the auto top-N (parking lot);
- the compression layout re-check on a full day of data (`segmentby` vs ordering, §9.2);
- Postgres tuning (`shared_buffers`, `max_wal_size`; untuned defaults today) and the WAL floor;
- whether 500 GB is the right budget, and the default `auto_top_n`.

**2.10b — hardware qualification.** Condition: the 1 TB Thunderbolt 4 SSD is attached to the
Mac Studio. Checklist, in order (show the commands before running any of them on that host):
1. Format and mount the drive at a stable path; move Docker Desktop's disk image onto it.
2. Disk tests inside the database container, internal disk versus the SSD: `pg_test_fsync`
   and a write-heavy `pgbench`; record fsync latency. Check the drive's endurance rating.
3. Create the server's own read-only Kalshi key; `init`; `up` (§8.2); install the host check
   LaunchDaemon (`deploy/host/`) and confirm `status` shows the drive.
4. Recovery tests while ingest runs: force-unmount or unplug the drive; confirm Postgres
   crash-recovers without corruption (data checksums are on), the gap log and backfill repair
   the trades, and `status` reports the outage and the return.
5. Reboot and power-loss behaviour: automatic restart after power failure, the drive mounted
   before Docker starts, ingest resumes by itself, UPS in place.
6. Record the results here, set the production budget, and make the Mac Studio the host.

Exit criteria: measured numbers recorded in §9 and §11, retention defaults retuned or
explicitly confirmed, the open questions above closed, the Mac Studio running and healthy.

### 10.3 Phase 3 slices (server API)

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
| 3.2 | Users and tokens: `users`, `api_tokens` (stored hashed; shown once), roles `read` / `admin`, `kterm-server user add/list/remove` and `token create/list/revoke`, bearer-auth dependency, `/v1/me`, admin-only `/status` (the `status` report), failed-auth throttling |
| 3.3 | TLS and deployment: `init` generates the self-signed certificate (SANs for host, `<host>.local`, LAN IP), certificate fingerprint command, `api` service in Compose (the only published port, 8700), tested with a real TLS handshake and a pinned fingerprint; rehearsed on the MacBook |
| 3.4 | Reference and candle endpoints: markets (filter, search, keyset pages), a market, an event, candles from `candles_*` / `ticker_*` |
| 3.5 | Raw-data endpoints: trades, ticker history, orderbook at a time (latest snapshot plus replayed deltas), gaps log |
| 3.6 | Per-user watchlists: `user_watchlists`, `/v1/watchlist` CRUD; the ingest controller watches the **union** of the config file, auto top-N and every user's list (a market leaves only when no one wants it) |
| 3.7 | Live push: ingest `NOTIFY`s per write batch, WebSocket `/v1/stream` (subscribe to markets / watchlist), `since` cursor catch-up on reconnect. The cursor design is settled at the start of the slice (rows have no unique sequence number; candidate: `received_at`, which also covers backfilled rows) |
| deferred | Prometheus `/metrics` (optional in §5.4) goes to the parking lot unless wanted; analytics and alert endpoints arrive with Phase 4 |

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
  main storage levers for the storage budget — to be settled by the Phase 2 calibration run.
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
| — | Server disk budget | **500 GB** on a dedicated 1 TB external Thunderbolt 4 SSD attached to the Mac Studio (decided 2026-10-08; was 100 GB internal); backups on NAS | **Decided** |
| 8 | Key types | Ed25519 only; RSA-PSS not supported | **Decided** |
| 7 | Multivariate (combo) markets | Keep: ingest and store them (decided 2026-10-06; they dominate the live trade stream) | **Decided** |
| — | Dev credentials | Read-only production Kalshi key (decided 2026-10-06), layered safeguards in §4; demo stays default | **Decided** |
| 9 | Trade retention | 30 days raw (watchlist markets forever), 1-min/1-hour candles forever | **Decided** |
| 17 | Combos: counters + large-trade log (supersedes 10 and 16) | Measured 2026-10-07: combos are ≈12 % of trades and ≈14 % of taker dollars (the 69 % share of contracts is cheap long-shot tickets); a combo's outcome is fully determined by its legs (345/345 settled combos matched "YES iff every leg matched its side"); 6.4 % of traded combos hold 73 % of the dollars; per-combo rows cost 436 B for ≈1 M combos/day. So: universe counters + a ≥ $500 trade log, no per-combo rows, lookups or leg placeholders | **Decided** |
| 16 | Combo storage (revises 10) | Superseded by decision 17 | Superseded |
| 10 | Combo-market storage | Compact: one row per market (legs as array + outcome), 30 days; raw combo tickers/trades 3 days; no candles | **Decided** |
| 12 | Watchlist removal | A market that has ever been watched keeps its raw trades forever (`trades_watchlist`); removal stops orderbook capture but deletes nothing | **Decided** |
| 15 | Settled-market retention | Full rows 30 days after settlement, then outcome-only rows to day 90, then deleted unless watched or pinned. Estimated steady state ≈4.6 GB of the 8 GB reference allocation (30 d × 85k/day × ~1 KB + 60 d × 85k/day × ~0.37 KB + ~0.2 GB unsettled). Implemented in slice 2.7; the `pins` table is added with it | **Decided** |
| 14 | Numeric representation | `bigint` fixed-point only: dollars/strikes at 10⁻⁶ (`*_e6`), counts at 10⁻² (`*_e2`); loud failure on finer precision | **Decided** |
| 13 | Auto top-N churn | 12-hour minimum dwell once added; manual entries never auto-removed | **Decided** |
| 11 | Calibration host | MacBook with sleep disabled (before the Mac Studio is the host); runs whenever convenient, does not gate later phases (§10.2) | **Decided** |
| — | Server host | Mac Studio M4, Docker on macOS (supersedes Pi/Windows ideas) | **Decided** |
| — | TLS approach | Self-signed + TOFU pinning | **Decided** |
| — | Server location | Remote host on same LAN | **Decided** |

## 13. Parking Lot

Ideas raised but **not** in scope. Not to be built until promoted into a phase.

- Automated trading strategies (decision #2: manual first; strategy interface later).
- Run the containers as a non-root user (needs a uid strategy that works on both Docker Desktop and Linux bind mounts). Raised 2026-10-08; today they run as root with a read-only key mount, no published ports and an internal-only database network.
- Make the first watchlist cycle retry after ~1 minute while there are no trades to rank, instead of waiting the full 5 minutes.
- Tombstones only for markets that have data (candles, lifecycle events); delete the rest outright. Raised 2026-10-08 after measuring ~7 GB/year of tombstones; decide after the 2.10 calibration run.
- Exclude the fast crypto 15-minute books from the auto top-N (they made 58% of orderbook deltas in the 2.7a sample). Raised 2026-10-07; deferred until the storage governor (2.8) shows whether it is needed.
