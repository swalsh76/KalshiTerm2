# KalshiTerm

A self-hosted toolkit for [Kalshi](https://kalshi.com) prediction markets. A **server** records live
market data and runs analytics on it; a **client** (command line, plus a local web UI) reads that
data and places trades.

> **Status:** a personal project under active development. The data side, the analytics and manual
> trading are built and have been checked against the live exchange. The terminal-style UI is in
> progress (an early version starts with `kterm ui`). Nothing here is financial advice, and you
> are responsible for any order you send.

## What you get

**The server** (runs in Docker on a machine on your network):
- Streams every ticker, trade and market lifecycle event from Kalshi into TimescaleDB, repairs the
  gaps after a restart, and keeps full orderbooks for the markets you watch.
- Keeps storage inside a size budget: compression, retention windows, and a governor that sheds
  load before the disk fills. `kterm-server status` shows what needs attention.
- Runs analyzers on a schedule (price moves, quotes, volatility, events whose bids sum to more
  than $1) and per-user alerts, and serves it all over an authenticated HTTPS API with a live
  websocket push.
- Backs up and restores the database, and supports several named users with their own tokens,
  watchlists, alerts and layouts.

**The client** (runs on your workstation):
- Searches markets and shows quotes, candles, trades, orderbooks and analytics from your server.
- Shows your Kalshi balance, positions, orders and fills, and places, amends and cancels orders
  with a risk layer and a kill switch in front of every order.
- Follows markets live in the terminal, and serves a local web UI.

## How it fits together

```
Kalshi ──(REST + websocket)──▶  kalshiterm-server   (Docker, on a machine on your LAN)
                                  ingest ▸ TimescaleDB ▸ analytics ▸ HTTPS API
                                                │
                       reads data and analytics │
                                                ▼
                                  kalshiterm-client  (your workstation: `kterm`)
                                                │
                                  sends orders  │  straight to Kalshi, never through the server
                                                ▼
                                             Kalshi
```

| Package | What it is |
|---|---|
| `kalshi-core` | The Kalshi SDK: request signing, REST, websocket, orderbook, fees, fixed-point money. |
| `kalshiterm-server` | Ingestion, storage (with a size budget), analytics, per-user alerts and the HTTPS API. |
| `kalshiterm-client` | The `kterm` command: market data, account, trading with a risk layer, and the web UI. |

## Safety defaults (read before trading)

- **Demo is the default.** Production needs `KALSHI_ENV=production`, `production_enabled = true`
  in your local `risk.toml`, and a typed confirmation on every order.
- **Trades never go through the server.** The trading key stays on your workstation; the server
  has its own read-only key. They must be different keys.
- **Every order passes a risk layer** with deliberately tiny default limits (a per-order cost of
  $1, a few open orders, a daily loss limit), and there is a kill switch. Change the limits only by
  editing `risk.toml`.
- **The server's certificate is pinned.** The client trusts exactly the certificate you confirmed
  once, and never sends a token to a server that presents a different one.

Never commit API keys, private keys, `.env` files or TLS material; see [SECURITY.md](SECURITY.md).

## Requirements

Python 3.11+ and [uv](https://docs.astral.sh/uv/). Docker for the server and for the database tests.
Node 22 only if you work on the web UI's front end. The server host needs an accurate clock (Kalshi
rejects requests signed with a skewed one) and, if you follow the macOS host steps below, an
external drive for the data.

## Your Kalshi keys

You need separate API keys, created in your Kalshi account:

| Key | Lives on | Used for |
|---|---|---|
| The server's data key (read-only) | The server host only, as `secrets/kalshi_key.pem` | Streaming and reading market data. `init` copies it into place. |
| Your trading key | Your workstation only, outside the repository | Placing orders. `KALSHI_TRADING_KEY_ID`, and the private key at `~/.kalshiterm/kalshi_trading_key.pem` by default. |

The client reads its `KALSHI_*` settings from the environment or a `.env` file (`KALSHI_ENV`,
`KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`, `KALSHI_TRADING_KEY_ID`, ...). The trading key must
differ from the data key, and the trading code refuses to run otherwise. PLAN §10.7 describes what each
key does.

## Try it

### 1. Install and test

```bash
git clone https://github.com/swalsh76/KalshiTerm2 && cd KalshiTerm2
uv sync
uv run pytest -m "not db"            # every test that does not need Docker (about two minutes)
```

### 2. Run a server

Do this on the machine that will host it. It is the sequence PLAN §8 explains. The `check` step is
read-only and prints the exact `init` command for that machine; `install` (macOS) sets up the
background jobs for the drive check and the LAN announcement:

```bash
cd deploy
host/install.sh check --key-file ~/.kalshiterm/server_key.pem --drive /Volumes/<data drive>
KEY_FILE=~/.kalshiterm/server_key.pem docker compose -f docker-compose.setup.yml run --build --rm \
  init --key-id <key id> --host <name>.local --ip <LAN address>
docker compose up -d --build
host/install.sh install --drive /Volumes/<data drive>
docker compose exec api kterm-server user add <you>
docker compose exec api kterm-server token create --user <you> --role admin   # shown once
```

What each step does:
1. `check` confirms Docker is running, that the key file is a real unencrypted PEM file, and that
   the drive is a mounted, writable volume. It exists because a mistake in any of these otherwise
   shows up later as a confusing error.
2. `init` writes the database password, the Kalshi key, a starter watchlist and a self-signed TLS
   certificate valid for exactly the names and address you give it. Use the name and address your
   clients will connect with.
3. `up` starts the database, the ingest service, the analyzers and the API. Only the API port
   (8700) is published.
4. `install` registers the drive check (and, optionally, the LAN announcement) so they run from
   boot, then waits for the first report. It asks for your password once, for `sudo`.
5. The last two commands create your user and its token. The token is shown once; keep it.

Then check on it: `docker compose exec ingest kterm-server status`. It should list the streams
with fresh timestamps, and any problems at the top.

### 3. Point the client at it

The server uses a self-signed certificate, which the client pins the first time. Compare the
fingerprint it shows with the one `init` printed (or `kterm-server cert show`) before you agree.

```bash
uv run kterm config add home --url https://<name>.local:8700   # asks for the token; keeps it in your keyring
uv run kterm server trust                                       # shows the fingerprint, pins it on "yes"
uv run kterm server status                                      # needs an admin token
```

The first profile you add is the default; use `kterm --profile <name> ...` for another one.

### 4. Read data

```bash
uv run kterm markets bitcoin --limit 10         # search by words in the ticker or event title
uv run kterm market <TICKER>                    # what it is, the last quote and the latest analytics
uv run kterm candles <TICKER> --interval 1h --limit 24
uv run kterm watchlist add <TICKER>             # have the server record its orderbook and analyze it
uv run kterm book <TICKER>                      # only markets on a watchlist have an orderbook
uv run kterm analytics <TICKER>
uv run kterm arbitrage                          # events whose YES bids sum to more than $1 right now
uv run kterm watch <TICKER> --book              # follow trades, quotes and the book live
uv run kterm alerts add market_moves.change_15m <TICKER> abs_gt 0.10   # then: kterm alerts watch
```

Add `--json` to any of them for machine-readable output.

### 5. Look at your account, and trade

These talk to Kalshi directly, with your trading key, and need no server:

```bash
uv run kterm balance                  # per exchange shard
uv run kterm positions
uv run kterm orders
uv run kterm buy <TICKER> 1 0.45 --side yes --dry-run   # shows the cost, the fee and the risk verdict; sends nothing
```

Leave out `--dry-run` to send the order: you are asked y/N first (`--yes` skips that), and in
production you must also type a confirmation that `--yes` never skips. `kterm kill` halts new orders and cancels everything that
is open, and `kterm resume` clears the halt. `uv run kterm ui` opens the web UI, which is
read-only until you switch trading on in it.

## When something goes wrong

- **`docker: command not found` over `ssh host 'command'`.** A non-interactive SSH shell skips your
  login profile, so Docker's directory is not on its `PATH`. Add it to `~/.zshenv` (on macOS,
  `/Applications/Docker.app/Contents/Resources/bin`); `host/install.sh check` also searches the usual
  places.
- **`init` says the key path is a directory.** Docker creates an empty directory for a bind-mount
  source that does not exist. Remove it with `rmdir`, put the key file at that path, and run
  `host/install.sh check --key-file <path>` first.
- **The drive check says "mounted" but not "writable".** macOS blocks background jobs from external
  drives until you allow it: System Settings > Privacy & Security > Full Disk Access > add
  `/bin/bash` (at the machine itself or over Screen Sharing; not over SSH). Then
  `host/install.sh verify`.
- **The client reports a certificate problem.** The certificate covers only the names and address
  given to `init`. If the server's address changes, re-issue the certificate with `cert rotate` (PLAN §8.2) and pin
  the new one.

## Repository layout

```
packages/kalshi-core/         the Kalshi SDK
packages/kalshiterm-server/   ingestion, storage, analytics, the API; the `kterm-server` command
packages/kalshiterm-client/   the `kterm` command, the web UI backend and frontend/ (React)
deploy/                       Compose files, the Dockerfile, and host/ (installer, LaunchDaemon templates)
docs/PLAN.md                  the design, the decisions and the build history
```

## Development

```bash
uv run pytest                        # everything; the database tests skip cleanly without Docker
docker compose -f deploy/docker-compose.dev.yml up -d           # dev TimescaleDB on 127.0.0.1:5433
KTERM_DB_URL=postgresql+asyncpg://kterm:kterm_dev_only@127.0.0.1:5433/kterm \
  uv run kterm-server db upgrade
uv run ruff check . && uv run ruff format --check . && uv run mypy packages
```

Front end (`packages/kalshiterm-client/frontend/`, Node 22): `npm ci`, then
`npm run lint | typecheck | test | build`.

Live tests against Kalshi are opt-in and read-only:
`KALSHI_INTEGRATION=1 uv run pytest packages/kalshi-core/tests/test_live.py -s`.

Work happens on a branch per slice and merges to `main` once approved and CI is green. The tests use real
objects and local fakes rather than mocks; `-m db` selects only the database tests.

## Documentation

- [docs/PLAN.md](docs/PLAN.md): the design, the decisions and the build history.
- [CHANGELOG.md](CHANGELOG.md): what was built, by phase.
- [SECURITY.md](SECURITY.md): reporting a vulnerability, and what the design relies on.
- [CLAUDE.md](CLAUDE.md): the working rules for AI-assisted changes in this repository.

## License

[MIT](LICENSE)
