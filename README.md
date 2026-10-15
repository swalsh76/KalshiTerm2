# KalshiTerm

A self-hosted toolkit for [Kalshi](https://kalshi.com) prediction markets: a **server** that records
live market data and runs analytics, and a **client** (command line and a local web UI) that reads
that data and places trades.

> **Status:** a personal project under active development. The data side, the analytics and manual
> trading are built and have been checked against the live exchange; the terminal-style UI is in
> progress (an early version starts with `kterm ui`). Nothing here is financial advice, and you are
> responsible for any order you send.

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
- **Every order passes a risk layer** with deliberately tiny default limits, and there is a kill
  switch. Change the limits only by editing `risk.toml`.

Never commit API keys, private keys, `.env` files or TLS material; see [SECURITY.md](SECURITY.md).

## Requirements

Python 3.11+ and [uv](https://docs.astral.sh/uv/). Docker for the server and for the database tests.
Node 22 only if you work on the web UI's front end.

## Try it

```bash
git clone https://github.com/swalsh76/KalshiTerm2 && cd KalshiTerm2
uv sync
uv run pytest -m "not db"            # all tests that do not need Docker
```

**Run a server** on the machine that will host it (details and the reasoning: PLAN §8). `check`
prints the exact `init` command for that machine; `install` (macOS) sets up the drive check:

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

**Point the client at it** (the server uses a self-signed certificate, pinned on first use after
you compare its fingerprint with the one the server printed):

```bash
uv run kterm config add home --url https://<name>.local:8700   # asks for the token
uv run kterm server trust                                       # the first profile is the default
uv run kterm markets bitcoin
uv run kterm ui                                                 # the local web UI
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

## Documentation

- [docs/PLAN.md](docs/PLAN.md): the design, the decisions and the build history.
- [CLAUDE.md](CLAUDE.md): the working rules for AI-assisted changes in this repository.

## License

[MIT](LICENSE)
