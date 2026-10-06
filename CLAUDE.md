# CLAUDE.md

## Start here

**Read [docs/PLAN.md](docs/PLAN.md) before doing anything.** It is the source of truth for
architecture, packaging, deployment, and build order. Update it when decisions change.

## Status

Pre-implementation. No code yet. Next step is **Phase 0 (scaffolding)**. Decisions made:
web UI + Typer CLI, manual trading first, multi-user server, MIT license, private
project (no PyPI). No **[OPEN]** items remain in PLAN.md §12.

## Non-negotiables

- **Trades go client → Kalshi directly, never through the server.** The trading key never
  leaves the workstation; the server has its own data-only key.
- **Demo environment is the default** everywhere. Production trading requires explicit
  opt-in and every order passes the client risk layer.
- Client runs natively (Windows/macOS), not in Docker.
- Server deploys **only via Docker Compose** on a remote LAN host; API is the only exposed
  port; self-signed TLS with trust-on-first-use pinning.
- Storage budget is **100 GB** (backups go to the NAS), enforced by the storage governor (PLAN.md §9).
- Server is multi-user: named users, per-user tokens, per-user watchlists/alerts/layouts.
- License is MIT.
- Never hard-code `localhost` or Kalshi URLs outside `kalshi-core` config.

## Conventions

- uv workspace, three packages under `packages/` (`kalshi-core`, `kalshiterm-server`,
  `kalshiterm-client`), `src/` layout, hatchling, Python 3.11+.
- ruff (lint + format), mypy strict, pytest + pytest-asyncio.
- Work on a branch; merge to `main` when the user approves.
