# CLAUDE.md

## Start here

**Read [docs/PLAN.md](docs/PLAN.md) before doing anything.** It is the source of truth for
architecture, packaging, deployment, and build order. Update it when decisions change.

## Status

Phases 0 (scaffolding) and 1 (`kalshi-core`) are complete and merged to `main`. Phase 2
(server storage and ingestion) is built through slice 2.9 (deployment rehearsed on the
MacBook); 2.10 (48-hour volume calibration on the MacBook; hardware tests when the Mac Studio's
SSD arrives) is tracked in PLAN.md §10.2 and does **not** block later phases. Next: Phase 3. Decisions made: web UI + Typer CLI, manual trading
first, multi-user server, MIT license, no PyPI publishing, public GitHub repository (since 2026-10-08), multivariate markets kept,
read-only production key for dev. No **[OPEN]** items remain in PLAN.md §12.

Live integration tests are opt-in: `KALSHI_INTEGRATION=1 uv run pytest
packages/kalshi-core/tests/test_live.py -s` (read-only; skipped by default and in CI).

## Non-negotiables

- **Trades go client → Kalshi directly, never through the server.** The trading key never
  leaves the workstation; the server has its own data-only key.
- **Demo environment is the default** in code and config. Production use is explicit
  opt-in (`KALSHI_ENV=production`) and **read-only until Phase 6** (read-only key, no
  write code, GET-only transport guard; PLAN.md §4). Production trading requires the
  user's sign-off and every order passes the client risk layer.
- Client runs natively (Windows/macOS), not in Docker.
- Server deploys **only via Docker Compose** on a remote LAN host; API is the only exposed
  port; self-signed TLS with trust-on-first-use pinning.
- Storage budget is **500 GB** on a dedicated 1 TB external Thunderbolt 4 SSD (backups go to
  the NAS), enforced by the storage governor (PLAN.md §9). Docker's disk image lives on that
  drive; never bind-mount the Postgres data directory.
- Server is multi-user: named users, per-user tokens, per-user watchlists/alerts/layouts.
- License is MIT.
- Never hard-code `localhost` or Kalshi URLs outside `kalshi-core` config.

## Scope discipline

KalshiTerm 1 died of feature creep and directional changes. Guard against it:

- Build only the current phase in PLAN.md §10. Anything else is out of scope until the
  user says otherwise.
- If a request, idea or "while we're here" improvement isn't in the plan, **say so
  explicitly** ("this is scope creep: ...") before doing anything, and ask whether to
  add it to the plan, defer it, or drop it.
- Changes to architecture or direction go into PLAN.md first (with the user's approval),
  then code. Never drift silently from the plan.
- Park deferred ideas in PLAN.md §13 (Parking lot), not in code or TODO comments.
- Prefer the smallest thing that satisfies the phase's deliverable.

## Dev commands

- Tests: `uv run pytest` (all; DB tests need Docker and skip cleanly without it);
  `uv run pytest -m "not db"` skips the database tests; `-m db` runs only them.
- Dev database: `docker compose -f deploy/docker-compose.dev.yml up -d` (TimescaleDB, bound to
  127.0.0.1:5433), then
  `KTERM_DB_URL=postgresql+asyncpg://kterm:kterm_dev_only@127.0.0.1:5433/kterm uv run kterm-server db upgrade`.
  Relative downgrades need `--`: `kterm-server db downgrade -- -1`.
- The TimescaleDB image tag is pinned in `deploy/docker-compose.dev.yml` (tests read it from there).

## Conventions

- The repository is **public**: never commit secrets, keys, `.env` files or personal data. Commit
  with the GitHub no-reply address (already set in this clone's git config); the old personal
  address was scrubbed from history on 2026-10-08.

- uv workspace, three packages under `packages/` (`kalshi-core`, `kalshiterm-server`,
  `kalshiterm-client`), `src/` layout, hatchling, Python 3.11+.
- ruff (lint + format), mypy strict, pytest + pytest-asyncio.
- Work on a branch; merge to `main` when the user approves.
