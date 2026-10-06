# CLAUDE.md

## Start here

**Read [docs/PLAN.md](docs/PLAN.md) before doing anything.** It is the source of truth for
architecture, packaging, deployment, and build order. Update it when decisions change.

## Status

Phase 0 (scaffolding) is built on branch `phase-0-scaffolding`, awaiting approval to merge;
next is Phase 1 (kalshi-core). Decisions made:
web UI + Typer CLI, manual trading first, multi-user server, MIT license, private
project (no PyPI). No **[OPEN]** items remain in PLAN.md §12.

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
- Storage budget is **100 GB** (backups go to the NAS), enforced by the storage governor (PLAN.md §9).
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

## Conventions

- uv workspace, three packages under `packages/` (`kalshi-core`, `kalshiterm-server`,
  `kalshiterm-client`), `src/` layout, hatchling, Python 3.11+.
- ruff (lint + format), mypy strict, pytest + pytest-asyncio.
- Work on a branch; merge to `main` when the user approves.
