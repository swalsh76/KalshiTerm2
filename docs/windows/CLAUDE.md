# CLAUDE.md (Windows-native line)

> **How to adopt this file.** It is a proposal and does not replace the live root `CLAUDE.md`.
> On a Windows machine: `git switch -c windows-native origin/main`, then copy this file over the
> root `CLAUDE.md` on that branch (`copy docs\windows\CLAUDE.md CLAUDE.md`). Do not merge that
> branch into `main` without the user's approval.

## Start here

1. Read [docs/windows/WINDOWS-PLAN.md](docs/windows/WINDOWS-PLAN.md): what the Windows line is, the
   tool choices, the file-by-file porting inventory, and the build order (W0 to W7).
2. Read [docs/PLAN.md](docs/PLAN.md) for the shared design (§1-7, §9 budget and retention, §11
   risks). Its Docker and macOS deployment sections (§8, parts of §11) are what this line replaces.
3. Skim [docs/BUILD-LOG.md](docs/BUILD-LOG.md) §10.2 (measurements), §10.9 to §10.11 (cleanups,
   NAS backup); every claim below about what was built is verified there.
4. **You start at phase W0 (assess).** Do not port anything until W1's spikes have passed and the
   user has said go. Keep a `docs/windows/BUILD-LOG-WINDOWS.md` of what you measure and decide.

## What exists (all on `main`, the Postgres line; it is the reference implementation)

Phases 0 to 6 are complete: scaffolding, `kalshi-core` (Ed25519 signing, REST and WebSocket
clients, rate limiter, multivariate markets), server storage and ingestion, the server API (REST
plus WebSocket push with catch-up, token auth, TLS), LAN features (backup/restore, client profiles
with keyring tokens, TOFU certificate pinning, mDNS discovery), analytics (plugin framework,
market metrics, arbitrage, per-user alerts), the client data side (typed API and stream client, CLI
views), and client trading (risk layer, orders, kill switch, shard funding; verified live with
about $10 under tiny limits). Phase 7 (terminal UI, a local FastAPI backend plus React front end)
was next when this line was proposed and is not part of it.

Layout: uv workspace, three packages under `packages/` (`kalshi-core`, `kalshiterm-server`,
`kalshiterm-client`), `src/` layout, hatchling, Python 3.11+, ruff, mypy strict, pytest and
pytest-asyncio. The server package splits its CLI into `cli.py` (wiring, db/cert groups, status,
health, init, api) and `cli_ingest.py`, `cli_access.py`, `cli_backup.py`, `cli_analyze.py`,
`cli_common.py`. Core server modules: `db.py`, `config.py`, `governor.py` (storage budget),
`status.py`, `backup.py`, `tls.py`, `init.py`, `shutdown.py`, `ingest/`, `analytics/`, `api/`,
`storage/`, `migrations/versions/0001..0020`.

## Non-negotiables (carried over; the Docker and macOS ones are replaced, see below)

- **Trades go client to Kalshi directly, never through the server.** The trading key never leaves
  the workstation. In single-box mode this means the server's service accounts must not be able to
  read the user's profile or Credential Manager; the server has its own data-only key.
- **Demo is the default** in code and config; production is explicit opt-in. `KalshiRestClient` is
  GET-only (guard and test). The write path is the separate `KalshiTradingClient` with the separate
  trading key, refused in production unless `production_enabled = true` in `risk.toml`, limited to a
  fixed list of order routes; every order passes the risk layer and, in production, a typed
  confirmation. **Get the user's go-ahead before sending any real order.**
- The server is multi-user: named users, per-user tokens, per-user watchlists, alerts and layouts.
- **The API is the only exposed port.** SQL Server listens on shared memory only (TCP/IP and named
  pipes off). Self-signed TLS with trust-on-first-use pinning.
- Storage budget **500 GB**, enforced by the storage governor and by a hard `MAXSIZE` on the data file.
- MIT license. The repository is **public**: never commit secrets, keys, product keys, `.env`
  files or personal data. Commit with the GitHub no-reply address.
- Never hard-code `localhost` or Kalshi URLs outside `kalshi-core` config.
- Money is fixed-point `bigint`: dollars at 10^-6 (`*_e6`), counts at 10^-2 (`*_e2`), via
  `kalshi_core.fixedpoint`; anything finer fails loudly. Never a float.

**Replaced in this line:** "server deploys only via Docker Compose" becomes *the server deploys only
via the PowerShell installer, as Windows services, with no Docker, WSL or Linux VM*; "Docker's
disk image on the external SSD" becomes *a dedicated NTFS volume*; "client runs natively" is
unchanged and the client may share the machine with the server (topology A).

## Scope discipline (KalshiTerm 1 died of feature creep)

- Build only the current phase of WINDOWS-PLAN.md §6. Anything else is out of scope.
- If a request or idea is not in the plan, say so explicitly ("this is scope creep: ...") and ask
  whether to add it to the plan, defer it, or drop it.
- Architecture or direction changes go into the plan first (with approval), then code.
- Park deferred ideas in PLAN.md §13 (the parking lot), never in code or TODO comments.
- Prefer the smallest thing that satisfies the phase. Work on a branch; merge only when the user approves.
- Describe an action and its effects before asking for approval. One approval to commit covers merge,
  push and waiting for CI. When live-testing against market data, repeat the data in the visible reply.

## Lessons learned (what the Postgres line paid for; do not relearn them)

**Durability is the reason this line exists.**
- On Docker Desktop for macOS a flush reached the drive cache only (`pg_test_fsync` showed it;
  BUILD-LOG §10.2). A hard power cut zeroed `pg_logical/replorigin_checkpoint`; Postgres would not
  start. A clean reboot recovered with no repair. Data checksums were on and caught what a page scan
  could not (wholly zeroed pages look valid).
- So: **measure the flush path first (S-1), then power-cut the real machine**, with the database
  under write load, and check integrity after each cut. Keep `DELAYED_DURABILITY = DISABLED` and
  `PAGE_VERIFY = CHECKSUM`. A VM power-off is not a power cut; a host cut is.
- Recovery watching worked well as a script: wait for the host, then the services, then fresh data,
  then compare row counts with a baseline taken beforehand. Rebuild that in PowerShell (the old
  scripts were throwaway and are not in the repo).

**Unattended operation.**
- Never design a step that can pop a dialog on an unwatched console; it hangs forever. Bound every
  wait (the old mount script killed commands after a timeout), log what was skipped and why, and
  refuse to try when a prerequisite (a saved credential) is missing rather than prompting.
- A job that cannot read the data drive or the share is a permissions problem first (on macOS it
  was a privacy block). On Windows run services as dedicated virtual accounts and test them
  non-interactively and after a reboot, not from your own elevated shell.
- A stale network mount after a network change must be forced off and remounted.
- With full-disk encryption and no TPM protector a machine waits at the unlock screen after a
  power cut; set the TPM protector and auto-unlock up front.

**Install and deploy.**
- A missing bind-mount source made Docker create a directory where a file was expected. The general
  lesson stands: check that the key file exists and is a file, and say exactly what is wrong.
- The installer asks for the data volume itself, supports `check` before anything is touched, a
  dry run, `verify` after, and an uninstall. Keep that shape.
- A tool found on an interactive PATH may not be on the PATH of an ssh or service session; resolve
  executables by explicit search.

**Backups.**
- A backup target must carry a marker file, or a missing mount silently fills the local disk.
- Retention was 7 daily + 4 weekly. A restore is not proven until it is restored **into a new
  database** and compared with a manifest of row counts. Nightly catch-up at start when the last
  good backup is over 24 hours old; after a failure the loop exits and the service restarts rather
  than retrying in place.
- Not covered by backups: the `.env`, `secrets/` and `config/` files. Decide where those go.

**Testing.**
- Windows CI runners are slow: 5-second hang guards flaked there; guard durations come from an
  environment variable (`HANG_GUARD`). Tests that need `bash` fail under a stripped PATH.
- Use differential testing when changing behavior-preserving code (the risk-layer split was fuzzed
  against the old version) and a contract test for every model that crosses the API; a newer
  pydantic stopped publishing the decimal pattern until the models documented it themselves.
- The API contract tests and generated front-end types are the oracle for a storage port.
- `shutdown.py` already tolerates Windows (it installs no SIGTERM handler there); a service needs its
  own stop path.
- Dependabot and CI: pinned action versions; the CI matrix includes `windows-latest`.

**Kalshi and data facts.**
- Ed25519 keys only. Trades missed during an outage are recovered by backfill and recorded in the gap
  log; **tickers and order books are not recoverable**, so downtime is lost data and `status` must
  say so. Combos are stored as counters plus a large-trade log, not per-combo rows (decisions 10 to 17).
- Reference size on 2026-10-15: a database of about 28 GB with ~0.9 M markets, ~12 M trades and
  ~50 M tickers; use it to size spikes.
- Docs are split: PLAN.md is design, BUILD-LOG.md is how each slice was built and verified; keep
  its section numbering.

## Dev commands (Windows PowerShell)

- Tests: `uv run pytest` (all; DB tests need `KTERM_TEST_DB_URL` and skip cleanly without it);
  `uv run pytest -m "not db"`. Lint and types: `uv run ruff check`, `uv run ruff format --check`,
  `uv run mypy`.
- Front end: `packages\kalshiterm-client\frontend`, Node 22: `npm ci`, `npm run lint|typecheck|test|build`;
  `npm run gen:types` after an API change (regenerate `openapi.json` first with
  `uv run python -m kalshiterm_client.ui.schema`).
- Live Kalshi tests are opt-in and read-only: `$env:KALSHI_INTEGRATION=1; uv run pytest packages\kalshi-core\tests\test_live.py -s`.
- The Postgres reference line: `git worktree add ..\kterm-main main` and use the Docker dev database
  there, only if you need to compare behavior.

## Definition of done for the line

W7's comparison against the Mac Studio results is written up, the user has chosen (adopt, keep the
Postgres line, or run both), and PLAN.md §12 decision 15 is updated to match. Until then, nothing
here changes `main`.
