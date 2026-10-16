# KalshiTerm on Windows: plan for a Windows-native line

**Status: PROPOSAL (2026-10-15).** Not adopted. `docs/PLAN.md` on `main` stays the source of truth
until the user approves this and the first spikes (§6, W1) pass. Companion file:
[CLAUDE.md](CLAUDE.md) (the instructions a Windows-side session should work under).

## 1. Why this exists

PLAN.md decision 15 (where the database runs) is open because Docker Desktop on macOS acknowledged
flushes without forcing them to the flash, and a hard power cut corrupted the Postgres data
(BUILD-LOG §10.2). This proposal tests a different answer: **no Docker and no Linux anywhere; the
database is SQL Server 2025 Enterprise and everything around it is a Windows feature.** SQL Server
writes through the file system with the flags Windows documents for durable writes, and its recovery
tooling (page checksums, `DBCC CHECKDB`, log backups) is built in.

It is a *line*, not a rewrite of the project: `kalshi-core`, the client, the API contract and the
risk layer are kept as they are. The Postgres line on `main` is the reference implementation and the
test oracle.

## 2. Requirements (from the user, 2026-10-15)

1. Database: **SQL Server 2025 Enterprise** (Developer edition has the same features and is the fallback for
   machines where Enterprise cannot be installed; see risk R1).
2. Windows native tooling only for everything around the code (§4). No Docker, no WSL, no Linux VM.
   The code stays Python (rewriting in .NET is out of scope; parking lot).
3. **Topology A, single box:** server and client on one Windows 11 x64 machine, an arbitrary setup
   (not a prepared server, possibly with other software and even another SQL Server on it).
4. **Topology B, remote host:** server host and SQL Server together on a Windows Server 2025 machine;
   clients elsewhere on the LAN (Windows 11, or the existing macOS client).
5. Everything in PLAN.md's non-negotiables that is not about Docker or macOS still holds (see
   CLAUDE.md in this folder).

## 3. Topologies

```
A. single box (Windows 11 x64)                B. remote host (Windows Server 2025)

 ┌───────────── one machine ──────────────┐    ┌──────── server host ────────┐   ┌─ client PC ─┐
 │ client (CLI, UI) ──https 127.0.0.1:8700─┐│    │ services: ingest, api,       │   │ kterm CLI/UI│
 │            trading key: user's           ││    │ analyze, backup              │◀──│ trading key │
 │            Credential Manager            ││    │ SQL Server (shared memory    │ https│ stays here  │
 │ services (virtual service accounts):     ││    │ only, TCP off)               │ 8700 └─────────────┘
 │  ingest, api, analyze, backup            ││    │ NAS share for backups        │
 │ SQL Server named instance KALSHITERM     ││    └──────────────────────────────┘
 │ (shared memory only, memory-capped)      ││
 └──────────────────────────────────────────┘
```

Same code and same installer in both; one setting differs: `[deployment] mode = "single-box" | "remote-host"`.

| | A. single box | B. remote host |
|---|---|---|
| API binds | 127.0.0.1 (a LAN bind is an explicit opt-in) | the LAN address; the only open port |
| Firewall rule | none | `New-NetFirewallRule` for the API port (and UDP 5353 for discovery) |
| Discovery (mDNS) | off; the installer creates the client profile `local` and pins the certificate | on, as today (zeroconf) |
| SQL Server | named instance, `max server memory` capped (workstation must stay usable), Resource Governor limits for ingest/analytics | the instance may take most of the RAM |
| Sleep / updates | `powercfg` keeps it awake on AC; gaps from sleep are recorded by the existing gap log | maintenance windows; service recovery handles reboots |
| Trading key | the user's Credential Manager; the server accounts cannot read the user profile | on the client PC only, as today |

## 4. Windows-native toolbox (replaces Docker Compose and the macOS host scripts)

| Job | Today (macOS + Docker) | Windows line |
|---|---|---|
| Database | `timescale/timescaledb` container | SQL Server 2025 Enterprise, named instance `KALSHITERM`, installed unattended by `setup.exe` (`/QS /ACTION=Install /FEATURES=SQLENGINE /INSTANCENAME=KALSHITERM /TCPENABLED=0 /NPENABLED=0 /IACCEPTSQLSERVERLICENSETERMS`); media and product key supplied by the user, never in the repo |
| Process supervision | `restart: unless-stopped` | Windows services (one per role) with `sc failure` recovery actions, delayed auto-start and `sc config depend=` on the SQL service. Wrapper: a small pywin32 service class in the repo (evaluate in W4; fallback: Task Scheduler "at startup" + restart on failure) |
| Isolation (only ingest holds the Kalshi key; only api holds the TLS key; internal DB network) | compose networks + mounts | one **virtual service account** per role (`NT SERVICE\KalshiTermIngest` ...), file ACLs by `icacls`, SQL logins mapped from those accounts (Windows authentication: **no database password exists**), SQL TCP/IP and named pipes disabled |
| Secrets | files in `secrets/`, mode 0600 | the same files in `%ProgramData%\KalshiTerm\secrets` with ACLs; Python's `os.chmod` does nothing on Windows, so ACLs are set explicitly (W4) |
| Firewall | compose publishes only 8700 | Windows Defender Firewall rules (`New-NetFirewallRule`) |
| Config / install / dry run | `install.sh --dry-run` | `install.ps1` with `check`, `install`, `verify`, `uninstall` and `-WhatIf` (PowerShell's own dry-run) |
| Scheduling | in-process loops | unchanged: the Python services own all schedules (backup, partition maintenance, candles, retention), so the logic is testable and edition-independent. SQL Server Agent is not used |
| Data volume | Docker disk image on external SSD; drive-check daemon | a dedicated NTFS volume (64 KB allocation unit); data file `MAXSIZE` is the hard budget; host check reads `Get-Volume` / `Get-PhysicalDisk` and writes the same `host.json` |
| Backup target | NAS share mounted by a LaunchAgent | `New-SmbGlobalMapping` (machine-wide, persists across reboots, works for services; user-mapped drives do not) or a UNC path with credentials from `cmdkey`; same marker-file guard |
| Encryption at rest, unattended boot | FileVault (cannot boot unattended) | BitLocker with a TPM protector (boots unattended) and auto-unlock for the data volume |
| Remote administration | ssh | OpenSSH Server (built in) and PowerShell |
| Logs | docker json-file, size-capped | Windows Event Log (a custom source) plus a rotating file |
| Power loss | none | `powercfg`, the built-in UPS service, and the power-cut test in W6 |
| Time sync | n/a | `w32tm` (verify the source; our timestamps matter) |

## 5. What changes in the code (inventory from the 2026-10-15 tree)

Kept as is: `kalshi-core` (all), the client package (already runs on Windows; CI has a
`windows-latest` leg), the REST/WS contracts and the generated front-end types, the risk layer,
pinning/TOFU, alert and analyzer plugin interfaces, fixed-point money.

| Area | Postgres line | Windows line |
|---|---|---|
| Driver and engine | `asyncpg`, SQLAlchemy `postgresql+asyncpg` (`db.py`) | SQLAlchemy `mssql+...`; driver and async story decided by spike S-3 (pyodbc with ODBC Driver 18 behind a thread pool, `aioodbc`, or Microsoft's `mssql-python`) |
| Migrations | 20 Alembic revisions; Timescale in 0001, 0003-0007, 0009, 0010, 0018 | a new baseline revision for SQL Server (the Windows line starts empty), then incremental revisions; Alembic supports `mssql` |
| Hypertables, 1-day chunks, compression after 1 day, retention | Timescale policies | Daily **partitioned** tables, columnstore compression, retention by `TRUNCATE ... WITH (PARTITIONS (...))`; a maintenance task in `analyze` creates tomorrow's partitions and closes yesterday's delta stores (`REORGANIZE ... COMPRESS_ALL_ROW_GROUPS`). Layout chosen by spike S-2 |
| Continuous aggregates (1 m / 1 h candles) | Timescale caggs | an incremental candle job in `analyze` (idempotent `MERGE`), because indexed views cannot hold `MIN`/`MAX`/first/last |
| `time_bucket`, `hypertable_size`, `timescaledb_information.*` | `status.py`, `governor.py`, `analytics/builtin/market_metrics.py`, `ingest/backfill.py`, `ingest/stream.py`, `backup.py` | `DATEADD/DATEDIFF` bucketing, `sys.dm_db_partition_stats` / `sys.master_files` |
| `ON CONFLICT` | `governor.py`, `storage/market_ids.py`, `layouts.py`, `ingest/stream.py`, `ingest/combos.py` | `MERGE ... WITH (HOLDLOCK)` or a unique index with `IGNORE_DUP_KEY` (check against columnstore in S-2) |
| `LISTEN/NOTIFY` (`kterm_ingest`, `kterm_alerts`) | `api/stream.py` Hub, `analytics/runner.py` (NOTIFY in the same transaction as results) | **Polling.** The Hub already polls every second when LISTEN is unavailable, so notification is only a latency optimisation. Option to measure in S-4: a cheap `MAX(seq)` poll at 200-500 ms or Service Broker |
| Advisory lock per analyzer (`analytics/runner.py`) | `pg_advisory_lock` | `sp_getapplock` |
| Backup | `pg_dump -Fc`, marker-file guard, 7 daily + 4 weekly, restore into a new DB and compare with a manifest (`backup.py`) | native `BACKUP DATABASE ... WITH COMPRESSION, CHECKSUM`: full nightly, differential, and log backups for point-in-time restore; same marker guard, retention and restore-into-a-new-database-and-compare drill (`RESTORE VERIFYONLY`, row-count manifest; columnstore counts are exact, so the compressed-chunk one-sided compare goes away) |
| Graceful stop | `shutdown.py` (SIGTERM from `docker stop`; does nothing on Windows) | the service's stop handler cancels the main task |
| File modes | `os.chmod` 0600/0700 in `tls.py`, `init.py`, and (client) `profiles.py`, `trading_state.py` | ACL helper in one module, used by all of them; the client uses Credential Manager (via `keyring`) as it already does |
| Shell | `deploy/host/*.sh`, Dockerfile, compose files | `deploy/windows/*.ps1`; the Postgres files stay on `main` |
| Tests | DB tests start a pinned Timescale container | DB tests use `KTERM_TEST_DB_URL`: CI runs SQL Server in a Linux container (test tooling only); a Windows dev machine uses a local Developer-edition instance |

The API contract does not change, so the **contract tests and a golden-response parity test are the
acceptance oracle** (W6).

## 6. Build order

Every phase ends with tests green and the BUILD-LOG updated. W1 can stop the whole line.

| Phase | Deliverable | Gate |
|---|---|---|
| **W0 Assess** | On a Windows 11 x64 dev machine: clone, `uv sync`, run the non-DB suite natively, record every Windows failure; read the files named in CLAUDE.md; confirm the OS/edition question (R1) from Microsoft's current requirements page | a written baseline in BUILD-LOG-WINDOWS.md |
| **W1 Spikes** (time-boxed, measured, throwaway code) | **S-1 durability:** a write-through micro-benchmark (`diskspd`, Microsoft's own tool) on the target volume and on a Hyper-V Server 2025 guest, then SQL Server commit latency (`sys.dm_io_virtual_file_stats`), then 3 hard power cuts under load with `DBCC CHECKDB` after each. **S-2 time-series layout:** one partitioned columnstore table with a nonclustered unique index (idempotent inserts) against hot rowstore + cold columnstore, measuring insert rate, storage per million rows against the Timescale numbers in BUILD-LOG §10.2, and query latency. **S-3 driver:** bulk-insert throughput and async behavior. **S-4 change signal:** poll vs Service Broker. **S-5 install:** unattended instance install on a clean Windows 11 VM, side by side with an existing instance | go/no-go. S-1 failing ends the line |
| **W2 Storage layer** | SQL Server baseline migration, the repository/storage functions ported, ingest writes real data from the demo feed | storage tests green; ingest rate at least the measured Postgres rate |
| **W3 API, analytics, governor, status** | everything that reads or maintains the data, including partitions, candles, retention, governor, alerts, stream | API contract tests unchanged and green |
| **W4 Windows host layer** | `install.ps1`, services with recovery, ACL helper, firewall, host check, NAS mapping, Event Log, both `mode` values | `install.ps1 verify` passes on a clean Windows 11 VM (A) and a Server 2025 VM (B) |
| **W5 Backup and restore** | full/diff/log schedule, retention, point-in-time restore, restore drill with manifest | restore to a chosen minute proven |
| **W6 Verification** | golden-response parity against the Postgres line (same fixture, same API answers); reboot test; 3 hard power cuts on real hardware; 48 h soak | all pass, numbers in the BUILD-LOG |
| **W7 Decision** | compare W6 with the Mac Studio results; propose the PLAN.md §12 decision 15 text | user decides (adopt, keep Postgres line, or run both) |

Out of scope until promoted: importing the existing 28 GB Postgres history (the Windows line starts
empty and backfills trades from Kalshi; tickers and order books Kalshi does not keep cannot be
rebuilt), replication/Always On, a .NET rewrite, Windows containers, Kubernetes.

## 7. Risks and unknowns (check these first)

- **R1: Enterprise on Windows 11.** Microsoft's SQL Server 2025 requirements page says Windows 10 or
  later and Windows Server 2019 or later; it did not say in what I read whether Enterprise (as opposed to
  Developer/Express) installs on a client OS, or confirm Windows 11 / Server 2025 by name. Verify on the
  official page and with a real install in W0/S-5. Fallback: Enterprise Developer edition (same
  features; its license forbids production use, so read the terms before relying on it for a trading tool).
- **R2: durability is unproven here.** The reason for the line is a hypothesis. S-1 must measure it, on
  the real disk and in the virtual case.
- **R3: arbitrary machines.** The installer must tolerate an existing SQL Server, an existing Python,
  no domain, a non-English locale, and a blocked execution policy. It needs an elevated PowerShell.
- **R4: time-series layout.** Columnstore does not allow some constraints the Timescale schema relied
  on; S-2 decides the layout before any porting.
- **R5: workstation impact (A).** Memory, disk and CPU caps, and Windows Update reboots. Measure.
- **R6: Windows Server 2025 hotpatching** needs an Azure Arc subscription; not assumed.
- **R7: the python runtime** is the one non-Windows component. Installed per-machine by `winget`
  (or an embedded distribution); revisit if it proves fragile.
- **R8: effort.** Roughly W0-W1 a few days, W2-W5 a few weeks, W6 a week of elapsed time. Decide after W1.
