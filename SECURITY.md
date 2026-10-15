# Security

KalshiTerm handles Kalshi API keys and can place real orders, so please report problems privately.

## Reporting a vulnerability

GitHub's private vulnerability reporting is not enabled on this repository, so please **do not**
open a public issue or pull request that describes a vulnerability. Contact the maintainer through
their GitHub profile ([@swalsh76](https://github.com/swalsh76)) and ask for a private channel, then
say what you found, how to reproduce it, and what it affects.

This is a personal project: there is no response-time promise, and only the `main` branch is
supported (nothing is released).

## What the design relies on

- **Separate keys.** The server holds a read-only data key. The trading key lives only on the
  workstation, outside the repository, and must differ from the data key.
- **Trades go from the client to Kalshi directly,** never through the server. Production trading
  needs three opt-ins (`KALSHI_ENV=production`, `production_enabled = true` in `risk.toml`, and a
  typed confirmation), and every order passes the client's risk limits.
- **The API is HTTPS only,** with a self-signed certificate that the client pins on first use. The
  client never sends a token to a server whose certificate is not the pinned one. Tokens are per
  user and are kept in the system keyring, not in files.
- **Only the API port is published** by the server's Compose stack; the database sits on an
  internal network with no route out.

## Never commit

API keys, private keys, `.env` files, tokens or TLS material. If you find one in the repository or
its history, report it privately as above rather than in a public issue.
