FROM python:3.12-slim-trixie
# pg_dump/pg_restore must be at least as new as the server (PostgreSQL 18); Debian ships 17.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -d /usr/share/postgresql-common/pgdg \
 && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
      https://www.postgresql.org/media/keys/ACCC4CF8.asc \
 && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client-18 \
 && apt-get purge -y curl gnupg && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock LICENSE ./
COPY packages/kalshi-core packages/kalshi-core
COPY packages/kalshiterm-server packages/kalshiterm-server
COPY packages/kalshiterm-client/pyproject.toml packages/kalshiterm-client/pyproject.toml
RUN uv sync --frozen --no-dev --package kalshiterm-server --no-editable
ENV PATH="/app/.venv/bin:$PATH"
CMD ["kterm-server", "--help"]
