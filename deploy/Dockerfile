FROM python:3.12-slim-trixie
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock LICENSE ./
COPY packages/kalshi-core packages/kalshi-core
COPY packages/kalshiterm-server packages/kalshiterm-server
COPY packages/kalshiterm-client/pyproject.toml packages/kalshiterm-client/pyproject.toml
RUN uv sync --frozen --no-dev --package kalshiterm-server --no-editable
ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-c", "import kalshiterm_server; print(kalshiterm_server.__version__)"]
