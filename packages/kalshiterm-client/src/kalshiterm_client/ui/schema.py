"""The browser contract as data: the OpenAPI schema of the REST routes and the JSON schema of the
WebSocket messages, written to ``frontend/src/api/`` and committed.

The front end generates its TypeScript types from these two files (slice 7.5), so the page and the
backend cannot drift: a test fails when the committed files differ from what the code now says,
and the fix is one command::

    uv run python -m kalshiterm_client.ui.schema
"""

import json
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from kalshiterm_client.ui import messages
from kalshiterm_client.ui.app import create_app

API_DIR = Path(__file__).resolve().parents[3] / "frontend" / "src" / "api"
OPENAPI_FILE = "openapi.json"
WS_FILE = "ws.schema.json"


def openapi() -> dict[str, Any]:
    app = create_app(port=1, token="schema", environment="demo")
    schema: dict[str, Any] = app.openapi()
    return schema


def websocket() -> dict[str, Any]:
    return {
        "server_to_browser": TypeAdapter(messages.ServerMessage).json_schema(mode="serialization"),
        "browser_to_server": TypeAdapter(messages.ClientMessage).json_schema(mode="validation"),
    }


def render() -> dict[str, str]:
    """File name -> exact text."""
    return {
        OPENAPI_FILE: json.dumps(openapi(), indent=2, sort_keys=True) + "\n",
        WS_FILE: json.dumps(websocket(), indent=2, sort_keys=True) + "\n",
    }


def main() -> None:
    API_DIR.mkdir(parents=True, exist_ok=True)
    for name, text in render().items():
        (API_DIR / name).write_text(text, encoding="utf-8")
        print(f"wrote {API_DIR / name}")


if __name__ == "__main__":
    main()
