"""``kterm ui``: start the local backend and open the browser."""

from typing import Annotated

import typer
from kalshi_core.config import KalshiSettings

from kalshiterm_client.cli_common import fail


def register(app: typer.Typer) -> None:
    @app.command("ui")
    def ui(
        port: Annotated[int, typer.Option(min=0, max=65535, help="0 picks a free port.")] = 0,
        no_browser: Annotated[
            bool, typer.Option("--no-browser", help="Print the address; do not open it.")
        ] = False,
    ) -> None:
        """Start the local web UI on 127.0.0.1 (read-only until you switch trading on in it).

        The address printed contains a one-time session token after the '#'; anything that has
        it can use the session, so do not paste it anywhere.
        """
        from kalshiterm_client.ui.run import serve

        try:
            environment = KalshiSettings().env.value
        except ValueError as exc:
            raise fail(f"cannot read the Kalshi settings: {exc}") from exc
        try:
            serve(port=port, open_browser=not no_browser, environment=environment, echo=typer.echo)
        except OSError as exc:
            raise fail(f"cannot listen on 127.0.0.1:{port}: {exc.strerror}") from exc
