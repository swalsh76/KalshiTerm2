"""What the ``kterm-server`` command modules share: the settings and the database URL."""

from kalshiterm_server.config import ServerSettings


def server_settings() -> ServerSettings:
    return ServerSettings()  # type: ignore[call-arg]  # db_url is read from KTERM_DB_URL


def db_url() -> str:
    return server_settings().db_url
