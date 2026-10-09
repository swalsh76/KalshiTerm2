"""Pulls in the client's autouse fixture. Kept thin: a second module named ``conftest`` confuses
mypy (the server has one too), so the code lives in ``client_fixtures``."""

from client_fixtures import isolated  # noqa: F401
