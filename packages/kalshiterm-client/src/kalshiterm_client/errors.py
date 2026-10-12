"""What can go wrong talking to the server, in terms a person can act on.

Everything derives from ``ProfileError`` so the CLI can print the message and exit 1. The wording
of the connection and certificate messages is what ``kterm server status`` has always said.
"""

from typing import Any

from kalshiterm_client.profiles import ProfileError


class ConnectionProblem(ProfileError):
    """Could not talk to the server; the message says why and what to do."""


class CertificateChanged(ConnectionProblem):
    """The server presented a certificate other than the pinned one."""


class ApiProblem(ConnectionProblem):
    """The server answered, and the answer was an error. ``code`` is its ``{"error": ...}``."""

    def __init__(self, status: int, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.extra = extra


class NotFound(ApiProblem):
    pass


class Unauthorized(ApiProblem):
    pass


class Forbidden(ApiProblem):
    pass


class InvalidRequest(ApiProblem):
    """422/409 and the like: the server rejected what was asked, ``code`` says why."""


class RateLimited(ApiProblem):
    def __init__(self, retry_after: float | None, message: str) -> None:
        super().__init__(429, "too_many_failed_attempts", message)
        self.retry_after = retry_after


class ServerError(ApiProblem):
    pass
