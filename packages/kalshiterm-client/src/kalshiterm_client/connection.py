"""Talking to a server: pinned TLS, bearer token, and errors a person can act on."""

import ssl
from typing import Any
from urllib.parse import urlsplit

import httpx

from kalshiterm_client import secrets
from kalshiterm_client.pinning import (
    fetch_certificate,
    fingerprint_of_pem,
    trusting,
)
from kalshiterm_client.profiles import Profile, ProfileError


class ConnectionProblem(ProfileError):
    """Could not talk to the server; the message says why and what to do."""


class CertificateChanged(ConnectionProblem):
    """The server presented a certificate other than the pinned one."""


def _ssl_cause(exc: BaseException) -> ssl.SSLCertVerificationError | None:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, ssl.SSLCertVerificationError):
            return exc
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__  # type: ignore[assignment]
    return None


def _explain_tls_failure(
    profile: Profile, cause: ssl.SSLCertVerificationError
) -> ConnectionProblem:
    """Tell a changed certificate (a security event) apart from an expired or mismatched one."""
    try:
        presented = fingerprint_of_pem(fetch_certificate(profile.url))
    except ProfileError:
        presented = None
    if presented is not None and presented != profile.fingerprint:
        return CertificateChanged(
            f"THE CERTIFICATE OF {profile.name} HAS CHANGED. Nothing was sent.\n"
            f"  pinned:    {profile.fingerprint}\n"
            f"  presented: {presented}\n"
            "If you rotated the server's certificate, check the new fingerprint with the "
            "server's operator (`kterm-server cert show`), then run "
            f"`kterm --profile {profile.name} server trust`. If you did not, someone may be "
            "impersonating the server: do not trust it."
        )
    return ConnectionProblem(
        f"the pinned certificate is no longer acceptable ({cause.verify_message}); the "
        "certificate is unchanged, so check the name in the URL against the certificate's "
        "names and its expiry (`kterm-server cert show`)"
    )


def request(profile: Profile, path: str, *, timeout: float = 10.0) -> Any:
    """GET ``path`` with the profile's token, over TLS that trusts only the pinned certificate.

    The pin is checked before any request is made, so the token is never sent to a server
    that has not been trusted or that presents a different certificate.
    """
    token = secrets.get_token(profile.name)
    if not token:
        raise ConnectionProblem(
            f"no token for {profile.name}: run `kterm config token {profile.name}`"
        )
    secure = urlsplit(profile.url).scheme == "https"
    verify: ssl.SSLContext | bool = trusting(profile) if secure else False
    try:
        with httpx.Client(
            base_url=profile.url,
            verify=verify,
            timeout=timeout,
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            response = client.get(path)
    except httpx.HTTPError as exc:
        cause = _ssl_cause(exc)
        if cause is not None:
            raise _explain_tls_failure(profile, cause) from exc
        raise ConnectionProblem(f"cannot reach {profile.url}: {exc.__class__.__name__}") from exc
    if response.status_code == 401:
        raise ConnectionProblem(
            f"the server rejected the token for {profile.name} (revoked, expired or wrong)"
        )
    if response.status_code == 403:
        raise ConnectionProblem("that needs an admin token; this one is read-only")
    if response.status_code == 429:
        raise ConnectionProblem(
            "too many failed attempts from this address; retry in "
            f"{response.headers.get('retry-after', 'a minute')} s"
        )
    if not response.is_success:
        raise ConnectionProblem(f"the server answered {response.status_code}")
    return response.json()
