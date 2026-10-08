import asyncio
import contextlib
import hashlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db
from kalshiterm_server.api.app import create_app
from kalshiterm_server.auth import FailureThrottle
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text

pytestmark = pytest.mark.db

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)


def settings(url: str, **extra: Any) -> ServerSettings:
    return ServerSettings(db_url=url, **extra)


def run(url: str, action: Any) -> Any:
    """Run one async database action from a synchronous test."""

    async def go() -> Any:
        engine = db.make_engine(url)
        try:
            return await action(engine)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def make_token(url: str, user: str = "alice", role: str = "read", **kw: Any) -> tuple[int, str]:
    async def go(engine: Any) -> tuple[int, str]:
        with contextlib.suppress(auth.AuthError):  # already there
            await auth.add_user(engine, user)
        return await auth.create_token(engine, user, role, **kw)

    result: tuple[int, str] = run(url, go)
    return result


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def client_for(url: str, **kw: Any) -> TestClient:
    clock = kw.pop("clock", lambda: NOW)
    return TestClient(create_app(settings(url, **kw), clock=clock))


def test_a_valid_token_identifies_its_owner(migrated_db_url: str) -> None:
    token_id, token = make_token(migrated_db_url, "alice", "read")
    with client_for(migrated_db_url) as client:
        response = client.get("/v1/me", headers=bearer(token))
    assert response.status_code == 200
    assert response.json() == {"user": "alice", "role": "read", "token_id": token_id}
    assert token not in response.text and token.split("_", 2)[2] not in response.text


def test_the_scheme_name_is_case_insensitive(migrated_db_url: str) -> None:
    _, token = make_token(migrated_db_url)
    with client_for(migrated_db_url) as client:
        for scheme in ("Bearer", "bearer", "BEARER"):
            assert (
                client.get("/v1/me", headers={"Authorization": f"{scheme} {token}"}).status_code
                == 200
            )


def test_every_kind_of_failure_looks_exactly_the_same_from_outside(migrated_db_url: str) -> None:
    url = migrated_db_url
    good_id, good = make_token(url, "alice")
    _, revoked = make_token(url, "bob")
    revoked_id = int(revoked.split("_")[1])
    run(url, lambda e: auth.revoke_token(e, revoked_id))
    _, expiring = make_token(url, "carol", expires_in=timedelta(days=1), now=NOW)
    _, doomed = make_token(url, "dave")
    run(url, lambda e: auth.remove_user(e, "dave"))
    secret = good.split("_", 2)[2]
    attempts = {
        "no header": {},
        "wrong scheme": {"Authorization": f"Basic {good}"},
        "garbage": bearer("garbage"),
        "right id, wrong secret": bearer(f"kt_{good_id}_{'A' * 43}"),
        "unknown id": bearer(f"kt_999999_{secret}"),
        "revoked": bearer(revoked),
        "removed user": bearer(doomed),
        "oversized header": bearer("a" * 10_000),
    }
    seen = set()
    with client_for(url, auth_failure_limit=1000) as client:
        for name, headers in attempts.items():
            response = client.get("/v1/me", headers=headers)
            assert response.status_code == 401, name
            seen.add((response.status_code, response.text, response.headers["www-authenticate"]))
    assert len(seen) == 1  # no attempt can be told apart from another

    later = lambda: NOW + timedelta(days=2)  # noqa: E731
    with client_for(url, clock=later) as client:  # the same expiring token, two days on
        response = client.get("/v1/me", headers=bearer(expiring))
        assert (
            response.status_code == 401 and (response.status_code, response.text) == seen.pop()[:2]
        )
    with client_for(url) as client:  # ...and valid before it expired
        assert client.get("/v1/me", headers=bearer(expiring)).status_code == 200


def test_the_secret_is_never_stored_only_its_hash(migrated_db_url: str) -> None:
    _, token = make_token(migrated_db_url)
    secret = token.split("_", 2)[2]

    async def dump(engine: Any) -> tuple[Any, str]:
        async with engine.connect() as conn:
            row = (await conn.execute(text("SELECT token_hash FROM api_tokens"))).scalar_one()
            everything = (await conn.execute(text("SELECT t::text FROM api_tokens t"))).scalar_one()
        return row, everything

    stored, whole_row = run(migrated_db_url, dump)
    assert bytes(stored) == hashlib.sha256(secret.encode()).digest()
    assert secret not in whole_row and token not in whole_row


def test_only_admin_tokens_may_read_the_status_report(migrated_db_url: str) -> None:
    _, reader = make_token(migrated_db_url, "alice", "read")
    _, admin = make_token(migrated_db_url, "root", "admin")
    with client_for(migrated_db_url) as client:
        assert client.get("/v1/status").status_code == 401
        denied = client.get("/v1/status", headers=bearer(reader))
        assert denied.status_code == 403 and denied.json() == {"error": "forbidden"}
        allowed = client.get("/v1/status", headers=bearer(admin))
    assert allowed.status_code == 200
    report = allowed.json()
    assert {"database", "storage", "streams", "gaps", "jobs", "problems"} <= set(report)
    assert report["database"]["revision"] == report["database"]["head"]


def test_last_use_is_recorded_but_not_written_on_every_request(migrated_db_url: str) -> None:
    token_id, token = make_token(migrated_db_url)

    def last_used() -> datetime | None:
        async def go(engine: Any) -> datetime | None:
            return (await auth.list_tokens(engine))[0]["last_used_at"]  # type: ignore[no-any-return]

        return run(migrated_db_url, go)  # type: ignore[no-any-return]

    assert last_used() is None
    times = iter([NOW, NOW + timedelta(seconds=20), NOW + timedelta(minutes=2)])
    with client_for(migrated_db_url, clock=lambda: next(times)) as client:
        client.get("/v1/me", headers=bearer(token))
        first = last_used()
        client.get("/v1/me", headers=bearer(token))
        assert last_used() == first == NOW  # twenty seconds later: no write
        client.get("/v1/me", headers=bearer(token))
        assert last_used() == NOW + timedelta(minutes=2)
    assert token_id


def test_repeated_failures_lock_an_address_out_even_for_a_valid_token(migrated_db_url: str) -> None:
    _, token = make_token(migrated_db_url)
    app = create_app(settings(migrated_db_url, auth_failure_limit=3), clock=lambda: NOW)
    now = [1_000.0]
    app.state.throttle = FailureThrottle(3, 60, clock=lambda: now[0])
    with TestClient(app, client=("10.1.1.1", 5000)) as attacker:
        for _ in range(3):
            assert attacker.get("/v1/me", headers=bearer("kt_1_" + "x" * 43)).status_code == 401
        locked = attacker.get("/v1/me", headers=bearer(token))  # even the right token
        assert locked.status_code == 429
        assert locked.json() == {"error": "too_many_failed_attempts"}
        assert 1 <= int(locked.headers["retry-after"]) <= 61
    with TestClient(app, client=("10.2.2.2", 5000)) as other:  # a different address is fine
        assert other.get("/v1/me", headers=bearer(token)).status_code == 200
    now[0] += 61
    with TestClient(app, client=("10.1.1.1", 5000)) as attacker:  # the lockout expired
        assert attacker.get("/v1/me", headers=bearer(token)).status_code == 200


def test_health_checks_are_never_throttled_or_authenticated(migrated_db_url: str) -> None:
    app = create_app(settings(migrated_db_url, auth_failure_limit=1), clock=lambda: NOW)
    with TestClient(app) as client:
        client.get("/v1/me")  # one failure: this address is now locked out of /v1
        assert client.get("/v1/me").status_code == 429
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code == 200


def test_tokens_never_appear_in_logs(
    migrated_db_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    _, token = make_token(migrated_db_url)
    secret = token.split("_", 2)[2]
    caplog.set_level(logging.DEBUG)
    with client_for(migrated_db_url, auth_failure_limit=100) as client:
        client.get("/v1/me", headers=bearer(token))
        client.get("/v1/me", headers=bearer(token[:-1] + "A"))
        client.get("/v1/me", headers=bearer("kt_1_" + secret))
    assert secret not in caplog.text and token not in caplog.text


def test_the_database_enforces_the_role_and_name_rules_too(migrated_db_url: str) -> None:
    async def poke(engine: Any) -> list[str]:
        problems = []
        for sql in (
            "INSERT INTO users (name) VALUES ('Bad Name')",
            "INSERT INTO api_tokens (user_id, role, token_hash) VALUES (1, 'root', '\\x00')",
        ):
            try:
                async with engine.begin() as conn:
                    await conn.execute(
                        text("INSERT INTO users (name) VALUES ('ok')  ON CONFLICT DO NOTHING")
                    )
                    await conn.execute(text(sql))
                problems.append("accepted: " + sql)
            except Exception:
                pass
        return problems

    assert run(migrated_db_url, poke) == []
