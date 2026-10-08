import hashlib

import pytest
from kalshiterm_server.auth import (
    MAX_HEADER,
    USER_NAME,
    FailureThrottle,
    Principal,
    bearer_token,
    format_token,
    hash_secret,
    new_secret,
    parse_token,
)


def test_secrets_are_long_random_and_never_repeat() -> None:
    secrets_ = {new_secret() for _ in range(2_000)}
    assert len(secrets_) == 2_000
    assert all(len(s) == 43 for s in secrets_)  # 256 bits, URL-safe base64


def test_a_token_round_trips_and_only_its_hash_is_derivable() -> None:
    secret = new_secret()
    token = format_token(17, secret)
    assert token == f"kt_17_{secret}" and parse_token(token) == (17, secret)
    digest = hash_secret(secret)
    assert digest == hashlib.sha256(secret.encode()).digest() and len(digest) == 32
    assert secret.encode() not in digest and token not in repr(digest)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "kt_",
        "kt_1_",
        "kt__" + "a" * 43,
        "kt_1_" + "a" * 42,  # one character short
        "kt_1_" + "a" * 44,  # one too long
        "kt_-1_" + "a" * 43,
        "kt_1234567890_" + "a" * 43,  # id longer than 9 digits
        "kt_١٢_" + "a" * 43,  # non-ASCII digits
        "xx_1_" + "a" * 43,
        "kt_1_" + "a" * 42 + "!",
        " kt_1_" + "a" * 43,
        "kt_1_" + "a" * 43 + "\n",
        "kt_1_" + "a" * 43 + " extra",
    ],
)
def test_malformed_tokens_are_rejected_before_any_database_work(bad: str) -> None:
    assert parse_token(bad) is None


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("Bearer abc", "abc"),
        ("bearer abc", "abc"),
        ("BEARER   abc  ", "abc"),
        ("Basic abc", None),
        ("Bearer", None),
        ("Bearer ", None),
        ("abc", None),
        ("", None),
        (None, None),
        ("Bearer " + "a" * MAX_HEADER, None),  # absurdly long headers are not even parsed
    ],
)
def test_only_a_bearer_header_yields_a_token(header: str | None, expected: str | None) -> None:
    assert bearer_token(header) == expected


@pytest.mark.parametrize("name", ["alice", "bob.smith", "a", "x-y_z.9", "a" * 63])
def test_reasonable_user_names_are_accepted(name: str) -> None:
    assert USER_NAME.fullmatch(name)


@pytest.mark.parametrize(
    "name", ["", "Alice", "1abc", "-a", "a b", "a/b", "a;b", "a\nb", "é", "a" * 64, "a'b"]
)
def test_other_user_names_are_rejected(name: str) -> None:
    assert not USER_NAME.fullmatch(name)


def test_a_principal_carries_no_token_material() -> None:
    who = Principal(user_id=3, user="alice", role="read", token_id=9)
    assert "kt_" not in repr(who) and not hasattr(who, "token")


class Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


def test_an_address_is_locked_out_after_too_many_failures_and_released_after_the_window() -> None:
    clock = Clock()
    throttle = FailureThrottle(max_failures=3, window=60, clock=clock)
    for _ in range(2):
        throttle.record_failure("10.0.0.1")
    assert throttle.retry_after("10.0.0.1") is None  # two failures: still allowed
    throttle.record_failure("10.0.0.1")
    wait = throttle.retry_after("10.0.0.1")
    assert wait is not None and 1 <= wait <= 61
    assert throttle.retry_after("10.0.0.2") is None  # other addresses are unaffected
    clock.now += 61
    assert throttle.retry_after("10.0.0.1") is None  # the window has passed


def test_failures_age_out_one_by_one_so_a_slow_trickle_never_locks_anyone() -> None:
    clock = Clock()
    throttle = FailureThrottle(max_failures=3, window=60, clock=clock)
    for _ in range(20):  # one failure every 30 seconds: never three inside a minute
        throttle.record_failure("10.0.0.1")
        assert throttle.retry_after("10.0.0.1") is None
        clock.now += 30


def test_the_throttle_cannot_be_made_to_grow_without_limit() -> None:
    throttle = FailureThrottle(max_failures=3, window=60, max_addresses=100)
    for i in range(5_000):
        throttle.record_failure(f"198.51.100.{i}")
    assert len(throttle._failures) <= 100  # noqa: SLF001
