import json
import os
import stat
from pathlib import Path

import pytest
from kalshiterm_client.profiles import (
    ProfileError,
    ProfileStore,
    check_name,
    check_token,
    normalise_url,
    profiles_path,
)

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX permissions")


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("https://mac-studio:8700", "https://mac-studio:8700"),
        ("  HTTPS://Mac-Studio:8700/ ", "https://mac-studio:8700"),
        ("https://192.168.1.20:8700", "https://192.168.1.20:8700"),
        ("https://[fd00::1]:8700", "https://[fd00::1]:8700"),
        ("https://studio.local", "https://studio.local"),
        ("http://127.0.0.1:8700", "http://127.0.0.1:8700"),  # this machine only
        ("http://localhost:8700", "http://localhost:8700"),
        ("http://[::1]:8700", "http://[::1]:8700"),
    ],
)
def test_addresses_are_normalised(given: str, expected: str) -> None:
    assert normalise_url(given) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "http://mac-studio:8700",  # a token would cross the LAN in the clear
        "http://192.168.1.20:8700",
        "ftp://host",
        "mac-studio:8700",
        "https://",
        "https://host:8700/v1",
        "https://user:pw@host:8700",
        "https://host:notaport",
        "https://host?x=1",
    ],
)
def test_unsafe_or_malformed_addresses_are_refused(bad: str) -> None:
    with pytest.raises(ProfileError):
        normalise_url(bad)


def test_names_and_token_shapes_are_checked() -> None:
    assert check_name("mac-studio_2") == "mac-studio_2"
    for bad in ("", "Bad", "-x", "a b", "x" * 33, "../etc"):
        with pytest.raises(ProfileError):
            check_name(bad)
    assert check_token("kt_12_" + "A" * 43)
    for bad in ("", "kt_x_abc", "kt_1_short", "Bearer kt_1_" + "A" * 43):
        with pytest.raises(ProfileError):
            check_token(bad)


def test_the_first_profile_becomes_the_default_and_later_ones_do_not() -> None:
    store = ProfileStore()
    assert store.names() == [] and store.default_name() is None
    store.add("home", "https://studio:8700")
    store.add("lab", "https://lab:8700")
    assert store.names() == ["home", "lab"] and store.default_name() == "home"
    store.use("lab")
    assert store.resolve().name == "lab"


def test_adding_an_existing_name_needs_replace() -> None:
    store = ProfileStore()
    store.add("home", "https://a:8700")
    with pytest.raises(ProfileError, match="already exists"):
        store.add("home", "https://b:8700")
    store.add("home", "https://b:8700", replace=True)
    assert store.get("home").url == "https://b:8700"


def test_removing_the_default_picks_another_or_none() -> None:
    store = ProfileStore()
    store.add("a", "https://a:8700")
    store.add("b", "https://b:8700")
    store.remove("a")
    assert store.default_name() == "b"
    store.remove("b")
    assert store.default_name() is None and store.names() == []
    with pytest.raises(ProfileError):
        store.remove("b")


def test_resolution_order_is_argument_then_environment_then_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ProfileStore()
    for name in ("a", "b", "c"):
        store.add(name, f"https://{name}:8700")
    assert store.resolve().name == "a"
    monkeypatch.setenv("KTERM_PROFILE", "b")
    assert store.resolve().name == "b"
    assert store.resolve("c").name == "c"


def test_unknown_profile_and_empty_store_give_helpful_errors() -> None:
    store = ProfileStore()
    with pytest.raises(ProfileError, match="no server configured"):
        store.resolve()
    store.add("home", "https://a:8700")
    with pytest.raises(ProfileError, match=r"no profile named 'nope' \(have: home\)"):
        store.resolve("nope")


def test_a_corrupt_file_is_reported_not_overwritten() -> None:
    path = profiles_path()
    path.parent.mkdir(parents=True)
    path.write_text("{not json")
    with pytest.raises(ProfileError, match="cannot read"):
        ProfileStore().add("home", "https://a:8700")
    assert path.read_text() == "{not json"
    path.write_text('["a list"]')
    with pytest.raises(ProfileError, match="not a profile file"):
        ProfileStore().names()


@posix_only
def test_the_file_is_private_and_leaves_no_temporary_files() -> None:
    ProfileStore().add("home", "https://a:8700")
    path = profiles_path()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert [p.name for p in path.parent.iterdir()] == ["profiles.json"]
    assert json.loads(path.read_text()) == {
        "default": "home",
        "profiles": {"home": {"url": "https://a:8700"}},
    }


def test_the_config_directory_can_be_overridden_for_tests_and_portable_installs(
    tmp_path: Path,
) -> None:
    assert profiles_path() == tmp_path / "config" / "profiles.json"
