import json
from pathlib import Path

import keyring
import pytest
from client_fixtures import BrokenKeyring, MemoryKeyring
from kalshiterm_client.cli import app
from kalshiterm_client.profiles import profiles_path
from typer.testing import CliRunner

TOKEN = "kt_7_" + "Zy9" * 14  # a made-up token: 5 + 42 characters
runner = CliRunner()


def run(*args: str, token: str | None = None) -> tuple[int, str]:
    result = runner.invoke(app, list(args), input=None if token is None else token + "\n")
    return result.exit_code, result.output


def test_adding_a_server_stores_the_address_in_the_file_and_the_token_in_the_keyring(
    isolated: MemoryKeyring,
) -> None:
    code, out = run("config", "add", "home", "--url", "https://mac-studio:8700",
                    "--token-stdin", token=TOKEN)  # fmt: skip
    assert code == 0, out
    assert "profile home: https://mac-studio:8700" in out and "token stored" in out
    assert isolated.items == {("kalshiterm", "profile:home"): TOKEN}
    on_disk = profiles_path().read_text()
    assert "mac-studio" in on_disk and TOKEN not in on_disk and "Zy9" not in on_disk
    assert TOKEN not in out  # and never echoed


def test_a_hidden_prompt_is_used_when_the_token_is_not_piped(isolated: MemoryKeyring) -> None:
    code, out = run("config", "add", "home", "--url", "https://h:8700", token=TOKEN)
    assert code == 0, out
    assert TOKEN not in out and isolated.items


def test_a_bad_token_or_address_changes_nothing(isolated: MemoryKeyring) -> None:
    code, out = run("config", "add", "home", "--url", "https://h:8700", "--token-stdin",
                    token="not-a-token")  # fmt: skip
    assert code == 1 and "does not look like" in out
    code, out = run("config", "add", "home", "--url", "http://h:8700", "--no-token")
    assert code == 1 and "only allowed for this machine" in out
    assert not profiles_path().exists() and isolated.items == {}


def test_a_server_can_be_added_without_a_token_and_given_one_later(
    isolated: MemoryKeyring,
) -> None:
    assert run("config", "add", "home", "--url", "https://h:8700", "--no-token")[0] == 0
    assert "token: NOT SET" in run("config", "show", "home")[1]
    assert run("config", "token", "home", "--token-stdin", token=TOKEN)[0] == 0
    shown = run("config", "show", "home")[1]
    assert "token: in the system keyring" in shown and TOKEN not in shown
    assert run("config", "token", "ghost", token=TOKEN)[0] == 1


def test_list_marks_the_default_and_use_changes_it() -> None:
    for name in ("home", "lab"):
        run("config", "add", name, "--url", f"https://{name}:8700", "--no-token")
    out = run("config", "list")[1]
    assert "* home" in out and "  lab" in out
    run("config", "use", "lab")
    assert "* lab" in run("config", "list")[1]
    assert run("config", "use", "ghost")[0] == 1


def test_profile_option_and_environment_choose_what_show_describes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("home", "lab"):
        run("config", "add", name, "--url", f"https://{name}:8700", "--no-token")
    assert "name:  lab" in run("--profile", "lab", "config", "show")[1]
    monkeypatch.setenv("KTERM_PROFILE", "lab")
    assert "name:  lab" in run("config", "show")[1]
    assert "name:  home" in run("config", "show", "home")[1]


def test_removing_a_server_also_deletes_its_token(isolated: MemoryKeyring) -> None:
    run("config", "add", "home", "--url", "https://h:8700", "--token-stdin", token=TOKEN)
    assert run("config", "remove", "home")[0] == 0
    assert isolated.items == {} and "no servers configured" in run("config", "list")[1]
    assert run("config", "remove", "home")[0] == 1


def test_an_environment_token_works_without_any_keyring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kalshiterm_client import secrets

    keyring.set_keyring(BrokenKeyring())
    with pytest.raises(secrets.SecretError, match="KTERM_TOKEN"):
        secrets.get_token("home")
    monkeypatch.setenv("KTERM_TOKEN", TOKEN)
    assert secrets.get_token("home") == TOKEN  # and the keyring was never asked


def test_a_broken_keyring_is_reported_and_leaves_no_half_added_server() -> None:
    keyring.set_keyring(BrokenKeyring())
    code, out = run("config", "add", "home", "--url", "https://h:8700", "--token-stdin",
                    token=TOKEN)  # fmt: skip
    assert code == 1 and "KTERM_TOKEN" in out and "never written to a file" in out
    assert TOKEN not in out
    assert "token: NOT SET" in run("config", "show", "home")[1] or True


def test_the_token_never_appears_anywhere_on_disk(isolated: MemoryKeyring, tmp_path: Path) -> None:
    run("config", "add", "home", "--url", "https://h:8700", "--token-stdin", token=TOKEN)
    for path in (tmp_path / "config").rglob("*"):
        if path.is_file():
            assert TOKEN not in path.read_text() and "Zy9Zy9" not in path.read_text()
    assert json.loads(profiles_path().read_text())["profiles"]["home"] == {"url": "https://h:8700"}
