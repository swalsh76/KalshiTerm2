import pytest
from fastapi.testclient import TestClient
from kalshiterm_server.api.app import create_app
from kalshiterm_server.auth import parse_token
from kalshiterm_server.cli import app as cli
from kalshiterm_server.config import ServerSettings
from typer.testing import CliRunner

pytestmark = pytest.mark.db


@pytest.fixture
def runner(migrated_db_url: str, monkeypatch: pytest.MonkeyPatch) -> CliRunner:
    monkeypatch.setenv("KTERM_DB_URL", migrated_db_url)
    return CliRunner()


def test_users_can_be_added_listed_and_rejected_when_invalid_or_duplicate(
    runner: CliRunner,
) -> None:
    assert runner.invoke(cli, ["user", "add", "alice"]).exit_code == 0
    dup = runner.invoke(cli, ["user", "add", "alice"])
    assert dup.exit_code == 1 and "already exists" in dup.output
    bad = runner.invoke(cli, ["user", "add", "Bad Name"])
    assert bad.exit_code == 1 and "lowercase" in bad.output
    listing = runner.invoke(cli, ["user", "list"])
    assert "alice" in listing.output and "0 active token(s)" in listing.output


def test_a_token_is_printed_once_alone_on_standard_output(runner: CliRunner) -> None:
    runner.invoke(cli, ["user", "add", "alice"])
    result = runner.invoke(cli, ["token", "create", "--user", "alice", "--role", "admin"])
    assert result.exit_code == 0
    token = result.stdout.strip()
    assert "\n" not in token and parse_token(token) is not None  # nothing else on stdout
    assert "cannot be shown again" in result.stderr and token not in result.stderr
    listing = runner.invoke(cli, ["token", "list"])
    secret = token.split("_", 2)[2]
    assert secret not in listing.output and token not in listing.output  # list shows no secret
    assert "alice" in listing.output and "admin" in listing.output and "active" in listing.output


def test_token_creation_rejects_unknown_users_and_roles(runner: CliRunner) -> None:
    missing = runner.invoke(cli, ["token", "create", "--user", "ghost"])
    assert missing.exit_code == 1 and "no such user" in missing.output
    runner.invoke(cli, ["user", "add", "alice"])
    bad = runner.invoke(cli, ["token", "create", "--user", "alice", "--role", "root"])
    assert bad.exit_code == 1 and "role must be one of" in bad.output


def test_a_cli_issued_token_works_against_the_api_until_it_is_revoked(
    runner: CliRunner, migrated_db_url: str
) -> None:
    runner.invoke(cli, ["user", "add", "alice"])
    token = runner.invoke(cli, ["token", "create", "--user", "alice"]).stdout.strip()
    api = create_app(ServerSettings(db_url=migrated_db_url))
    headers = {"Authorization": f"Bearer {token}"}
    with TestClient(api) as client:
        assert client.get("/v1/me", headers=headers).json()["user"] == "alice"
        token_id = int(token.split("_")[1])
        assert runner.invoke(cli, ["token", "revoke", str(token_id)]).exit_code == 0
        assert client.get("/v1/me", headers=headers).status_code == 401  # effective at once
    again = runner.invoke(cli, ["token", "revoke", str(token_id)])
    assert again.exit_code == 1 and "already revoked" in again.output
    assert "revoked" in runner.invoke(cli, ["token", "list", "--user", "alice"]).output


def test_removing_a_user_asks_first_and_takes_their_tokens_with_them(
    runner: CliRunner, migrated_db_url: str
) -> None:
    runner.invoke(cli, ["user", "add", "alice"])
    token = runner.invoke(cli, ["token", "create", "--user", "alice"]).stdout.strip()
    declined = runner.invoke(cli, ["user", "remove", "alice"], input="n\n")
    assert declined.exit_code != 0 and "alice" in runner.invoke(cli, ["user", "list"]).output
    done = runner.invoke(cli, ["user", "remove", "alice", "--yes"])
    assert done.exit_code == 0 and "1 token(s)" in done.output
    assert "(no tokens)" in runner.invoke(cli, ["token", "list"]).output
    api = create_app(ServerSettings(db_url=migrated_db_url))
    with TestClient(api) as client:
        assert client.get("/v1/me", headers={"Authorization": f"Bearer {token}"}).status_code == 401
    assert runner.invoke(cli, ["user", "remove", "ghost", "--yes"]).exit_code == 1


def test_a_token_can_be_given_a_lifetime(runner: CliRunner) -> None:
    runner.invoke(cli, ["user", "add", "alice"])
    runner.invoke(
        cli, ["token", "create", "--user", "alice", "--expires-days", "30", "--label", "laptop"]
    )
    listing = runner.invoke(cli, ["token", "list"]).output
    assert "laptop" in listing and "no expiry" not in listing
