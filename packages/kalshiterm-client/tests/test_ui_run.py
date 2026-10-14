"""Starting the UI backend against whatever server setup the user has (or has not)."""

import asyncio

import pytest
from kalshiterm_client import secrets
from kalshiterm_client.profiles import ProfileStore
from kalshiterm_client.stream import StreamClient
from kalshiterm_client.ui.run import connect_server


def test_with_no_profile_the_ui_still_starts_and_says_why_there_is_no_data() -> None:
    data, factory = connect_server()
    assert data.client is None and factory is None
    assert data.profile is None and data.reason  # e.g. "no servers configured..."


def test_a_profile_without_a_token_names_the_command_that_fixes_it() -> None:
    ProfileStore().add("lan", "http://127.0.0.1:8700")
    data, factory = connect_server()
    assert data.client is None and factory is None
    assert data.profile == "lan" and "kterm config token lan" in data.reason


async def test_a_full_profile_gives_a_client_and_a_stream_factory() -> None:
    ProfileStore().add("lan", "http://127.0.0.1:8700")
    secrets.set_token("lan", "kt_7_" + "Zy9" * 14)
    data, factory = connect_server()
    try:
        assert data.client is not None and data.owns_client
        assert data.profile == "lan" and data.url == "http://127.0.0.1:8700"
        assert factory is not None
        stream = factory(["ticker", "alerts"], ["KXA-1"])
        assert isinstance(stream, StreamClient)
    finally:
        await data.close()


async def test_the_app_closes_a_client_it_owns_but_not_one_it_was_lent(
    tmp_path: pytest.TempPathFactory,
) -> None:
    from kalshiterm_client.client import ServerClient
    from kalshiterm_client.ui.app import create_app
    from kalshiterm_client.ui.data_routes import DataAccess
    from starlette.testclient import TestClient

    closed: list[str] = []

    class Spy(ServerClient):
        def __init__(self, label: str) -> None:
            super().__init__("http://127.0.0.1:1", "t")
            self.label = label

        async def aclose(self) -> None:
            closed.append(self.label)
            await super().aclose()

    owned, lent = Spy("owned"), Spy("lent")
    for client, owns in ((owned, True), (lent, False)):
        app = create_app(
            port=1, token="t", environment="demo", data=DataAccess(client, owns_client=owns)
        )
        with TestClient(app):
            pass
    assert closed == ["owned"]
    await asyncio.sleep(0)
    await lent.aclose()
