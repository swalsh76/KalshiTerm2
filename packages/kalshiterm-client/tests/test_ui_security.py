import asyncio
import json
import logging
import threading
import time
from pathlib import Path

import httpx
import pytest
from kalshiterm_client.ui.app import PLACEHOLDER, create_app
from kalshiterm_client.ui.run import LOOPBACK, bind_loopback, build_url, make_server
from kalshiterm_client.ui.security import (
    CONTENT_SECURITY_POLICY,
    bearer_token,
    new_token,
    token_matches,
)
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

PORT = 51234
TOKEN = new_token()
GOOD = {"Authorization": f"Bearer {TOKEN}"}
GOOD_ORIGIN = f"http://127.0.0.1:{PORT}"


def make_client(assets: Path | None = None, port: int = PORT) -> TestClient:
    app = create_app(port=PORT, token=TOKEN, environment="production", assets_dir=assets)
    return TestClient(app, base_url=f"http://127.0.0.1:{port}")


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    return make_client(tmp_path / "no-assets")


# --- the token ----------------------------------------------------------------------------------


def test_tokens_are_long_random_and_url_safe() -> None:
    tokens = {new_token() for _ in range(50)}
    assert len(tokens) == 50
    assert all(len(t) >= 43 and t.replace("-", "").replace("_", "").isalnum() for t in tokens)


def test_the_url_carries_the_token_in_the_fragment_never_the_query() -> None:
    url = build_url(8123, "abc")
    assert url == "http://127.0.0.1:8123/#token=abc"
    assert "?" not in url


@pytest.mark.parametrize(
    ("presented", "expected", "ok"),
    [
        ("abc", "abc", True),
        ("abd", "abc", False),
        ("ab", "abc", False),
        ("", "abc", False),
        (None, "abc", False),
        ("abcd", "abc", False),
    ],
)
def test_the_token_comparison(presented: str | None, expected: str, ok: bool) -> None:
    assert token_matches(presented, expected) is ok


@pytest.mark.parametrize(
    ("header", "token"),
    [
        ("Bearer abc", "abc"),
        ("bearer abc", "abc"),
        ("Basic abc", None),
        ("Bearer", None),
        ("Bearer a b", None),
        ("abc", None),
        ("", None),
    ],
)
def test_bearer_parsing(header: str, token: str | None) -> None:
    assert bearer_token({"authorization": header}) == token


# --- every /api request needs the token ---------------------------------------------------------


def test_the_right_token_is_accepted(client: TestClient) -> None:
    assert client.get("/api/health", headers=GOOD).json()["status"] == "ok"
    session = client.get("/api/session", headers=GOOD).json()
    assert session["mode"] == "read-only" and session["environment"] == "production"


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer wrong"},
        {"Authorization": f"Bearer {TOKEN[:-1]}"},
        {"Authorization": f"Bearer {TOKEN}x"},
        {"Authorization": f"Basic {TOKEN}"},
        {"Authorization": TOKEN},
        {"Authorization": "Bearer "},
    ],
)
def test_a_missing_or_wrong_token_is_401_on_every_api_path(
    client: TestClient, headers: dict[str, str]
) -> None:
    for path in ("/api/health", "/api/session", "/api/nope", "/api", "/api/ws"):
        assert client.get(path, headers=headers).status_code == 401, path
    post = client.post("/api/session/touch", headers={**headers, "Origin": GOOD_ORIGIN})
    assert post.status_code == 401


def test_the_token_in_the_query_string_or_a_cookie_does_not_count(client: TestClient) -> None:
    assert client.get(f"/api/health?token={TOKEN}").status_code == 401
    assert client.get("/api/health", headers={"Cookie": f"token={TOKEN}"}).status_code == 401


def test_an_unknown_api_path_is_404_only_after_authentication(client: TestClient) -> None:
    assert client.get("/api/nope").status_code == 401  # does not even reveal that it is absent
    assert client.get("/api/nope", headers=GOOD).status_code == 404


# --- DNS rebinding: the Host header ------------------------------------------------------------


@pytest.mark.parametrize(
    "host",
    [
        "attacker.example",
        f"attacker.example:{PORT}",
        f"127.0.0.1:{PORT + 1}",
        "127.0.0.1",
        f"[::1]:{PORT}",
        f"127.0.0.1.attacker.example:{PORT}",
        f"localhost.attacker.example:{PORT}",
    ],
)
def test_a_foreign_host_is_refused_even_with_the_right_token(client: TestClient, host: str) -> None:
    for path in ("/", "/api/health"):
        response = client.get(path, headers={**GOOD, "Host": host})
        assert response.status_code == 400, (host, path)
        assert "data" not in response.text and TOKEN not in response.text


@pytest.mark.parametrize("host", [f"127.0.0.1:{PORT}", f"localhost:{PORT}"])
def test_the_two_loopback_names_on_this_port_are_accepted(client: TestClient, host: str) -> None:
    assert client.get("/api/health", headers={**GOOD, "Host": host}).status_code == 200


# --- cross-site requests: the Origin header -----------------------------------------------------


@pytest.mark.parametrize(
    "origin",
    [
        None,
        "https://attacker.example",
        "null",
        f"http://127.0.0.1:{PORT + 1}",
        f"https://127.0.0.1:{PORT}",
        f"http://127.0.0.1.attacker.example:{PORT}",
        "",
    ],
)
def test_a_state_changing_request_needs_this_exact_origin(
    client: TestClient, origin: str | None
) -> None:
    headers = dict(GOOD)
    if origin is not None:
        headers["Origin"] = origin
    for method in ("post", "put", "patch", "delete"):
        response = getattr(client, method)("/api/session/touch", headers=headers)
        assert response.status_code == 403, (method, origin)


@pytest.mark.parametrize("origin", [f"http://127.0.0.1:{PORT}", f"http://localhost:{PORT}"])
def test_the_right_origin_lets_a_state_changing_request_through(
    client: TestClient, origin: str
) -> None:
    response = client.post("/api/session/touch", headers={**GOOD, "Origin": origin})
    assert response.json() == {"ok": True}


def test_a_foreign_origin_does_not_matter_for_a_plain_read_but_the_token_still_does(
    client: TestClient,
) -> None:
    # reading is protected by the token and the same-origin policy (no CORS headers), not Origin
    assert (
        client.get(
            "/api/health", headers={**GOOD, "Origin": "https://attacker.example"}
        ).status_code
        == 200
    )
    assert (
        client.get("/api/health", headers={"Origin": "https://attacker.example"}).status_code == 401
    )


# --- no CORS, no cookies, hardened responses ----------------------------------------------------


def test_there_are_no_cors_headers_and_a_preflight_gets_nowhere(client: TestClient) -> None:
    preflight = client.options(
        "/api/session/touch",
        headers={
            "Origin": "https://attacker.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization",
        },
    )
    assert preflight.status_code in (401, 403, 405)
    for response in (preflight, client.get("/api/health", headers=GOOD), client.get("/")):
        assert not [h for h in response.headers if h.lower().startswith("access-control-")]
        assert "set-cookie" not in response.headers


def test_responses_forbid_framing_sniffing_referrers_and_caching(client: TestClient) -> None:
    for response in (
        client.get("/"),
        client.get("/api/health", headers=GOOD),
        client.get("/api/health"),
        client.get("/", headers={"Host": "evil.example"}),
    ):
        assert response.headers["x-frame-options"] == "DENY"
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["referrer-policy"] == "no-referrer"
        assert response.headers["content-security-policy"] == CONTENT_SECURITY_POLICY
        assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert client.get("/api/health", headers=GOOD).headers["cache-control"] == "no-store"


def test_there_is_no_interactive_docs_or_schema_endpoint(client: TestClient) -> None:
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code != 200
        assert client.get(path, headers=GOOD).status_code in (401, 404)


# --- the WebSocket ------------------------------------------------------------------------------


def ws_headers(origin: str | None = GOOD_ORIGIN, host: str = f"127.0.0.1:{PORT}") -> dict[str, str]:
    """The test client sends ``Host: testserver`` on WebSockets whatever its base URL says, so the
    Host (and Origin) a real browser would send are set explicitly."""
    headers = {"host": host}
    if origin is not None:
        headers["origin"] = origin
    return headers


def test_the_websocket_authenticates_in_its_first_message(client: TestClient) -> None:
    with client.websocket_connect("/api/ws", headers=ws_headers()) as ws:
        ws.send_json({"type": "auth", "token": TOKEN})
        assert ws.receive_json() == {
            "type": "hello",
            "mode": "read-only",
            "environment": "production",
        }
        ws.send_json({"type": "ping"})
        assert ws.receive_json() == {"type": "pong"}
        ws.send_json({"type": "something-else"})
        assert ws.receive_json()["type"] == "error"


@pytest.mark.parametrize(
    "first",
    [
        {"type": "auth", "token": "wrong"},
        {"type": "auth"},
        {"type": "auth", "token": 5},
        {"type": "ping"},
        {"token": TOKEN},
        {"type": "auth", "token": None},
        [TOKEN],
        "text",
    ],
)
def test_a_bad_first_message_is_closed_with_4401_and_gets_no_data(
    client: TestClient, first: object
) -> None:
    with client.websocket_connect("/api/ws", headers=ws_headers()) as ws:
        ws.send_json(first)
        with pytest.raises(WebSocketDisconnect) as info:
            ws.receive_json()
        assert info.value.code == 4401


def test_a_non_json_first_message_is_closed(client: TestClient) -> None:
    with client.websocket_connect("/api/ws", headers=ws_headers()) as ws:
        ws.send_text("not json")
        with pytest.raises(WebSocketDisconnect) as info:
            ws.receive_json()
        assert info.value.code == 4401


@pytest.mark.parametrize(
    "origin", [None, "https://attacker.example", "null", f"http://127.0.0.1:{PORT + 1}"]
)
def test_a_websocket_from_the_wrong_origin_never_connects(
    client: TestClient, origin: str | None
) -> None:
    with (
        pytest.raises(WebSocketDisconnect) as info,
        client.websocket_connect("/api/ws", headers=ws_headers(origin)),
    ):
        pass
    assert info.value.code == 1008


@pytest.mark.parametrize(
    "host", ["testserver", f"attacker.example:{PORT}", f"127.0.0.1:{PORT + 7}", "127.0.0.1"]
)
def test_a_websocket_to_a_foreign_host_never_connects(client: TestClient, host: str) -> None:
    with (
        pytest.raises(WebSocketDisconnect) as info,
        client.websocket_connect("/api/ws", headers=ws_headers(host=host)),
    ):
        pass
    assert info.value.code == 1008


def test_the_websocket_gives_up_on_a_silent_client(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from kalshiterm_client.ui import app as app_module

    monkeypatch.setattr(app_module, "AUTH_TIMEOUT_SECONDS", 0.2)
    client = make_client(tmp_path / "x")
    with (
        client.websocket_connect("/api/ws", headers=ws_headers()) as ws,
        pytest.raises(WebSocketDisconnect) as info,
    ):
        ws.receive_json()  # we never authenticate
    assert info.value.code == 4401


# --- the page ------------------------------------------------------------------------------------


def test_without_a_built_front_end_the_page_says_so(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200 and response.text == PLACEHOLDER
    assert "read-only" in response.text and "<script" not in response.text  # CSP: no inline script


def test_a_built_front_end_is_served_and_cannot_be_escaped(tmp_path: Path) -> None:
    assets = tmp_path / "ui_assets"
    (assets / "assets").mkdir(parents=True)
    (assets / "index.html").write_text("<!doctype html><title>app</title>")
    (assets / "assets" / "app.js").write_text("console.log(1)")
    (tmp_path / "secret.txt").write_text("do not serve")
    client = make_client(assets)
    assert "<title>app</title>" in client.get("/").text
    js = client.get("/assets/app.js")
    assert js.status_code == 200 and "javascript" in js.headers["content-type"]
    for path in (
        "/../secret.txt",
        "/%2e%2e/secret.txt",
        "/assets/../../secret.txt",
        "/..%2fsecret.txt",
        "/assets/%2e%2e/%2e%2e/secret.txt",
    ):
        response = client.get(path)
        assert "do not serve" not in response.text, path


# --- the real server ----------------------------------------------------------------------------


def test_the_socket_is_loopback_only() -> None:
    sock = bind_loopback(0)
    try:
        host, port = sock.getsockname()
        assert host == LOOPBACK == "127.0.0.1" and port > 0
    finally:
        sock.close()


def test_a_real_server_answers_on_loopback_and_refuses_a_rebinding_host(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    sock = bind_loopback(0)
    port = sock.getsockname()[1]
    token = new_token()
    server = make_server(sock, token, "demo", tmp_path / "none")
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.05)
        assert server.started
        base = f"http://127.0.0.1:{port}"
        auth = {"Authorization": f"Bearer {token}"}
        assert httpx.get(f"{base}/api/health", headers=auth).status_code == 200
        assert httpx.get(f"{base}/api/health").status_code == 401
        rebinding = httpx.get(
            f"{base}/api/health", headers={**auth, "Host": f"attacker.example:{port}"}
        )
        assert rebinding.status_code == 400
        wrong_origin = httpx.post(
            f"{base}/api/session/touch", headers={**auth, "Origin": "https://attacker.example"}
        )
        assert wrong_origin.status_code == 403
        assert "server" not in {h.lower() for h in httpx.get(f"{base}/").headers}  # no banner
        assert not [
            r for r in caplog.records if r.name.startswith("uvicorn.access")
        ]  # no request log
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        sock.close()


async def test_a_real_websocket_over_tcp_authenticates_and_a_wrong_origin_is_refused(
    tmp_path: Path,
) -> None:
    import websockets
    from websockets.exceptions import InvalidStatus
    from websockets.typing import Origin

    sock = bind_loopback(0)
    port = sock.getsockname()[1]
    token = new_token()
    server = make_server(sock, token, "demo", tmp_path / "none")
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.05)
        url = f"ws://127.0.0.1:{port}/api/ws"
        async with websockets.connect(url, origin=Origin(f"http://127.0.0.1:{port}")) as ws:
            await ws.send(json.dumps({"type": "auth", "token": token}))
            assert '"hello"' in await ws.recv()
        with pytest.raises(InvalidStatus) as info:
            async with websockets.connect(url, origin=Origin("https://attacker.example")):
                pass
        assert info.value.response.status_code == 403
        with pytest.raises(InvalidStatus):  # no Origin header at all
            async with websockets.connect(url, origin=None):
                pass
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        sock.close()


def test_the_server_is_configured_to_log_no_requests_and_announce_nothing(tmp_path: Path) -> None:
    sock = bind_loopback(0)
    try:
        config = make_server(sock, new_token(), "demo", tmp_path / "none").config
        assert config.access_log is False and config.server_header is False
        assert config.date_header is False and config.log_level == "warning"
        assert config.ws_max_size == 65_536  # control messages only
    finally:
        sock.close()


def test_the_token_never_reaches_a_log(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    caplog.set_level(logging.DEBUG)
    client = make_client(tmp_path / "x")
    client.get("/api/health", headers=GOOD)
    client.get("/api/health", headers={"Authorization": "Bearer wrong-token-value"})
    with client.websocket_connect("/api/ws", headers=ws_headers()) as ws:
        ws.send_json({"type": "auth", "token": TOKEN})
        ws.receive_json()
    assert TOKEN not in caplog.text and "wrong-token-value" not in caplog.text
