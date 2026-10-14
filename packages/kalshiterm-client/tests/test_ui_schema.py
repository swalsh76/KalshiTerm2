"""The browser contract: the committed schemas match the code, and no money is a JSON number."""

from typing import Any

from kalshiterm_client.ui import schema


def test_the_committed_schemas_are_what_the_code_now_says() -> None:
    for name, text in schema.render().items():
        committed = (schema.API_DIR / name).read_text(encoding="utf-8")
        assert committed == text, (
            f"{name} is out of date: run `uv run python -m kalshiterm_client.ui.schema` "
            "and commit the result (the front end's types are generated from it)"
        )


def _number_fields(node: Any, field: str = "") -> set[str]:
    """Names of every property whose JSON type is ``number``."""
    found: set[str] = set()
    if isinstance(node, dict):
        if node.get("type") == "number":
            found.add(field)
        for key, value in node.items():
            found |= _number_fields(value, key if key not in _STRUCTURE else field)
        props = node.get("properties")
        if isinstance(props, dict):
            for name, value in props.items():
                found |= _number_fields(value, name)
    elif isinstance(node, list):
        for value in node:
            found |= _number_fields(value, field)
    return found


_STRUCTURE = {"properties", "anyOf", "oneOf", "allOf", "items", "$defs", "components", "schemas"}


def test_no_price_size_or_amount_is_ever_a_json_number() -> None:
    """Decimals cross as strings so JavaScript can never round them. The one ``number`` allowed
    is a duration in seconds."""
    found = _number_fields(schema.openapi()) | _number_fields(schema.websocket())
    assert found <= {"retry_in"}, sorted(found)


def test_the_number_check_has_teeth() -> None:
    assert _number_fields({"properties": {"price": {"type": "number"}}}) == {"price"}
    assert _number_fields(
        {"properties": {"price": {"anyOf": [{"type": "number"}, {"type": "string"}]}}}
    ) == {"price"}


def test_money_fields_are_decimal_strings_with_a_numeric_pattern() -> None:
    spec = schema.openapi()["components"]["schemas"]
    for model, field in (
        ("Trade", "yes_price"),
        ("Trade", "count"),
        ("Bar", "close"),
        ("OrderBook", "best_yes_bid"),
        ("AlertRule", "threshold"),
    ):
        prop = spec[model]["properties"][field]
        options = prop.get("anyOf", [prop])
        assert any(o.get("type") == "string" and "pattern" in o for o in options), (model, field)


def test_every_route_is_under_api_and_documented_once() -> None:
    paths = schema.openapi()["paths"]
    assert all(path.startswith("/api/") for path in paths)
    operations = [(p, m) for p, item in paths.items() for m in item]
    assert len(operations) == len(set(operations)) >= 25


def test_the_websocket_contract_names_every_message_type() -> None:
    contract = schema.websocket()
    server = set(contract["server_to_browser"]["discriminator"]["mapping"])
    client = set(contract["browser_to_server"]["discriminator"]["mapping"])
    assert server == {
        *("hello", "pong", "error", "subscribed", "link"),
        *("resync", "book", "ticker", "trade", "alert"),
    }
    assert client == {"auth", "ping", "subscribe", "unsubscribe", "alerts"}
