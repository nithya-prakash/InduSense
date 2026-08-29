from starlette.requests import Request

from pagination import DEFAULT_LIMIT, MAX_LIMIT, parse_limit_offset


def _make_request(query_string: str) -> Request:
    scope = {
        "type": "http", "method": "GET", "path": "/",
        "query_string": query_string.encode(), "headers": [], "client": ("127.0.0.1", 1),
    }
    return Request(scope)


def test_parse_limit_offset_defaults():
    limit, offset = parse_limit_offset(_make_request(""))
    assert limit == DEFAULT_LIMIT
    assert offset == 0


def test_parse_limit_offset_clamps_to_max():
    limit, _ = parse_limit_offset(_make_request("limit=99999"))
    assert limit == MAX_LIMIT


def test_parse_limit_offset_ignores_invalid_values():
    limit, offset = parse_limit_offset(_make_request("limit=-5&offset=-1"))
    assert limit == DEFAULT_LIMIT
    assert offset == 0


def test_parse_limit_offset_respects_valid_values():
    limit, offset = parse_limit_offset(_make_request("limit=5&offset=10"))
    assert limit == 5
    assert offset == 10
