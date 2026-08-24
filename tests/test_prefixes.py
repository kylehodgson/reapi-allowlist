# tests/test_prefixes.py
from reapi_allowlist.prefixes import to_prefix


def test_ipv4_gets_slash_32():
    assert to_prefix("203.0.113.7") == "203.0.113.7/32"


def test_ipv6_gets_slash_128():
    assert to_prefix("2001:db8::1") == "2001:db8::1/128"


def test_ipv6_is_normalised_to_compressed_form():
    assert to_prefix("2001:0db8:0000:0000:0000:0000:0000:0001") == "2001:db8::1/128"


def test_already_prefixed_input_is_rejected():
    # Sources emit bare addresses. A prefix here means we misparsed something.
    assert to_prefix("203.0.113.0/24") is None


def test_garbage_returns_none():
    assert to_prefix("port") is None
    assert to_prefix("") is None
    assert to_prefix("not-an-address") is None
