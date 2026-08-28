# tests/test_prefixes.py
from reapi_allowlist.prefixes import is_internal_prefix, to_prefix


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


def test_ipv4_mapped_ipv6_is_unwrapped_to_a_v4_prefix():
    # A dual-stack listener reports IPv4 peers as ::ffff:a.b.c.d. Emitting that
    # as a /128 produces an entry that cannot match the feeder's actual IPv4
    # traffic -- observed live against a real feeder through an haproxy bound
    # v4v6, which yielded "TCP6 ::ffff:192.168.2.131 ...".
    assert to_prefix("::ffff:192.168.2.131") == "192.168.2.131/32"


def test_ipv4_mapped_in_uppercase_and_expanded_form_also_unwraps():
    assert to_prefix("::FFFF:203.0.113.7") == "203.0.113.7/32"
    assert to_prefix("0:0:0:0:0:ffff:cb00:7107") == "203.0.113.7/32"


def test_genuine_ipv6_is_still_a_slash_128():
    assert to_prefix("fdb4:94d8:9df6:bc39:1b3:72cf:3213:75ec") == \
        "fdb4:94d8:9df6:bc39:1b3:72cf:3213:75ec/128"


def test_internal_addresses_are_flagged_not_dropped():
    """A broken PROXY path on the mlat side yields haproxy's own pod IP.

    mlat-server falls back to the socket peer with no error, so that address
    is syntactically valid and enters the set. It stays -- denying it would
    lock out feeders on any deployment that shares a network with the cluster
    -- but it is counted, so the silence is broken.
    """
    for internal in ("10.42.0.10/32", "192.168.1.1/32", "172.16.0.1/32",
                     "127.0.0.1/32", "169.254.1.1/32", "::1/128",
                     "fd00::1/128", "fe80::1/128"):
        assert to_prefix(internal.split("/")[0]) is not None, internal
        assert is_internal_prefix(internal), internal


def test_public_and_cgnat_addresses_are_not_flagged():
    # A feeder behind carrier-grade NAT is an ordinary feeder.
    for ok in ("203.0.113.7/32", "2001:db8::1/128", "100.64.1.1/32"):
        assert not is_internal_prefix(ok), ok
