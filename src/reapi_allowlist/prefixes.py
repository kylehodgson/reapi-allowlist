# src/reapi_allowlist/prefixes.py
"""Normalise bare IP addresses into the CIDR form Cilium expects."""

import ipaddress

_SUFFIX = {4: "/32", 6: "/128"}

# Ranges that cannot be a real feeder's source address arriving from the
# internet. Deliberately an explicit list rather than `is_global` or
# `is_private`: those also exclude carrier-grade NAT (a feeder really can
# arrive from 100.64/10) and the documentation ranges used in tests and lab
# work, neither of which indicates anything wrong.
_BOGUS = tuple(
    ipaddress.ip_network(n)
    for n in (
        "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",  # RFC 1918
        "127.0.0.0/8", "169.254.0.0/16",                   # loopback, link-local
        "::1/128", "fc00::/7", "fe80::/10",                # v6 equivalents
    )
)


def is_internal_prefix(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected. mlat-server falls back to the socket peer when
    no PROXY line arrives (`jsonclient.py`), so a broken PROXY path yields
    haproxy's own pod IP -- syntactically valid, and it would otherwise enter
    the allowlist with nothing to distinguish it. readsb's equivalent failure
    produces the literal "port" and is already counted as an anomaly.

    Not a rejection, because a private address is not always wrong: where the
    feeders and the cluster share a network -- any lab, and some real
    deployments -- RFC 1918 and ULA are exactly what a feeder looks like.
    Denying them would lock out every feeder in that setup. The failure this
    guards is silence, so the answer is a metric, not a refusal.
    """
    try:
        addr = ipaddress.ip_address(prefix.split("/")[0])
    except ValueError:
        return False
    if addr.is_multicast or addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def to_prefix(value: str) -> str | None:
    """Return `value` as a single-host CIDR, or None if it is not a bare address.

    Cilium's CIDR fields validate as `format: cidr`, so bare addresses are
    rejected by the API server. Sources give us bare addresses, so every one
    must be suffixed before it goes anywhere near a manifest.
    """
    try:
        addr = ipaddress.ip_address(value.strip())
    except ValueError:
        return None

    # A dual-stack listener reports IPv4 peers as ::ffff:a.b.c.d, and haproxy
    # passes that straight through in the PROXY header ("TCP6 ::ffff:1.2.3.4").
    # Emitting it as a /128 yields an entry that cannot match the feeder's
    # actual IPv4 traffic, so a live feeder would be denied. Unwrap it.
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped

    return f"{addr.compressed}{_SUFFIX[addr.version]}"
