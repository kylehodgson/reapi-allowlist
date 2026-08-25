# src/reapi_allowlist/prefixes.py
"""Normalise bare IP addresses into the CIDR form Cilium expects."""

import ipaddress

_SUFFIX = {4: "/32", 6: "/128"}


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
