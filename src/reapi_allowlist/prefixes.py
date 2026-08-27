"""Normalise bare IP addresses into the CIDR form Cilium expects."""

import ipaddress

_SUFFIX = {4: "/32", 6: "/128"}

# Explicit, not is_private/is_global: those also catch carrier-grade NAT and
# the documentation ranges, neither of which indicates anything wrong.
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

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
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

    # ::ffff:a.b.c.d as a /128 cannot match the feeder's actual IPv4 traffic.
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped

    return f"{addr.compressed}{_SUFFIX[addr.version]}"
