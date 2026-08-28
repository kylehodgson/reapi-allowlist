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


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict
mutants_x_is_internal_prefix__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_is_internal_prefix__mutmut)
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


def x_is_internal_prefix__mutmut_orig(prefix: str) -> bool:
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


def x_is_internal_prefix__mutmut_1(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
    """
    try:
        addr = None
    except ValueError:
        return False
    if addr.is_multicast or addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_2(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
    """
    try:
        addr = ipaddress.ip_address(None)
    except ValueError:
        return False
    if addr.is_multicast or addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_3(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
    """
    try:
        addr = ipaddress.ip_address(prefix.split(None)[0])
    except ValueError:
        return False
    if addr.is_multicast or addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_4(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
    """
    try:
        addr = ipaddress.ip_address(prefix.split("XX/XX")[0])
    except ValueError:
        return False
    if addr.is_multicast or addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_5(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
    """
    try:
        addr = ipaddress.ip_address(prefix.split("/")[1])
    except ValueError:
        return False
    if addr.is_multicast or addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_6(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
    """
    try:
        addr = ipaddress.ip_address(prefix.split("/")[0])
    except ValueError:
        return True
    if addr.is_multicast or addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_7(prefix: str) -> bool:
    """True for a prefix that cannot be an internet feeder's source address.

    Reported, never rejected: where feeders and cluster share a network,
    RFC 1918 and ULA are what a real feeder looks like, and denying them
    would lock out everyone.
    """
    try:
        addr = ipaddress.ip_address(prefix.split("/")[0])
    except ValueError:
        return False
    if addr.is_multicast and addr.is_unspecified:
        return True
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_8(prefix: str) -> bool:
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
        return False
    return any(addr in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_9(prefix: str) -> bool:
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
    return any(None)


def x_is_internal_prefix__mutmut_10(prefix: str) -> bool:
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
    return any(addr not in net for net in _BOGUS if net.version == addr.version)


def x_is_internal_prefix__mutmut_11(prefix: str) -> bool:
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
    return any(addr in net for net in _BOGUS if net.version != addr.version)

mutants_x_is_internal_prefix__mutmut['_mutmut_orig'] = x_is_internal_prefix__mutmut_orig # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_1'] = x_is_internal_prefix__mutmut_1 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_2'] = x_is_internal_prefix__mutmut_2 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_3'] = x_is_internal_prefix__mutmut_3 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_4'] = x_is_internal_prefix__mutmut_4 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_5'] = x_is_internal_prefix__mutmut_5 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_6'] = x_is_internal_prefix__mutmut_6 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_7'] = x_is_internal_prefix__mutmut_7 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_8'] = x_is_internal_prefix__mutmut_8 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_9'] = x_is_internal_prefix__mutmut_9 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_10'] = x_is_internal_prefix__mutmut_10 # type: ignore # mutmut generated
mutants_x_is_internal_prefix__mutmut['x_is_internal_prefix__mutmut_11'] = x_is_internal_prefix__mutmut_11 # type: ignore # mutmut generated
mutants_x_to_prefix__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_to_prefix__mutmut)
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


def x_to_prefix__mutmut_orig(value: str) -> str | None:
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


def x_to_prefix__mutmut_1(value: str) -> str | None:
    """Return `value` as a single-host CIDR, or None if it is not a bare address.

    Cilium's CIDR fields validate as `format: cidr`, so bare addresses are
    rejected by the API server. Sources give us bare addresses, so every one
    must be suffixed before it goes anywhere near a manifest.
    """
    try:
        addr = None
    except ValueError:
        return None

    # ::ffff:a.b.c.d as a /128 cannot match the feeder's actual IPv4 traffic.
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped

    return f"{addr.compressed}{_SUFFIX[addr.version]}"


def x_to_prefix__mutmut_2(value: str) -> str | None:
    """Return `value` as a single-host CIDR, or None if it is not a bare address.

    Cilium's CIDR fields validate as `format: cidr`, so bare addresses are
    rejected by the API server. Sources give us bare addresses, so every one
    must be suffixed before it goes anywhere near a manifest.
    """
    try:
        addr = ipaddress.ip_address(None)
    except ValueError:
        return None

    # ::ffff:a.b.c.d as a /128 cannot match the feeder's actual IPv4 traffic.
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped

    return f"{addr.compressed}{_SUFFIX[addr.version]}"


def x_to_prefix__mutmut_3(value: str) -> str | None:
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
    if isinstance(addr, ipaddress.IPv6Address) or addr.ipv4_mapped:
        addr = addr.ipv4_mapped

    return f"{addr.compressed}{_SUFFIX[addr.version]}"


def x_to_prefix__mutmut_4(value: str) -> str | None:
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
        addr = None

    return f"{addr.compressed}{_SUFFIX[addr.version]}"

mutants_x_to_prefix__mutmut['_mutmut_orig'] = x_to_prefix__mutmut_orig # type: ignore # mutmut generated
mutants_x_to_prefix__mutmut['x_to_prefix__mutmut_1'] = x_to_prefix__mutmut_1 # type: ignore # mutmut generated
mutants_x_to_prefix__mutmut['x_to_prefix__mutmut_2'] = x_to_prefix__mutmut_2 # type: ignore # mutmut generated
mutants_x_to_prefix__mutmut['x_to_prefix__mutmut_3'] = x_to_prefix__mutmut_3 # type: ignore # mutmut generated
mutants_x_to_prefix__mutmut['x_to_prefix__mutmut_4'] = x_to_prefix__mutmut_4 # type: ignore # mutmut generated
