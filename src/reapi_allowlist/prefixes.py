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
    return f"{addr.compressed}{_SUFFIX[addr.version]}"
