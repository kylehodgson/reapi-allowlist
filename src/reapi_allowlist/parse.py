"""Turn readsb and mlat-server clients.json payloads into prefix sets."""

from dataclasses import dataclass, field

from .prefixes import to_prefix


@dataclass(frozen=True)
class ParseResult:
    prefixes: set[str] = field(default_factory=set)
    anomalies: int = 0


def parse_readsb_clients(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for entry in payload.get("clients") or []:
        if not isinstance(entry, list) or len(entry) < 2:
            anomalies += 1
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def parse_mlat_clients(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for client in (payload or {}).values():
        if not isinstance(client, dict):
            anomalies += 1
            continue
        prefix = to_prefix(str(client.get("source_ip", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)
