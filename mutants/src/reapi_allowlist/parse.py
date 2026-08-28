"""Turn readsb and mlat-server clients.json payloads into prefix sets."""

from dataclasses import dataclass, field

from .prefixes import to_prefix


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict


@dataclass(frozen=True)
class ParseResult:
    prefixes: set[str] = field(default_factory=set)
    anomalies: int = 0
mutants_x_parse_readsb_clients__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_parse_readsb_clients__mutmut)
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


def x_parse_readsb_clients__mutmut_orig(payload: dict) -> ParseResult:
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


def x_parse_readsb_clients__mutmut_1(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = None
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


def x_parse_readsb_clients__mutmut_2(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = set()
    anomalies = None
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


def x_parse_readsb_clients__mutmut_3(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = set()
    anomalies = 1
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


def x_parse_readsb_clients__mutmut_4(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for entry in payload.get("clients") and []:
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


def x_parse_readsb_clients__mutmut_5(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for entry in payload.get(None) or []:
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


def x_parse_readsb_clients__mutmut_6(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for entry in payload.get("XXclientsXX") or []:
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


def x_parse_readsb_clients__mutmut_7(payload: dict) -> ParseResult:
    """Extract client addresses from readsb's clients.json.

    Each entry is a list whose index 1 is readsb's `proxy_string`. With PROXY
    protocol that reads "TCP4 <src> <dst> <sport> <dport>" and index 1 of the
    split is the client address. Without it, readsb writes "<host> port <port>"
    and index 1 is the literal "port" -- which fails to parse and is counted.
    A non-zero anomaly count means PROXY protocol is not active on that path.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for entry in payload.get("CLIENTS") or []:
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


def x_parse_readsb_clients__mutmut_8(payload: dict) -> ParseResult:
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
        if not isinstance(entry, list) and len(entry) < 2:
            anomalies += 1
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_9(payload: dict) -> ParseResult:
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
        if isinstance(entry, list) or len(entry) < 2:
            anomalies += 1
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_10(payload: dict) -> ParseResult:
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
        if not isinstance(entry, list) or len(entry) <= 2:
            anomalies += 1
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_11(payload: dict) -> ParseResult:
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
        if not isinstance(entry, list) or len(entry) < 3:
            anomalies += 1
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_12(payload: dict) -> ParseResult:
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
            anomalies = 1
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_13(payload: dict) -> ParseResult:
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
            anomalies -= 1
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_14(payload: dict) -> ParseResult:
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
            anomalies += 2
            continue
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_15(payload: dict) -> ParseResult:
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
            break
        tokens = str(entry[1]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_16(payload: dict) -> ParseResult:
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
        tokens = None
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_17(payload: dict) -> ParseResult:
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
        tokens = str(None).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_18(payload: dict) -> ParseResult:
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
        tokens = str(entry[2]).split()
        prefix = to_prefix(tokens[1]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_19(payload: dict) -> ParseResult:
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
        prefix = None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_20(payload: dict) -> ParseResult:
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
        prefix = to_prefix(None) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_21(payload: dict) -> ParseResult:
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
        prefix = to_prefix(tokens[2]) if len(tokens) >= 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_22(payload: dict) -> ParseResult:
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
        prefix = to_prefix(tokens[1]) if len(tokens) > 2 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_23(payload: dict) -> ParseResult:
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
        prefix = to_prefix(tokens[1]) if len(tokens) >= 3 else None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_24(payload: dict) -> ParseResult:
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
        if prefix is not None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_25(payload: dict) -> ParseResult:
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
            anomalies = 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_26(payload: dict) -> ParseResult:
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
            anomalies -= 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_27(payload: dict) -> ParseResult:
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
            anomalies += 2
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_28(payload: dict) -> ParseResult:
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
            prefixes.add(None)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_29(payload: dict) -> ParseResult:
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
    return ParseResult(prefixes=None, anomalies=anomalies)


def x_parse_readsb_clients__mutmut_30(payload: dict) -> ParseResult:
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
    return ParseResult(prefixes=prefixes, anomalies=None)


def x_parse_readsb_clients__mutmut_31(payload: dict) -> ParseResult:
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
    return ParseResult(anomalies=anomalies)


def x_parse_readsb_clients__mutmut_32(payload: dict) -> ParseResult:
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
    return ParseResult(prefixes=prefixes, )

mutants_x_parse_readsb_clients__mutmut['_mutmut_orig'] = x_parse_readsb_clients__mutmut_orig # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_1'] = x_parse_readsb_clients__mutmut_1 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_2'] = x_parse_readsb_clients__mutmut_2 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_3'] = x_parse_readsb_clients__mutmut_3 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_4'] = x_parse_readsb_clients__mutmut_4 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_5'] = x_parse_readsb_clients__mutmut_5 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_6'] = x_parse_readsb_clients__mutmut_6 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_7'] = x_parse_readsb_clients__mutmut_7 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_8'] = x_parse_readsb_clients__mutmut_8 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_9'] = x_parse_readsb_clients__mutmut_9 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_10'] = x_parse_readsb_clients__mutmut_10 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_11'] = x_parse_readsb_clients__mutmut_11 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_12'] = x_parse_readsb_clients__mutmut_12 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_13'] = x_parse_readsb_clients__mutmut_13 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_14'] = x_parse_readsb_clients__mutmut_14 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_15'] = x_parse_readsb_clients__mutmut_15 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_16'] = x_parse_readsb_clients__mutmut_16 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_17'] = x_parse_readsb_clients__mutmut_17 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_18'] = x_parse_readsb_clients__mutmut_18 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_19'] = x_parse_readsb_clients__mutmut_19 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_20'] = x_parse_readsb_clients__mutmut_20 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_21'] = x_parse_readsb_clients__mutmut_21 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_22'] = x_parse_readsb_clients__mutmut_22 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_23'] = x_parse_readsb_clients__mutmut_23 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_24'] = x_parse_readsb_clients__mutmut_24 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_25'] = x_parse_readsb_clients__mutmut_25 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_26'] = x_parse_readsb_clients__mutmut_26 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_27'] = x_parse_readsb_clients__mutmut_27 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_28'] = x_parse_readsb_clients__mutmut_28 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_29'] = x_parse_readsb_clients__mutmut_29 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_30'] = x_parse_readsb_clients__mutmut_30 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_31'] = x_parse_readsb_clients__mutmut_31 # type: ignore # mutmut generated
mutants_x_parse_readsb_clients__mutmut['x_parse_readsb_clients__mutmut_32'] = x_parse_readsb_clients__mutmut_32 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_parse_mlat_clients__mutmut)
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


def x_parse_mlat_clients__mutmut_orig(payload: dict) -> ParseResult:
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


def x_parse_mlat_clients__mutmut_1(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = None
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


def x_parse_mlat_clients__mutmut_2(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = None
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


def x_parse_mlat_clients__mutmut_3(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 1
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


def x_parse_mlat_clients__mutmut_4(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for client in (payload and {}).values():
        if not isinstance(client, dict):
            anomalies += 1
            continue
        prefix = to_prefix(str(client.get("source_ip", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_5(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for client in (payload or {}).values():
        if isinstance(client, dict):
            anomalies += 1
            continue
        prefix = to_prefix(str(client.get("source_ip", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_6(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for client in (payload or {}).values():
        if not isinstance(client, dict):
            anomalies = 1
            continue
        prefix = to_prefix(str(client.get("source_ip", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_7(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for client in (payload or {}).values():
        if not isinstance(client, dict):
            anomalies -= 1
            continue
        prefix = to_prefix(str(client.get("source_ip", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_8(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for client in (payload or {}).values():
        if not isinstance(client, dict):
            anomalies += 2
            continue
        prefix = to_prefix(str(client.get("source_ip", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_9(payload: dict) -> ParseResult:
    """Extract client addresses from mlat-server's clients.json.

    The payload is a dict keyed by username; each value carries `source_ip`,
    set from the PROXY line in mlat/jsonclient.py.
    """
    prefixes: set[str] = set()
    anomalies = 0
    for client in (payload or {}).values():
        if not isinstance(client, dict):
            anomalies += 1
            break
        prefix = to_prefix(str(client.get("source_ip", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_10(payload: dict) -> ParseResult:
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
        prefix = None
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_11(payload: dict) -> ParseResult:
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
        prefix = to_prefix(None)
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_12(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(None))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_13(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(client.get(None, "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_14(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(client.get("source_ip", None)))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_15(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(client.get("")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_16(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(client.get("source_ip", )))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_17(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(client.get("XXsource_ipXX", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_18(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(client.get("SOURCE_IP", "")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_19(payload: dict) -> ParseResult:
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
        prefix = to_prefix(str(client.get("source_ip", "XXXX")))
        if prefix is None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_20(payload: dict) -> ParseResult:
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
        if prefix is not None:
            anomalies += 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_21(payload: dict) -> ParseResult:
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
            anomalies = 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_22(payload: dict) -> ParseResult:
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
            anomalies -= 1
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_23(payload: dict) -> ParseResult:
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
            anomalies += 2
        else:
            prefixes.add(prefix)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_24(payload: dict) -> ParseResult:
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
            prefixes.add(None)
    return ParseResult(prefixes=prefixes, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_25(payload: dict) -> ParseResult:
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
    return ParseResult(prefixes=None, anomalies=anomalies)


def x_parse_mlat_clients__mutmut_26(payload: dict) -> ParseResult:
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
    return ParseResult(prefixes=prefixes, anomalies=None)


def x_parse_mlat_clients__mutmut_27(payload: dict) -> ParseResult:
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
    return ParseResult(anomalies=anomalies)


def x_parse_mlat_clients__mutmut_28(payload: dict) -> ParseResult:
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
    return ParseResult(prefixes=prefixes, )

mutants_x_parse_mlat_clients__mutmut['_mutmut_orig'] = x_parse_mlat_clients__mutmut_orig # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_1'] = x_parse_mlat_clients__mutmut_1 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_2'] = x_parse_mlat_clients__mutmut_2 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_3'] = x_parse_mlat_clients__mutmut_3 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_4'] = x_parse_mlat_clients__mutmut_4 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_5'] = x_parse_mlat_clients__mutmut_5 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_6'] = x_parse_mlat_clients__mutmut_6 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_7'] = x_parse_mlat_clients__mutmut_7 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_8'] = x_parse_mlat_clients__mutmut_8 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_9'] = x_parse_mlat_clients__mutmut_9 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_10'] = x_parse_mlat_clients__mutmut_10 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_11'] = x_parse_mlat_clients__mutmut_11 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_12'] = x_parse_mlat_clients__mutmut_12 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_13'] = x_parse_mlat_clients__mutmut_13 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_14'] = x_parse_mlat_clients__mutmut_14 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_15'] = x_parse_mlat_clients__mutmut_15 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_16'] = x_parse_mlat_clients__mutmut_16 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_17'] = x_parse_mlat_clients__mutmut_17 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_18'] = x_parse_mlat_clients__mutmut_18 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_19'] = x_parse_mlat_clients__mutmut_19 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_20'] = x_parse_mlat_clients__mutmut_20 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_21'] = x_parse_mlat_clients__mutmut_21 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_22'] = x_parse_mlat_clients__mutmut_22 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_23'] = x_parse_mlat_clients__mutmut_23 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_24'] = x_parse_mlat_clients__mutmut_24 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_25'] = x_parse_mlat_clients__mutmut_25 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_26'] = x_parse_mlat_clients__mutmut_26 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_27'] = x_parse_mlat_clients__mutmut_27 # type: ignore # mutmut generated
mutants_x_parse_mlat_clients__mutmut['x_parse_mlat_clients__mutmut_28'] = x_parse_mlat_clients__mutmut_28 # type: ignore # mutmut generated
