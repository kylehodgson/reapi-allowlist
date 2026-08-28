"""Fetch clients.json from every ingest readsb pod and every mlat server.

A failed fetch returns ok=False with an empty set and never raises. The guards
in guards.py turn that into "additive only", so an unreachable pod cannot
evict its feeders. Exceptions must not escape this module.
"""

import asyncio
import ipaddress
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

import aiohttp
import orjson

from .parse import ParseResult, parse_mlat_clients, parse_readsb_clients

log = logging.getLogger(__name__)

Parser = Callable[[dict], ParseResult]


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict


@dataclass(frozen=True)
class SourceResult:
    name: str
    prefixes: set[str] = field(default_factory=set)
    anomalies: int = 0
    ok: bool = False
mutants_x__bracket_if_ipv6__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x__bracket_if_ipv6__mutmut)
def _bracket_if_ipv6(host: str) -> str:
    """Bracket a bare IPv6 literal so it can be embedded in a URL authority.

    Leaves IPv4 addresses, hostnames, and already-bracketed literals alone.
    A colon alone isn't sufficient evidence (e.g. test doubles pass
    "127.0.0.1:8080" as a single "host" string), so this only brackets
    strings that actually parse as an IPv6 address.
    """
    if host.startswith("["):
        return host
    try:
        ipaddress.IPv6Address(host)
    except ValueError:
        return host
    return f"[{host}]"


def x__bracket_if_ipv6__mutmut_orig(host: str) -> str:
    """Bracket a bare IPv6 literal so it can be embedded in a URL authority.

    Leaves IPv4 addresses, hostnames, and already-bracketed literals alone.
    A colon alone isn't sufficient evidence (e.g. test doubles pass
    "127.0.0.1:8080" as a single "host" string), so this only brackets
    strings that actually parse as an IPv6 address.
    """
    if host.startswith("["):
        return host
    try:
        ipaddress.IPv6Address(host)
    except ValueError:
        return host
    return f"[{host}]"


def x__bracket_if_ipv6__mutmut_1(host: str) -> str:
    """Bracket a bare IPv6 literal so it can be embedded in a URL authority.

    Leaves IPv4 addresses, hostnames, and already-bracketed literals alone.
    A colon alone isn't sufficient evidence (e.g. test doubles pass
    "127.0.0.1:8080" as a single "host" string), so this only brackets
    strings that actually parse as an IPv6 address.
    """
    if host.startswith(None):
        return host
    try:
        ipaddress.IPv6Address(host)
    except ValueError:
        return host
    return f"[{host}]"


def x__bracket_if_ipv6__mutmut_2(host: str) -> str:
    """Bracket a bare IPv6 literal so it can be embedded in a URL authority.

    Leaves IPv4 addresses, hostnames, and already-bracketed literals alone.
    A colon alone isn't sufficient evidence (e.g. test doubles pass
    "127.0.0.1:8080" as a single "host" string), so this only brackets
    strings that actually parse as an IPv6 address.
    """
    if host.startswith("XX[XX"):
        return host
    try:
        ipaddress.IPv6Address(host)
    except ValueError:
        return host
    return f"[{host}]"


def x__bracket_if_ipv6__mutmut_3(host: str) -> str:
    """Bracket a bare IPv6 literal so it can be embedded in a URL authority.

    Leaves IPv4 addresses, hostnames, and already-bracketed literals alone.
    A colon alone isn't sufficient evidence (e.g. test doubles pass
    "127.0.0.1:8080" as a single "host" string), so this only brackets
    strings that actually parse as an IPv6 address.
    """
    if host.startswith("["):
        return host
    try:
        ipaddress.IPv6Address(None)
    except ValueError:
        return host
    return f"[{host}]"

mutants_x__bracket_if_ipv6__mutmut['_mutmut_orig'] = x__bracket_if_ipv6__mutmut_orig # type: ignore # mutmut generated
mutants_x__bracket_if_ipv6__mutmut['x__bracket_if_ipv6__mutmut_1'] = x__bracket_if_ipv6__mutmut_1 # type: ignore # mutmut generated
mutants_x__bracket_if_ipv6__mutmut['x__bracket_if_ipv6__mutmut_2'] = x__bracket_if_ipv6__mutmut_2 # type: ignore # mutmut generated
mutants_x__bracket_if_ipv6__mutmut['x__bracket_if_ipv6__mutmut_3'] = x__bracket_if_ipv6__mutmut_3 # type: ignore # mutmut generated
mutants_x__url__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x__url__mutmut)
def _url(host: str, port: int | None) -> str:
    host = _bracket_if_ipv6(host)
    return f"http://{host}:{port}/clients.json" if port else f"http://{host}/clients.json"


def x__url__mutmut_orig(host: str, port: int | None) -> str:
    host = _bracket_if_ipv6(host)
    return f"http://{host}:{port}/clients.json" if port else f"http://{host}/clients.json"


def x__url__mutmut_1(host: str, port: int | None) -> str:
    host = None
    return f"http://{host}:{port}/clients.json" if port else f"http://{host}/clients.json"


def x__url__mutmut_2(host: str, port: int | None) -> str:
    host = _bracket_if_ipv6(None)
    return f"http://{host}:{port}/clients.json" if port else f"http://{host}/clients.json"

mutants_x__url__mutmut['_mutmut_orig'] = x__url__mutmut_orig # type: ignore # mutmut generated
mutants_x__url__mutmut['x__url__mutmut_1'] = x__url__mutmut_1 # type: ignore # mutmut generated
mutants_x__url__mutmut['x__url__mutmut_2'] = x__url__mutmut_2 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_fetch_source__mutmut)
async def fetch_source(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_orig(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_1(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 6.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_2(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = None
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_3(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=None, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_4(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=None)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_5(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_6(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, )
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_7(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout * 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_8(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 3)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_9(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(None, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_10(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=None) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_11(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_12(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, ) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_13(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status == 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_14(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 201:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_15(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning(None, name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_16(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", None, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_17(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, None)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_18(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning(name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_19(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_20(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, )
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_21(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("XX%s: HTTP %sXX", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_22(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: http %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_23(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%S: HTTP %S", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_24(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=None)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_25(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = None
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_26(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(None)
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_27(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = None
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_28(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(None)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_29(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning(None, name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_30(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", None, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_31(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, None)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_32(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning(name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_33(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_34(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, )
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_35(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("XX%s: %sXX", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_36(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%S: %S", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_37(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=None)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_38(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=None, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_39(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=None, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_40(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=None, ok=True
    )


async def x_fetch_source__mutmut_41(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=None
    )


async def x_fetch_source__mutmut_42(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_43(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, anomalies=parsed.anomalies, ok=True
    )


async def x_fetch_source__mutmut_44(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, ok=True
    )


async def x_fetch_source__mutmut_45(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, )


async def x_fetch_source__mutmut_46(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
    parser: Parser,
    timeout: float = 5.0,
) -> SourceResult:
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout / 2)
        async with session.get(url, timeout=client_timeout) as response:
            if response.status != 200:
                log.warning("%s: HTTP %s", name, response.status)
                return SourceResult(name=name)
            payload = orjson.loads(await response.read())
        # Parsing runs inside the same guarded block: a syntactically valid
        # but structurally wrong body (e.g. "[]" or "42") must degrade the
        # same way a connection failure does, not raise past this function.
        parsed = parser(payload)
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=False
    )

mutants_x_fetch_source__mutmut['_mutmut_orig'] = x_fetch_source__mutmut_orig # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_1'] = x_fetch_source__mutmut_1 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_2'] = x_fetch_source__mutmut_2 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_3'] = x_fetch_source__mutmut_3 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_4'] = x_fetch_source__mutmut_4 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_5'] = x_fetch_source__mutmut_5 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_6'] = x_fetch_source__mutmut_6 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_7'] = x_fetch_source__mutmut_7 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_8'] = x_fetch_source__mutmut_8 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_9'] = x_fetch_source__mutmut_9 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_10'] = x_fetch_source__mutmut_10 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_11'] = x_fetch_source__mutmut_11 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_12'] = x_fetch_source__mutmut_12 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_13'] = x_fetch_source__mutmut_13 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_14'] = x_fetch_source__mutmut_14 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_15'] = x_fetch_source__mutmut_15 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_16'] = x_fetch_source__mutmut_16 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_17'] = x_fetch_source__mutmut_17 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_18'] = x_fetch_source__mutmut_18 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_19'] = x_fetch_source__mutmut_19 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_20'] = x_fetch_source__mutmut_20 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_21'] = x_fetch_source__mutmut_21 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_22'] = x_fetch_source__mutmut_22 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_23'] = x_fetch_source__mutmut_23 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_24'] = x_fetch_source__mutmut_24 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_25'] = x_fetch_source__mutmut_25 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_26'] = x_fetch_source__mutmut_26 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_27'] = x_fetch_source__mutmut_27 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_28'] = x_fetch_source__mutmut_28 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_29'] = x_fetch_source__mutmut_29 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_30'] = x_fetch_source__mutmut_30 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_31'] = x_fetch_source__mutmut_31 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_32'] = x_fetch_source__mutmut_32 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_33'] = x_fetch_source__mutmut_33 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_34'] = x_fetch_source__mutmut_34 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_35'] = x_fetch_source__mutmut_35 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_36'] = x_fetch_source__mutmut_36 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_37'] = x_fetch_source__mutmut_37 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_38'] = x_fetch_source__mutmut_38 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_39'] = x_fetch_source__mutmut_39 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_40'] = x_fetch_source__mutmut_40 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_41'] = x_fetch_source__mutmut_41 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_42'] = x_fetch_source__mutmut_42 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_43'] = x_fetch_source__mutmut_43 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_44'] = x_fetch_source__mutmut_44 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_45'] = x_fetch_source__mutmut_45 # type: ignore # mutmut generated
mutants_x_fetch_source__mutmut['x_fetch_source__mutmut_46'] = x_fetch_source__mutmut_46 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_resolve_hosts__mutmut)
async def resolve_hosts(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_orig(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_1(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = None
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_2(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = None
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_3(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("XXAXX", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_4(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("a", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_5(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "XXAAAAXX"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_6(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "aaaa"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_7(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = None
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_8(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(None, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_9(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, None)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_10(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_11(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, )
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_12(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(None)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_13(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(None)
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_14(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_15(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning(None, dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_16(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", None, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_17(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, None)
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_18(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning(dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_19(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_20(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, )
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_21(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("XXresolve %s: %sXX", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_22(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("RESOLVE %S: %S", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_23(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) and "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_24(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(None) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_25(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "XX; XX".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_26(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "XXno recordsXX")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_27(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "NO RECORDS")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(hosts))


async def x_resolve_hosts__mutmut_28(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(None)


async def x_resolve_hosts__mutmut_29(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure.

    Both families are queried. The cluster convention is
    `ipFamilyPolicy: RequireDualStack`, so an A-only lookup would miss every
    pod on an IPv6-only or v6-preferred Service. One family returning NXDOMAIN
    is normal and not worth a warning; only a total failure is.
    """
    hosts: list[str] = []
    errors: list[str] = []
    for record in ("A", "AAAA"):
        try:
            answers = await resolver.query(dns_name, record)
            hosts.extend(a.host for a in answers)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record}: {exc}")
    if not hosts:
        log.warning("resolve %s: %s", dns_name, "; ".join(errors) or "no records")
    # A dual-stack Service can return the same pod under both families.
    return list(dict.fromkeys(None))

mutants_x_resolve_hosts__mutmut['_mutmut_orig'] = x_resolve_hosts__mutmut_orig # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_1'] = x_resolve_hosts__mutmut_1 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_2'] = x_resolve_hosts__mutmut_2 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_3'] = x_resolve_hosts__mutmut_3 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_4'] = x_resolve_hosts__mutmut_4 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_5'] = x_resolve_hosts__mutmut_5 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_6'] = x_resolve_hosts__mutmut_6 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_7'] = x_resolve_hosts__mutmut_7 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_8'] = x_resolve_hosts__mutmut_8 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_9'] = x_resolve_hosts__mutmut_9 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_10'] = x_resolve_hosts__mutmut_10 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_11'] = x_resolve_hosts__mutmut_11 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_12'] = x_resolve_hosts__mutmut_12 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_13'] = x_resolve_hosts__mutmut_13 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_14'] = x_resolve_hosts__mutmut_14 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_15'] = x_resolve_hosts__mutmut_15 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_16'] = x_resolve_hosts__mutmut_16 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_17'] = x_resolve_hosts__mutmut_17 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_18'] = x_resolve_hosts__mutmut_18 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_19'] = x_resolve_hosts__mutmut_19 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_20'] = x_resolve_hosts__mutmut_20 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_21'] = x_resolve_hosts__mutmut_21 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_22'] = x_resolve_hosts__mutmut_22 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_23'] = x_resolve_hosts__mutmut_23 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_24'] = x_resolve_hosts__mutmut_24 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_25'] = x_resolve_hosts__mutmut_25 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_26'] = x_resolve_hosts__mutmut_26 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_27'] = x_resolve_hosts__mutmut_27 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_28'] = x_resolve_hosts__mutmut_28 # type: ignore # mutmut generated
mutants_x_resolve_hosts__mutmut['x_resolve_hosts__mutmut_29'] = x_resolve_hosts__mutmut_29 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_gather_sources__mutmut)
async def gather_sources(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_orig(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_1(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 6.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_2(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = None
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_3(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(None, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_4(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, None)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_5(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_6(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, )
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_7(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = None
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_8(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = None
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_9(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(None, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_10(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, None)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_11(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_12(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, )
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_13(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = None
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_14(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(None)
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_15(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(None))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_16(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) - discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_17(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(None) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_18(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning(None, mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_19(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", None)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_20(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning(mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_21(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", )
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_22(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("XXno addresses for %sXX", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_23(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("NO ADDRESSES FOR %S", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_24(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(None)
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_25(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=None))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_26(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_27(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning(None, ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_28(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", None)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_29(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning(ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_30(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", )
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_31(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("XXno addresses for %sXX", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_32(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("NO ADDRESSES FOR %S", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_33(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(None)
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_34(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=None))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_35(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = None
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_36(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] - [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_37(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(None, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_38(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, None, _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_39(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", None,
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_40(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      None, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_41(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, None))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_42(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_43(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_44(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_45(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_46(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, ))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_47(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(None, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_48(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, None),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_49(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_50(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_51(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(None, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_52(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, None, _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_53(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", None,
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_54(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    None, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_55(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, None))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_56(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_57(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_58(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_59(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_60(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, ))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_61(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(None, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_62(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, None),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_63(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_64(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, ),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_65(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = None
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_66(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=None)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_67(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_68(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), )
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_69(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=False)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_70(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = None
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_71(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(None, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_72(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, None):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_73(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_74(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, ):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_75(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning(None, name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_76(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", None, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_77(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, None)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_78(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning(name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_79(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_80(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, )
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_81(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("XX%s: unexpected error: %sXX", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_82(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%S: UNEXPECTED ERROR: %S", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_83(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(None)
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_84(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=None))
        else:
            results.append(value)
    return extra + results


async def x_gather_sources__mutmut_85(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(None)
    return extra + results


async def x_gather_sources__mutmut_86(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    mlat_dns: str | None = None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    extra: list[SourceResult] = []
    if mlat_dns:
        discovered = await resolve_hosts(resolver, mlat_dns)
        if discovered:
            mlat_hosts = list(dict.fromkeys(list(mlat_hosts) + discovered))
        else:
            log.warning("no addresses for %s", mlat_dns)
            extra.append(SourceResult(name=f"mlat-dns:{mlat_dns}"))
    if not ingest_hosts:
        # A failed resolve is a failed source, not the absence of one.
        log.warning("no addresses for %s", ingest_dns)
        extra.append(SourceResult(name=f"ingest-dns:{ingest_dns}"))
    named = [
        (f"ingest:{h}", fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                                      parse_readsb_clients, timeout))
        for h in ingest_hosts
    ] + [
        (f"mlat:{h}", fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                                    parse_mlat_clients, timeout))
        for h in mlat_hosts
    ]
    raw = await asyncio.gather(*(coro for _, coro in named), return_exceptions=True)
    results: list[SourceResult] = []
    for (name, _), value in zip(named, raw):
        if isinstance(value, BaseException):
            log.warning("%s: unexpected error: %s", name, value)
            results.append(SourceResult(name=name))
        else:
            results.append(value)
    return extra - results

mutants_x_gather_sources__mutmut['_mutmut_orig'] = x_gather_sources__mutmut_orig # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_1'] = x_gather_sources__mutmut_1 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_2'] = x_gather_sources__mutmut_2 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_3'] = x_gather_sources__mutmut_3 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_4'] = x_gather_sources__mutmut_4 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_5'] = x_gather_sources__mutmut_5 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_6'] = x_gather_sources__mutmut_6 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_7'] = x_gather_sources__mutmut_7 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_8'] = x_gather_sources__mutmut_8 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_9'] = x_gather_sources__mutmut_9 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_10'] = x_gather_sources__mutmut_10 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_11'] = x_gather_sources__mutmut_11 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_12'] = x_gather_sources__mutmut_12 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_13'] = x_gather_sources__mutmut_13 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_14'] = x_gather_sources__mutmut_14 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_15'] = x_gather_sources__mutmut_15 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_16'] = x_gather_sources__mutmut_16 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_17'] = x_gather_sources__mutmut_17 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_18'] = x_gather_sources__mutmut_18 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_19'] = x_gather_sources__mutmut_19 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_20'] = x_gather_sources__mutmut_20 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_21'] = x_gather_sources__mutmut_21 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_22'] = x_gather_sources__mutmut_22 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_23'] = x_gather_sources__mutmut_23 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_24'] = x_gather_sources__mutmut_24 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_25'] = x_gather_sources__mutmut_25 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_26'] = x_gather_sources__mutmut_26 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_27'] = x_gather_sources__mutmut_27 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_28'] = x_gather_sources__mutmut_28 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_29'] = x_gather_sources__mutmut_29 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_30'] = x_gather_sources__mutmut_30 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_31'] = x_gather_sources__mutmut_31 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_32'] = x_gather_sources__mutmut_32 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_33'] = x_gather_sources__mutmut_33 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_34'] = x_gather_sources__mutmut_34 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_35'] = x_gather_sources__mutmut_35 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_36'] = x_gather_sources__mutmut_36 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_37'] = x_gather_sources__mutmut_37 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_38'] = x_gather_sources__mutmut_38 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_39'] = x_gather_sources__mutmut_39 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_40'] = x_gather_sources__mutmut_40 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_41'] = x_gather_sources__mutmut_41 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_42'] = x_gather_sources__mutmut_42 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_43'] = x_gather_sources__mutmut_43 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_44'] = x_gather_sources__mutmut_44 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_45'] = x_gather_sources__mutmut_45 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_46'] = x_gather_sources__mutmut_46 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_47'] = x_gather_sources__mutmut_47 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_48'] = x_gather_sources__mutmut_48 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_49'] = x_gather_sources__mutmut_49 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_50'] = x_gather_sources__mutmut_50 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_51'] = x_gather_sources__mutmut_51 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_52'] = x_gather_sources__mutmut_52 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_53'] = x_gather_sources__mutmut_53 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_54'] = x_gather_sources__mutmut_54 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_55'] = x_gather_sources__mutmut_55 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_56'] = x_gather_sources__mutmut_56 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_57'] = x_gather_sources__mutmut_57 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_58'] = x_gather_sources__mutmut_58 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_59'] = x_gather_sources__mutmut_59 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_60'] = x_gather_sources__mutmut_60 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_61'] = x_gather_sources__mutmut_61 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_62'] = x_gather_sources__mutmut_62 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_63'] = x_gather_sources__mutmut_63 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_64'] = x_gather_sources__mutmut_64 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_65'] = x_gather_sources__mutmut_65 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_66'] = x_gather_sources__mutmut_66 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_67'] = x_gather_sources__mutmut_67 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_68'] = x_gather_sources__mutmut_68 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_69'] = x_gather_sources__mutmut_69 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_70'] = x_gather_sources__mutmut_70 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_71'] = x_gather_sources__mutmut_71 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_72'] = x_gather_sources__mutmut_72 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_73'] = x_gather_sources__mutmut_73 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_74'] = x_gather_sources__mutmut_74 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_75'] = x_gather_sources__mutmut_75 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_76'] = x_gather_sources__mutmut_76 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_77'] = x_gather_sources__mutmut_77 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_78'] = x_gather_sources__mutmut_78 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_79'] = x_gather_sources__mutmut_79 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_80'] = x_gather_sources__mutmut_80 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_81'] = x_gather_sources__mutmut_81 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_82'] = x_gather_sources__mutmut_82 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_83'] = x_gather_sources__mutmut_83 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_84'] = x_gather_sources__mutmut_84 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_85'] = x_gather_sources__mutmut_85 # type: ignore # mutmut generated
mutants_x_gather_sources__mutmut['x_gather_sources__mutmut_86'] = x_gather_sources__mutmut_86 # type: ignore # mutmut generated
