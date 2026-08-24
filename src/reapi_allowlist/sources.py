"""Fetch clients.json from every ingest readsb pod and every mlat server.

A failed fetch returns ok=False with an empty set and never raises. The guards
in guards.py turn that into "additive only", so an unreachable pod cannot
evict its feeders. Exceptions must not escape this module.
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

import aiohttp
import orjson

from .parse import ParseResult, parse_mlat_clients, parse_readsb_clients

log = logging.getLogger(__name__)

Parser = Callable[[dict], ParseResult]


@dataclass(frozen=True)
class SourceResult:
    name: str
    prefixes: set[str] = field(default_factory=set)
    anomalies: int = 0
    ok: bool = False


def _url(host: str, port: int | None) -> str:
    return f"http://{host}:{port}/clients.json" if port else f"http://{host}/clients.json"


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
    except Exception as exc:  # noqa: BLE001 - never let a source failure escape
        log.warning("%s: %s", name, exc)
        return SourceResult(name=name)

    parsed = parser(payload)
    return SourceResult(
        name=name, prefixes=parsed.prefixes, anomalies=parsed.anomalies, ok=True
    )


async def resolve_hosts(resolver, dns_name: str) -> list[str]:
    """Resolve a headless Service to its pod addresses. Empty list on failure."""
    try:
        answers = await resolver.query(dns_name, "A")
        return [a.host for a in answers]
    except Exception as exc:  # noqa: BLE001
        log.warning("resolve %s: %s", dns_name, exc)
        return []


async def gather_sources(
    session: aiohttp.ClientSession,
    resolver,
    *,
    ingest_dns: str,
    ingest_port: int | None,
    mlat_hosts: list[str],
    mlat_port: int | None,
    timeout: float = 5.0,
) -> list[SourceResult]:
    ingest_hosts = await resolve_hosts(resolver, ingest_dns)
    tasks = [
        fetch_source(session, f"ingest:{h}", _url(h, ingest_port),
                     parse_readsb_clients, timeout)
        for h in ingest_hosts
    ] + [
        fetch_source(session, f"mlat:{h}", _url(h, mlat_port),
                     parse_mlat_clients, timeout)
        for h in mlat_hosts
    ]
    return list(await asyncio.gather(*tasks))
