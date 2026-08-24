import aiohttp
import pytest
from aiohttp import web

from reapi_allowlist.parse import parse_mlat_clients, parse_readsb_clients
from reapi_allowlist.sources import fetch_source, gather_sources

READSB_BODY = {"clients": [["uuid", "TCP4 203.0.113.7 10.0.0.1 1 2", 0, 0]]}
MLAT_BODY = {"alice": {"source_ip": "198.51.100.20"}}


async def make_server(aiohttp_server, handler, path="/clients.json"):
    app = web.Application()
    app.router.add_get(path, handler)
    return await aiohttp_server(app)


async def test_fetch_source_parses_a_good_response(aiohttp_server):
    server = await make_server(aiohttp_server, lambda r: web.json_response(READSB_BODY))
    async with aiohttp.ClientSession() as session:
        url = f"http://{server.host}:{server.port}/clients.json"
        result = await fetch_source(session, "ingest-1", url, parse_readsb_clients)
    assert result.ok is True
    assert result.prefixes == {"203.0.113.7/32"}


async def test_fetch_source_reports_not_ok_on_http_error(aiohttp_server):
    server = await make_server(aiohttp_server, lambda r: web.Response(status=503))
    async with aiohttp.ClientSession() as session:
        url = f"http://{server.host}:{server.port}/clients.json"
        result = await fetch_source(session, "ingest-1", url, parse_readsb_clients)
    assert result.ok is False
    assert result.prefixes == set()


async def test_fetch_source_reports_not_ok_on_connection_refused():
    async with aiohttp.ClientSession() as session:
        # Port 1 is reserved and nothing listens on it.
        result = await fetch_source(
            session, "dead", "http://127.0.0.1:1/clients.json",
            parse_readsb_clients, timeout=0.5,
        )
    assert result.ok is False


async def test_fetch_source_reports_not_ok_on_invalid_json(aiohttp_server):
    server = await make_server(aiohttp_server, lambda r: web.Response(text="not json"))
    async with aiohttp.ClientSession() as session:
        url = f"http://{server.host}:{server.port}/clients.json"
        result = await fetch_source(session, "ingest-1", url, parse_readsb_clients)
    assert result.ok is False


async def test_gather_sources_unions_readsb_and_mlat(aiohttp_server, monkeypatch):
    readsb = await make_server(aiohttp_server, lambda r: web.json_response(READSB_BODY))
    mlat = await make_server(aiohttp_server, lambda r: web.json_response(MLAT_BODY))

    async def fake_resolve(resolver, dns_name):
        return [f"{readsb.host}:{readsb.port}"]

    monkeypatch.setattr("reapi_allowlist.sources.resolve_hosts", fake_resolve)

    async with aiohttp.ClientSession() as session:
        results = await gather_sources(
            session, resolver=None,
            ingest_dns="ingest-readsb-headless", ingest_port=None,
            mlat_hosts=[f"{mlat.host}:{mlat.port}"], mlat_port=None,
        )

    merged = set().union(*(r.prefixes for r in results))
    assert merged == {"203.0.113.7/32", "198.51.100.20/32"}
    assert all(r.ok for r in results)
