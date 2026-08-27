import aiohttp
import pytest
from aiohttp import web

from reapi_allowlist.parse import parse_mlat_clients, parse_readsb_clients
from reapi_allowlist.sources import _url, fetch_source, gather_sources, resolve_hosts

READSB_BODY = {"clients": [["uuid", "TCP4 203.0.113.7 10.0.0.1 1 2", 0, 0]]}
MLAT_BODY = {"alice": {"source_ip": "198.51.100.20"}}


async def make_server(aiohttp_server, handler, path="/clients.json"):
    app = web.Application()
    app.router.add_get(path, handler)
    return await aiohttp_server(app)


async def _readsb_handler(request):
    return web.json_response(READSB_BODY)


async def _mlat_handler(request):
    return web.json_response(MLAT_BODY)


async def _http_error_handler(request):
    return web.Response(status=503)


async def _invalid_json_handler(request):
    return web.Response(text="not json")


async def _empty_list_handler(request):
    return web.json_response([])


async def _scalar_handler(request):
    return web.json_response(42)


async def test_fetch_source_parses_a_good_response(aiohttp_server):
    server = await make_server(aiohttp_server, _readsb_handler)
    async with aiohttp.ClientSession() as session:
        url = f"http://{server.host}:{server.port}/clients.json"
        result = await fetch_source(session, "ingest-1", url, parse_readsb_clients)
    assert result.ok is True
    assert result.prefixes == {"203.0.113.7/32"}


async def test_fetch_source_reports_not_ok_on_http_error(aiohttp_server):
    server = await make_server(aiohttp_server, _http_error_handler)
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
    server = await make_server(aiohttp_server, _invalid_json_handler)
    async with aiohttp.ClientSession() as session:
        url = f"http://{server.host}:{server.port}/clients.json"
        result = await fetch_source(session, "ingest-1", url, parse_readsb_clients)
    assert result.ok is False


async def test_gather_sources_unions_readsb_and_mlat(aiohttp_server, monkeypatch):
    readsb = await make_server(aiohttp_server, _readsb_handler)
    mlat = await make_server(aiohttp_server, _mlat_handler)

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


async def test_fetch_source_reports_not_ok_when_readsb_parser_raises_on_non_object_json(
    aiohttp_server,
):
    # A readsb pod with no clients currently connected returns "[]", not an
    # object -- payload.get(...) then raises AttributeError inside the parser.
    server = await make_server(aiohttp_server, _empty_list_handler)
    async with aiohttp.ClientSession() as session:
        url = f"http://{server.host}:{server.port}/clients.json"
        result = await fetch_source(session, "ingest-1", url, parse_readsb_clients)
    assert result.ok is False
    assert result.prefixes == set()


async def test_fetch_source_reports_not_ok_when_mlat_parser_raises_on_non_object_json(
    aiohttp_server,
):
    # A bare JSON scalar is valid JSON but breaks (payload or {}).values().
    server = await make_server(aiohttp_server, _scalar_handler)
    async with aiohttp.ClientSession() as session:
        url = f"http://{server.host}:{server.port}/clients.json"
        result = await fetch_source(session, "mlat-1", url, parse_mlat_clients)
    assert result.ok is False
    assert result.prefixes == set()


async def test_gather_sources_isolates_a_source_whose_parser_raises(
    aiohttp_server, monkeypatch
):
    bad = await make_server(aiohttp_server, _empty_list_handler)
    good = await make_server(aiohttp_server, _readsb_handler)

    async def fake_resolve(resolver, dns_name):
        return [f"{bad.host}:{bad.port}", f"{good.host}:{good.port}"]

    monkeypatch.setattr("reapi_allowlist.sources.resolve_hosts", fake_resolve)

    async with aiohttp.ClientSession() as session:
        results = await gather_sources(
            session, resolver=None,
            ingest_dns="ingest-readsb-headless", ingest_port=None,
            mlat_hosts=[], mlat_port=None,
        )

    assert len(results) == 2
    good_results = [r for r in results if r.ok]
    bad_results = [r for r in results if not r.ok]
    assert len(good_results) == 1
    assert len(bad_results) == 1
    assert good_results[0].prefixes == {"203.0.113.7/32"}
    assert bad_results[0].prefixes == set()


async def test_gather_sources_converts_an_escaped_exception_into_a_failed_source(
    monkeypatch,
):
    # Nothing should be able to raise out of fetch_source -- but if a future
    # change ever lets one through, gather_sources must still return one
    # SourceResult per source instead of aborting the whole cycle.
    async def exploding_fetch(session, name, url, parser, timeout=5.0):
        raise RuntimeError("boom")

    async def fake_resolve(resolver, dns_name):
        return ["ingest-a", "ingest-b"]

    monkeypatch.setattr("reapi_allowlist.sources.fetch_source", exploding_fetch)
    monkeypatch.setattr("reapi_allowlist.sources.resolve_hosts", fake_resolve)

    results = await gather_sources(
        None, resolver=None,
        ingest_dns="ingest-readsb-headless", ingest_port=150,
        mlat_hosts=["mlat-a"], mlat_port=150,
    )

    assert len(results) == 3
    assert all(r.ok is False for r in results)
    assert all(r.prefixes == set() for r in results)
    assert [r.name for r in results] == ["ingest:ingest-a", "ingest:ingest-b", "mlat:mlat-a"]


async def test_gather_sources_counts_a_failed_dns_resolution_as_a_failed_source(
    monkeypatch,
):
    # resolve_hosts() swallows every resolution failure into []. Iterating an
    # empty list to build the ingest source list means a failed resolution
    # previously produced zero SourceResults instead of a failed one -- so a
    # source class that never appeared was invisible to source-error
    # accounting, and the additive-only rail never engaged.
    async def fake_resolve(resolver, dns_name):
        return []

    monkeypatch.setattr("reapi_allowlist.sources.resolve_hosts", fake_resolve)

    results = await gather_sources(
        None, resolver=None,
        ingest_dns="ingest-readsb-headless", ingest_port=150,
        mlat_hosts=[], mlat_port=150,
    )

    assert len(results) == 1
    assert results[0].ok is False
    assert results[0].prefixes == set()


async def test_gather_sources_counts_a_raising_resolver_as_a_failed_source():
    # The resolver-raises case has never been covered through gather_sources.
    # resolve_hosts() itself catches the exception and returns [], so the
    # real (unpatched) resolve_hosts is exercised here via a resolver whose
    # .query() raises -- the outcome must be identical to the empty-list
    # case: one failed SourceResult reported, nothing propagates.
    class ExplodingResolver:
        async def query(self, dns_name, record_type):
            raise OSError("resolver exploded")

    results = await gather_sources(
        None, resolver=ExplodingResolver(),
        ingest_dns="ingest-readsb-headless", ingest_port=150,
        mlat_hosts=[], mlat_port=150,
    )

    assert len(results) == 1
    assert results[0].ok is False
    assert results[0].prefixes == set()


def test_url_brackets_a_bare_ipv6_host():
    # Not reachable today (resolve_hosts only queries A records), but the
    # spec treats dual-stack as required -- an IPv6 host must not build a
    # malformed URL missing its brackets.
    assert _url("2001:db8::1", 150) == "http://[2001:db8::1]:150/clients.json"
    assert _url("2001:db8::1", None) == "http://[2001:db8::1]/clients.json"


def test_url_does_not_double_bracket_an_already_bracketed_host():
    assert _url("[2001:db8::1]", 150) == "http://[2001:db8::1]:150/clients.json"


def test_url_leaves_ipv4_and_hostnames_unbracketed():
    assert _url("10.0.0.1", 150) == "http://10.0.0.1:150/clients.json"
    assert _url("ingest-a", 150) == "http://ingest-a:150/clients.json"


class _Answer:
    def __init__(self, host):
        self.host = host


class _FakeResolver:
    """Records which record types were asked for."""

    def __init__(self, by_record):
        self.by_record = by_record
        self.asked = []

    async def query(self, name, record):
        self.asked.append(record)
        if record not in self.by_record:
            raise Exception(f"NXDOMAIN {record}")
        return [_Answer(h) for h in self.by_record[record]]


@pytest.mark.asyncio
async def test_resolve_hosts_asks_for_both_families():
    """The cluster convention is RequireDualStack, so A alone misses pods."""
    r = _FakeResolver({"A": ["10.0.0.1"], "AAAA": ["2001:db8::1"]})
    assert await resolve_hosts(r, "svc") == ["10.0.0.1", "2001:db8::1"]
    assert r.asked == ["A", "AAAA"]


@pytest.mark.asyncio
async def test_resolve_hosts_tolerates_one_family_missing():
    # A v4-only Service NXDOMAINs on AAAA; that is normal, not a failure.
    r = _FakeResolver({"A": ["10.0.0.1"]})
    assert await resolve_hosts(r, "svc") == ["10.0.0.1"]


@pytest.mark.asyncio
async def test_resolve_hosts_dedupes_across_families():
    r = _FakeResolver({"A": ["10.0.0.1"], "AAAA": ["10.0.0.1"]})
    assert await resolve_hosts(r, "svc") == ["10.0.0.1"]


@pytest.mark.asyncio
async def test_resolve_hosts_returns_empty_when_both_families_fail():
    assert await resolve_hosts(_FakeResolver({}), "svc") == []
