import json
from pathlib import Path

from reapi_allowlist.parse import parse_mlat_clients, parse_readsb_clients

FIXTURES = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FIXTURES / name).read_text())


def test_readsb_extracts_v4_and_v6_from_proxy_strings():
    result = parse_readsb_clients(load("readsb_clients.json"))
    assert result.prefixes == {"203.0.113.7/32", "2001:db8::1/128"}


def test_readsb_counts_non_proxy_strings_as_anomalies():
    # "somehost.example port 40999" and "" both fail to yield an address.
    result = parse_readsb_clients(load("readsb_clients.json"))
    assert result.anomalies == 2


def test_readsb_tolerates_missing_clients_key():
    assert parse_readsb_clients({}).prefixes == set()


def test_mlat_extracts_source_ip():
    result = parse_mlat_clients(load("mlat_clients.json"))
    assert result.prefixes == {"198.51.100.20/32", "2001:db8::2/128"}


def test_mlat_counts_missing_and_invalid_source_ip_as_anomalies():
    # "carol" has no source_ip; "dave" has an unparseable one.
    result = parse_mlat_clients(load("mlat_clients.json"))
    assert result.anomalies == 2


def test_mlat_tolerates_empty_payload():
    assert parse_mlat_clients({}).prefixes == set()
