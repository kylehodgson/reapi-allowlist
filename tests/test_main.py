"""Tests for the CLI and the run loop.

Mutation testing found this module had no covering test at all -- 198 mutants
survived because nothing exercised it. It owns the startup-seeding flip, which
until now had only ever been verified by watching a real feeder in a lab.
"""

import socket
import time

import aiohttp
import pytest

from reapi_allowlist.__main__ import build_emitter, parse_args, run, serve_metrics
from reapi_allowlist.decay import FeederSet
from reapi_allowlist.emitters import CCGEmitter, CGCCEmitter
from reapi_allowlist.metrics import Metrics


# --- parse_args -------------------------------------------------------------

def test_defaults_match_the_documented_ones():
    a = parse_args([])
    assert (a.emit, a.name, a.namespace) == ("ccg", "adsblol-feeders", "adsblol")
    assert (a.interval, a.window) == (60, 3600)
    assert a.ingest_dns == "ingest-readsb-headless.adsblol.svc.cluster.local"
    assert (a.ingest_port, a.mlat_port, a.metrics_port) == (150, 150, 9090)
    assert a.mlat_host == []
    assert a.mlat_dns is None


def test_mlat_host_accumulates():
    # Eight shards are passed as eight flags; an overwriting flag would read
    # only the last and silently deny seven shards' worth of feeders.
    a = parse_args(["--mlat-host=a", "--mlat-host=b", "--mlat-host=c"])
    assert a.mlat_host == ["a", "b", "c"]


def test_numeric_flags_are_ints_not_strings():
    a = parse_args(["--interval=5", "--window=90", "--ingest-port=1",
                    "--mlat-port=2", "--metrics-port=3"])
    assert (a.interval, a.window) == (5, 90)
    assert (a.ingest_port, a.mlat_port, a.metrics_port) == (1, 2, 3)


def test_an_unknown_emitter_is_rejected():
    with pytest.raises(SystemExit):
        parse_args(["--emit=nonsense"])


# --- build_emitter ----------------------------------------------------------

def test_ccg_builds_a_cluster_scoped_emitter():
    e = build_emitter(parse_args(["--emit=ccg", "--name=grp", "--namespace=ignored"]))
    assert isinstance(e, CCGEmitter)
    assert e.ref.name == "grp"
    assert e.ref.namespace is None


def test_cgcc_builds_a_namespaced_emitter():
    e = build_emitter(parse_args(["--emit=cgcc", "--name=cfg", "--namespace=ns"]))
    assert isinstance(e, CGCCEmitter)
    assert (e.ref.name, e.ref.namespace) == ("cfg", "ns")


# --- serve_metrics ----------------------------------------------------------

async def test_serve_metrics_answers_on_the_configured_port():
    """The readiness and liveness probes both GET /metrics.

    If this does not bind, or answers something other than 200, Kubernetes
    never marks the pod ready and the controller never runs.
    """
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    metrics = Metrics()
    metrics.set_size = 7
    # last_success set, so seconds_since_success is computed from the clock
    # rather than short-circuiting to -1. This is the series the README tells
    # operators to alert on; a handler that passed a fixed or absent clock
    # would report a constant and never fire.
    metrics.last_success = time.time() - 30
    runner = await serve_metrics(metrics, port)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/metrics") as resp:
                body = await resp.text()
                assert resp.status == 200
                assert resp.content_type == "text/plain"
    finally:
        await runner.cleanup()

    assert "adsb_reapi_allowlist_size 7" in body


# --- the run loop -----------------------------------------------------------

class _Recorder:
    """Stands in for reconcile, recording how it was called."""

    def __init__(self, explode_on=()):
        self.seeds = []
        self.explode_on = set(explode_on)

    async def __call__(self, **kwargs):
        self.seeds.append(kwargs["seed_existing"])
        if len(self.seeds) in self.explode_on:
            raise RuntimeError("boom")


async def _drive(monkeypatch, cycles, recorder, interval=60):
    slept = []

    async def fake_gather(*_a, **_k):
        return []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr("reapi_allowlist.__main__.gather_sources", fake_gather)
    monkeypatch.setattr("reapi_allowlist.__main__.reconcile", recorder)
    await run(parse_args([f"--interval={interval}"]),
              k8s=None, session=None, resolver=None, emitter=None,
              feeders=FeederSet(window_seconds=10), metrics=Metrics(),
              cycles=cycles, sleep=fake_sleep)
    return slept


async def test_seeding_happens_on_the_first_cycle_only(monkeypatch):
    """Re-seeding every cycle would re-stamp every prefix as just-seen.

    Decay could then never evict anything and the set could only grow.
    """
    rec = _Recorder()
    await _drive(monkeypatch, cycles=4, recorder=rec)
    assert rec.seeds == [True, False, False, False]


async def test_a_failed_cycle_does_not_stop_the_loop(monkeypatch):
    rec = _Recorder(explode_on={1})
    await _drive(monkeypatch, cycles=3, recorder=rec)
    assert len(rec.seeds) == 3


async def test_a_failed_first_cycle_still_seeds_on_the_next(monkeypatch):
    """seed_existing is only cleared after reconcile returns.

    If the first cycle dies before seeding takes effect, the recovered set
    would otherwise be lost for the life of the process.
    """
    rec = _Recorder(explode_on={1})
    await _drive(monkeypatch, cycles=2, recorder=rec)
    assert rec.seeds == [True, True]


async def test_every_cycle_sleeps_the_configured_interval(monkeypatch):
    slept = await _drive(monkeypatch, cycles=3, recorder=_Recorder(), interval=17)
    assert slept == [17, 17, 17]


async def test_a_failed_cycle_still_sleeps(monkeypatch):
    # Without this a persistently failing source becomes a hot loop against
    # the API server.
    slept = await _drive(monkeypatch, cycles=2, recorder=_Recorder(explode_on={1, 2}),
                         interval=11)
    assert slept == [11, 11]
