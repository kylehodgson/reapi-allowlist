from reapi_allowlist.controller import reconcile
from reapi_allowlist.decay import FeederSet
from reapi_allowlist.emitters import CCGEmitter
from reapi_allowlist.metrics import Metrics
from reapi_allowlist.sources import SourceResult

A, B = "1.0.0.1/32", "1.0.0.2/32"


class FakeK8s:
    def __init__(self, obj=None):
        self.obj = obj
        self.patched = []

    async def get(self, ref):
        return self.obj

    async def patch(self, ref, body):
        self.patched.append(body)


async def test_healthy_reconcile_patches_the_object():
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": []}}), Metrics()
    decision = await reconcile(
        sources=[SourceResult("ingest:1", {A, B}, 0, True)],
        feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert decision.write is True
    assert k8s.patched[0]["spec"]["externalCIDRs"] == [A, B]
    assert metrics.set_size == 2
    assert metrics.last_success == 1000.0


async def test_all_sources_failing_does_not_patch():
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A, B]}}), Metrics()
    decision = await reconcile(
        sources=[SourceResult("ingest:1", set(), 0, False)],
        feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert (decision.write, decision.reason) == (False, "no-sources")
    assert k8s.patched == []
    assert metrics.refusals["no-sources"] == 1


async def test_existing_object_seeds_the_decay_set():
    # B is in the cluster object but absent from this poll; it must survive.
    # seed_existing=True models the startup-recovery cycle only.
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A, B]}}), Metrics()
    feeders = FeederSet(window_seconds=3600)
    await reconcile(
        sources=[SourceResult("ingest:1", {A}, 0, True)],
        feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0, seed_existing=True,
    )
    assert feeders.active(now=1000.0) == {A, B}


async def test_decay_evicts_a_feeder_that_stops_reporting_across_cycles():
    # Regression: re-seeding from the persisted object on every cycle would
    # re-stamp B as just-seen forever, making decay and the shrink-guard
    # unreachable -- the set could then only ever grow. Two full decay
    # windows pass between cycles with only A still feeding; B must go.
    # 2 -> 1 prefixes stays clear of the shrink-guard (1 < 2*0.5 is false).
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": []}}), Metrics()
    feeders = FeederSet(window_seconds=3600)

    await reconcile(
        sources=[SourceResult("ingest:1", {A, B}, 0, True)],
        feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0, seed_existing=True,
    )
    k8s.obj = {"spec": {"externalCIDRs": sorted({A, B})}}

    decision = await reconcile(
        sources=[SourceResult("ingest:1", {A}, 0, True)],
        feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0 + 7200, seed_existing=False,
    )

    assert decision.write is True
    assert B not in k8s.patched[-1]["spec"]["externalCIDRs"]
    assert k8s.patched[-1]["spec"]["externalCIDRs"] == [A]


async def test_unchanged_set_does_not_patch():
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A]}}), Metrics()
    decision = await reconcile(
        sources=[SourceResult("ingest:1", {A}, 0, True)],
        feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert (decision.write, decision.reason) == (False, "unchanged")
    assert k8s.patched == []


async def test_anomalies_and_source_errors_are_recorded():
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": []}}), Metrics()
    await reconcile(
        sources=[
            SourceResult("ingest:1", {A}, 3, True),
            SourceResult("ingest:2", set(), 0, False),
        ],
        feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert metrics.anomalies == 3
    assert metrics.source_errors == 1


async def test_unchanged_reconcile_still_advances_last_success():
    # A fully healthy reconcile that correctly decides nothing changed is
    # still healthy -- it must not leave last_success stuck at None forever,
    # which would make adsb_reapi_allowlist_seconds_since_success rise
    # indefinitely on a perfectly stable, working controller.
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A]}}), Metrics()
    decision = await reconcile(
        sources=[SourceResult("ingest:1", {A}, 0, True)],
        feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert decision.reason == "unchanged"
    assert metrics.last_success == 1000.0


async def test_unchanged_increments_no_change_not_refusals():
    # "unchanged" is the healthy steady state, not a refusal -- it must not
    # pollute the refusals metric, which is meant to be alertable.
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A]}}), Metrics()
    await reconcile(
        sources=[SourceResult("ingest:1", {A}, 0, True)],
        feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert metrics.no_change == 1
    assert metrics.refusals == {}


async def test_consecutive_partial_cycles_tracks_persistent_partial_additive():
    # One source failing while the proposed set differs from current yields
    # write=True, reason="partial-additive". This must accumulate across
    # cycles so an operator can tell "one partial cycle" from "partial for
    # six hours", and reset the moment a cycle is not partial-additive.
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": []}}), Metrics()
    feeders = FeederSet(window_seconds=3600)

    sources = [
        SourceResult("ingest:1", {A}, 0, True),
        SourceResult("ingest:2", set(), 0, False),
    ]

    decision = await reconcile(
        sources=sources, feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert decision.reason == "partial-additive"
    assert metrics.consecutive_partial_cycles == 1

    sources_more = [
        SourceResult("ingest:1", {A, B}, 0, True),
        SourceResult("ingest:2", set(), 0, False),
    ]
    decision = await reconcile(
        sources=sources_more, feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1010.0,
    )
    assert decision.reason == "partial-additive"
    assert metrics.consecutive_partial_cycles == 2

    healthy_sources = [SourceResult("ingest:1", {A, B}, 0, True)]
    decision = await reconcile(
        sources=healthy_sources, feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1020.0,
    )
    assert decision.reason != "partial-additive"
    assert metrics.consecutive_partial_cycles == 0


class RaisingK8s(FakeK8s):
    """patch() always raises, as it would against a target object that was
    never created (see README section 8)."""

    async def patch(self, ref, body):
        raise RuntimeError("patch failed: object not found")


async def test_no_sources_cycle_does_not_advance_last_success():
    # A refusal is precisely what this metric exists to surface -- it must
    # not be masked by a fresh-looking last_success.
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A, B]}}), Metrics()
    decision = await reconcile(
        sources=[SourceResult("ingest:1", set(), 0, False)],
        feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert decision.reason == "no-sources"
    assert metrics.last_success is None


async def test_a_raising_patch_does_not_advance_last_success():
    # The 404 case: the target object was never created, so k8s.patch raises
    # on every cycle. last_success must not be stamped on the way past a
    # write that then fails -- otherwise a persistently-broken write reads
    # as healthy.
    k8s, metrics = RaisingK8s(obj={"spec": {"externalCIDRs": []}}), Metrics()
    try:
        await reconcile(
            sources=[SourceResult("ingest:1", {A, B}, 0, True)],
            feeders=FeederSet(), emitter=CCGEmitter(), k8s=k8s,
            metrics=metrics, now=1000.0,
        )
    except RuntimeError:
        pass
    assert metrics.last_success is None


async def test_partial_cycles_count_degraded_cycles_even_when_nothing_changes():
    # Observed live: with one source failing but the union equal to the current
    # set, decide() returns "unchanged" -- so a counter driven off the decision
    # reason resets, and reads 0 during exactly the sustained degradation it
    # exists to surface. It must key off source health, not the write outcome.
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A]}}), Metrics()
    feeders = FeederSet(window_seconds=3600)
    for cycle in range(3):
        decision = await reconcile(
            sources=[SourceResult("ok-src", {A}, 0, True),
                     SourceResult("dead-src", set(), 0, False)],
            feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
            metrics=metrics, now=1000.0 + cycle, seed_existing=(cycle == 0),
        )
        assert decision.reason == "unchanged"
    assert metrics.consecutive_partial_cycles == 3


async def test_partial_cycles_reset_once_every_source_is_healthy_again():
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A]}}), Metrics()
    feeders = FeederSet(window_seconds=3600)
    await reconcile(sources=[SourceResult("dead", set(), 0, False),
                             SourceResult("ok", {A}, 0, True)],
                    feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
                    metrics=metrics, now=1000.0, seed_existing=True)
    assert metrics.consecutive_partial_cycles == 1
    await reconcile(sources=[SourceResult("ok", {A}, 0, True)],
                    feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
                    metrics=metrics, now=1001.0)
    assert metrics.consecutive_partial_cycles == 0
