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
    k8s, metrics = FakeK8s(obj={"spec": {"externalCIDRs": [A, B]}}), Metrics()
    feeders = FeederSet(window_seconds=3600)
    await reconcile(
        sources=[SourceResult("ingest:1", {A}, 0, True)],
        feeders=feeders, emitter=CCGEmitter(), k8s=k8s,
        metrics=metrics, now=1000.0,
    )
    assert feeders.active(now=1000.0) == {A, B}


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
