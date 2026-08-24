"""One reconcile pass: observe, decay, guard, write."""

import logging

from .decay import FeederSet
from .guards import WriteDecision, decide
from .k8s import K8sClient
from .metrics import Metrics
from .sources import SourceResult

log = logging.getLogger(__name__)


async def reconcile(
    *,
    sources: list[SourceResult],
    feeders: FeederSet,
    emitter,
    k8s: K8sClient,
    metrics: Metrics,
    now: float,
    seed_existing: bool = False,
) -> WriteDecision:
    """Fold one poll into the cluster object. Returns what was decided and why.

    Fetching happens in the caller so this stays testable with plain fakes.
    """
    existing = emitter.extract(await k8s.get(emitter.ref) or {})
    current = frozenset(existing)

    # Startup recovery only. Re-seeding every cycle would re-stamp every
    # persisted prefix as just-seen, which makes decay and the shrink-guard
    # unreachable -- the set could then only ever grow.
    if seed_existing:
        feeders.seed(existing, now)

    observed: set[str] = set()
    anomalies = source_errors = 0
    for source in sources:
        if source.ok:
            observed |= source.prefixes
            anomalies += source.anomalies
        else:
            source_errors += 1
    feeders.observe(observed, now)
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    if not decision.write:
        if decision.reason != "unchanged":
            log.warning("refusing write: %s", decision.reason)
        metrics.refusals[decision.reason] = metrics.refusals.get(decision.reason, 0) + 1
        return decision

    metrics.adds = len(decision.prefixes - current)
    metrics.removes = len(current - decision.prefixes)
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision
