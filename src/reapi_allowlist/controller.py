"""One reconcile pass: observe, decay, guard, write."""

import logging

from .decay import FeederSet
from .prefixes import is_internal_prefix
from .guards import WriteDecision, decide
from .k8s import K8sClient
from .metrics import Metrics
from .sources import SourceResult

log = logging.getLogger(__name__)

LARGE_SHRINK_RATIO = 0.5


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

    # Startup only: re-seeding every cycle would make decay unreachable.
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
    metrics.internal_prefixes = sum(1 for p in proposed if is_internal_prefix(p))
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason == "unchanged":
            metrics.no_change += 1
            metrics.last_success = now
        else:
            log.warning("refusing write: %s", decision.reason)
            metrics.refusals[decision.reason] = metrics.refusals.get(decision.reason, 0) + 1
        return decision

    metrics.adds = len(decision.prefixes - current)
    metrics.removes = len(current - decision.prefixes)
    if current and len(decision.prefixes) < len(current) * LARGE_SHRINK_RATIO:
        metrics.large_shrink += 1
        log.warning("large shrink: %d -> %d prefixes -- check parse_anomalies "
                    "and source_errors before assuming the feeders left",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision
