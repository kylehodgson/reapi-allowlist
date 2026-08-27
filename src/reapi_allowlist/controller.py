"""One reconcile pass: observe, decay, guard, write."""

import logging

from .decay import FeederSet
from .guards import WriteDecision, decide
from .k8s import K8sClient
from .metrics import Metrics
from .sources import SourceResult

log = logging.getLogger(__name__)

# A drop this steep is almost certainly a bug on our side rather than every
# feeder leaving -- a clients.json format change, a bad --ingest-dns, a PROXY
# version change. We still perform the write: the harm is up to one poll
# interval without re-api access and it heals itself, whereas refusing
# deadlocked until someone hand-patched the object. This counter is the signal
# to alert on.
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

    # Startup recovery only. Re-seeding every cycle would re-stamp every
    # persisted prefix as just-seen, which makes decay and eviction
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

    # Keyed off source health, NOT decision.reason. When the additive union
    # equals the current set, decide() returns "unchanged" before ever
    # reporting "partial-additive" -- so a reason-driven counter reads zero
    # during exactly the sustained degradation it exists to surface. Observed
    # live with ingest scaled to zero: source_errors=1, counter stuck at 0.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason == "unchanged":
            # Correctly finding nothing to do is the controller doing its
            # job. last_success advances here, but deliberately not for
            # no-sources/over-cap below -- a refusal is exactly
            # what this metric exists to surface, so it must not advance.
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
    # Set only after k8s.patch returns: if patch raises, this line is never
    # reached, so a persistently-failing write (e.g. the target object was
    # never created) cannot masquerade as a healthy cycle.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision
