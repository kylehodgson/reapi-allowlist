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


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict
mutants_x_reconcile__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_reconcile__mutmut)
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_orig(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_1(
    *,
    sources: list[SourceResult],
    feeders: FeederSet,
    emitter,
    k8s: K8sClient,
    metrics: Metrics,
    now: float,
    seed_existing: bool = True,
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_2(
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
    existing = None
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_3(
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
    existing = emitter.extract(None)
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_4(
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
    existing = emitter.extract(await k8s.get(emitter.ref) and {})
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_5(
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
    existing = emitter.extract(await k8s.get(None) or {})
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_6(
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
    current = None

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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_7(
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
    current = frozenset(None)

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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_8(
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
        feeders.seed(None, now)

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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_9(
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
        feeders.seed(existing, None)

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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_10(
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
        feeders.seed(now)

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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_11(
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
        feeders.seed(existing, )

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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_12(
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

    observed: set[str] = None
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_13(
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
    anomalies = source_errors = None
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_14(
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
    anomalies = source_errors = 1
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_15(
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
            observed = source.prefixes
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_16(
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
            observed &= source.prefixes
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_17(
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
            anomalies = source.anomalies
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_18(
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
            anomalies -= source.anomalies
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_19(
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
            source_errors = 1
    feeders.observe(observed, now)
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_20(
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
            source_errors -= 1
    feeders.observe(observed, now)
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_21(
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
            source_errors += 2
    feeders.observe(observed, now)
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_22(
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
    feeders.observe(None, now)
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_23(
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
    feeders.observe(observed, None)
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_24(
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
    feeders.observe(now)
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_25(
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
    feeders.observe(observed, )
    feeders.prune(now)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_26(
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
    feeders.prune(None)

    proposed = frozenset(feeders.active(now))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_27(
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

    proposed = None
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_28(
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

    proposed = frozenset(None)
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_29(
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

    proposed = frozenset(feeders.active(None))
    decision = decide(
        current, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_30(
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
    decision = None

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_31(
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
        None, proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_32(
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
        current, None,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_33(
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
        all_sources_ok=None,
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_34(
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
        any_source_ok=None,
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_35(
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
        proposed,
        all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_36(
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
        current, all_sources_ok=(source_errors == 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_37(
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
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_38(
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
        )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_39(
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
        all_sources_ok=(source_errors != 0),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_40(
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
        all_sources_ok=(source_errors == 1),
        any_source_ok=any(s.ok for s in sources),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_41(
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
        any_source_ok=any(None),
    )

    metrics.anomalies = anomalies
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_42(
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

    metrics.anomalies = None
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_43(
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
    metrics.internal_prefixes = None
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


async def x_reconcile__mutmut_44(
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
    metrics.internal_prefixes = sum(
        None
    )
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


async def x_reconcile__mutmut_45(
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
    metrics.internal_prefixes = sum(
        2 for p in decision.prefixes if is_internal_prefix(p)
    )
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


async def x_reconcile__mutmut_46(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(None)
    )
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


async def x_reconcile__mutmut_47(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = None
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


async def x_reconcile__mutmut_48(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = None

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


async def x_reconcile__mutmut_49(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles = 1
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


async def x_reconcile__mutmut_50(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles -= 1
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


async def x_reconcile__mutmut_51(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 2
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


async def x_reconcile__mutmut_52(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = None

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


async def x_reconcile__mutmut_53(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 1

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


async def x_reconcile__mutmut_54(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if decision.write:
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


async def x_reconcile__mutmut_55(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason != "unchanged":
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


async def x_reconcile__mutmut_56(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason == "XXunchangedXX":
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


async def x_reconcile__mutmut_57(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason == "UNCHANGED":
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


async def x_reconcile__mutmut_58(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason == "unchanged":
            metrics.no_change = 1
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


async def x_reconcile__mutmut_59(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason == "unchanged":
            metrics.no_change -= 1
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


async def x_reconcile__mutmut_60(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
    metrics.source_errors = source_errors
    metrics.set_size = len(decision.prefixes)

    # Keyed off source health, not decision.reason.
    if source_errors:
        metrics.consecutive_partial_cycles += 1
    else:
        metrics.consecutive_partial_cycles = 0

    if not decision.write:
        if decision.reason == "unchanged":
            metrics.no_change += 2
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


async def x_reconcile__mutmut_61(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.last_success = None
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


async def x_reconcile__mutmut_62(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            log.warning(None, decision.reason)
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


async def x_reconcile__mutmut_63(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            log.warning("refusing write: %s", None)
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


async def x_reconcile__mutmut_64(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            log.warning(decision.reason)
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


async def x_reconcile__mutmut_65(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            log.warning("refusing write: %s", )
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


async def x_reconcile__mutmut_66(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            log.warning("XXrefusing write: %sXX", decision.reason)
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


async def x_reconcile__mutmut_67(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            log.warning("REFUSING WRITE: %S", decision.reason)
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


async def x_reconcile__mutmut_68(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = None
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


async def x_reconcile__mutmut_69(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = metrics.refusals.get(decision.reason, 0) - 1
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


async def x_reconcile__mutmut_70(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = metrics.refusals.get(None, 0) + 1
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


async def x_reconcile__mutmut_71(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = metrics.refusals.get(decision.reason, None) + 1
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


async def x_reconcile__mutmut_72(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = metrics.refusals.get(0) + 1
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


async def x_reconcile__mutmut_73(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = metrics.refusals.get(decision.reason, ) + 1
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


async def x_reconcile__mutmut_74(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = metrics.refusals.get(decision.reason, 1) + 1
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


async def x_reconcile__mutmut_75(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
            metrics.refusals[decision.reason] = metrics.refusals.get(decision.reason, 0) + 2
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


async def x_reconcile__mutmut_76(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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

    metrics.adds = None
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


async def x_reconcile__mutmut_77(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    metrics.removes = None
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


async def x_reconcile__mutmut_78(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    if current or len(decision.prefixes) < len(current) * LARGE_SHRINK_RATIO:
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


async def x_reconcile__mutmut_79(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    if current and len(decision.prefixes) <= len(current) * LARGE_SHRINK_RATIO:
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


async def x_reconcile__mutmut_80(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    if current and len(decision.prefixes) < len(current) / LARGE_SHRINK_RATIO:
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


async def x_reconcile__mutmut_81(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
        metrics.large_shrink = 1
        log.warning("large shrink: %d -> %d prefixes -- check parse_anomalies "
                    "and source_errors before assuming the feeders left",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_82(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
        metrics.large_shrink -= 1
        log.warning("large shrink: %d -> %d prefixes -- check parse_anomalies "
                    "and source_errors before assuming the feeders left",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_83(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
        metrics.large_shrink += 2
        log.warning("large shrink: %d -> %d prefixes -- check parse_anomalies "
                    "and source_errors before assuming the feeders left",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_84(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
        log.warning(None,
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_85(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
                    None, len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_86(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
                    len(current), None)
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_87(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
        log.warning(len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_88(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
                    len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_89(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
                    len(current), )
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_90(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
        log.warning("XXlarge shrink: %d -> %d prefixes -- check parse_anomalies XX"
                    "and source_errors before assuming the feeders left",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_91(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
        log.warning("LARGE SHRINK: %D -> %D PREFIXES -- CHECK PARSE_ANOMALIES "
                    "and source_errors before assuming the feeders left",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_92(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
                    "XXand source_errors before assuming the feeders leftXX",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_93(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
                    "AND SOURCE_ERRORS BEFORE ASSUMING THE FEEDERS LEFT",
                    len(current), len(decision.prefixes))
    await k8s.patch(emitter.ref, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_94(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    await k8s.patch(None, emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_95(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    await k8s.patch(emitter.ref, None)
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_96(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    await k8s.patch(emitter.render(decision.prefixes))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_97(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    await k8s.patch(emitter.ref, )
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_98(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    await k8s.patch(emitter.ref, emitter.render(None))
    # After the patch returns, never before: a failed write is not a success.
    metrics.last_success = now
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_99(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    metrics.last_success = None
    log.info("wrote %d prefixes (+%d/-%d, %s)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_100(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    log.info(None,
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_101(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             None, metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_102(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             len(decision.prefixes), None, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_103(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             len(decision.prefixes), metrics.adds, None, decision.reason)
    return decision


async def x_reconcile__mutmut_104(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             len(decision.prefixes), metrics.adds, metrics.removes, None)
    return decision


async def x_reconcile__mutmut_105(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    log.info(len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_106(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_107(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             len(decision.prefixes), metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_108(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             len(decision.prefixes), metrics.adds, decision.reason)
    return decision


async def x_reconcile__mutmut_109(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
             len(decision.prefixes), metrics.adds, metrics.removes, )
    return decision


async def x_reconcile__mutmut_110(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    log.info("XXwrote %d prefixes (+%d/-%d, %s)XX",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision


async def x_reconcile__mutmut_111(
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
    metrics.internal_prefixes = sum(
        1 for p in decision.prefixes if is_internal_prefix(p)
    )
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
    log.info("WROTE %D PREFIXES (+%D/-%D, %S)",
             len(decision.prefixes), metrics.adds, metrics.removes, decision.reason)
    return decision

mutants_x_reconcile__mutmut['_mutmut_orig'] = x_reconcile__mutmut_orig # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_1'] = x_reconcile__mutmut_1 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_2'] = x_reconcile__mutmut_2 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_3'] = x_reconcile__mutmut_3 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_4'] = x_reconcile__mutmut_4 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_5'] = x_reconcile__mutmut_5 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_6'] = x_reconcile__mutmut_6 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_7'] = x_reconcile__mutmut_7 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_8'] = x_reconcile__mutmut_8 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_9'] = x_reconcile__mutmut_9 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_10'] = x_reconcile__mutmut_10 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_11'] = x_reconcile__mutmut_11 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_12'] = x_reconcile__mutmut_12 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_13'] = x_reconcile__mutmut_13 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_14'] = x_reconcile__mutmut_14 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_15'] = x_reconcile__mutmut_15 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_16'] = x_reconcile__mutmut_16 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_17'] = x_reconcile__mutmut_17 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_18'] = x_reconcile__mutmut_18 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_19'] = x_reconcile__mutmut_19 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_20'] = x_reconcile__mutmut_20 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_21'] = x_reconcile__mutmut_21 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_22'] = x_reconcile__mutmut_22 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_23'] = x_reconcile__mutmut_23 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_24'] = x_reconcile__mutmut_24 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_25'] = x_reconcile__mutmut_25 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_26'] = x_reconcile__mutmut_26 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_27'] = x_reconcile__mutmut_27 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_28'] = x_reconcile__mutmut_28 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_29'] = x_reconcile__mutmut_29 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_30'] = x_reconcile__mutmut_30 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_31'] = x_reconcile__mutmut_31 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_32'] = x_reconcile__mutmut_32 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_33'] = x_reconcile__mutmut_33 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_34'] = x_reconcile__mutmut_34 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_35'] = x_reconcile__mutmut_35 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_36'] = x_reconcile__mutmut_36 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_37'] = x_reconcile__mutmut_37 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_38'] = x_reconcile__mutmut_38 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_39'] = x_reconcile__mutmut_39 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_40'] = x_reconcile__mutmut_40 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_41'] = x_reconcile__mutmut_41 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_42'] = x_reconcile__mutmut_42 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_43'] = x_reconcile__mutmut_43 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_44'] = x_reconcile__mutmut_44 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_45'] = x_reconcile__mutmut_45 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_46'] = x_reconcile__mutmut_46 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_47'] = x_reconcile__mutmut_47 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_48'] = x_reconcile__mutmut_48 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_49'] = x_reconcile__mutmut_49 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_50'] = x_reconcile__mutmut_50 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_51'] = x_reconcile__mutmut_51 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_52'] = x_reconcile__mutmut_52 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_53'] = x_reconcile__mutmut_53 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_54'] = x_reconcile__mutmut_54 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_55'] = x_reconcile__mutmut_55 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_56'] = x_reconcile__mutmut_56 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_57'] = x_reconcile__mutmut_57 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_58'] = x_reconcile__mutmut_58 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_59'] = x_reconcile__mutmut_59 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_60'] = x_reconcile__mutmut_60 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_61'] = x_reconcile__mutmut_61 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_62'] = x_reconcile__mutmut_62 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_63'] = x_reconcile__mutmut_63 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_64'] = x_reconcile__mutmut_64 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_65'] = x_reconcile__mutmut_65 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_66'] = x_reconcile__mutmut_66 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_67'] = x_reconcile__mutmut_67 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_68'] = x_reconcile__mutmut_68 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_69'] = x_reconcile__mutmut_69 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_70'] = x_reconcile__mutmut_70 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_71'] = x_reconcile__mutmut_71 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_72'] = x_reconcile__mutmut_72 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_73'] = x_reconcile__mutmut_73 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_74'] = x_reconcile__mutmut_74 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_75'] = x_reconcile__mutmut_75 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_76'] = x_reconcile__mutmut_76 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_77'] = x_reconcile__mutmut_77 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_78'] = x_reconcile__mutmut_78 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_79'] = x_reconcile__mutmut_79 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_80'] = x_reconcile__mutmut_80 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_81'] = x_reconcile__mutmut_81 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_82'] = x_reconcile__mutmut_82 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_83'] = x_reconcile__mutmut_83 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_84'] = x_reconcile__mutmut_84 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_85'] = x_reconcile__mutmut_85 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_86'] = x_reconcile__mutmut_86 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_87'] = x_reconcile__mutmut_87 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_88'] = x_reconcile__mutmut_88 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_89'] = x_reconcile__mutmut_89 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_90'] = x_reconcile__mutmut_90 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_91'] = x_reconcile__mutmut_91 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_92'] = x_reconcile__mutmut_92 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_93'] = x_reconcile__mutmut_93 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_94'] = x_reconcile__mutmut_94 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_95'] = x_reconcile__mutmut_95 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_96'] = x_reconcile__mutmut_96 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_97'] = x_reconcile__mutmut_97 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_98'] = x_reconcile__mutmut_98 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_99'] = x_reconcile__mutmut_99 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_100'] = x_reconcile__mutmut_100 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_101'] = x_reconcile__mutmut_101 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_102'] = x_reconcile__mutmut_102 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_103'] = x_reconcile__mutmut_103 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_104'] = x_reconcile__mutmut_104 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_105'] = x_reconcile__mutmut_105 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_106'] = x_reconcile__mutmut_106 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_107'] = x_reconcile__mutmut_107 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_108'] = x_reconcile__mutmut_108 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_109'] = x_reconcile__mutmut_109 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_110'] = x_reconcile__mutmut_110 # type: ignore # mutmut generated
mutants_x_reconcile__mutmut['x_reconcile__mutmut_111'] = x_reconcile__mutmut_111 # type: ignore # mutmut generated
