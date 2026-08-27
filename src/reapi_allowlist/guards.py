"""Decide whether a computed set is safe to write.

Every rail here biases toward staying open. Adding an address is cheap and
reversible; removing one locks a real person out of the API.
"""

from dataclasses import dataclass

DEFAULT_MAX_ENTRIES = 50_000


@dataclass(frozen=True)
class WriteDecision:
    write: bool
    reason: str
    prefixes: frozenset[str]


def decide(
    current: frozenset[str],
    proposed: frozenset[str],
    *,
    all_sources_ok: bool,
    any_source_ok: bool,
    max_entries: int = DEFAULT_MAX_ENTRIES,
) -> WriteDecision:
    """Return whether to write, why, and what the resulting set would be.

    `reason` values are stable: they are used as metric label values and in
    logs. Do not reword them without updating both.
    """
    if not any_source_ok:
        return WriteDecision(False, "no-sources", current)

    if len(proposed) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if all_sources_ok:
        result, reason = proposed, "ok"
    else:
        # A source being unreachable is not evidence a feeder left.
        result, reason = current | proposed, "partial-additive"

    # Cap check on the actual write: the first check (line 37-38) bounds
    # what sources proposed; this bounds what would actually be written.
    # With all_sources_ok=False, result can exceed proposed via the union.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)
