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

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)
