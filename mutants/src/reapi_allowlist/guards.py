"""Decide whether a computed set is safe to write.

Every rail here biases toward staying open. Adding an address is cheap and
reversible; removing one locks a real person out of the API.
"""

from dataclasses import dataclass

DEFAULT_MAX_ENTRIES = 50_000


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict


@dataclass(frozen=True)
class WriteDecision:
    write: bool
    reason: str
    prefixes: frozenset[str]
mutants_x_decide__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_decide__mutmut)
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


def x_decide__mutmut_orig(
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


def x_decide__mutmut_1(
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
    if any_source_ok:
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


def x_decide__mutmut_2(
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
        return WriteDecision(None, "no-sources", current)

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


def x_decide__mutmut_3(
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
        return WriteDecision(False, None, current)

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


def x_decide__mutmut_4(
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
        return WriteDecision(False, "no-sources", None)

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


def x_decide__mutmut_5(
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
        return WriteDecision("no-sources", current)

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


def x_decide__mutmut_6(
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
        return WriteDecision(False, current)

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


def x_decide__mutmut_7(
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
        return WriteDecision(False, "no-sources", )

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


def x_decide__mutmut_8(
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
        return WriteDecision(True, "no-sources", current)

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


def x_decide__mutmut_9(
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
        return WriteDecision(False, "XXno-sourcesXX", current)

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


def x_decide__mutmut_10(
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
        return WriteDecision(False, "NO-SOURCES", current)

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


def x_decide__mutmut_11(
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

    if len(proposed) >= max_entries:
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


def x_decide__mutmut_12(
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
        return WriteDecision(None, "over-cap", current)

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


def x_decide__mutmut_13(
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
        return WriteDecision(False, None, current)

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


def x_decide__mutmut_14(
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
        return WriteDecision(False, "over-cap", None)

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


def x_decide__mutmut_15(
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
        return WriteDecision("over-cap", current)

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


def x_decide__mutmut_16(
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
        return WriteDecision(False, current)

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


def x_decide__mutmut_17(
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
        return WriteDecision(False, "over-cap", )

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


def x_decide__mutmut_18(
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
        return WriteDecision(True, "over-cap", current)

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


def x_decide__mutmut_19(
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
        return WriteDecision(False, "XXover-capXX", current)

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


def x_decide__mutmut_20(
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
        return WriteDecision(False, "OVER-CAP", current)

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


def x_decide__mutmut_21(
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
        result, reason = None
    else:
        # A source being unreachable is not evidence a feeder left.
        result, reason = current | proposed, "partial-additive"

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_22(
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
        result, reason = proposed, "XXokXX"
    else:
        # A source being unreachable is not evidence a feeder left.
        result, reason = current | proposed, "partial-additive"

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_23(
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
        result, reason = proposed, "OK"
    else:
        # A source being unreachable is not evidence a feeder left.
        result, reason = current | proposed, "partial-additive"

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_24(
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
        result, reason = None

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_25(
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
        result, reason = current & proposed, "partial-additive"

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_26(
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
        result, reason = current | proposed, "XXpartial-additiveXX"

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_27(
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
        result, reason = current | proposed, "PARTIAL-ADDITIVE"

    # The union can exceed `proposed`, so the cap is checked again here.
    if len(result) > max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_28(
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
    if len(result) >= max_entries:
        return WriteDecision(False, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_29(
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
        return WriteDecision(None, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_30(
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
        return WriteDecision(False, None, current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_31(
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
        return WriteDecision(False, "over-cap", None)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_32(
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
        return WriteDecision("over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_33(
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
        return WriteDecision(False, current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_34(
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
        return WriteDecision(False, "over-cap", )

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_35(
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
        return WriteDecision(True, "over-cap", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_36(
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
        return WriteDecision(False, "XXover-capXX", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_37(
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
        return WriteDecision(False, "OVER-CAP", current)

    if result == current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_38(
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

    if result != current:
        return WriteDecision(False, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_39(
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
        return WriteDecision(None, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_40(
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
        return WriteDecision(False, None, current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_41(
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
        return WriteDecision(False, "unchanged", None)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_42(
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
        return WriteDecision("unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_43(
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
        return WriteDecision(False, current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_44(
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
        return WriteDecision(False, "unchanged", )

    return WriteDecision(True, reason, result)


def x_decide__mutmut_45(
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
        return WriteDecision(True, "unchanged", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_46(
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
        return WriteDecision(False, "XXunchangedXX", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_47(
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
        return WriteDecision(False, "UNCHANGED", current)

    return WriteDecision(True, reason, result)


def x_decide__mutmut_48(
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

    return WriteDecision(None, reason, result)


def x_decide__mutmut_49(
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

    return WriteDecision(True, None, result)


def x_decide__mutmut_50(
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

    return WriteDecision(True, reason, None)


def x_decide__mutmut_51(
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

    return WriteDecision(reason, result)


def x_decide__mutmut_52(
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

    return WriteDecision(True, result)


def x_decide__mutmut_53(
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

    return WriteDecision(True, reason, )


def x_decide__mutmut_54(
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

    return WriteDecision(False, reason, result)

mutants_x_decide__mutmut['_mutmut_orig'] = x_decide__mutmut_orig # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_1'] = x_decide__mutmut_1 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_2'] = x_decide__mutmut_2 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_3'] = x_decide__mutmut_3 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_4'] = x_decide__mutmut_4 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_5'] = x_decide__mutmut_5 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_6'] = x_decide__mutmut_6 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_7'] = x_decide__mutmut_7 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_8'] = x_decide__mutmut_8 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_9'] = x_decide__mutmut_9 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_10'] = x_decide__mutmut_10 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_11'] = x_decide__mutmut_11 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_12'] = x_decide__mutmut_12 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_13'] = x_decide__mutmut_13 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_14'] = x_decide__mutmut_14 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_15'] = x_decide__mutmut_15 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_16'] = x_decide__mutmut_16 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_17'] = x_decide__mutmut_17 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_18'] = x_decide__mutmut_18 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_19'] = x_decide__mutmut_19 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_20'] = x_decide__mutmut_20 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_21'] = x_decide__mutmut_21 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_22'] = x_decide__mutmut_22 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_23'] = x_decide__mutmut_23 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_24'] = x_decide__mutmut_24 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_25'] = x_decide__mutmut_25 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_26'] = x_decide__mutmut_26 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_27'] = x_decide__mutmut_27 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_28'] = x_decide__mutmut_28 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_29'] = x_decide__mutmut_29 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_30'] = x_decide__mutmut_30 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_31'] = x_decide__mutmut_31 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_32'] = x_decide__mutmut_32 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_33'] = x_decide__mutmut_33 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_34'] = x_decide__mutmut_34 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_35'] = x_decide__mutmut_35 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_36'] = x_decide__mutmut_36 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_37'] = x_decide__mutmut_37 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_38'] = x_decide__mutmut_38 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_39'] = x_decide__mutmut_39 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_40'] = x_decide__mutmut_40 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_41'] = x_decide__mutmut_41 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_42'] = x_decide__mutmut_42 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_43'] = x_decide__mutmut_43 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_44'] = x_decide__mutmut_44 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_45'] = x_decide__mutmut_45 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_46'] = x_decide__mutmut_46 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_47'] = x_decide__mutmut_47 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_48'] = x_decide__mutmut_48 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_49'] = x_decide__mutmut_49 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_50'] = x_decide__mutmut_50 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_51'] = x_decide__mutmut_51 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_52'] = x_decide__mutmut_52 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_53'] = x_decide__mutmut_53 # type: ignore # mutmut generated
mutants_x_decide__mutmut['x_decide__mutmut_54'] = x_decide__mutmut_54 # type: ignore # mutmut generated
