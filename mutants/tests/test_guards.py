from reapi_allowlist.guards import decide

A, B, C, D = "1.0.0.1/32", "1.0.0.2/32", "1.0.0.3/32", "1.0.0.4/32"


def test_writes_when_everything_is_healthy():
    d = decide(frozenset({A, B}), frozenset({A, B, C}),
               all_sources_ok=True, any_source_ok=True)
    assert (d.write, d.reason) == (True, "ok")
    assert d.prefixes == frozenset({A, B, C})


def test_refuses_when_no_source_responded():
    d = decide(frozenset({A, B}), frozenset(),
               all_sources_ok=False, any_source_ok=False)
    assert (d.write, d.reason) == (False, "no-sources")
    assert d.prefixes == frozenset({A, B})


def test_refuses_when_proposed_set_exceeds_the_cap():
    proposed = frozenset(f"10.0.{i // 256}.{i % 256}/32" for i in range(11))
    d = decide(frozenset({A}), proposed,
               all_sources_ok=True, any_source_ok=True, max_entries=10)
    assert (d.write, d.reason) == (False, "over-cap")


def test_partial_sources_are_additive_and_never_evict():
    # C is missing because a pod was unreachable, not because it left.
    d = decide(frozenset({A, B, C}), frozenset({A, B, D}),
               all_sources_ok=False, any_source_ok=True)
    assert (d.write, d.reason) == (True, "partial-additive")
    assert d.prefixes == frozenset({A, B, C, D})


def test_partial_sources_adding_nothing_new_is_a_no_write():
    d = decide(frozenset({A, B}), frozenset({A}),
               all_sources_ok=False, any_source_ok=True)
    assert (d.write, d.reason) == (False, "unchanged")


def test_a_large_shrink_is_now_performed_not_refused():
    # The shrink guard used to refuse this and never relent, deadlocking until
    # someone hand-patched the object. The harm it guarded against is up to one
    # poll interval without re-api access, self-healing; the deadlock was worse.
    # Detection moved to an observability counter -- see test_controller.
    d = decide(frozenset({A, B, C, D}), frozenset({A}),
               all_sources_ok=True, any_source_ok=True)
    assert (d.write, d.reason) == (True, "ok")
    assert d.prefixes == frozenset({A})


def test_a_collapse_to_empty_is_also_performed():
    d = decide(frozenset({A, B, C, D}), frozenset(),
               all_sources_ok=True, any_source_ok=True)
    assert (d.write, d.reason) == (True, "ok")
    assert d.prefixes == frozenset()


def test_allows_a_shrink_that_stays_above_the_ratio():
    d = decide(frozenset({A, B, C, D}), frozenset({A, B, C}),
               all_sources_ok=True, any_source_ok=True)
    assert (d.write, d.reason) == (True, "ok")


def test_no_write_when_the_set_is_unchanged():
    d = decide(frozenset({A, B}), frozenset({A, B}),
               all_sources_ok=True, any_source_ok=True)
    assert (d.write, d.reason) == (False, "unchanged")


def test_growing_from_empty_is_allowed():
    # First run: current is empty, so the shrink ratio must not block us.
    d = decide(frozenset(), frozenset({A, B}),
               all_sources_ok=True, any_source_ok=True)
    assert (d.write, d.reason) == (True, "ok")


def test_refuses_when_the_additive_union_exceeds_the_cap():
    # Neither set alone is over the cap, but the union that would be
    # written when a source is unreachable is.
    current = frozenset(f"10.0.0.{i}/32" for i in range(6))
    proposed = frozenset(f"10.0.1.{i}/32" for i in range(6))
    d = decide(current, proposed,
               all_sources_ok=False, any_source_ok=True, max_entries=10)
    assert (d.write, d.reason) == (False, "over-cap")
    assert d.prefixes == current


def test_a_refusal_still_carries_the_current_set():
    """Callers read decision.prefixes unconditionally.

    controller.reconcile uses it for set_size and internal_prefixes on every
    cycle, refusal or not. A refusal that returned no set would raise there
    once per interval for as long as the condition lasted.
    """
    current = frozenset({"1.0.0.1/32", "1.0.0.2/32"})
    over_cap = decide(current, frozenset({f"10.0.0.{n}/32" for n in range(5)}),
                      all_sources_ok=True, any_source_ok=True, max_entries=2)
    assert over_cap.reason == "over-cap"
    assert over_cap.prefixes == current

    no_sources = decide(current, frozenset(),
                        all_sources_ok=False, any_source_ok=False)
    assert no_sources.reason == "no-sources"
    assert no_sources.prefixes == current


def test_the_cap_admits_a_set_of_exactly_max_entries():
    # The boundary: max_entries is the largest allowed size, not the first
    # refused one.
    exactly = frozenset({f"10.0.0.{n}/32" for n in range(3)})
    d = decide(frozenset(), exactly, all_sources_ok=True, any_source_ok=True,
               max_entries=3)
    assert d.write is True
    assert d.prefixes == exactly
