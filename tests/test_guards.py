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


def test_refuses_a_write_that_would_halve_the_set():
    d = decide(frozenset({A, B, C, D}), frozenset({A}),
               all_sources_ok=True, any_source_ok=True)
    assert (d.write, d.reason) == (False, "shrink-guard")
    assert d.prefixes == frozenset({A, B, C, D})


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
