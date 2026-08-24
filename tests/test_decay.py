from reapi_allowlist.decay import FeederSet


def test_observed_prefix_is_active():
    fs = FeederSet(window_seconds=3600)
    fs.observe({"203.0.113.7/32"}, now=1000.0)
    assert fs.active(now=1000.0) == {"203.0.113.7/32"}


def test_prefix_stays_active_inside_the_window():
    fs = FeederSet(window_seconds=3600)
    fs.observe({"203.0.113.7/32"}, now=1000.0)
    assert fs.active(now=1000.0 + 3599) == {"203.0.113.7/32"}


def test_prefix_expires_after_the_window():
    fs = FeederSet(window_seconds=3600)
    fs.observe({"203.0.113.7/32"}, now=1000.0)
    assert fs.active(now=1000.0 + 3601) == set()


def test_reobserving_refreshes_the_deadline():
    fs = FeederSet(window_seconds=3600)
    fs.observe({"203.0.113.7/32"}, now=1000.0)
    fs.observe({"203.0.113.7/32"}, now=4000.0)
    assert fs.active(now=4000.0 + 3599) == {"203.0.113.7/32"}


def test_seed_treats_existing_entries_as_just_seen():
    # On startup we read the cluster object; everyone in it gets a full window.
    fs = FeederSet(window_seconds=3600)
    fs.seed({"198.51.100.20/32"}, now=1000.0)
    assert fs.active(now=1000.0 + 3599) == {"198.51.100.20/32"}


def test_prune_removes_expired_entries_and_reports_the_count():
    fs = FeederSet(window_seconds=3600)
    fs.observe({"203.0.113.7/32", "198.51.100.20/32"}, now=1000.0)
    fs.observe({"203.0.113.7/32"}, now=4000.0)
    assert fs.prune(now=4000.0 + 3601) == 2
    assert len(fs) == 0
