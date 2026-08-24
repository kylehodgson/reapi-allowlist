from reapi_allowlist.metrics import Metrics


def test_render_emits_the_core_series():
    m = Metrics()
    m.set_size = 5802
    m.anomalies = 0
    m.last_success = 1000.0
    out = m.render(now=1060.0)
    assert "adsb_reapi_allowlist_size 5802" in out
    assert "adsb_reapi_allowlist_parse_anomalies 0" in out
    assert "adsb_reapi_allowlist_seconds_since_success 60" in out


def test_refusals_render_one_series_per_reason():
    m = Metrics()
    m.refusals["shrink-guard"] = 2
    out = m.render(now=0.0)
    assert 'adsb_reapi_allowlist_refusals{reason="shrink-guard"} 2' in out


def test_seconds_since_success_is_minus_one_before_any_success():
    m = Metrics()
    assert "adsb_reapi_allowlist_seconds_since_success -1" in m.render(now=1000.0)
