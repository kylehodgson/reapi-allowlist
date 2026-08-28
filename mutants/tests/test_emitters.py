from reapi_allowlist.emitters import CCGEmitter, CGCCEmitter

def test_ccg_ref_is_cluster_scoped_v2():
    ref = CCGEmitter().ref
    assert ref.api_version == "cilium.io/v2"
    assert ref.kind == "CiliumCIDRGroup"
    assert ref.plural == "ciliumcidrgroups"
    assert ref.namespace is None


def test_ccg_renders_sorted_external_cidrs():
    body = CCGEmitter().render({"1.0.0.2/32", "1.0.0.1/32"})
    assert body["spec"]["externalCIDRs"] == ["1.0.0.1/32", "1.0.0.2/32"]
    assert body["metadata"]["name"] == "adsblol-feeders"


def test_ccg_extract_reads_the_list_back():
    obj = {"spec": {"externalCIDRs": ["1.0.0.1/32", "2001:db8::1/128"]}}
    assert CCGEmitter().extract(obj) == {"1.0.0.1/32", "2001:db8::1/128"}


def test_ccg_extract_tolerates_a_missing_object():
    assert CCGEmitter().extract({}) == set()


def test_cgcc_ref_is_namespaced_v2alpha1():
    ref = CGCCEmitter("reapi-config", "adsblol").ref
    assert ref.api_version == "cilium.io/v2alpha1"
    assert ref.kind == "CiliumGatewayClassConfig"
    assert ref.plural == "ciliumgatewayclassconfigs"
    assert ref.namespace == "adsblol"


def test_cgcc_writes_only_the_source_ranges():
    """The controller owns loadBalancerSourceRanges and nothing else.

    Every other field belongs to whoever manages the manifest -- Flux, in
    production. This is a merge patch, so fields we omit are left untouched;
    sending them would silently revert operator changes on the next cycle.
    Sending externalTrafficPolicy in particular reintroduced `Local`, which
    leaves a Cilium Gateway with no address at all.
    """
    service = CGCCEmitter("reapi-config", "adsblol").render(
        {"1.0.0.2/32", "1.0.0.1/32"}
    )["spec"]["service"]
    assert service == {"loadBalancerSourceRanges": ["1.0.0.1/32", "1.0.0.2/32"]}


def test_cgcc_render_touches_no_other_spec_field():
    body = CGCCEmitter("reapi-config", "adsblol").render({"1.0.0.1/32"})
    assert set(body["spec"].keys()) == {"service"}


def test_cgcc_extract_reads_the_list_back():
    obj = {"spec": {"service": {"loadBalancerSourceRanges": ["1.0.0.1/32"]}}}
    emitter = CGCCEmitter("c", "adsblol")
    assert emitter.extract(obj) == {"1.0.0.1/32"}
