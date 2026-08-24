from reapi_allowlist.emitters import CCGEmitter, CGCCEmitter

SERVICE_DEFAULTS = {
    "type": "LoadBalancer",
    "loadBalancerClass": "io.cilium/node",
    "externalTrafficPolicy": "Local",
    "ipFamilyPolicy": "RequireDualStack",
    "loadBalancerSourceRangesPolicy": "Allow",
}


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
    ref = CGCCEmitter("reapi-gateway-config", "adsblol", SERVICE_DEFAULTS).ref
    assert ref.api_version == "cilium.io/v2alpha1"
    assert ref.kind == "CiliumGatewayClassConfig"
    assert ref.plural == "ciliumgatewayclassconfigs"
    assert ref.namespace == "adsblol"


def test_cgcc_preserves_service_defaults_alongside_the_ranges():
    emitter = CGCCEmitter("reapi-gateway-config", "adsblol", SERVICE_DEFAULTS)
    service = emitter.render({"1.0.0.2/32", "1.0.0.1/32"})["spec"]["service"]
    assert service["loadBalancerSourceRanges"] == ["1.0.0.1/32", "1.0.0.2/32"]
    # Losing loadBalancerClass would leave the Gateway with no address.
    assert service["loadBalancerClass"] == "io.cilium/node"
    assert service["ipFamilyPolicy"] == "RequireDualStack"


def test_cgcc_render_does_not_mutate_the_defaults():
    defaults = dict(SERVICE_DEFAULTS)
    CGCCEmitter("c", "adsblol", defaults).render({"1.0.0.1/32"})
    assert "loadBalancerSourceRanges" not in defaults


def test_cgcc_extract_reads_the_list_back():
    obj = {"spec": {"service": {"loadBalancerSourceRanges": ["1.0.0.1/32"]}}}
    emitter = CGCCEmitter("c", "adsblol", SERVICE_DEFAULTS)
    assert emitter.extract(obj) == {"1.0.0.1/32"}
