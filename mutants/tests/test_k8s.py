import pytest

from reapi_allowlist.emitters import ResourceRef
from reapi_allowlist.k8s import K8sClient

CLUSTER_REF = ResourceRef("cilium.io/v2", "CiliumCIDRGroup",
                          "ciliumcidrgroups", "adsblol-feeders", None)
NS_REF = ResourceRef("cilium.io/v2alpha1", "CiliumGatewayClassConfig",
                     "ciliumgatewayclassconfigs", "cfg", "adsblol")


class FakeApi:
    """Stands in for kubernetes_asyncio's CustomObjectsApi."""

    def __init__(self, obj=None, raise_status=None):
        self.obj = obj
        self.raise_status = raise_status
        self.calls = []

    async def get_cluster_custom_object(self, **kw):
        self.calls.append(("get_cluster", kw))
        return self._maybe()

    async def get_namespaced_custom_object(self, **kw):
        self.calls.append(("get_ns", kw))
        return self._maybe()

    async def patch_cluster_custom_object(self, **kw):
        self.calls.append(("patch_cluster", kw))

    async def patch_namespaced_custom_object(self, **kw):
        self.calls.append(("patch_ns", kw))

    def _maybe(self):
        if self.raise_status:
            from kubernetes_asyncio.client.exceptions import ApiException
            raise ApiException(status=self.raise_status)
        return self.obj


async def test_get_uses_the_cluster_scoped_call_for_a_cluster_ref():
    api = FakeApi(obj={"spec": {"externalCIDRs": ["1.0.0.1/32"]}})
    result = await K8sClient(api).get(CLUSTER_REF)
    assert result == {"spec": {"externalCIDRs": ["1.0.0.1/32"]}}
    assert api.calls[0][0] == "get_cluster"
    assert api.calls[0][1]["plural"] == "ciliumcidrgroups"


async def test_get_uses_the_namespaced_call_for_a_namespaced_ref():
    api = FakeApi(obj={})
    await K8sClient(api).get(NS_REF)
    assert api.calls[0][0] == "get_ns"
    assert api.calls[0][1]["namespace"] == "adsblol"


async def test_get_returns_none_when_the_object_does_not_exist():
    api = FakeApi(raise_status=404)
    assert await K8sClient(api).get(CLUSTER_REF) is None


async def test_get_reraises_other_api_errors():
    from kubernetes_asyncio.client.exceptions import ApiException
    api = FakeApi(raise_status=403)
    with pytest.raises(ApiException):
        await K8sClient(api).get(CLUSTER_REF)


async def test_patch_sends_the_body_to_the_cluster_scoped_call():
    api = FakeApi()
    body = {"spec": {"externalCIDRs": ["1.0.0.1/32"]}}
    await K8sClient(api).patch(CLUSTER_REF, body)
    kind, kw = api.calls[0]
    assert kind == "patch_cluster"
    assert kw["body"] == body
    assert kw["name"] == "adsblol-feeders"
    assert kw["_content_type"] == "application/merge-patch+json"


async def test_patch_sends_the_body_to_the_namespaced_call():
    api = FakeApi()
    body = {"spec": {"service": {"loadBalancerSourceRanges": ["1.0.0.1/32"]}}}
    await K8sClient(api).patch(NS_REF, body)
    kind, kw = api.calls[0]
    assert kind == "patch_ns"
    assert kw["body"] == body
    assert kw["namespace"] == "adsblol"
    assert kw["_content_type"] == "application/merge-patch+json"
