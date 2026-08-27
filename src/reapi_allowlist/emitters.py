"""Render the prefix set as a Cilium object.

Two shapes are supported so the enforcement decision stays open: a
CiliumCIDRGroup (referenced from a CiliumClusterwideNetworkPolicy) or a
CiliumGatewayClassConfig (whose loadBalancerSourceRanges Cilium enforces at
the LoadBalancer). See the design doc's "Enforcement" section.
"""

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class ResourceRef:
    api_version: str
    kind: str
    plural: str
    name: str
    namespace: str | None


class CCGEmitter:
    """CiliumCIDRGroup: cluster-scoped, one security identity for the whole group."""

    def __init__(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def render(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def extract(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get("externalCIDRs") or [])


class CGCCEmitter:
    """CiliumGatewayClassConfig: namespaced, v2alpha1 only as of Cilium 1.20.1."""

    def __init__(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def render(self, prefixes: Iterable[str]) -> dict:
        # Write loadBalancerSourceRanges and NOTHING else. This is a merge
        # patch, so every field we omit keeps whatever the manifest set. An
        # earlier version sent a whole block of service defaults with each
        # write, which silently reverted operator changes once a minute and
        # reimposed externalTrafficPolicy: Local -- under which a Cilium
        # Gateway's selector-less Service gets no address at all.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def extract(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])
