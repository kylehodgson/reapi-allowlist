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

    def __init__(self, name: str, namespace: str, service_defaults: dict) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )
        self._service_defaults = dict(service_defaults)

    def render(self, prefixes: Iterable[str]) -> dict:
        # Copy, so the caller's defaults are never mutated and every write
        # carries loadBalancerClass, ipFamilyPolicy and friends intact.
        service = dict(self._service_defaults)
        service["loadBalancerSourceRanges"] = sorted(prefixes)
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def extract(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])
