"""Render the prefix set as a Cilium object.

Two shapes are supported so the enforcement decision stays open: a
CiliumCIDRGroup (referenced from a CiliumClusterwideNetworkPolicy) or a
CiliumGatewayClassConfig (whose loadBalancerSourceRanges Cilium enforces at
the LoadBalancer). See the design doc's "Enforcement" section.
"""

from collections.abc import Iterable
from dataclasses import dataclass


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict


@dataclass(frozen=True)
class ResourceRef:
    api_version: str
    kind: str
    plural: str
    name: str
    namespace: str | None
mutants_xǁCCGEmitterǁ__init____mutmut: MutantDict = {}  # type: ignore
mutants_xǁCCGEmitterǁrender__mutmut: MutantDict = {}  # type: ignore
mutants_xǁCCGEmitterǁextract__mutmut: MutantDict = {}  # type: ignore


class CCGEmitter:
    """CiliumCIDRGroup: cluster-scoped, one security identity for the whole group."""

    @_mutmut_mutated(mutants_xǁCCGEmitterǁ__init____mutmut)
    def __init__(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_orig(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_1(self, name: str = "XXadsblol-feedersXX") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_2(self, name: str = "ADSBLOL-FEEDERS") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_3(self, name: str = "adsblol-feeders") -> None:
        self.ref = None

    def xǁCCGEmitterǁ__init____mutmut_4(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version=None,
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_5(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind=None,
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_6(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural=None,
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_7(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=None,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_8(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_9(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_10(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_11(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_12(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            )

    def xǁCCGEmitterǁ__init____mutmut_13(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="XXcilium.io/v2XX",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_14(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="CILIUM.IO/V2",
            kind="CiliumCIDRGroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_15(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="XXCiliumCIDRGroupXX",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_16(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="ciliumcidrgroup",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_17(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CILIUMCIDRGROUP",
            plural="ciliumcidrgroups",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_18(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="XXciliumcidrgroupsXX",
            name=name,
            namespace=None,
        )

    def xǁCCGEmitterǁ__init____mutmut_19(self, name: str = "adsblol-feeders") -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2",
            kind="CiliumCIDRGroup",
            plural="CILIUMCIDRGROUPS",
            name=name,
            namespace=None,
        )

    @_mutmut_mutated(mutants_xǁCCGEmitterǁrender__mutmut)
    def render(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_orig(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_1(self, prefixes: Iterable[str]) -> dict:
        return {
            "XXapiVersionXX": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_2(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiversion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_3(self, prefixes: Iterable[str]) -> dict:
        return {
            "APIVERSION": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_4(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "XXkindXX": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_5(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "KIND": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_6(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "XXmetadataXX": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_7(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "METADATA": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_8(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"XXnameXX": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_9(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"NAME": self.ref.name},
            "spec": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_10(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "XXspecXX": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_11(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "SPEC": {"externalCIDRs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_12(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"XXexternalCIDRsXX": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_13(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalcidrs": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_14(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"EXTERNALCIDRS": sorted(prefixes)},
        }

    def xǁCCGEmitterǁrender__mutmut_15(self, prefixes: Iterable[str]) -> dict:
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name},
            "spec": {"externalCIDRs": sorted(None)},
        }

    @_mutmut_mutated(mutants_xǁCCGEmitterǁextract__mutmut)
    def extract(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_orig(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_1(self, obj: dict) -> set[str]:
        return set(None)

    def xǁCCGEmitterǁextract__mutmut_2(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get("externalCIDRs") and [])

    def xǁCCGEmitterǁextract__mutmut_3(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get(None) or [])

    def xǁCCGEmitterǁextract__mutmut_4(self, obj: dict) -> set[str]:
        return set((obj or {}).get(None, {}).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_5(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", None).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_6(self, obj: dict) -> set[str]:
        return set((obj or {}).get({}).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_7(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", ).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_8(self, obj: dict) -> set[str]:
        return set((obj and {}).get("spec", {}).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_9(self, obj: dict) -> set[str]:
        return set((obj or {}).get("XXspecXX", {}).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_10(self, obj: dict) -> set[str]:
        return set((obj or {}).get("SPEC", {}).get("externalCIDRs") or [])

    def xǁCCGEmitterǁextract__mutmut_11(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get("XXexternalCIDRsXX") or [])

    def xǁCCGEmitterǁextract__mutmut_12(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get("externalcidrs") or [])

    def xǁCCGEmitterǁextract__mutmut_13(self, obj: dict) -> set[str]:
        return set((obj or {}).get("spec", {}).get("EXTERNALCIDRS") or [])

mutants_xǁCCGEmitterǁ__init____mutmut['_mutmut_orig'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_orig # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_1'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_1 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_2'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_2 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_3'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_3 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_4'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_4 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_5'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_5 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_6'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_6 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_7'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_7 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_8'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_8 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_9'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_9 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_10'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_10 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_11'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_11 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_12'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_12 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_13'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_13 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_14'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_14 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_15'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_15 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_16'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_16 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_17'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_17 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_18'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_18 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁ__init____mutmut['xǁCCGEmitterǁ__init____mutmut_19'] = CCGEmitter.xǁCCGEmitterǁ__init____mutmut_19 # type: ignore # mutmut generated

mutants_xǁCCGEmitterǁrender__mutmut['_mutmut_orig'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_orig # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_1'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_1 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_2'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_2 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_3'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_3 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_4'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_4 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_5'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_5 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_6'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_6 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_7'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_7 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_8'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_8 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_9'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_9 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_10'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_10 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_11'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_11 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_12'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_12 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_13'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_13 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_14'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_14 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁrender__mutmut['xǁCCGEmitterǁrender__mutmut_15'] = CCGEmitter.xǁCCGEmitterǁrender__mutmut_15 # type: ignore # mutmut generated

mutants_xǁCCGEmitterǁextract__mutmut['_mutmut_orig'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_orig # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_1'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_1 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_2'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_2 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_3'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_3 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_4'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_4 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_5'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_5 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_6'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_6 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_7'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_7 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_8'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_8 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_9'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_9 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_10'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_10 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_11'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_11 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_12'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_12 # type: ignore # mutmut generated
mutants_xǁCCGEmitterǁextract__mutmut['xǁCCGEmitterǁextract__mutmut_13'] = CCGEmitter.xǁCCGEmitterǁextract__mutmut_13 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut: MutantDict = {}  # type: ignore
mutants_xǁCGCCEmitterǁrender__mutmut: MutantDict = {}  # type: ignore
mutants_xǁCGCCEmitterǁextract__mutmut: MutantDict = {}  # type: ignore


class CGCCEmitter:
    """CiliumGatewayClassConfig: namespaced, v2alpha1 only as of Cilium 1.20.1."""

    @_mutmut_mutated(mutants_xǁCGCCEmitterǁ__init____mutmut)
    def __init__(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_orig(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_1(self, name: str, namespace: str) -> None:
        self.ref = None

    def xǁCGCCEmitterǁ__init____mutmut_2(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version=None,
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_3(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind=None,
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_4(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural=None,
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_5(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=None,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_6(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=None,
        )

    def xǁCGCCEmitterǁ__init____mutmut_7(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_8(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_9(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_10(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_11(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            )

    def xǁCGCCEmitterǁ__init____mutmut_12(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="XXcilium.io/v2alpha1XX",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_13(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="CILIUM.IO/V2ALPHA1",
            kind="CiliumGatewayClassConfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_14(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="XXCiliumGatewayClassConfigXX",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_15(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="ciliumgatewayclassconfig",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_16(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CILIUMGATEWAYCLASSCONFIG",
            plural="ciliumgatewayclassconfigs",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_17(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="XXciliumgatewayclassconfigsXX",
            name=name,
            namespace=namespace,
        )

    def xǁCGCCEmitterǁ__init____mutmut_18(self, name: str, namespace: str) -> None:
        self.ref = ResourceRef(
            api_version="cilium.io/v2alpha1",
            kind="CiliumGatewayClassConfig",
            plural="CILIUMGATEWAYCLASSCONFIGS",
            name=name,
            namespace=namespace,
        )

    @_mutmut_mutated(mutants_xǁCGCCEmitterǁrender__mutmut)
    def render(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_orig(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_1(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = None
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_2(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"XXloadBalancerSourceRangesXX": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_3(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadbalancersourceranges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_4(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"LOADBALANCERSOURCERANGES": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_5(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(None)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_6(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "XXapiVersionXX": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_7(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiversion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_8(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "APIVERSION": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_9(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "XXkindXX": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_10(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "KIND": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_11(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "XXmetadataXX": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_12(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "METADATA": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_13(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"XXnameXX": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_14(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"NAME": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_15(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "XXnamespaceXX": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_16(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "NAMESPACE": self.ref.namespace},
            "spec": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_17(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "XXspecXX": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_18(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "SPEC": {"service": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_19(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"XXserviceXX": service},
        }

    def xǁCGCCEmitterǁrender__mutmut_20(self, prefixes: Iterable[str]) -> dict:
        # Source ranges and nothing else: omitted fields keep the manifest's values.
        service = {"loadBalancerSourceRanges": sorted(prefixes)}
        return {
            "apiVersion": self.ref.api_version,
            "kind": self.ref.kind,
            "metadata": {"name": self.ref.name, "namespace": self.ref.namespace},
            "spec": {"SERVICE": service},
        }

    @_mutmut_mutated(mutants_xǁCGCCEmitterǁextract__mutmut)
    def extract(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_orig(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_1(self, obj: dict) -> set[str]:
        service = None
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_2(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") and {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_3(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get(None) or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_4(self, obj: dict) -> set[str]:
        service = (obj or {}).get(None, {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_5(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", None).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_6(self, obj: dict) -> set[str]:
        service = (obj or {}).get({}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_7(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", ).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_8(self, obj: dict) -> set[str]:
        service = (obj and {}).get("spec", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_9(self, obj: dict) -> set[str]:
        service = (obj or {}).get("XXspecXX", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_10(self, obj: dict) -> set[str]:
        service = (obj or {}).get("SPEC", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_11(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("XXserviceXX") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_12(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("SERVICE") or {}
        return set(service.get("loadBalancerSourceRanges") or [])

    def xǁCGCCEmitterǁextract__mutmut_13(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(None)

    def xǁCGCCEmitterǁextract__mutmut_14(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("loadBalancerSourceRanges") and [])

    def xǁCGCCEmitterǁextract__mutmut_15(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get(None) or [])

    def xǁCGCCEmitterǁextract__mutmut_16(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("XXloadBalancerSourceRangesXX") or [])

    def xǁCGCCEmitterǁextract__mutmut_17(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("loadbalancersourceranges") or [])

    def xǁCGCCEmitterǁextract__mutmut_18(self, obj: dict) -> set[str]:
        service = (obj or {}).get("spec", {}).get("service") or {}
        return set(service.get("LOADBALANCERSOURCERANGES") or [])

mutants_xǁCGCCEmitterǁ__init____mutmut['_mutmut_orig'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_orig # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_1'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_1 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_2'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_2 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_3'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_3 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_4'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_4 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_5'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_5 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_6'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_6 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_7'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_7 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_8'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_8 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_9'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_9 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_10'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_10 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_11'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_11 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_12'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_12 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_13'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_13 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_14'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_14 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_15'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_15 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_16'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_16 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_17'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_17 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁ__init____mutmut['xǁCGCCEmitterǁ__init____mutmut_18'] = CGCCEmitter.xǁCGCCEmitterǁ__init____mutmut_18 # type: ignore # mutmut generated

mutants_xǁCGCCEmitterǁrender__mutmut['_mutmut_orig'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_orig # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_1'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_1 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_2'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_2 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_3'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_3 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_4'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_4 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_5'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_5 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_6'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_6 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_7'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_7 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_8'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_8 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_9'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_9 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_10'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_10 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_11'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_11 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_12'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_12 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_13'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_13 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_14'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_14 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_15'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_15 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_16'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_16 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_17'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_17 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_18'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_18 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_19'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_19 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁrender__mutmut['xǁCGCCEmitterǁrender__mutmut_20'] = CGCCEmitter.xǁCGCCEmitterǁrender__mutmut_20 # type: ignore # mutmut generated

mutants_xǁCGCCEmitterǁextract__mutmut['_mutmut_orig'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_orig # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_1'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_1 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_2'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_2 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_3'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_3 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_4'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_4 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_5'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_5 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_6'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_6 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_7'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_7 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_8'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_8 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_9'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_9 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_10'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_10 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_11'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_11 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_12'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_12 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_13'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_13 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_14'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_14 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_15'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_15 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_16'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_16 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_17'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_17 # type: ignore # mutmut generated
mutants_xǁCGCCEmitterǁextract__mutmut['xǁCGCCEmitterǁextract__mutmut_18'] = CGCCEmitter.xǁCGCCEmitterǁextract__mutmut_18 # type: ignore # mutmut generated
