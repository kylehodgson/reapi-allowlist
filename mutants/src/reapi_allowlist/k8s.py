"""Read back and patch exactly one named object.

RBAC for this controller is get + patch on a single named resource per
emitter. If this module ever grows list, watch, create or delete, the RBAC
in the deployment manifests must grow with it -- so it does not.
"""

import logging

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from .emitters import ResourceRef

log = logging.getLogger(__name__)


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict


async def load_config() -> None:
    """In-cluster service account first, kubeconfig when running locally."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        await config.load_kube_config()
mutants_x__group_version__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x__group_version__mutmut)
def _group_version(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split("/", 1)
    return group, version


def x__group_version__mutmut_orig(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split("/", 1)
    return group, version


def x__group_version__mutmut_1(ref: ResourceRef) -> tuple[str, str]:
    group, version = None
    return group, version


def x__group_version__mutmut_2(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split(None, 1)
    return group, version


def x__group_version__mutmut_3(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split("/", None)
    return group, version


def x__group_version__mutmut_4(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split(1)
    return group, version


def x__group_version__mutmut_5(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split("/", )
    return group, version


def x__group_version__mutmut_6(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.rsplit("/", 1)
    return group, version


def x__group_version__mutmut_7(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split("XX/XX", 1)
    return group, version


def x__group_version__mutmut_8(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split("/", 2)
    return group, version

mutants_x__group_version__mutmut['_mutmut_orig'] = x__group_version__mutmut_orig # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_1'] = x__group_version__mutmut_1 # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_2'] = x__group_version__mutmut_2 # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_3'] = x__group_version__mutmut_3 # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_4'] = x__group_version__mutmut_4 # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_5'] = x__group_version__mutmut_5 # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_6'] = x__group_version__mutmut_6 # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_7'] = x__group_version__mutmut_7 # type: ignore # mutmut generated
mutants_x__group_version__mutmut['x__group_version__mutmut_8'] = x__group_version__mutmut_8 # type: ignore # mutmut generated
mutants_xǁK8sClientǁ__init____mutmut: MutantDict = {}  # type: ignore
mutants_xǁK8sClientǁget__mutmut: MutantDict = {}  # type: ignore
mutants_xǁK8sClientǁpatch__mutmut: MutantDict = {}  # type: ignore


class K8sClient:
    @_mutmut_mutated(mutants_xǁK8sClientǁ__init____mutmut)
    def __init__(self, api: client.CustomObjectsApi) -> None:
        self._api = api
    def xǁK8sClientǁ__init____mutmut_orig(self, api: client.CustomObjectsApi) -> None:
        self._api = api
    def xǁK8sClientǁ__init____mutmut_1(self, api: client.CustomObjectsApi) -> None:
        self._api = None

    @_mutmut_mutated(mutants_xǁK8sClientǁget__mutmut)
    async def get(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_orig(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_1(self, ref: ResourceRef) -> dict | None:
        group, version = None
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_2(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(None)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_3(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is not None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_4(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=None, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_5(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=None, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_6(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=None, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_7(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=None
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_8(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_9(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_10(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_11(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_12(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=None, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_13(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=None, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_14(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=None,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_15(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=None, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_16(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=None,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_17(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_18(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_19(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_20(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_21(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_22(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status != 404:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_23(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 405:
                log.info("%s/%s does not exist yet", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_24(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info(None, ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_25(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", None, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_26(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, None)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_27(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info(ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_28(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_29(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%s/%s does not exist yet", ref.kind, )
                return None
            raise

    async def xǁK8sClientǁget__mutmut_30(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("XX%s/%s does not exist yetXX", ref.kind, ref.name)
                return None
            raise

    async def xǁK8sClientǁget__mutmut_31(self, ref: ResourceRef) -> dict | None:
        group, version = _group_version(ref)
        try:
            if ref.namespace is None:
                return await self._api.get_cluster_custom_object(
                    group=group, version=version, plural=ref.plural, name=ref.name
                )
            return await self._api.get_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name,
            )
        except ApiException as exc:
            if exc.status == 404:
                log.info("%S/%S DOES NOT EXIST YET", ref.kind, ref.name)
                return None
            raise

    @_mutmut_mutated(mutants_xǁK8sClientǁpatch__mutmut)
    async def patch(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_orig(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_1(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = None
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_2(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(None)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_3(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is not None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_4(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=None, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_5(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=None, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_6(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=None,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_7(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=None, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_8(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=None,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_9(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type=None,
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_10(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_11(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_12(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_13(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_14(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_15(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_16(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="XXapplication/merge-patch+jsonXX",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_17(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="APPLICATION/MERGE-PATCH+JSON",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_18(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=None, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_19(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=None, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_20(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=None,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_21(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=None, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_22(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=None, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_23(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=None,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_24(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type=None,
            )

    async def xǁK8sClientǁpatch__mutmut_25(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_26(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_27(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, plural=ref.plural, name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_28(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_29(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, body=body,
                _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_30(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, _content_type="application/merge-patch+json",
            )

    async def xǁK8sClientǁpatch__mutmut_31(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                )

    async def xǁK8sClientǁpatch__mutmut_32(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="XXapplication/merge-patch+jsonXX",
            )

    async def xǁK8sClientǁpatch__mutmut_33(self, ref: ResourceRef, body: dict) -> None:
        """JSON merge patch, which replaces list fields wholesale -- what we want."""
        group, version = _group_version(ref)
        if ref.namespace is None:
            await self._api.patch_cluster_custom_object(
                group=group, version=version, plural=ref.plural,
                name=ref.name, body=body,
                _content_type="application/merge-patch+json",
            )
        else:
            await self._api.patch_namespaced_custom_object(
                group=group, version=version, namespace=ref.namespace,
                plural=ref.plural, name=ref.name, body=body,
                _content_type="APPLICATION/MERGE-PATCH+JSON",
            )

mutants_xǁK8sClientǁ__init____mutmut['_mutmut_orig'] = K8sClient.xǁK8sClientǁ__init____mutmut_orig # type: ignore # mutmut generated
mutants_xǁK8sClientǁ__init____mutmut['xǁK8sClientǁ__init____mutmut_1'] = K8sClient.xǁK8sClientǁ__init____mutmut_1 # type: ignore # mutmut generated

mutants_xǁK8sClientǁget__mutmut['_mutmut_orig'] = K8sClient.xǁK8sClientǁget__mutmut_orig # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_1'] = K8sClient.xǁK8sClientǁget__mutmut_1 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_2'] = K8sClient.xǁK8sClientǁget__mutmut_2 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_3'] = K8sClient.xǁK8sClientǁget__mutmut_3 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_4'] = K8sClient.xǁK8sClientǁget__mutmut_4 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_5'] = K8sClient.xǁK8sClientǁget__mutmut_5 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_6'] = K8sClient.xǁK8sClientǁget__mutmut_6 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_7'] = K8sClient.xǁK8sClientǁget__mutmut_7 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_8'] = K8sClient.xǁK8sClientǁget__mutmut_8 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_9'] = K8sClient.xǁK8sClientǁget__mutmut_9 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_10'] = K8sClient.xǁK8sClientǁget__mutmut_10 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_11'] = K8sClient.xǁK8sClientǁget__mutmut_11 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_12'] = K8sClient.xǁK8sClientǁget__mutmut_12 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_13'] = K8sClient.xǁK8sClientǁget__mutmut_13 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_14'] = K8sClient.xǁK8sClientǁget__mutmut_14 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_15'] = K8sClient.xǁK8sClientǁget__mutmut_15 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_16'] = K8sClient.xǁK8sClientǁget__mutmut_16 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_17'] = K8sClient.xǁK8sClientǁget__mutmut_17 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_18'] = K8sClient.xǁK8sClientǁget__mutmut_18 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_19'] = K8sClient.xǁK8sClientǁget__mutmut_19 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_20'] = K8sClient.xǁK8sClientǁget__mutmut_20 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_21'] = K8sClient.xǁK8sClientǁget__mutmut_21 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_22'] = K8sClient.xǁK8sClientǁget__mutmut_22 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_23'] = K8sClient.xǁK8sClientǁget__mutmut_23 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_24'] = K8sClient.xǁK8sClientǁget__mutmut_24 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_25'] = K8sClient.xǁK8sClientǁget__mutmut_25 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_26'] = K8sClient.xǁK8sClientǁget__mutmut_26 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_27'] = K8sClient.xǁK8sClientǁget__mutmut_27 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_28'] = K8sClient.xǁK8sClientǁget__mutmut_28 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_29'] = K8sClient.xǁK8sClientǁget__mutmut_29 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_30'] = K8sClient.xǁK8sClientǁget__mutmut_30 # type: ignore # mutmut generated
mutants_xǁK8sClientǁget__mutmut['xǁK8sClientǁget__mutmut_31'] = K8sClient.xǁK8sClientǁget__mutmut_31 # type: ignore # mutmut generated

mutants_xǁK8sClientǁpatch__mutmut['_mutmut_orig'] = K8sClient.xǁK8sClientǁpatch__mutmut_orig # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_1'] = K8sClient.xǁK8sClientǁpatch__mutmut_1 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_2'] = K8sClient.xǁK8sClientǁpatch__mutmut_2 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_3'] = K8sClient.xǁK8sClientǁpatch__mutmut_3 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_4'] = K8sClient.xǁK8sClientǁpatch__mutmut_4 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_5'] = K8sClient.xǁK8sClientǁpatch__mutmut_5 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_6'] = K8sClient.xǁK8sClientǁpatch__mutmut_6 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_7'] = K8sClient.xǁK8sClientǁpatch__mutmut_7 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_8'] = K8sClient.xǁK8sClientǁpatch__mutmut_8 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_9'] = K8sClient.xǁK8sClientǁpatch__mutmut_9 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_10'] = K8sClient.xǁK8sClientǁpatch__mutmut_10 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_11'] = K8sClient.xǁK8sClientǁpatch__mutmut_11 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_12'] = K8sClient.xǁK8sClientǁpatch__mutmut_12 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_13'] = K8sClient.xǁK8sClientǁpatch__mutmut_13 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_14'] = K8sClient.xǁK8sClientǁpatch__mutmut_14 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_15'] = K8sClient.xǁK8sClientǁpatch__mutmut_15 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_16'] = K8sClient.xǁK8sClientǁpatch__mutmut_16 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_17'] = K8sClient.xǁK8sClientǁpatch__mutmut_17 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_18'] = K8sClient.xǁK8sClientǁpatch__mutmut_18 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_19'] = K8sClient.xǁK8sClientǁpatch__mutmut_19 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_20'] = K8sClient.xǁK8sClientǁpatch__mutmut_20 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_21'] = K8sClient.xǁK8sClientǁpatch__mutmut_21 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_22'] = K8sClient.xǁK8sClientǁpatch__mutmut_22 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_23'] = K8sClient.xǁK8sClientǁpatch__mutmut_23 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_24'] = K8sClient.xǁK8sClientǁpatch__mutmut_24 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_25'] = K8sClient.xǁK8sClientǁpatch__mutmut_25 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_26'] = K8sClient.xǁK8sClientǁpatch__mutmut_26 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_27'] = K8sClient.xǁK8sClientǁpatch__mutmut_27 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_28'] = K8sClient.xǁK8sClientǁpatch__mutmut_28 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_29'] = K8sClient.xǁK8sClientǁpatch__mutmut_29 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_30'] = K8sClient.xǁK8sClientǁpatch__mutmut_30 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_31'] = K8sClient.xǁK8sClientǁpatch__mutmut_31 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_32'] = K8sClient.xǁK8sClientǁpatch__mutmut_32 # type: ignore # mutmut generated
mutants_xǁK8sClientǁpatch__mutmut['xǁK8sClientǁpatch__mutmut_33'] = K8sClient.xǁK8sClientǁpatch__mutmut_33 # type: ignore # mutmut generated
