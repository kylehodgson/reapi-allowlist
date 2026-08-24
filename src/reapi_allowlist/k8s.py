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


async def load_config() -> None:
    """In-cluster service account first, kubeconfig when running locally."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        await config.load_kube_config()


def _group_version(ref: ResourceRef) -> tuple[str, str]:
    group, version = ref.api_version.split("/", 1)
    return group, version


class K8sClient:
    def __init__(self, api: client.CustomObjectsApi) -> None:
        self._api = api

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
