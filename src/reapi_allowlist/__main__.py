"""CLI: poll the sources on an interval and maintain the cluster object."""

import argparse
import asyncio
import logging
import time

import aiodns
import aiohttp
from aiohttp import web
from kubernetes_asyncio import client

from .controller import reconcile
from .decay import DEFAULT_WINDOW_SECONDS, FeederSet
from .emitters import CCGEmitter, CGCCEmitter
from .k8s import K8sClient, load_config
from .metrics import Metrics
from .sources import gather_sources

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)


def build_emitter(args):
    if args.emit == "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, args.namespace)


async def serve_metrics(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, args.metrics_port)

    async with client.ApiClient() as api_client, aiohttp.ClientSession() as session:
        k8s = K8sClient(client.CustomObjectsApi(api_client))
        seed_existing = True
        while True:
            try:
                sources = await gather_sources(
                    session, resolver,
                    ingest_dns=args.ingest_dns, ingest_port=args.ingest_port,
                    mlat_hosts=args.mlat_host, mlat_port=args.mlat_port,
                    mlat_dns=args.mlat_dns,
                )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


if __name__ == "__main__":
    asyncio.run(main())
