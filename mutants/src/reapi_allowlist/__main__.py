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


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict
mutants_x_parse_args__mutmut: MutantDict = {}  # type: ignore

@_mutmut_mutated(mutants_x_parse_args__mutmut)
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

def x_parse_args__mutmut_orig(argv=None):
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

def x_parse_args__mutmut_1(argv=None):
    p = None
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

def x_parse_args__mutmut_2(argv=None):
    p = argparse.ArgumentParser(prog=None)
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

def x_parse_args__mutmut_3(argv=None):
    p = argparse.ArgumentParser(prog="XXreapi-allowlistXX")
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

def x_parse_args__mutmut_4(argv=None):
    p = argparse.ArgumentParser(prog="REAPI-ALLOWLIST")
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

def x_parse_args__mutmut_5(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument(None, choices=["ccg", "cgcc"], default="ccg")
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

def x_parse_args__mutmut_6(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=None, default="ccg")
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

def x_parse_args__mutmut_7(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default=None)
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

def x_parse_args__mutmut_8(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument(choices=["ccg", "cgcc"], default="ccg")
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

def x_parse_args__mutmut_9(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", default="ccg")
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

def x_parse_args__mutmut_10(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], )
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

def x_parse_args__mutmut_11(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("XX--emitXX", choices=["ccg", "cgcc"], default="ccg")
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

def x_parse_args__mutmut_12(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--EMIT", choices=["ccg", "cgcc"], default="ccg")
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

def x_parse_args__mutmut_13(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["XXccgXX", "cgcc"], default="ccg")
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

def x_parse_args__mutmut_14(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["CCG", "cgcc"], default="ccg")
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

def x_parse_args__mutmut_15(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "XXcgccXX"], default="ccg")
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

def x_parse_args__mutmut_16(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "CGCC"], default="ccg")
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

def x_parse_args__mutmut_17(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="XXccgXX")
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

def x_parse_args__mutmut_18(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="CCG")
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

def x_parse_args__mutmut_19(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument(None, default="adsblol-feeders")
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

def x_parse_args__mutmut_20(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default=None)
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

def x_parse_args__mutmut_21(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument(default="adsblol-feeders")
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

def x_parse_args__mutmut_22(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", )
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

def x_parse_args__mutmut_23(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("XX--nameXX", default="adsblol-feeders")
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

def x_parse_args__mutmut_24(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--NAME", default="adsblol-feeders")
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

def x_parse_args__mutmut_25(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="XXadsblol-feedersXX")
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

def x_parse_args__mutmut_26(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="ADSBLOL-FEEDERS")
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

def x_parse_args__mutmut_27(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument(None, default="adsblol")
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

def x_parse_args__mutmut_28(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default=None)
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

def x_parse_args__mutmut_29(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument(default="adsblol")
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

def x_parse_args__mutmut_30(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", )
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

def x_parse_args__mutmut_31(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("XX--namespaceXX", default="adsblol")
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

def x_parse_args__mutmut_32(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--NAMESPACE", default="adsblol")
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

def x_parse_args__mutmut_33(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="XXadsblolXX")
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

def x_parse_args__mutmut_34(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="ADSBLOL")
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

def x_parse_args__mutmut_35(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument(None, type=int, default=60)
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

def x_parse_args__mutmut_36(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=None, default=60)
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

def x_parse_args__mutmut_37(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=None)
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

def x_parse_args__mutmut_38(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument(type=int, default=60)
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

def x_parse_args__mutmut_39(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", default=60)
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

def x_parse_args__mutmut_40(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, )
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

def x_parse_args__mutmut_41(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("XX--intervalXX", type=int, default=60)
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

def x_parse_args__mutmut_42(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--INTERVAL", type=int, default=60)
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

def x_parse_args__mutmut_43(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=61)
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

def x_parse_args__mutmut_44(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument(None, type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_45(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=None, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_46(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=None)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_47(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument(type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_48(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_49(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, )
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_50(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("XX--windowXX", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_51(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--WINDOW", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_52(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument(None, default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_53(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default=None)
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_54(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument(default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_55(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", )
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_56(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("XX--ingest-dnsXX", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_57(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--INGEST-DNS", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_58(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="XXingest-readsb-headless.adsblol.svc.cluster.localXX")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_59(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="INGEST-READSB-HEADLESS.ADSBLOL.SVC.CLUSTER.LOCAL")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_60(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument(None, type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_61(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=None, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_62(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=None)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_63(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument(type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_64(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_65(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, )
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_66(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("XX--ingest-portXX", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_67(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--INGEST-PORT", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_68(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=151)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_69(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument(None, action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_70(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action=None, default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_71(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=None)
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_72(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument(action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_73(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_74(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", )
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_75(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("XX--mlat-hostXX", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_76(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--MLAT-HOST", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_77(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="XXappendXX", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_78(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="APPEND", default=[])
    p.add_argument("--mlat-port", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_79(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument(None, type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_80(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=None, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_81(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=None)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_82(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument(type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_83(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_84(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, )
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_85(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("XX--mlat-portXX", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_86(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--MLAT-PORT", type=int, default=150)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_87(argv=None):
    p = argparse.ArgumentParser(prog="reapi-allowlist")
    p.add_argument("--emit", choices=["ccg", "cgcc"], default="ccg")
    p.add_argument("--name", default="adsblol-feeders")
    p.add_argument("--namespace", default="adsblol")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW_SECONDS)
    p.add_argument("--ingest-dns", default="ingest-readsb-headless.adsblol.svc.cluster.local")
    p.add_argument("--ingest-port", type=int, default=150)
    p.add_argument("--mlat-host", action="append", default=[])
    p.add_argument("--mlat-port", type=int, default=151)
    p.add_argument("--mlat-dns", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_88(argv=None):
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
    p.add_argument(None, default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_89(argv=None):
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
                   help=None)
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_90(argv=None):
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
    p.add_argument(default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_91(argv=None):
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
    p.add_argument("--mlat-dns", help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_92(argv=None):
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
                   )
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_93(argv=None):
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
    p.add_argument("XX--mlat-dnsXX", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_94(argv=None):
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
    p.add_argument("--MLAT-DNS", default=None,
                   help="headless Service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_95(argv=None):
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
                   help="XXheadless Service resolved to mlat pod addresses, the XX"
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_96(argv=None):
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
                   help="headless service resolved to mlat pod addresses, the "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_97(argv=None):
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
                   help="HEADLESS SERVICE RESOLVED TO MLAT POD ADDRESSES, THE "
                        "way --ingest-dns works. Combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_98(argv=None):
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
                        "XXway --ingest-dns works. Combined with any --mlat-host.XX")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_99(argv=None):
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
                        "way --ingest-dns works. combined with any --mlat-host.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_100(argv=None):
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
                        "WAY --INGEST-DNS WORKS. COMBINED WITH ANY --MLAT-HOST.")
    p.add_argument("--metrics-port", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_101(argv=None):
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
    p.add_argument(None, type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_102(argv=None):
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
    p.add_argument("--metrics-port", type=None, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_103(argv=None):
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
    p.add_argument("--metrics-port", type=int, default=None)
    return p.parse_args(argv)

def x_parse_args__mutmut_104(argv=None):
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
    p.add_argument(type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_105(argv=None):
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
    p.add_argument("--metrics-port", default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_106(argv=None):
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
    p.add_argument("--metrics-port", type=int, )
    return p.parse_args(argv)

def x_parse_args__mutmut_107(argv=None):
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
    p.add_argument("XX--metrics-portXX", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_108(argv=None):
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
    p.add_argument("--METRICS-PORT", type=int, default=9090)
    return p.parse_args(argv)

def x_parse_args__mutmut_109(argv=None):
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
    p.add_argument("--metrics-port", type=int, default=9091)
    return p.parse_args(argv)

def x_parse_args__mutmut_110(argv=None):
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
    return p.parse_args(None)

mutants_x_parse_args__mutmut['_mutmut_orig'] = x_parse_args__mutmut_orig # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_1'] = x_parse_args__mutmut_1 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_2'] = x_parse_args__mutmut_2 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_3'] = x_parse_args__mutmut_3 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_4'] = x_parse_args__mutmut_4 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_5'] = x_parse_args__mutmut_5 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_6'] = x_parse_args__mutmut_6 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_7'] = x_parse_args__mutmut_7 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_8'] = x_parse_args__mutmut_8 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_9'] = x_parse_args__mutmut_9 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_10'] = x_parse_args__mutmut_10 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_11'] = x_parse_args__mutmut_11 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_12'] = x_parse_args__mutmut_12 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_13'] = x_parse_args__mutmut_13 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_14'] = x_parse_args__mutmut_14 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_15'] = x_parse_args__mutmut_15 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_16'] = x_parse_args__mutmut_16 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_17'] = x_parse_args__mutmut_17 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_18'] = x_parse_args__mutmut_18 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_19'] = x_parse_args__mutmut_19 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_20'] = x_parse_args__mutmut_20 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_21'] = x_parse_args__mutmut_21 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_22'] = x_parse_args__mutmut_22 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_23'] = x_parse_args__mutmut_23 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_24'] = x_parse_args__mutmut_24 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_25'] = x_parse_args__mutmut_25 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_26'] = x_parse_args__mutmut_26 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_27'] = x_parse_args__mutmut_27 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_28'] = x_parse_args__mutmut_28 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_29'] = x_parse_args__mutmut_29 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_30'] = x_parse_args__mutmut_30 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_31'] = x_parse_args__mutmut_31 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_32'] = x_parse_args__mutmut_32 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_33'] = x_parse_args__mutmut_33 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_34'] = x_parse_args__mutmut_34 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_35'] = x_parse_args__mutmut_35 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_36'] = x_parse_args__mutmut_36 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_37'] = x_parse_args__mutmut_37 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_38'] = x_parse_args__mutmut_38 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_39'] = x_parse_args__mutmut_39 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_40'] = x_parse_args__mutmut_40 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_41'] = x_parse_args__mutmut_41 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_42'] = x_parse_args__mutmut_42 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_43'] = x_parse_args__mutmut_43 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_44'] = x_parse_args__mutmut_44 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_45'] = x_parse_args__mutmut_45 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_46'] = x_parse_args__mutmut_46 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_47'] = x_parse_args__mutmut_47 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_48'] = x_parse_args__mutmut_48 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_49'] = x_parse_args__mutmut_49 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_50'] = x_parse_args__mutmut_50 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_51'] = x_parse_args__mutmut_51 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_52'] = x_parse_args__mutmut_52 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_53'] = x_parse_args__mutmut_53 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_54'] = x_parse_args__mutmut_54 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_55'] = x_parse_args__mutmut_55 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_56'] = x_parse_args__mutmut_56 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_57'] = x_parse_args__mutmut_57 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_58'] = x_parse_args__mutmut_58 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_59'] = x_parse_args__mutmut_59 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_60'] = x_parse_args__mutmut_60 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_61'] = x_parse_args__mutmut_61 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_62'] = x_parse_args__mutmut_62 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_63'] = x_parse_args__mutmut_63 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_64'] = x_parse_args__mutmut_64 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_65'] = x_parse_args__mutmut_65 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_66'] = x_parse_args__mutmut_66 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_67'] = x_parse_args__mutmut_67 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_68'] = x_parse_args__mutmut_68 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_69'] = x_parse_args__mutmut_69 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_70'] = x_parse_args__mutmut_70 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_71'] = x_parse_args__mutmut_71 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_72'] = x_parse_args__mutmut_72 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_73'] = x_parse_args__mutmut_73 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_74'] = x_parse_args__mutmut_74 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_75'] = x_parse_args__mutmut_75 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_76'] = x_parse_args__mutmut_76 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_77'] = x_parse_args__mutmut_77 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_78'] = x_parse_args__mutmut_78 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_79'] = x_parse_args__mutmut_79 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_80'] = x_parse_args__mutmut_80 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_81'] = x_parse_args__mutmut_81 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_82'] = x_parse_args__mutmut_82 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_83'] = x_parse_args__mutmut_83 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_84'] = x_parse_args__mutmut_84 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_85'] = x_parse_args__mutmut_85 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_86'] = x_parse_args__mutmut_86 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_87'] = x_parse_args__mutmut_87 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_88'] = x_parse_args__mutmut_88 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_89'] = x_parse_args__mutmut_89 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_90'] = x_parse_args__mutmut_90 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_91'] = x_parse_args__mutmut_91 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_92'] = x_parse_args__mutmut_92 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_93'] = x_parse_args__mutmut_93 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_94'] = x_parse_args__mutmut_94 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_95'] = x_parse_args__mutmut_95 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_96'] = x_parse_args__mutmut_96 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_97'] = x_parse_args__mutmut_97 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_98'] = x_parse_args__mutmut_98 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_99'] = x_parse_args__mutmut_99 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_100'] = x_parse_args__mutmut_100 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_101'] = x_parse_args__mutmut_101 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_102'] = x_parse_args__mutmut_102 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_103'] = x_parse_args__mutmut_103 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_104'] = x_parse_args__mutmut_104 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_105'] = x_parse_args__mutmut_105 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_106'] = x_parse_args__mutmut_106 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_107'] = x_parse_args__mutmut_107 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_108'] = x_parse_args__mutmut_108 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_109'] = x_parse_args__mutmut_109 # type: ignore # mutmut generated
mutants_x_parse_args__mutmut['x_parse_args__mutmut_110'] = x_parse_args__mutmut_110 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_build_emitter__mutmut)
def build_emitter(args):
    if args.emit == "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, args.namespace)


def x_build_emitter__mutmut_orig(args):
    if args.emit == "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, args.namespace)


def x_build_emitter__mutmut_1(args):
    if args.emit != "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, args.namespace)


def x_build_emitter__mutmut_2(args):
    if args.emit == "XXccgXX":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, args.namespace)


def x_build_emitter__mutmut_3(args):
    if args.emit == "CCG":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, args.namespace)


def x_build_emitter__mutmut_4(args):
    if args.emit == "ccg":
        return CCGEmitter(None)
    return CGCCEmitter(args.name, args.namespace)


def x_build_emitter__mutmut_5(args):
    if args.emit == "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(None, args.namespace)


def x_build_emitter__mutmut_6(args):
    if args.emit == "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, None)


def x_build_emitter__mutmut_7(args):
    if args.emit == "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.namespace)


def x_build_emitter__mutmut_8(args):
    if args.emit == "ccg":
        return CCGEmitter(args.name)
    return CGCCEmitter(args.name, )

mutants_x_build_emitter__mutmut['_mutmut_orig'] = x_build_emitter__mutmut_orig # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_1'] = x_build_emitter__mutmut_1 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_2'] = x_build_emitter__mutmut_2 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_3'] = x_build_emitter__mutmut_3 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_4'] = x_build_emitter__mutmut_4 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_5'] = x_build_emitter__mutmut_5 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_6'] = x_build_emitter__mutmut_6 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_7'] = x_build_emitter__mutmut_7 # type: ignore # mutmut generated
mutants_x_build_emitter__mutmut['x_build_emitter__mutmut_8'] = x_build_emitter__mutmut_8 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_serve_metrics__mutmut)
async def serve_metrics(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_orig(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_1(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=None,
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_2(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type=None)

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_3(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_4(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            )

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_5(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(None),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_6(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="XXtext/plainXX")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_7(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="TEXT/PLAIN")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_8(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = None
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_9(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get(None, handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_10(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", None)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_11(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get(handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_12(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", )
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_13(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("XX/metricsXX", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_14(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/METRICS", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_15(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = None
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_16(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_17(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(None, "0.0.0.0", port).start()


async def x_serve_metrics__mutmut_18(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, None, port).start()


async def x_serve_metrics__mutmut_19(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", None).start()


async def x_serve_metrics__mutmut_20(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite("0.0.0.0", port).start()


async def x_serve_metrics__mutmut_21(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, port).start()


async def x_serve_metrics__mutmut_22(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", ).start()


async def x_serve_metrics__mutmut_23(metrics: Metrics, port: int) -> None:
    async def handler(_request):
        return web.Response(text=metrics.render(time.time()),
                            content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "XX0.0.0.0XX", port).start()

mutants_x_serve_metrics__mutmut['_mutmut_orig'] = x_serve_metrics__mutmut_orig # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_1'] = x_serve_metrics__mutmut_1 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_2'] = x_serve_metrics__mutmut_2 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_3'] = x_serve_metrics__mutmut_3 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_4'] = x_serve_metrics__mutmut_4 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_5'] = x_serve_metrics__mutmut_5 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_6'] = x_serve_metrics__mutmut_6 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_7'] = x_serve_metrics__mutmut_7 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_8'] = x_serve_metrics__mutmut_8 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_9'] = x_serve_metrics__mutmut_9 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_10'] = x_serve_metrics__mutmut_10 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_11'] = x_serve_metrics__mutmut_11 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_12'] = x_serve_metrics__mutmut_12 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_13'] = x_serve_metrics__mutmut_13 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_14'] = x_serve_metrics__mutmut_14 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_15'] = x_serve_metrics__mutmut_15 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_16'] = x_serve_metrics__mutmut_16 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_17'] = x_serve_metrics__mutmut_17 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_18'] = x_serve_metrics__mutmut_18 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_19'] = x_serve_metrics__mutmut_19 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_20'] = x_serve_metrics__mutmut_20 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_21'] = x_serve_metrics__mutmut_21 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_22'] = x_serve_metrics__mutmut_22 # type: ignore # mutmut generated
mutants_x_serve_metrics__mutmut['x_serve_metrics__mutmut_23'] = x_serve_metrics__mutmut_23 # type: ignore # mutmut generated
mutants_x_main__mutmut: MutantDict = {}  # type: ignore


@_mutmut_mutated(mutants_x_main__mutmut)
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


async def x_main__mutmut_orig() -> None:
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


async def x_main__mutmut_1() -> None:
    logging.basicConfig(level=None, format="%(levelname)s %(name)s %(message)s")
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


async def x_main__mutmut_2() -> None:
    logging.basicConfig(level=logging.INFO, format=None)
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


async def x_main__mutmut_3() -> None:
    logging.basicConfig(format="%(levelname)s %(name)s %(message)s")
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


async def x_main__mutmut_4() -> None:
    logging.basicConfig(level=logging.INFO, )
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


async def x_main__mutmut_5() -> None:
    logging.basicConfig(level=logging.INFO, format="XX%(levelname)s %(name)s %(message)sXX")
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


async def x_main__mutmut_6() -> None:
    logging.basicConfig(level=logging.INFO, format="%(LEVELNAME)S %(NAME)S %(MESSAGE)S")
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


async def x_main__mutmut_7() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = None

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


async def x_main__mutmut_8() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = None
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


async def x_main__mutmut_9() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=None)
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


async def x_main__mutmut_10() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = None
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


async def x_main__mutmut_11() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(None)
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


async def x_main__mutmut_12() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = None

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


async def x_main__mutmut_13() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(None, args.metrics_port)

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


async def x_main__mutmut_14() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, None)

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


async def x_main__mutmut_15() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(args.metrics_port)

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


async def x_main__mutmut_16() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, )

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


async def x_main__mutmut_17() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, args.metrics_port)

    async with client.ApiClient() as api_client, aiohttp.ClientSession() as session:
        k8s = None
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


async def x_main__mutmut_18() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, args.metrics_port)

    async with client.ApiClient() as api_client, aiohttp.ClientSession() as session:
        k8s = K8sClient(None)
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


async def x_main__mutmut_19() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, args.metrics_port)

    async with client.ApiClient() as api_client, aiohttp.ClientSession() as session:
        k8s = K8sClient(client.CustomObjectsApi(None))
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


async def x_main__mutmut_20() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, args.metrics_port)

    async with client.ApiClient() as api_client, aiohttp.ClientSession() as session:
        k8s = K8sClient(client.CustomObjectsApi(api_client))
        seed_existing = None
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


async def x_main__mutmut_21() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    await load_config()
    metrics, feeders = Metrics(), FeederSet(window_seconds=args.window)
    emitter = build_emitter(args)
    resolver = aiodns.DNSResolver()

    await serve_metrics(metrics, args.metrics_port)

    async with client.ApiClient() as api_client, aiohttp.ClientSession() as session:
        k8s = K8sClient(client.CustomObjectsApi(api_client))
        seed_existing = False
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


async def x_main__mutmut_22() -> None:
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
        while False:
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


async def x_main__mutmut_23() -> None:
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
                sources = None
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_24() -> None:
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
                    None, resolver,
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


async def x_main__mutmut_25() -> None:
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
                    session, None,
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


async def x_main__mutmut_26() -> None:
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
                    ingest_dns=None, ingest_port=args.ingest_port,
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


async def x_main__mutmut_27() -> None:
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
                    ingest_dns=args.ingest_dns, ingest_port=None,
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


async def x_main__mutmut_28() -> None:
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
                    mlat_hosts=None, mlat_port=args.mlat_port,
                    mlat_dns=args.mlat_dns,
                )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_29() -> None:
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
                    mlat_hosts=args.mlat_host, mlat_port=None,
                    mlat_dns=args.mlat_dns,
                )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_30() -> None:
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
                    mlat_dns=None,
                )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_31() -> None:
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
                    resolver,
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


async def x_main__mutmut_32() -> None:
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
                    session, ingest_dns=args.ingest_dns, ingest_port=args.ingest_port,
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


async def x_main__mutmut_33() -> None:
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
                    ingest_port=args.ingest_port,
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


async def x_main__mutmut_34() -> None:
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
                    ingest_dns=args.ingest_dns, mlat_hosts=args.mlat_host, mlat_port=args.mlat_port,
                    mlat_dns=args.mlat_dns,
                )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_35() -> None:
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
                    mlat_port=args.mlat_port,
                    mlat_dns=args.mlat_dns,
                )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_36() -> None:
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
                    mlat_hosts=args.mlat_host, mlat_dns=args.mlat_dns,
                )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_37() -> None:
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
                    )
                await reconcile(sources=sources, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_38() -> None:
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
                await reconcile(sources=None, feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_39() -> None:
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
                await reconcile(sources=sources, feeders=None, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_40() -> None:
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
                await reconcile(sources=sources, feeders=feeders, emitter=None,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_41() -> None:
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
                                k8s=None, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_42() -> None:
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
                                k8s=k8s, metrics=None, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_43() -> None:
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
                                k8s=k8s, metrics=metrics, now=None,
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_44() -> None:
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
                                seed_existing=None)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_45() -> None:
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
                await reconcile(feeders=feeders, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_46() -> None:
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
                await reconcile(sources=sources, emitter=emitter,
                                k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_47() -> None:
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
                await reconcile(sources=sources, feeders=feeders, k8s=k8s, metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_48() -> None:
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
                                metrics=metrics, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_49() -> None:
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
                                k8s=k8s, now=time.time(),
                                seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_50() -> None:
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
                                k8s=k8s, metrics=metrics, seed_existing=seed_existing)
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_51() -> None:
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
                                )
                seed_existing = False
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_52() -> None:
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
                seed_existing = None
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_53() -> None:
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
                seed_existing = True
            except Exception:
                logging.exception("reconcile failed")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_54() -> None:
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
                logging.exception(None)
            await asyncio.sleep(args.interval)


async def x_main__mutmut_55() -> None:
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
                logging.exception("XXreconcile failedXX")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_56() -> None:
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
                logging.exception("RECONCILE FAILED")
            await asyncio.sleep(args.interval)


async def x_main__mutmut_57() -> None:
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
            await asyncio.sleep(None)

mutants_x_main__mutmut['_mutmut_orig'] = x_main__mutmut_orig # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_1'] = x_main__mutmut_1 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_2'] = x_main__mutmut_2 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_3'] = x_main__mutmut_3 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_4'] = x_main__mutmut_4 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_5'] = x_main__mutmut_5 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_6'] = x_main__mutmut_6 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_7'] = x_main__mutmut_7 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_8'] = x_main__mutmut_8 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_9'] = x_main__mutmut_9 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_10'] = x_main__mutmut_10 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_11'] = x_main__mutmut_11 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_12'] = x_main__mutmut_12 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_13'] = x_main__mutmut_13 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_14'] = x_main__mutmut_14 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_15'] = x_main__mutmut_15 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_16'] = x_main__mutmut_16 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_17'] = x_main__mutmut_17 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_18'] = x_main__mutmut_18 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_19'] = x_main__mutmut_19 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_20'] = x_main__mutmut_20 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_21'] = x_main__mutmut_21 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_22'] = x_main__mutmut_22 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_23'] = x_main__mutmut_23 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_24'] = x_main__mutmut_24 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_25'] = x_main__mutmut_25 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_26'] = x_main__mutmut_26 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_27'] = x_main__mutmut_27 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_28'] = x_main__mutmut_28 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_29'] = x_main__mutmut_29 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_30'] = x_main__mutmut_30 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_31'] = x_main__mutmut_31 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_32'] = x_main__mutmut_32 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_33'] = x_main__mutmut_33 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_34'] = x_main__mutmut_34 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_35'] = x_main__mutmut_35 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_36'] = x_main__mutmut_36 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_37'] = x_main__mutmut_37 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_38'] = x_main__mutmut_38 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_39'] = x_main__mutmut_39 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_40'] = x_main__mutmut_40 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_41'] = x_main__mutmut_41 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_42'] = x_main__mutmut_42 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_43'] = x_main__mutmut_43 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_44'] = x_main__mutmut_44 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_45'] = x_main__mutmut_45 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_46'] = x_main__mutmut_46 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_47'] = x_main__mutmut_47 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_48'] = x_main__mutmut_48 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_49'] = x_main__mutmut_49 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_50'] = x_main__mutmut_50 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_51'] = x_main__mutmut_51 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_52'] = x_main__mutmut_52 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_53'] = x_main__mutmut_53 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_54'] = x_main__mutmut_54 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_55'] = x_main__mutmut_55 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_56'] = x_main__mutmut_56 # type: ignore # mutmut generated
mutants_x_main__mutmut['x_main__mutmut_57'] = x_main__mutmut_57 # type: ignore # mutmut generated


if __name__ == "__main__":
    asyncio.run(main())
