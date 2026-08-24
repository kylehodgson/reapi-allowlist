# reapi-allowlist

> ## ⚠ Image prerequisite — read before applying the manifests
>
> **`ghcr.io/kylehodgson/reapi-allowlist:latest` does not exist yet.** It has
> never been built or pushed anywhere. The Kubernetes manifests in
> `infra/manifests/default/reapi-allowlist` reference that image path as a
> placeholder, and applying them (or merging them into the branch Flux syncs)
> **before the image exists produces a crash-looping `ImagePullBackOff` pod in
> production.**
>
> Before applying those manifests, either:
> - build and push the image yourself under `ghcr.io/adsblol/...` and update
>   the Deployment's `image:` field to match, or
> - pull `ghcr.io/kylehodgson/reapi-allowlist` once it has actually been
>   published.
>
> Until one of those is true, do not apply the manifests — the Deployment will
> not come up.

## 1. What this does

`reapi-allowlist` maintains the set of currently-connected ADSB.lol feeder IP
addresses as a Cilium object, so that a later enforcement layer can restrict
`re-api.adsb.lol` to people who actually feed data instead of leaving it open
to the internet. **This controller enforces nothing on its own.** It reads
`clients.json` from readsb and mlat-server, reconciles a set of addresses, and
writes that set to one Kubernetes object. Nothing in this repository or in
the manifests it ships consumes that object yet — no `CiliumClusterwideNetworkPolicy`,
no Gateway, no traffic is affected by running this controller. It is a
data-plane-adjacent, control-plane-only change: Phase 0 of a staged rollout,
observe-only by construction.

The address set is sourced by resolving the `ingest-readsb-headless` service
to its pod IPs and fetching `:150/clients.json` from each (readsb), and by
fetching the same path from each configured mlat-server host. Both feeds are
polled directly rather than through `adsblol/api`'s Redis cache, so the
allowlist's dependency graph stays one node deep instead of depending on a
public-facing API service.

## 2. Why the addresses churn

ADSB.lol has roughly 6,000 concurrently-connected feeders, the large majority
on home broadband and consumer routers: DHCP lease renewals, ISP-side
reassignment, and ordinary reboots and reconnects churn the address set
constantly. A naive "connected right now" rule would flap access on every
reconnect, so the controller keeps an address allowed for a decay window
after it was last seen (default one hour) rather than dropping it the instant
a connection closes.

## 3. Flags

| Flag | Default | Meaning |
|---|---|---|
| `--emit` | `ccg` | Which object to write: `ccg` (`CiliumCIDRGroup`) or `cgcc` (`CiliumGatewayClassConfig`). |
| `--name` | `adsblol-feeders` | Name of the object to patch. |
| `--namespace` | `adsblol` | Namespace of the object to patch (used by the `cgcc` emitter; `CiliumCIDRGroup` is cluster-scoped). |
| `--interval` | `60` | Seconds between reconcile loops. |
| `--window` | `3600` | Seconds an address stays allowed after it was last seen. |
| `--ingest-dns` | `ingest-readsb-headless.adsblol.svc.cluster.local` | Headless service resolved to readsb pod IPs. |
| `--ingest-port` | `150` | Port serving `clients.json` on each readsb pod. |
| `--mlat-host` | `[]` (repeatable) | One or more mlat-server hostnames to poll. Pass once per host. |
| `--mlat-port` | `150` | Port serving `clients.json` on each mlat-server host. |
| `--metrics-port` | `9090` | Port serving `/metrics` in Prometheus text format. |

There is no `--seed-existing` flag. On startup the controller always reads
the existing target object and seeds every entry currently listed as
last-seen-now, so a restart degrades to "everyone currently listed gets one
more decay window" rather than to an empty set. This seeding is internal
behaviour, not a configurable option.

## 4. Emitters

The controller builds one sorted, deduplicated list of feeder prefixes
(rendered as `/32` for IPv4 and `/128` for IPv6) and hands it to whichever
emitter `--emit` selects:

- **`ccg`** patches `CiliumCIDRGroup.spec.externalCIDRs` on the named
  `CiliumCIDRGroup`.
- **`cgcc`** patches `CiliumGatewayClassConfig.spec.service.loadBalancerSourceRanges`
  on the named object in the given namespace.

Set-building has no knowledge of which emitter is active — that split lives
entirely at the write boundary. Run one emitter per process: if both object
shapes are ever needed at once, run this Deployment twice with different
`--emit` and `--name` values rather than adding a multi-target mode. That
keeps each process's RBAC scoped to the single object it writes, and a bug in
one emitter can't touch the other's resource.

## 5. Safety rails

The failure mode of this component is locking real feeders out of an API
they're entitled to use, so every rail is biased toward staying open: adding
an address is cheap and reversible, removing one breaks somebody's feeder
access. The rails below bias accordingly — every one of them either does
nothing, or does the safe (additive-only or no-op) thing, in preference to
removing an address on shaky evidence.

| Condition | Behaviour |
|---|---|
| Any source unreachable | Additive only — never remove on a partial fetch |
| All sources unreachable | No write. Last known good persists |
| New set < 50% of current | Refuse the write, log loudly, expose a metric |
| Set exceeds hard cap (default 50,000) | Refuse, alarm |
| Set unchanged | No write — do not touch etcd to say nothing |

## 6. Metrics

Served as Prometheus text format on `/metrics` (port `--metrics-port`,
default `9090`). Every series comes from `Metrics.render`:

| Metric | Meaning |
|---|---|
| `adsb_reapi_allowlist_size` | Current number of prefixes in the maintained set. |
| `adsb_reapi_allowlist_adds` | Prefixes added in the most recent reconcile. |
| `adsb_reapi_allowlist_removes` | Prefixes removed in the most recent reconcile. |
| `adsb_reapi_allowlist_parse_anomalies` | Count of client entries that didn't parse as expected. Non-zero here means PROXY protocol is not active on that path — the parser saw the no-PROXY fallback shape instead of a real source address. |
| `adsb_reapi_allowlist_source_errors` | Fetch errors against readsb or mlat-server sources. |
| `adsb_reapi_allowlist_seconds_since_success` | Seconds since the last fully-successful reconcile (`-1` if none has ever succeeded). **This is the series worth alerting on** — a rising value means the controller is stuck or every source is unreachable. |
| `adsb_reapi_allowlist_refusals{reason="..."}` | One counter per distinct reason a write was refused by a safety rail (e.g. the >50% shrink guard, the hard-cap guard). |

## 7. RBAC

The `ClusterRole` grants `get` and `patch` on exactly one named
`CiliumCIDRGroup` (`adsblol-feeders`) — no `create`, no `list`, no `watch`,
and no access to any other object. This is the ceiling, not a starting point:
the `CiliumCIDRGroup` ships empty in the manifests, and the controller only
ever patches an object that already exists. If the RBAC ever needs to grow
past "get and patch one named object of one kind," that should be treated as
a design change worth its own review, not a routine permissions bump.
