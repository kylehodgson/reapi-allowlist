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
| New set < 50% of current | **Perform the write**, but log loudly and increment `large_shrink` |
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
| `adsb_reapi_allowlist_large_shrink` | Times the set dropped by more than half in a single cycle. The write still happens — this is a signal, not a rail. A steep drop is far more likely to be a bug on our side (a `clients.json` format change, a wrong `--ingest-dns`, a PROXY version change) than every feeder leaving at once, so **alert on it** — but check `parse_anomalies` and `source_errors` before assuming anything. Deliberately not a refusal: the harm of a wrong drop is up to one poll interval without re-api access and it heals itself, whereas refusing deadlocked permanently and needed a hand-patched object to clear. |
| `adsb_reapi_allowlist_parse_anomalies` | Count of client entries that didn't parse as expected. Non-zero here means PROXY protocol is not active on that path — the parser saw the no-PROXY fallback shape instead of a real source address. |
| `adsb_reapi_allowlist_source_errors` | Fetch errors against readsb or mlat-server sources. |
| `adsb_reapi_allowlist_seconds_since_success` | Seconds since the controller last did its job (`-1` if it never has). "Did its job" means exactly two things: it wrote successfully, or it correctly found nothing to change (`unchanged`). It deliberately does **not** advance on a refused write (`no-sources`, `over-cap`) or when `k8s.patch` itself fails — those are precisely the conditions this metric exists to surface. **This is the series worth alerting on** — a rising value means the controller is stuck, every source is unreachable, or writes are failing. |
| `adsb_reapi_allowlist_no_change` | Count of reconciles where the computed set exactly matched what's already live — the healthy, do-nothing steady state. Not a refusal; kept out of the `refusals` series so that series stays alertable. |
| `adsb_reapi_allowlist_refusals{reason="..."}` | One counter per distinct reason a write was refused by a safety rail (`no-sources`, `over-cap`). **This is the other series worth alerting on** — every reason it tracks is an anomaly; `unchanged` is deliberately excluded (see `no_change` above) so the healthy steady state can't drown out real refusals. |
| `adsb_reapi_allowlist_consecutive_partial_cycles` | Number of reconciles in a row in which **at least one source failed** (`source_errors > 0`), so the set could only grow. Resets to 0 the first time every source reports. Distinguishes "one transient blip" from "we've been additive-only for hours", which nothing else surfaces. Deliberately keyed off source health rather than the write outcome: when the additive union equals the current set, `guards.decide` returns `unchanged`, so a counter driven off the decision reason reads 0 during exactly the sustained degradation this exists to detect. `guards.decide` itself is unchanged and stays additive-only for as long as a source is down. |

## 7. RBAC

The `ClusterRole` grants `get` and `patch` on exactly one named
`CiliumCIDRGroup` (`adsblol-feeders`) — no `create`, no `list`, no `watch`,
and no access to any other object. This is the ceiling, not a starting point:
the `CiliumCIDRGroup` ships empty in the manifests, and the controller only
ever patches an object that already exists. If the RBAC ever needs to grow
past "get and patch one named object of one kind," that should be treated as
a design change worth its own review, not a routine permissions bump.

## 8. Install notes

The `CiliumCIDRGroup` (`adsblol-feeders`) is cluster-scoped, so it is kept
out of the kustomize tree Flux syncs — see
`infra/manifests/default/reapi-allowlist/cluster-scoped/adsblol-feeders.yaml`.
Before the controller's Deployment starts, create it once by hand:

```
kubectl apply -f infra/manifests/default/reapi-allowlist/cluster-scoped/adsblol-feeders.yaml
```

This is not a workaround for a missing feature: the controller only ever
`patch`es this object, it never `create`s it, and that is precisely what
keeps its RBAC down to `get`+`patch` on one named object (section 7). The
one-time manual `apply` and the minimal RBAC are the same design decision
seen from two sides — the controller doesn't need `create` permission
because someone (or the initial rollout process) creates the empty object
first, once.

If the object doesn't exist yet when the controller starts: the `get` itself
does not fail (a missing object is treated as an empty starting set), but the
`patch` at the end of the reconcile does — patching a nonexistent object
errors, is caught by the top-level handler, and is logged as `reconcile
failed` once per `--interval`. `adsb_reapi_allowlist_seconds_since_success`
is only stamped *after* `k8s.patch` returns, so a persistently-missing
target object correctly shows up as a rising `seconds_since_success` in
addition to the `reconcile failed` log line — alerting on that metric
exceeding a threshold does catch this case.

## 9. Licence

BSD-3-Clause. See [`LICENSE`](./LICENSE).
