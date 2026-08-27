# reapi-allowlist

**Status:** the controller is complete and tested (79 tests). Enforcement is
proven on a lab k3s cluster fed by a real Raspberry Pi, not on ADSB.lol's
production cluster — we have no access to it. Treat this as a reference
implementation to read, adapt, or ignore.

A prebuilt image is published at `ghcr.io/kylehodgson/reapi-allowlist:v0.1.0`
(amd64 + arm64) for convenience. Building your own from this source and pushing
it somewhere you control is equally reasonable — update the Deployment's
`image:` field to match.
## 1. What this does

`reapi-allowlist` maintains the set of currently-connected ADSB.lol feeder IP
addresses as a Cilium object, so that a later enforcement layer can restrict
`re-api.adsb.lol` to people who actually feed data instead of leaving it open
to the internet. **This controller enforces nothing on its own.** It reads
`clients.json` from readsb and mlat-server, reconciles a set of addresses, and
writes that set to one Kubernetes object. Enforcement is a separate, deliberate
step: the accompanying manifests put a Gateway behind that object, and until
those are applied nothing this controller writes affects any traffic. Running it
observe-only first — watching the metrics for a few cycles before anything reads
the object it maintains — is the intended way to adopt it.

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
| `--emit` | `ccg` | Which object to write: `ccg` (`CiliumCIDRGroup`) or `cgcc` (`CiliumGatewayClassConfig`). The default is historical — `cgcc` is the recommended mode, see section 8. |
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

**The consequence is that a restart resets the decay clock for the whole set**,
because there is nowhere to persist per-prefix timestamps between processes.
Under frequent restarts -- rolling updates, node drains, evictions -- an
address can therefore stay listed far longer than `--window` suggests, in
principle indefinitely if restarts are more frequent than the window. That
error runs in the safe direction for this component (a stale feeder keeps read
access it no longer strictly qualifies for), but `--window` is a floor on
residency, not the ceiling it reads like.

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
| `adsb_reapi_allowlist_internal_prefixes` | Prefixes in the set that are RFC 1918, ULA, loopback or link-local. On a deployment whose feeders come from the internet, **any** non-zero value means a PROXY header did not arrive: mlat-server falls back to the socket peer, so haproxy's own pod IP enters the set looking like a feeder, with `parse_anomalies` staying at zero. Not filtered out, because where feeders and cluster share a network those addresses are legitimate and dropping them would deny everyone. Alert on it only if your feeders are external. |
| `adsb_reapi_allowlist_seconds_since_success` | Seconds since the controller last did its job (`-1` if it never has). "Did its job" means exactly two things: it wrote successfully, or it correctly found nothing to change (`unchanged`). It deliberately does **not** advance on a refused write (`no-sources`, `over-cap`) or when `k8s.patch` itself fails — those are precisely the conditions this metric exists to surface. **This is the series worth alerting on** — a rising value means the controller is stuck, every source is unreachable, or writes are failing. |
| `adsb_reapi_allowlist_no_change` | Count of reconciles where the computed set exactly matched what's already live — the healthy, do-nothing steady state. Not a refusal; kept out of the `refusals` series so that series stays alertable. |
| `adsb_reapi_allowlist_refusals{reason="..."}` | One counter per distinct reason a write was refused by a safety rail (`no-sources`, `over-cap`). **This is the other series worth alerting on** — every reason it tracks is an anomaly; `unchanged` is deliberately excluded (see `no_change` above) so the healthy steady state can't drown out real refusals. |
| `adsb_reapi_allowlist_consecutive_partial_cycles` | Number of reconciles in a row in which **at least one source failed** (`source_errors > 0`), so the set could only grow. Resets to 0 the first time every source reports. Distinguishes "one transient blip" from "we've been additive-only for hours", which nothing else surfaces. Deliberately keyed off source health rather than the write outcome: when the additive union equals the current set, `guards.decide` returns `unchanged`, so a counter driven off the decision reason reads 0 during exactly the sustained degradation this exists to detect. `guards.decide` itself is unchanged and stays additive-only for as long as a source is down. |

## 7. RBAC

A namespaced `Role` grants `get` and `patch` on exactly one named
`CiliumGatewayClassConfig` (`reapi-config`) — no `create`, no `list`, no
`watch`, no `delete`, and no access to any other object in any other namespace.

This is the ceiling, not a starting point. The controller only ever patches an
object that already exists, which is what keeps `create` off the list. If the
RBAC ever needs to grow past "get and patch one named object of one kind," treat
that as a design change worth its own review, not a routine permissions bump.

The `ccg` emitter needs a `ClusterRole` instead, because `CiliumCIDRGroup` is
cluster-scoped. That is one of several reasons `cgcc` is the recommended mode —
see section 8.

## 8. Install notes

### Which emitter to use

`cgcc` is the recommended mode, and the one the shipped manifests use.

`ccg` writes a `CiliumCIDRGroup` for a `CiliumClusterwideNetworkPolicy` to
reference. That works, and it denies with a clean 403 where `cgcc` denies by
dropping the packet (the client hangs) — a real advantage. But a CCNP has no
destination match, and its `toPorts` matches *the port the client dialled*, not
the listener port. Where every endpoint shares port 443, as on ADSB.lol, a
policy scoped to one service necessarily covers all of them. That rules the
mode out for this specific job, not in general.

`cgcc` scopes per GatewayClass instead, so the target service gets its own class
and nothing else is affected.

### Applying it

The `GatewayClass` is cluster-scoped, so it is kept out of the kustomize tree
Flux syncs — kustomize's namespace transformer stamps a namespace onto
cluster-scoped CRDs it does not recognise, which silently breaks them. Create it
once by hand before the Gateway:

```
kubectl apply -f manifests/default/reapi-gateway/cluster-scoped/gatewayclass.yaml
```

Note that the `CiliumGatewayClassConfig` the controller writes deliberately does
**not** declare `loadBalancerSourceRanges` in git. That field belongs to the
controller; declaring it in the manifest would make Flux and the controller
fight over ownership of it.

If the target object doesn't exist when the controller starts: the `get` does
not fail (a missing object is treated as an empty starting set), but the `patch`
at the end of the reconcile does, is caught by the top-level handler, and is
logged as `reconcile failed` once per `--interval`.
`adsb_reapi_allowlist_seconds_since_success` is only stamped after a successful
write, so a persistently-missing target shows up as a rising
`seconds_since_success` as well as the log line — alerting on that metric does
catch this case.

## 9. Licence

BSD-3-Clause. See [`LICENSE`](./LICENSE).
