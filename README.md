# reapi-allowlist

Maintains the set of currently-connected ADSB.lol feeder addresses as a Cilium
object, so `re-api.adsb.lol` can be restricted to feeders. It reads `clients.json` 
from readsb and mlat-server, applies a decay window and some safety rails, and 
patches `loadBalancerSourceRanges` on one `CiliumGatewayClassConfig`.

Cilium does the enforcing - it propagates that field onto the re-api 
Gateway's Service, and its eBPF datapath drops packets from sources outside the 
list before the connection is accepted. Until that Gateway exists the object has 
no reader, so running the controller on its own changes nothing.

This has been tested on a lab k3s cluster (Cilium 1.20.1, Gateway API v1.6.1,
Node IPAM LB) fed by a real Raspberry Pi with ultrafeeder.

## Run it

```
kubectl apply -f deploy/cluster-scoped/gatewayclass.yaml   # once, by hand
kubectl apply -k deploy/base                               # observe-only
```

`deploy/base` runs the controller and creates the object it writes. Nothing
reads that object yet, so no traffic is affected — watch `size`,
`parse_anomalies` and `internal_prefixes` for a few cycles and check the set
looks like your feeder population. The `--mlat-host` values in
`deploy/base/deployment.yaml` are inferred; the shard IDs come from
`api.adsb.lol/metrics`.

```
kubectl apply -k deploy/monitoring                         # if you run prometheus-operator
kubectl apply -k deploy/enforce                            # enforcement on
```

`deploy/enforce` adds the Gateway. From the next reconcile a client outside the
set cannot reach re-api — **it hangs rather than getting a 403**, because the
packet is dropped in eBPF before anything speaks HTTP.

The `GatewayClass` is applied by hand because kustomize stamps a namespace onto
cluster-scoped CRDs it does not recognise. The controller holds a namespaced
`Role` with `get` and `patch` on one named object, and needs nothing else.
`loadBalancerSourceRanges` is deliberately not declared in git: it belongs to
the controller, and declaring it would make Flux fight for ownership.

Image and version live in `deploy/base/deployment.yaml`. Building your own is
equally reasonable.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--emit` | `ccg` | Object to write: `ccg` or `cgcc`. The default is historical; `cgcc` is recommended. |
| `--name` | `adsblol-feeders` | Name of the object to patch. |
| `--namespace` | `adsblol` | Namespace of the object (`cgcc` only). |
| `--interval` | `60` | Seconds between reconciles. |
| `--window` | `3600` | Seconds an address stays allowed after it was last seen. |
| `--ingest-dns` | `ingest-readsb-headless.adsblol.svc.cluster.local` | Headless Service resolved to readsb pod addresses. |
| `--ingest-port` | `150` | Port serving `clients.json` on each readsb pod. |
| `--mlat-host` | `[]` (repeatable) | An mlat-server hostname to poll. Pass once per host. |
| `--mlat-port` | `150` | Port serving `clients.json` on each mlat-server host. |
| `--mlat-dns` | unset | Headless Service resolved to mlat pod addresses. Prefer this: a shard added later is otherwise missed silently, and its feeders denied silently. |
| `--metrics-port` | `9090` | Port serving `/metrics`. |

On startup the controller seeds itself from the existing object, stamping every
entry as seen now. **A restart therefore resets the decay clock for the whole
set** — under frequent restarts an address can stay listed far longer than
`--window` suggests. It errs toward staying open, but `--window` is a floor on
residency, not a ceiling.

## Two emitters

`cgcc` patches `CiliumGatewayClassConfig.spec.service.loadBalancerSourceRanges`
and is what the shipped manifests use. `ccg` patches
`CiliumCIDRGroup.spec.externalCIDRs` for a `CiliumClusterwideNetworkPolicy` to
reference; it denies with a clean 403, but cannot be scoped to one service on a
cluster where everything is on port 443. See [`docs/design.md`](docs/design.md).

## Safety rails

Locking out a real feeder is worse than admitting a stale address, so every rail
biases toward staying open.

| Condition | Behaviour |
|---|---|
| Any source unreachable | Additive only — never remove on a partial fetch |
| All sources unreachable | No write. Last known good persists |
| New set < 50% of current | **Perform the write**, but log loudly and increment `large_shrink` |
| Set exceeds hard cap (default 50,000) | Refuse, alarm |
| Set unchanged | No write — do not touch etcd to say nothing |

## Metrics

Prometheus text format on `/metrics`, all prefixed `adsb_reapi_allowlist_`.

| Metric | Meaning |
|---|---|
| `size` | Prefixes in the maintained set. |
| `adds` / `removes` | Changes in the most recent reconcile. |
| `large_shrink` | Times the set more than halved in one cycle. The write still happens; this is the signal. **Alert.** |
| `parse_anomalies` | Client entries that yielded no address. Non-zero means PROXY protocol is not active on that path. |
| `internal_prefixes` | RFC 1918, ULA, loopback or link-local addresses in the set. If your feeders are external, any non-zero value means a PROXY header did not arrive. Reported, never filtered. |
| `source_errors` | Failed fetches in the most recent reconcile. |
| `seconds_since_success` | Since the last successful write or confirmed no-op; `-1` if never. Does not advance on a refusal or a failed patch. **Alert.** |
| `no_change` | Reconciles that correctly found nothing to do. |
| `refusals{reason="..."}` | Writes refused by a rail (`no-sources`, `over-cap`). **Alert.** |
| `consecutive_partial_cycles` | Reconciles in a row with a failed source, during which the set could only grow. |

## Tests

```
pip install -e ".[dev]"
pytest -q
```

No cluster required: the sources, the Kubernetes client and the clock are all
injected, so decay and the safety rails are tested against explicit timestamps.
CI runs these on Python 3.12 and 3.13, builds the overlays, and asserts that
every object carries a namespace and that `deploy/base` contains no Gateway.

Mutation testing is configured — `mutmut run`, then `mutmut results`. Two known
gaps, neither yet closed: `__main__.py` has no test at all (198 mutants
uncovered — it is the CLI and run loop, including the startup-seeding flip),
and 124 mutants survive elsewhere, concentrated in `sources.py`. Notably
nothing asserts that a fetch timeout is applied, so a hung source would stall
a reconcile.

## More

- [`docs/design.md`](docs/design.md) — how it works, and why not a network policy
- [`docs/measurements.md`](docs/measurements.md) — what was measured, on what, and what changed the design

## Licence

BSD-3-Clause. See [`LICENSE`](./LICENSE).
