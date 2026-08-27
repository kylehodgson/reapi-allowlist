# Measurements

Every design decision here was settled by measurement rather than argument. This
records what was measured, on what, and what it showed — including the results that
changed the design.

**Rig.** A single-node k3s cluster (`--flannel-backend=none --disable-network-policy
--disable-kube-proxy`) running Cilium 1.20.1 with kube-proxy replacement, Gateway API
v1.6.1 experimental channel, and Node IPAM LB. Fed by a real Raspberry Pi running
ultrafeeder, which fed production ADSB.lol simultaneously throughout. A second,
unrelated Gateway acted as a control on every enforcement test.

This is not ADSB.lol's cluster. We have no access to it.

## Enforcement

| source | re-api gateway | control gateway |
|---|---|---|
| Pi, feeding, in the list | **HTTP 200** in 75 ms over TLS | 200 |
| laptop, not feeding, not in the list | **times out — packets dropped** | 200 |

The control column is the point: the restriction is scoped to one Gateway, not applied
cluster-wide.

**A denied client hangs.** The packet is dropped in eBPF before anything speaks HTTP, so
there is no 403 and no reset. That is the real cost of this approach.

## Scale

Source ranges rewritten directly, measuring propagation to the Gateway's Service:

| ranges | propagation |
|---|---|
| 1,000 | 124 ms |
| 6,001 | 178 ms |
| 20,000 | 535 ms |

Sub-linear. The Service object stayed flat at 116 KB across seven consecutive rewrites,
and `cilium#43942` (Cilium overwriting `loadBalancerSourceRanges`) did not reproduce
through a `CiliumGatewayClassConfig`.

Bursts do not stress this design: the controller writes the whole set once per interval,
so a hundred feeders arriving at once changes the set's *size*, never the write *count*.
Measured 6,000 → 7,000 → 6,000 with no spike.

For comparison, the rejected network-policy approach at 10,000 prefixes: one security
identity (`16777217`), 65 cluster-wide, under 100 ms, +46 MB RSS.

## Behaviour over time

With `--window=120` and `--interval=10`, using a synthetic source that could be edited
mid-run:

- **Decay.** A feeder removed from its source stayed listed for the full window and was
  dropped at **t+120s** exactly.
- **Source failure.** With a source killed, the set held for four minutes and removed
  nothing, though two entries were long past their window. `source_errors=1` and
  `consecutive_partial_cycles` climbed 0→21.
- **Restart seeding.** A prefix present in no source survived a controller restart and
  was held ~110 s — the 120 s window minus the ~10 s the restart took.

## Results that changed the design

**`toPorts` matches the port the client dialled**, not the port of the service being
protected. This ruled out the `CiliumClusterwideNetworkPolicy` approach entirely: port is
its only scoping handle, so on a cluster where every endpoint is on 443 the rule covers
every Gateway or none. Neither of the two open upstream issues mentions this.

**`externalTrafficPolicy: Local` leaves a Cilium Gateway with no address.** Its generated
Service is selector-less, so no EndpointSlice names a node and nodeIPAM assigns nothing
(`nodesvclb.go:243`). Observed as `Programmed=False (AddressNotAssigned)`; changing to
`Cluster` produced an address within 8 seconds. This is worth flagging because
`Local` *is* the right choice for ordinary Services and is the convention in
`iakat/adl-backend` — it simply does not carry over to a Gateway.

**IPv4-mapped IPv6.** A dual-stack listener reports IPv4 peers as `::ffff:a.b.c.d`, which
rendered as a `/128` that could never match the feeder's actual traffic. No unit test
caught this; it took a real feeder.

**The controller was overwriting the manifest.** An earlier version wrote a whole block of
service defaults on every reconcile, reverting operator changes within a minute and
forcing `externalTrafficPolicy` back to `Local`. Found only by applying the manifests to
a clean cluster — every prior measurement had patched the target object directly, so the
controller's own write path had never been exercised.
