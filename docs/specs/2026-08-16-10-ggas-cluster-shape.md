# Cluster shape for 10 Ggas/s

Derived from the measured stage costs in
`docs/specs/2026-08-16-pipeline-cost-model.md`. Every core count below
comes from a measured per-tx cost, not an estimate.

## What 10 Ggas/s means

| Mix | tx/s needed | Sig-verify cores | Executor nodes (4 workers) |
|-----|------------|------------------|----------------------------|
| transfer-heavy (21k gas) | 476,000 | 20.0 | 0.90 |
| defi/uniswap (46k gas) | 216,000 | 9.1 | 1.03 |
| blended (30k gas) | 333,000 | 14.0 | 1.00 |

**The executor is ONE node.** At 4 workers it already measures 9.7-14.3
Ggas/s. The cluster around it is dominated by signature verification —
10 to 20 times the executor's core count for the same throughput — and
its hard external limit is data availability.

## Tiers

**1. Ingress / signature verification — 4 nodes x 8 cores.**
14 cores of recovery at the blended mix (20 for transfer-heavy) plus
RPC/tokio headroom. Stateless by design (#115/#117), so it scales
horizontally; haproxy must keep `nbthread` tuned (untuned it capped
admission at 2,750 against 6,000 tuned).

**2. Sequencer — shard it.**
Sub-µs of CPU per tx, but it is a single ordering point and two things
break at 333k tx/s: the dedup window (131k records is 0.4 SECONDS of
history at this rate — size it for ~30s, ~10M entries, ~1GB) and the
leader's egress fan-out. Shard by sender prefix per
`replicated-sequencer-shards-spec.md`, 2-4 shards, deterministic merge
at the sealer by (shard, sequence). This is the piece needing design
work, not more hardware.

**3. Executor — one node, 12 cores, BIG blocks.**
- 8 workers rather than 4: the extra headroom absorbs bursts, and it is
  where sharded admission becomes necessary (the serial feed binds at
  w>=8 for cheap transactions).
- Block size 16-32k txs, NOT 4k. At 4,000 txs and 10 Ggas/s the cadence
  is 12 ms against ~10 ms of tail+feed — no margin. At 20,000 txs the
  cadence is 60 ms against ~22 ms. Bigger blocks also amortize the
  commit fsync and compress better for DA.
- A hot standby re-executing the same stream gives failover.

**4. State + receipts — split them.**
Receipt puts are 1.4-2.5 µs/tx: at 20,000-tx blocks that is 28-50 ms of
mdbx work per block, which does not fit a 60 ms cadence alongside
account/storage commits and fsync. Either give receipts their own
environment and writer thread, or stop persisting them in the executor
and serve them from an indexer fed by the egress stream (already flagged
as a product follow-up in the throughput campaign). Account/storage puts
are cheap by comparison (0.03-0.75 µs/tx).

**5. Egress — batch per block.**
333k receipts/s at ~235 B is 78 MB/s. The current per-receipt blocking
publish with a clone cannot carry that; frames must be per-block batches
with a non-blocking offer and an explicit drop policy (a back-pressured
consumer currently stalls cluster-wide egress).

**6. Validator — a full duplicate, or change the check.**
Re-execution doubles system CPU: another executor-class node. BAL-based
attribution (`bal-attribution-parallel-validation-spec.md`) or sampling
is the alternative; at 10 Ggas/s the duplicate is a real cost.

**7. Data availability — THE WALL.**

| Mix | tx/s | Raw (~110 B/tx) | Compressed (~45 B/tx) |
|-----|------|-----------------|------------------------|
| 21k gas | 476,000 | 52.4 MB/s | 21.4 MB/s |
| 30k gas | 333,000 | 36.7 MB/s | 15.0 MB/s |
| 46k gas | 216,000 | 23.8 MB/s | 9.7 MB/s |

Ethereum blobs deliver on the order of 0.1 MB/s — two to three orders of
magnitude short. 10 Ggas/s requires an alternative DA layer (EigenDA,
Celestia, Avail) or an explicit validium/offchain-DA posture. This is a
product decision, and it binds long before execution does.

## Total budget

~70-80 cores, five to eight servers, plus DA capacity:

    4 x ingress        32 cores
    2-4 sequencer shards (3-node quorums)  16 cores
    1 executor + 1 standby                 24 cores
    1 validator                             8 cores
    2 egress/indexer                       16 cores

## Order of work

1. Sharded admission — unlocks w>=8 on the executor (designed, unbuilt).
2. Block sizing 16-32k + split the receipt write path.
3. Egress batching + non-blocking offer.
4. Sequencer sharding.
5. DA decision — the earliest hard limit, and the one with the longest
   lead time.

## What breaks first if the current cluster is simply scaled up

The sequencer's single ordering point and DA bandwidth, in that order —
and before either, co-location: the measured latency knee tracks host
CPU pressure 1:1, so every tier needs dedicated cores before any of
this arithmetic holds.
