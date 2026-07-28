# Throughput campaign: subscription ingress, egress fan-out, wakeups, batching (2026-07-25 → 07-28)

Validated end-to-end on the DinD cluster, production config (100-conn pin intact):
750 tx/s baseline → **6,000 tx/s admission / 3,800 tx/s zero-loss sustained**
(912k/912k, p50 11ms / p99 47ms). PRs #101–#104, #107, #118. Full data:
report artifact + `target/perf/` run dirs per PR body.

## Client API (PR #102)
- `kardamom_sendRawTransactionAsync(rawTx) -> txHash` — fast-ack: validates
  (rate limit, shed valve, decode, sig-recovery, dedup) and publishes to
  tx_data; does NOT park awaiting the receipt. In-flight txs hold no
  connections.
- `kardamom_subscribeReceipts(senders?)` (WebSocket) — deduped push feed.
  Frames: `{type: receipt, receipt}` · `{type: txError, sender, nonce,
  reason: duplicated-tx|evicted, expectedNonce}` · `{type: lagged, skipped}`
  (fall back to `eth_getTransactionReceipt` for the gap).
- `eth_sendRawTransaction` unchanged (parks until receipt + ack-policy gate).
- `Evicted` (PR #103) means the sequencer shed the tx under overload — it
  will never sequence; resubmit when the nonce is within the reorder window.
  Overloaded ingress rejects with a retryable error past `pending_shed_depth`.

## Cluster wire (PRs #104, #118)
- Ingress kinds: 0 record · 1 replay · 2 SUBSCRIBE (egress consumers announce
  on every session establishment; replay implies it; empty consumer set ⇒
  broadcast-to-all fallback) · 3 BATCH (`[count:u16][len:u32 + entry]*`, ≤20
  entries ≈ one MTU; entries process exactly like single records).
- Boundaries broadcast to EVERY session (the sequencer's boundary-only lag
  feed consumes without SUBSCRIBE); relayed records go to consumers only.

## Deploy
- `deploy/cluster/nomad/rpc-proxy.nomad.hcl`: haproxy on the aux node
  balancing both ingress replicas (`http-reuse always`, `nbthread 4` — both
  required; untuned it capped admission at 2,750). Image rides the in-cluster
  registry: `192.168.56.10:5000/haproxy:2.9-alpine`.

## Harness notes
- `kardamom-perf up` now builds the sealer shadowJar itself (a stale jar
  silently drops KIND_BATCH frames — cost two false debugging trails).
- Subscribe-mode ramp requires receipts to keep pace (≥0.95/step); the fast
  ack otherwise lets discovery overshoot the drain rate.
- `kardamom_sequencer_start_time_seconds` (gauge): HTTP-only restart proof
  for chaos assertions — counters reset to values identical to fresh
  baselines; a start time after a known event is unambiguous.

## Known follow-ups
- Executor receipt/BAL batching (per-receipt blocking publish w/ clone,
  `crates/engine/src/actor.rs` commit thread) + mdbx boundary-fsync overlap.
- Leader still blocks on back-pressured CONSUMER sessions (frozen executor =
  cluster-wide egress stall) — needs non-blocking offer + drop policy.
- Fragmented cluster ingress messages untested (KIND_BATCH capped ≤ MTU).
- Retire the #86 conn pin only after the real #85 fix.
