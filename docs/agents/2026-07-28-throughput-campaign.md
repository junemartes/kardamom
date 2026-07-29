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

## Cluster wire (PRs #104, #118, #85 fix B)
- Ingress record frame: `[kind:0][sender:20][nonce:u64 LE][canonical_id:32]
  [record_type][fields…]`. The guard header (sender/nonce) feeds the sealer's
  per-sender contiguity guard and is NEVER relayed — executors see
  `[canonical_id:32][record_type][fields…]` unchanged. Zero sender
  (deposits) is guard-exempt.
- Ingress kinds: 0 record · 1 replay · 2 SUBSCRIBE (egress consumers announce
  on every session establishment; replay implies it; empty consumer set ⇒
  broadcast-to-all fallback) · 3 BATCH (`[count:u16][len:u32 + entry]*`, ≤16
  entries ≈ one MTU with the guard header — 20 × 79B would fragment; entries
  process exactly like single records).
- Egress kind 5 CONTIGUITY_REJECT `[sender:20][nonce:u64][expected:u64]`, to
  the OFFERING session only: a known sender's first-seen ref whose nonce ≠
  expected next would seal a canonical nonce gap (the #85 failure); the
  sequencer rewinds its unconfirmed ledger (#114) from `expected` and
  republishes immediately instead of waiting out the 15s confirm timeout.
- Guard state: LRU map bounded at dedupCapacity, snapshot v2 (v1 loads with
  an empty map; evicted/unknown senders seed at any nonce — degrades toward
  accept, never a false reject). Dedup runs BEFORE the guard so #114's
  republished committed copies absorb as duplicates.
- Confirm-by-reject: a reject with `nonce < expected` proves the ref already
  committed (its dedup entry aged out) — the sequencer drops the unconfirmed
  entry like a receipt confirmation. Found live on day one: a sender whose
  ONLY tx is nonce 0 (smoke-gate accounts) never gets a confirming receipt
  (nonce-0 receipts are deposit-indistinguishable), so its ledger entry
  republished every 15s forever; once the dedup window rolled past 131k
  records those re-offers would have DOUBLE-ORDERED without the guard
  (observed: 128+ rejects/run, all `nonce=0 expected=1`).
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

## Idle backoff (sequencer profile follow-up)
- perf @2,000 tx/s legacy: 66% of sequencer CPU in the three AeronRuntime
  cmd loops' fixed 100µs recv_timeout (crossbeam pre-park sched_yield
  storm), 8% in the session thread's fixed 1ms select; REAL sequencing work
  <2% of a core. `IdleBackoff` (kardamom-log) ports the sealer stack's
  BackoffIdleStrategy concept: base cadence while working, double per empty
  iteration to a cap (100µs→1ms aeron loop, 1ms→5ms session thread), reset
  on any work; pending publishes pin base (retry timing unchanged).
- Validated: idle aeron threads 12.3%→1.7% each, sequencer containers
  ~100%→~41% idle (~1.2 cores returned); legacy ramp edge 2,500→3,250 tx/s
  (+30% — less scheduler churn on the shared 12-core host speeds the whole
  blocking round trip), clean soak 2,600 tx/s ×240s zero-loss p50 11ms.
  Loaded sequencer CPU 139%@2,000 → ~111%@2,600.

## Known follow-ups
- Executor receipt/BAL batching (per-receipt blocking publish w/ clone,
  `crates/engine/src/actor.rs` commit thread) + mdbx boundary-fsync overlap.
- Leader still blocks on back-pressured CONSUMER sessions (frozen executor =
  cluster-wide egress stall) — needs non-blocking offer + drop policy.
- Fragmented cluster ingress messages untested (KIND_BATCH capped ≤ MTU).
- `aeron_live` subscriptions now REASSEMBLE fragments (AeronFragmentAssembler
  — found live: >MTU `Vec<Receipt>` batches decode-failed at every consumer,
  parked submits hung 60s, edge collapsed to 500). The `subscriber.rs` /
  `replay.rs` poll paths (archive replay, batcher) still deliver RAW
  fragments — a >MTU archived tx would break refetch there; same fix applies.
- ~~Retire the #86 conn pin only after the real #85 fix~~ DONE: #114
  (offer-until-confirmed) + fix B (contiguity guard) shipped; ingress
  `--rpc-max-connections` raised 100 → 8192.
- Subscribe-mode leader-kill chaos variant (validate the guard + ledger
  under failover with fast-acks in flight).
