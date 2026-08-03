# BAL access attribution + parallel validator re-execution (spec)

Status: DESIGN v3 — not implemented. Companion to the throughput campaign
notes (`2026-07-28-throughput-campaign.md`).

v2 supersedes the custom dictionary encoding of v1: **revm 38 ships native
EIP-7928 support** (`revm_state::bal` — per-account, per-slot
`(tx_index, value)` write lists plus `storage_reads`, with
`into_alloy_bal()` exporting the standard `alloy_eip7928::BlockAccessList`),
so capture, wire shape, and even the verification comparison come from the
library and the Ethereum standard instead of bespoke code. v2 also
incorporates three review decisions: **scheduling granularity is decoupled
from attribution granularity** (execute in 5-10-tx work units; attribute
per tx), oversized frames degrade down a **granularity ladder** instead
of falling off a cliff, and — v3, the load-bearing one — the validator
executes batches **fully in parallel by seeding each batch's inputs from
the BAL's own claimed write values** (execute-from-claims + verify-claims
by induction), which supersedes the wave-DAG model entirely and makes the
ladder's batch-deduped rung cost nothing in parallelism.

## Goal

Let the validator re-execute a block's transactions **in parallel** while
*strengthening* — not weakening — the verification contract, by publishing
an EIP-7928 Block Access List alongside the existing merged BAL: one frame
per block, organized per slot, each state item carrying the ordered list of
`(tx_index, value)` writes and the set of read-only accessors.

Secondary goals, in order:

1. **Enforced access contract**: the validator verifies each tx's *actual*
   accesses against its *declared* set during re-execution — an undeclared
   access is a divergence → fail-stop. The BAL stops being a passive
   cross-check artifact and becomes a checked claim (EIP-7928 in spirit).
2. **State prefetch**: the slot dictionary is exactly the block's touched
   set — one batched mdbx read pass warms everything before execution
   (valuable even for the sequential path).
3. **Forward reuse**: the same capture + DAG machinery is what an eventual
   parallel *executor* needs; proving it on the validator first means a
   concurrency bug surfaces as a loud divergence, never as silent state
   corruption (the validator's output is checked against the executor's).

## Non-goals

- Changing the executor's sequential execution (this spec only *captures*
  more from it).
- Trusting BAL final values instead of re-executing (self-checking a claim
  validates nothing).
- Cross-block parallelism (blocks stay sequential; the depth-K commit
  pipeline already overlaps commit with the next block).

## Wire format

Today the `tx_bal` frame is an rkyv-encoded `BlockDelta` with `receipts`
stripped (the #113 lesson: receipts dominated byte size and fat frames
collapsed the validator's lapse window on the byte-bounded term buffer).

New frame — a versioned wrapper so the validator accepts both during
rollout, and degrades gracefully forever:

```rust
enum BalFrame {
    /// Exactly today's payload (receipts-stripped BlockDelta).
    V1(BlockDelta),
    /// V1 + the EIP-7928 Block Access List. The merged section is
    /// UNCHANGED — consumers that only want final values (prefetch, the
    /// existing write-set-hash cross-check) read it exactly as before.
    V2 {
        delta: BlockDelta,
        /// `alloy_eip7928::BlockAccessList`, canonical encoding, carried
        /// as bytes inside the rkyv frame. Per account:
        /// `storage_changes` (per slot: ordered `(tx_index, new_value)`
        /// writes), `storage_reads` (read-only slots),
        /// `balance/nonce/code_changes` (per-field `(tx_index, value)`).
        /// tx_index follows revm's BalIndex convention: 0 = pre-execution,
        /// 1..=n = txs in block order, n+1 = post-execution.
        bal: Vec<u8>,
        /// Attribution granularity: 1 = per-tx (default). K > 1 means
        /// BalIndex was quantized to ceil(idx/K) — the size-degradation
        /// ladder (see Size budget); the DAG then orders CHUNKS of K txs.
        granularity: u16,
    },
}
```

Adopting the standard type buys: zero bespoke encoding, ecosystem
compatibility (the same artifact upstream Ethereum tooling understands),
and a validator check that is *structural equality of two independently
computed standard objects* rather than custom set arithmetic.

## Size budget and the granularity ladder

Per-slot organization already deduplicates keys (a slot's 52-byte identity
appears once, its writers as compact `(tx_index, value)` pairs). Transfer
blocks at 17k txs land near ~1MB; contract-heavy blocks with wide unique
footprints can exceed the 2MB Aeron frame ceiling (term/8 at the default
16MB term; Aeron-level fragment reassembly is already wired).

When the encoded V2 frame exceeds `MAX_BAL_FRAME_BYTES`, the encoder walks
a ladder instead of dropping attribution outright:

1. `granularity = 1` — full per-tx attribution (default).
2. `granularity = K` (5, then 10) — quantize BalIndex to tx chunks:
   within-chunk writes to a slot collapse to the chunk-final value.
   Under the seeded execution model (see the validator engine) this
   costs NO parallelism — batches execute from claimed values, not in
   conflict order — it only coarsens the artifact itself (claims are
   verifiable at chunk granularity rather than per tx; the per-tx
   receipt cross-check still covers per-tx outcomes). Kept as a
   fallback rather than the default to preserve the standard per-tx
   EIP-7928 artifact when it fits.
3. `V1` — no attribution; sequential validation for that block. Liveness
   never depends on attribution fitting.

Emit `kardamom_bal_frame_bytes` + a per-granularity counter from day one;
revisit compression (lz4) only if telemetry shows rungs 2-3 are common.

## Executor-side capture

revm does the work. After each tx's `transact`, the executor already
iterates `outcome.state` to build its per-tx `WriteSet`; the same loop
feeds `Bal::update_account(tx_index, address, &account)` — revm classifies
each state item (write: present != original, appended as
`(tx_index, value)`; read-only: present == original, recorded into
`storage_reads`) and maintains the per-slot structure incrementally. At
the boundary the exec thread hands the completed `Bal` (a move, no copy)
to a dedicated publisher thread and continues — encode and delivery are
entirely off the hot path (see Delivery guarantees; the v1 "publish
best-effort from the exec thread" design is superseded together with the
custom capture).

Determinism: identical execution ⇒ identical `outcome.state` ⇒ identical
BAL across replicas, by construction. Skipped txs (`invalid_skip`)
contribute only their deterministic sender-account read.

Cost: one map iteration per tx that already happens, plus BAL map inserts
— measure with the perf harness; budget ≤ 2% at 8.5k tps.

## Delivery guarantees — emission is NOT best-effort

Today's merged BAL is fire-and-forget: a dropped frame degrades to the
validator's bounded `bal_missing` tolerance. That contract is WRONG once
BALs drive parallel validation: a validator catching up re-executes at its
parallel rate only where BALs exist — systematic loss while it lags forces
sequential re-execution, and at high tps sequential re-execution can be
slower than the chain, i.e. the validator never catches up. BAL
availability over the catch-up window is therefore a LIVENESS property.

Requirements: (a) every block's state transition is emitted as a BAL —
no frame is ever silently dropped; (b) the exec thread never blocks on
emission.

Design — the same shape as the sealer's retained-egress replay (proven in
this repo):

1. **Dedicated publisher thread.** The exec thread's boundary handoff is
   one bounded-channel send of the owned `Bal` + boundary (a move). The
   publisher encodes and offers with ACK'D delivery and a bounded retry
   deadline per frame (transient back-pressure / NOT_CONNECTED never
   drops a frame silently).
2. **Retained replay window.** The publisher keeps the last R encoded
   frames (R sized to the catch-up SLA, e.g. 256 blocks ≈ 8.5 min at the
   2s tick; ~R×1MB memory). "Emitted" is defined as
   retained-and-replayable: a frame whose live offer exhausted its
   deadline (no live validator) still enters the ring and is served
   later. A (re)connecting validator sends `BAL_REPLAY_FROM(block)` and
   receives every retained frame from its cursor, then live frames.
3. **Beyond-window catch-up degrades honestly**: requests below the
   retention floor get `BAL_REPLAY_UNAVAILABLE` — the validator falls
   back to sequential re-execution (or checkpoint restore) for the
   pre-window gap only, exactly like the canonical-stream replay
   contract. `bal_missing` stops being a steady-state tolerance and
   becomes a hard alarm: with reliable delivery + replay, a missing BAL
   inside the window is a bug, not weather.
4. **Back-pressure bound**: the exec→publisher channel is deep enough to
   absorb encode jitter (a few blocks); if the publisher somehow wedges
   past it, the exec thread blocks — at that point something is
   fundamentally broken and back-pressure is the correct behavior (same
   philosophy as the depth-K writer bound).

## Validator-side parallel engine — seeded full parallelism

The BAL carries write VALUES, not just locations. That upgrades batch
parallelism from "ordered waves" to "fully independent execution":

1. Decode; `V1` → today's sequential path (always kept).
2. **Prefetch**: the BAL's account/slot key set IS the block's touched
   set — one batched mdbx read pass warms everything.
3. **Partition** the block into batches of 5-10 txs in block order
   (scheduling granularity — independent of wire granularity).
4. **Seed each batch independently**: for every slot the batch accesses,
   its input value is (a) the latest BAL-claimed write by any tx BEFORE
   the batch, else (b) the pre-block snapshot. Both are locally
   available; deriving the per-batch seed view from the per-tx frame is
   one pass (this is where "batch-level dedup" lives — as a derived
   view, not a wire obligation).
5. **Execute ALL batches concurrently** — no DAG, no wave barriers, no
   cross-batch waiting. Sequential inside a batch; embarrassingly
   parallel across batches. Conflicts are resolved by VALUE-PASSING
   (the seed), not by ordering — which is why chunk-merged access sets
   no longer cost parallelism (v2's false-conflict analysis applied to
   the ordering model only).
6. **Verify claims where they are produced**: each batch's computed
   writes must equal the BAL's claimed writes for its txs, and computed
   receipts must match the published receipts (existing check). Soundness
   is an induction anchored at the snapshot: batch 1 executes from pure
   ground truth, so its verified claims are true; batch 2's seeds are
   then verified-true inputs; EVM determinism forces every verified
   batch's claims to equal sequential execution. A wrong claim is caught
   at its producing batch — mutually-consistent-but-wrong chains cannot
   form. Any mismatch → divergence fail-stop. The merged write-set-hash
   check runs unchanged.
7. **Why the validator gets this free lunch**: it VERIFIES claims rather
   than discovering truth. The executor has no claims to seed from —
   executor-side parallelism remains a Block-STM problem, out of scope.

## Deposits are claims too

Every state transition is emitted as BAL claims — deposits included.
The executor's streaming path captures a deposit's write set into the
block Bal at its block index (`tx_index_in_block + 1`, the same index
space as txs), via the synthetic `record_writeset_into_bal` path. Because
revm classifies changes per FIELD against an `original` value, and the
synthetic path only knows post-values, the fabricated original must
differ in every field a later batch may seed from: nonce and balance
always (the MINT is a balance claim), code when the deposit deployed it
(a CREATE deposit's bytecode is a code claim, same class as the
cross-chunk CREATE-then-CALL divergence). The first version fabricated
only the nonce and per-field classification silently dropped the mint —
nothing consumed deposit claims then, so it was latent.

The validator executes deposit records inside seeded batches through
`execute_deposit_tx` with the SAME capture handle (symmetric
construction), folds the deposit's writes into the batch scope, and
verifies its claims at the producing unit like any tx. Blocks containing
deposits validate fully in parallel; the sequential path remains only as
the claims-timeout fallback.

## Granularity policy: K=20 to run, K=1 to debug

Production runs `KARDAMOM_BAL_GRANULARITY=20` (chunk-collapsed claims,
chunk-level verification). K>1 verification is deliberately coarse: an
intra-chunk divergence on a slot re-written later in the same chunk is
invisible to the chunk-final comparison (the receipt `write_set_hash`
check is the per-tx backstop). When validating that executor and
validator produce the SAME behavior — reproducing a divergence, chasing
a receipt-wsh mismatch, qualifying an engine change — redeploy with
granularity 1: claims become per-tx, verification runs at every tx
through the exact capture path, and the flight recorder dump names the
first diverging transaction instead of a 20-tx chunk.

## Rollout

1. **Capture + publish** (executor): V2 frames via the publisher thread
   + retained replay window; validator ignores attribution but exercises
   `BAL_REPLAY_FROM` on (re)connect; watch size/granularity/capture-
   overhead telemetry under the perf harness.
2. **Prefetch** (validator): dictionary-driven warm reads, still sequential
   execution — a latency win on its own.
3. **Parallel waves behind a flag** (`--parallel-validation`), soak +
   chain-semantics + chaos (validator-lapse, leader-kill) at parity with
   sequential before defaulting on.
4. Revisit for the executor once contract-heavy workloads make sequential
   execution the binding constraint.

## Test plan

- Property: random blocks with controlled conflict density — parallel
  result ≡ sequential result (state, receipts, write-set hashes).
- Divergence: a tampered attribution (missing declared read; forged write)
  must fail-stop, not mis-execute.
- Granularity ladder: oversized attribution degrades per-tx → K=5 →
  K=10 → V1; the validator handles every rung; no frame above the cap.
- Delivery: kill/restart the validator mid-soak — it must catch up via
  BAL replay at parallel speed and end with bal_missing == 0 inside the
  retention window; a wedged publisher must back-pressure, not drop.
- Determinism across two validator instances with different worker counts.
- Perf: capture overhead on the executor ≤ 2% at 8.5k tps (harness A/B);
  validator wall-clock per block vs sequential at varying conflict density.

## Open questions

- Read-set granularity for account fields: nonce/balance/code_hash are one
  dictionary unit here (matches `WriteSet`); splitting them would shrink
  false conflicts (gas-payment balance writes serialize same-sender txs
  anyway, which per-sender nonce ordering already forces) — likely not
  worth it in v2.
- ~~Whether `code` reads (by hash) need attribution at all~~ **ANSWERED
  IN BLOOD**: the "account-entry dependency already orders it" argument
  was correct for the wave-DAG model and WRONG for the seeded model —
  batches never wait, so a CREATE in chunk i + CALL in chunk j>i (one
  block: the stall-then-burst block shape) left chunk j executing against
  an account entry with EMPTY bytecode; every call no-op'd and
  verification reported "recomputed absent" (the deterministic live
  divergence, reproduced by `create_then_call_across_chunks_in_one_block`).
  Code IS attributed (EIP-7928 `code_changes`), seeds carry the bytecode,
  and unit verification compares code hashes.
- Compression (lz4 over the rkyv frame) if truncation telemetry says
  contract workloads routinely exceed 2MB.
