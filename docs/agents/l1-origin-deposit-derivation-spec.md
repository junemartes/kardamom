# L1-Origin Deposit Derivation — Spec

- **Date:** 2026-07-25
- **Status:** In progress — phases A, B, C and D landed. Epochs are the ONLY deposit path: the da-watcher emits one per finalized L1 block, `DepositRef` is retired, and 17 e2e scenarios (S10a–e cover the rules directly) run green
- **Motivated by:** the chain-semantics suite's S8 (`docs/agents/chain-semantics-e2e-suite-spec.md`), which must keep its workload **deposit-free** because deposits cannot survive the DA round-trip today
- **Goal (definition of done):** a chain reconstructed from L1 alone — blobs + L1 logs + genesis — reproduces the validator's state root **including deposits**, and a sequencer cannot omit or reorder a deposit without producing a chain that verifiers reject.

## Decisions taken

1. **Deposits are a typed canonical item and appear only at the front of a block.**
2. **Every L2 block carries an `l1_origin` block number**, which is what makes deposit derivation deterministic.
3. **The origin may only lag L1 by a bounded amount.**

The rest of this document is what those three imply.

## Background: why deposits break reconstruction today

A deposit mints ETH into L2 state (`engine::executor::execute_deposit_tx`: unconditional mint pre-credit, then an inner call with `disable_nonce_check` and gas price 0). None of it reaches the DA payload — `multi_archive_reader` skips `DepositRef`, and KAR1 frames carry only `{correlation_id, sender, tx_hash, raw_tx}` per transaction. Rebuilding from L1 therefore yields a chain where that ETH was never minted, and every later balance diverges.

Deposits also cannot simply be added to the existing frame: a deposit is **not a signed transaction**. It has no `raw_tx` — it is derived from an L1 log (`source_hash(l1_block_hash, log_index)`, the aliased `from`, `mint`, `gas_limit`, `is_system_transaction`).

And placement is currently a **runtime** outcome, not a function of L1: the da-watcher publishes when it happens to poll the finalized tag, each of M sequencers republishes a `DepositRef`, and the cluster picks a winner by first-seen dedup. Nothing in L1 says which L2 block a deposit landed in, so no reconstructor can place it.

## The rule

**Epoch = one L1 block.** Every L2 block has an `l1_origin`: the L1 block number whose epoch it belongs to.

1. **Origin is monotonic.** `origin(block n+1) >= origin(block n)`.
2. **Origin never skips.** Advancing from `N` to `N+2` without an epoch for `N+1` is invalid. This is the anti-censorship rule: skipping is precisely how deposits would be dropped.
3. **An epoch's deposits lead its first block.** The first L2 block with origin `N` begins with exactly the deposits of L1 block `N`, all of them, ordered by log index — then ordinary transactions follow.
4. **Origin ≤ L1 finalized at production time.** Kardamom's da-watcher already reads only the `finalized` tag; keeping that means a finalized block never reorgs, so `origin` (a number) maps to exactly one hash forever, and no L1-reorg machinery is needed. That simplification is deliberate and worth preserving.
5. **Bounded lag.** The head block's origin must be within `MAX_ORIGIN_LAG` L1 blocks of the L1 finalized head.

Rules 1–3 are **deterministic and verifiable from the chain alone**. Rule 5 is a *liveness* property: a verifier reading history cannot tell how fresh the origin was when a block was produced, only how fresh the **head** is against its own L1 view. So rule 5 is enforced as a fail-stop/alarm on the head, not as a validity rule on history. Stating that plainly avoids a spec that pretends to prove something it can't.

`MAX_ORIGIN_LAG` also bounds catch-up work: a sequencer down for an hour must replay ~300 epochs on restart, not an unbounded number.

## Wire changes

### Canonical stream

One new record type, atomic by construction:

```
EpochRecord {
    l1_number: u64,
    l1_hash:   B256,
    deposits:  Vec<Deposit>,   // full content, log order
}
```

Emitted by the da-watcher when a new finalized L1 block is observed, including when it carries **no** deposits (rule 2 requires every epoch to appear). Deduped by the cluster on `canonical_id = keccak(l1_hash)`, so racing replicas that each emit the same epoch collapse to one — the property the cluster's first-seen dedup already provides for `DepositRef`.

**An epoch claims a contiguous range of canonical slots**: the marker, plus one per deposit. Every other record maps 1:1 onto a slot, and three separate mechanisms rely on that — the sealer's cumulative `canonicalCount` (the alignment key in `BlockBoundaryStart::end_tx_idx`), the egress reader's dense `next_index` cursor (a hole reads as a gap and triggers replay catch-up), and the executor's per-tx `tx_idx` (which keys receipts and the BAL, so no two txs may share one). An epoch is the one record that carries N transactions, so it reserves N+1 slots: slot 0 is the marker (advances the origin, applies no tx), slots `1..=N` are the deposits. Counting the marker even when `N == 0` keeps the range non-empty, so an empty epoch owns a distinct index instead of colliding with the record after it.

The count rides on the ingress frame (`slot_count: u32`) because the Java sealer never parses payloads; every consumer that *does* parse re-derives it and fail-stops on a mismatch. This is what keeps the sealer schema-agnostic: it learns "close the block, adopt this origin, consume this many slots" without knowing what an epoch or a deposit is.

Two consequences worth calling out:

- **The deposit join disappears.** Today a `DepositRef` on the canonical stream must be joined against a `Deposit` envelope arriving separately on `tx_deposits`; that join is what times out and aborts the executor when an envelope is lost (the failure S9b reproduces, and a recurring theme in the recovery series). Carrying deposit content in the canonical record removes the join entirely.
- **`tx_deposits` stops being an execution dependency.** It remains useful as the da-watcher→sequencer transport, but the canonical stream becomes self-contained.

### Block boundary

`BlockBoundary` gains `l1_origin: u64`.

**An `EpochRecord` forces a boundary.** On ordering one, the sealer closes the current block *first*, then the epoch's deposits open the new block. This is what makes rule 3 true by construction rather than by convention — otherwise an `EpochRecord` arriving mid-block would strand deposits in the middle of it.

⚠ **The sealer must not read L1.** `CanonicalSealerState` is a Raft-replicated deterministic state machine; giving it external I/O would let replicas observe different L1 states and diverge. The origin therefore arrives *as ordered data* (the `EpochRecord`) and the sealer only tracks "the origin of the latest epoch I have ordered". This is the single most important constraint in this design.

Cost: one extra boundary per epoch — with 12 s L1 blocks and a 250 ms tick, roughly one per 48 blocks. Negligible. The forced boundary carries the **old** origin: it closes a block belonging to the outgoing epoch. It is also skipped when the open block is still empty, so an L1 catch-up burst does not emit a run of empty blocks.

Two smaller consequences of forcing boundaries off-tick:

- **Timestamps are now forced strictly increasing.** A tick-floored timestamp alone stopped being unique once a second boundary can fall inside the same 250 ms window, and two blocks sharing a timestamp is a trap for anything reasoning about block time. Under pure tick cadence the clamp is a no-op.
- **Sealer snapshot v2.** `l1Origin`, `lastL2Timestamp` and `lastBoundaryCount` are replicated state and must round-trip. v1 snapshots still load, with the trio defaulting to zero — exactly the pre-origin state — so this part needs no coordinated migration.

### DA payload (KAR1 → KAR2)

Per block, add `l1_origin` (varint delta against the previous block — usually 0, so ~1 byte). **Deposits are not written to the blob at all.**

That is the point of deriving rather than recording: deposits are unsigned, so a blob-carried deposit would be a batcher's unverifiable claim to mint ETH. With derivation, the blob says only *which epoch* a block belongs to, and L1 itself supplies what the deposits were.

Reconstruction becomes: for each block, if `l1_origin` advanced from `M` to `N`, fetch L1 block `N`'s lockbox logs, derive each deposit via the existing `da_watcher::derive::{source_hash, alias_l1_address}`, and insert them at the front of that block. `kardamom-reconstruct` gains `--lockbox`; it already takes `--l1-rpc`.

Bump `VERSION` in `frame.rs` (the byte exists) and keep a v1 decode path.

### Replay

`ReplayBlock.txs: Vec<TxEnvelope>` becomes `Vec<ReplayItem>` (`Tx(TxEnvelope) | Deposit(Deposit)`), and `replay_blocks` dispatches deposits to the existing `execute_deposit_tx`, preserving mint-before-call, `disable_nonce_check` and gas price 0 exactly — any deviation shows up as a root mismatch.

## Verification

Deriving deposits is only half the guarantee; someone must **check** that the chain obeyed the rule, or a buggy sequencer silently produces a chain nobody can rebuild.

**Phase 1 shipped.** Notes on what landed, and what it does NOT buy:

- Split by cost. Rules 1–2 (monotonic, no-skip) read only local state, so they
  run INLINE on the exec thread and reject before the epoch's deposits are
  applied. The content check (hash + exact deposits) needs an L1 round trip, so
  it runs on a background task; its verdict lands on the next epoch. That
  deferral is the honest price of phase 1 — a bad epoch is DETECTED, not
  PREVENTED, and one more epoch may be applied before the halt. Preventing it
  is phase 2.
- Rule 4 is enforced as "L1 still does not have this block after the retry
  window". An epoch anchored to a block that has not happened is a fault, not
  an outage. The distinction matters: an unreachable L1 must never read as a
  divergence, or an RPC blip takes the validator down.
- Verification is retried (8 attempts, 2 s apart) before any verdict. A
  one-shot check would silently drop coverage for an epoch that arrived during
  a blip — exactly where a forgery would hide — and would also fail the honest
  race where this validator's L1 view lags the producer's.
- The validator derives through the producer's own `derive_epoch`. A verifier
  running a second, subtly different copy of the rule would verify nothing.
- Enabled by `--lockbox` alongside `--l1-rpc-url`. Without it the origin
  SEQUENCE is still enforced; only content checking is off.
- **S11** is the drill: a forged epoch must halt the validator. S10a–e prove an
  honest producer builds a derivable chain; S11 proves the guarantee has teeth.
  The whole L1-backed suite now runs with verification ON, so a producer that
  quietly stopped matching L1 would fail every bridge scenario, not just S11.

- **Phase 1 — validator verifies.** The validator gains an L1 read connection and, for each `EpochRecord`, re-derives the epoch's deposits from L1 and requires an exact match (set, order, and `l1_hash` for `l1_number`), plus rules 1–2 on the origin sequence. Mismatch is a divergence → the existing fail-stop machinery. Safety without new executor dependencies.
- **Phase 2 — executors verify too.** Executors gain a read-only `--l1-rpc` and reject a bad epoch rather than committing it. This is the difference between "we notice" and "it cannot happen", and it costs every executor an L1 dependency — a real deployment change, hence its own phase.

The validator already holds an L1 connection when the attester is enabled, so Phase 1 is mostly wiring.

## Edge cases to pin down

- **Genesis origin** — the chain TOML gains `l1_origin_genesis`; block 0's origin is that value, and rule 2 counts from it. **Still open, and phase C's tests made the consequence concrete:** the watcher seeds its cursor at the finalized tip it first observes, so the origin jumps from 0 to that block in one step. Deposits made in L1 blocks *before* the seed are therefore underivable — a chain's verifiable history starts at its first epoch, not at L1 genesis. S10a/S10c exempt exactly that one transition and assert `+1` steps everywhere after it.
- **Empty epochs** — still emit `EpochRecord` with no deposits, or rule 2 is unenforceable.
- **L1 unavailable** — origin stalls, L2 keeps producing blocks with the old origin, deposits are delayed but never lost, and the rule-5 alarm fires. No liveness loss for ordinary transactions.
- **Huge epochs** — a pathological L1 block with thousands of deposits could exceed the cluster's max message length (~128 KB). Either cap deposits per record and allow an epoch to span several records with an explicit "final" flag, or bound it and document the ceiling. **Open.**
- **Deposit ordering within an epoch** — log index, which is already the `source_hash` input.
- **Migration** — this changes `BlockBoundary`, the canonical record set, and the DA format. It is a breaking chain change: fresh chain or a coordinated cutover, not a rolling upgrade. **Open: which.**

## Testing

Phase C landed with five scenarios (S10a–e) that assert the rules against
**persisted headers and executed receipts**, not logs — a component can log
the right thing and still build the wrong chain:

| Scenario | Rule | What it would catch |
| --- | --- | --- |
| S10a | 1, 2, 4 | An idle L1 emitting nothing, so the origin sequence gets a hole |
| S10b | 3 | Deposits landing mid-block when L2 traffic is flowing concurrently |
| S10c | 3 | One L1 block's deposits split across L2 blocks, or reordered |
| S10d | 5 | A stalled L1 stalling L2, or the origin advancing past a halted L1 |
| S10e | 1, 2 | Any L1-recorded deposit missing from L2, or applied twice |

S10e derives its expected set straight from L1 logs through the same
`derive_epoch` the producer uses — the property a verifier will enforce in
phase D, asserted here against the producer.



- **S8** drops its deposit-free constraint; the receipt-based block recovery already handles deposits (their receipt is keyed by `source_hash` and carries `blockNumber`/`transactionIndex` like any other), so parity coverage extends for free once derivation lands.
- **New negative scenarios**, in S7's injection style: a sequencer that omits an epoch's deposit, one that skips an epoch, and one that reorders deposits within an epoch — each must be rejected, not silently accepted. These are the tests that make the anti-censorship claim real.
- **Freshness alarm** — a paused da-watcher must trip the rule-5 alarm within `MAX_ORIGIN_LAG`, without stalling ordinary transaction flow.
- **Target C** needs the cluster genesis to carry `l1_origin_genesis`, alongside the contract-deploy wiring the semantics shard already requires.

## Implementation status

| Phase | Scope | State |
| --- | --- | --- |
| A | `EpochRecord` + `derive_epoch` as the one shared definition (`crates/types/src/epoch.rs`), moved out of the da-watcher so validator and reconstructor share it | **Done** |
| B | Canonical stream: `TxOrderingMessage::Epoch`, `KIND_ORIGIN_RECORD` framing with `l1_origin` + `slot_count`, `l1_origin` on both boundary types, sealer forced boundary + origin tracking + snapshot v2, executor expansion of an epoch into marker + deposits | **Done** |
| C | Producer switch-over: da-watcher emits one epoch per finalized L1 block (including empty ones), sequencer forwards them verbatim, executor's `tx_deposits` join retired, `l1_origin` persisted in block headers | **Done** |
| D | Verification (phase 1): the validator re-derives every epoch from L1 and fail-stops on disagreement — rules 1, 2 and 4 plus exact deposit content | **Done** |
| E | KAR2 DA payload carries the origin; `kardamom-reconstruct` re-derives deposits from L1 | Not started |

Phase C makes this the live path. Notes on what it changed beyond the obvious:

- **`tx_deposits` now carries `EpochRecord`, not `Deposit`.** It is a da-watcher → sequencer transport only; the executor no longer subscribes to it at all, and the ref↔envelope join (with its timeout, its archive refetch, and its ability to strand a deposit) is gone. A `DepositRef` arriving on the canonical stream is now a fail-stop: this is a breaking chain change, not a rolling upgrade.
- **An epoch is never skipped on a transport error.** The old per-deposit policy was "log it and carry on", which for epochs would punch a permanent hole in the origin sequence — exactly what rule 2 forbids. The watcher now stops the range and retries from the failed block.
- **The watcher fetches each block's hash.** A block with no deposits has no log to carry its hash, and the hash is what the canonical id derives from. `derive_epoch` then cross-checks every log's `block_hash` against it, which catches a reorg landing between the two reads.
- **Headers grew to 24 bytes** to persist `l1_origin`. 20-byte rows still decode as origin 0, so an existing state DB needs no migration.

## Open questions

1. **`MAX_ORIGIN_LAG` value** — in L1 blocks. Long enough that ordinary da-watcher hiccups don't alarm, short enough to bound catch-up and censorship. A first guess is ~50 finalized blocks (~10 min), but it should be derived from the deposit-latency SLO we want to promise.
2. **Huge-epoch handling** — cap-and-span vs. hard ceiling (above). Note the slot accounting already tolerates a spanning epoch: each record would declare its own slot range.
3. **Migration path** — fresh chain vs. coordinated cutover.
4. **Does the origin belong in the block header hash?** Kardamom's `headers` table stores `(end_tx_idx, l2_timestamp)` and there is no block-header commitment today, so `l1_origin` would live alongside them. If a header commitment is ever added, the origin must be inside it.
5. **Phase 2 scope** — whether every executor taking an L1 dependency is acceptable operationally, or whether validator-only verification plus the DA-parity test is judged sufficient.
