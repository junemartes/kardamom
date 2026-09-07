# exec-core

## Summary

The group holds about 10,500 lines across `exec-core`, `footprint`, `reconstruct`, and `guest`. Code health is good on the concurrency axis: there are no manual `drop` calls, no channels, no `Box<dyn Error>`, and only two sync primitives, both justified. The real debt is shape and staleness. `crates/exec-core/src/executor/scope.rs` is the hot spot: 809 code lines, of which 317 are inline tests, and 7 functions hide behind `allow(clippy::too_many_arguments)`. One `Copy` argument-group struct (`TxSlot`) removes all 7 allows. A second cluster is stale doc comments: `ExecScope` (renamed to `Executor`), a free `execute_tx` (renamed to `execute_once`), and `write_set_from_evm_state` are all named in doc links that no longer resolve, and several comments carry PR numbers, phase labels, and "an earlier version" archaeology. Counts: R1 24, R2 17, R3 1 file plus 3 KEEP notes, R4 0, R5 2, R6 0, R7 0, R8 6, R9 5, R10 11.

`crates/footprint` carries a dead hot-path field: `TxObs.args` is filled by every producer (`decoded_view` allocates up to six `U256` words per transaction) and is read by nobody. `crates/exec-core` targets `no_std` in part; every R10 rewrite noted below stays inside `alloc`, which the crate already imports, except where marked.

## R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/exec-core/src/executor/mod.rs:16 | `Module layout (a plain split of the former single-file executor.rs)` | history of a past refactor | drop "(a plain split of the former single-file `executor.rs`)" |
| crates/exec-core/src/executor/mod.rs:21 | ``- `scope`: [`ExecScope`] (per-block EVM and cache), and the per-call`` | `ExecScope` does not exist; the type is `Executor` | rename the link to `[`Executor`]` |
| crates/exec-core/src/executor/mod.rs:22 | ``[`execute_tx`] wrapper, including the deterministic invalid-skip path`` | there is no free `execute_tx`; it is `Executor::execute_once` | rename the link |
| crates/exec-core/src/executor/db.rs:3 | ``Both the tx path ([`super::ExecScope`]) and the deposit path`` | broken doc link to a renamed type | use `[`super::Executor`]` |
| crates/exec-core/src/executor/db.rs:25 | ``An owned variant of [`SnapshotRef`]. [`super::ExecScope`] must be`` | broken doc link | use `[`super::Executor`]` |
| crates/exec-core/src/executor/db.rs:134 | ``([`super::ExecScope::seed_layer`]) and the deposit path`` | broken doc link | use `[`super::Executor::seed_layer`]` |
| crates/exec-core/src/executor/db.rs:108 | `Consensus rule (version 0, pinned by phase 3): BLOCKHASH returns` | "phase 3" is project-plan context | keep the rule, drop "pinned by phase 3" |
| crates/exec-core/src/executor/scope.rs:2 | ``plus the free [`execute_tx`] compatibility wrapper`` | no such free function | say "the `execute_once` one-shot constructor" |
| crates/exec-core/src/executor/scope.rs:50 | `This replaces the old per-tx construction, which DHAT measured at` | describes what changed, not what is | state the invariant: one EVM and one cache per block |
| crates/exec-core/src/executor/scope.rs:58 | ``The free [`execute_tx`] wrapper (one scope per call) keeps the old`` | stale name plus "the old signature" | rewrite around `execute_once` |
| crates/exec-core/src/executor/scope.rs:121 | `Execute a DEPOSIT on this block scope. The historic free function` | contrasts with a past implementation | describe the current behaviour only |
| crates/exec-core/src/executor/scope.rs:130 | `BAL claims match the historic path's true changes. The` | ties the contract to a legacy path | state the artifact contract directly |
| crates/exec-core/src/executor/scope.rs:254 | ``BAL claims match the historic free function (`execute_xchain_tx`)`` | same | same |
| crates/exec-core/src/executor/scope.rs:516 | `them directly, with no per-tx re-seeding (that used to cause` | past-tense allocation archaeology | keep "later txs read them directly"; drop the rest |
| crates/exec-core/src/executor/scope.rs:560-578 | `/// Execute one tx against a snapshot and the current PendingDelta.` | a 16-line doc comment plus an `allow` sits on an `impl` block, not a function | move the docs onto `execute_once`; delete the impl-level allow |
| crates/exec-core/src/executor/scope.rs:577 | `"execute one tx" entry point. Packaging them into a struct would just` | justifies the allow the R3 plan removes | delete with the allow |
| crates/exec-core/src/executor/tx_env.rs:22 | `This newtype owns both halves of the old free-function pair` | names removed functions | say what `DecodedTx` holds |
| crates/exec-core/src/executor/tx_env.rs:61 | ``This uses a full struct literal on purpose. It used to end in`` | keep the rule, drop the history | "Use a full struct literal. A revm field addition must break the build." |
| crates/exec-core/src/executor/write_set.rs:35 | `a seed; see ClaimIndex::code). An earlier version fabricated only` | past-bug archaeology | keep the per-field rule; drop the sentence |
| crates/exec-core/src/executor/write_set.rs:54 | ``byte-identical to the historic free-function path`` | ties the contract to a legacy path | state the two extra rules directly |
| crates/exec-core/src/executor/write_set.rs:182 | ``[`write_set_from_evm_state`] (which iterates revm's per-tx`` | broken doc link; the function is `write_set_from_evm_state_inner` | fix the link |
| crates/exec-core/src/executor/write_set.rs:301 | `Public API: the Block-STM engine (kardamom-stm) builds per-tx write` | the item is a private `fn` | reword, or move the note to `WriteSet::from_evm_state` |
| crates/exec-core/src/executor/write_set.rs:318 | `is populated on load). An earlier version copied the full` | past-bug archaeology with cost figures | keep the CREATE-only rule; drop the history |
| crates/exec-core/src/delta.rs:27 | ``This used to store three `BTreeMap`s. A B-tree allocates a 1KB-class`` | past data-structure choice | state why `SmallVec` fits, in the present tense |
| crates/exec-core/src/delta.rs:272 | ``These are hash maps on purpose (they used to be `BTreeMap`s). The`` | same | drop the parenthesis |
| crates/exec-core/src/anchor/mod.rs:3 | `Phase 2's witness fails closed, but is not anchored. WitnessDb` | project-phase label | describe the gap without the phase |
| crates/exec-core/src/stateless.rs:22 | `The witness itself is unanchored until phase 3b (MPT proofs against` | phase label | name the missing capability |
| crates/exec-core/src/stateless.rs:170 | `[Executor::execute_deposit]). This replaces the old snapshot,` | describes what changed | drop the last sentence |
| crates/exec-core/src/state.rs:18 | `keeps the old behavior of returning an immutable snapshot` | "the old behavior" | "returns an immutable snapshot" |
| crates/exec-core/src/block_env.rs:7 | ``docs/agents/l1-client-suite-port-spec.md``). Kardamom supports exactly one`` | spec doc named by path | drop the path; keep the rule |
| crates/exec-core/src/block_env.rs:39 | `Revm's own doc comment on Precompiles::cancun still claims c-kzg` | a note about another crate's stale comment | drop; it rots with every revm bump |
| crates/exec-core/src/features.rs:25 | ``See `docs/specs/2026-08-16-l1-upgrade-feature-flags-design.md`.`` | dated spec-doc reference | drop |
| crates/exec-core/src/bal_ladder.rs:2 | `See docs/agents/bal-attribution-parallel-validation-spec.md.` | spec-doc reference | drop |
| crates/exec-core/src/executor/deposit.rs:29 | `Deposit semantics (OP-aligned, ported from the old` | "ported from the old crates/node/src/executor.rs" | keep "OP-aligned"; drop the provenance |
| crates/footprint/src/classifier.rs:4-14 | `An earlier version recovered mapping base slots by keccak inversion:` | 11 lines about a removed technique | keep the two-tier description at line 17; delete lines 4-14 |
| guest/kardamom-zk-host/src/main.rs:6 | `round-trip contract of phase 3c.` | phase label | "the guest and host must commit identical public values" |

## R2 long methods

| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/exec-core/src/executor/scope.rs:419 | `Executor::execute_tx_decoded` | 112 | `classify_transact_error` (the 3-arm `Err` match at 444-480), `capture_touches` (502-514), `build_tx_receipt` (523-555) |
| crates/exec-core/src/executor/scope.rs:136 | `Executor::execute_deposit` | 88 | `credit_mint` (147-168), `ensure_sender_in_write_set` (197-210), `build_deposit_receipt` (215-241) |
| crates/exec-core/src/executor/scope.rs:263 | `Executor::execute_xchain` | 66 | `reject_valued_message` (273-282), `transact_without_nonce_check` (284-304, shared with `execute_deposit`), `build_xchain_receipt` (315-342) |
| crates/exec-core/src/executor/deposit.rs:43 | `execute_deposit_tx` | 87 | `seed_composed_cache` (63-74, shared with `execute_xchain_tx`), `credit_mint` (76-100), `build_deposit_receipt` (137-163) |
| crates/exec-core/src/executor/xchain.rs:53 | `execute_xchain_tx` | 90 | `seed_composed_cache` (80-90), `deliver_call` (92-118), `build_xchain_receipt` (139-165) |
| crates/exec-core/src/anchor/mod.rs:175 | `verify_witness_anchored` | 97 | `prove_accounts` (184-236), `prove_storage` (240-279), `verify_code_blobs` (283-289) |
| crates/exec-core/src/anchor/mod.rs:306 | `recompute_post_root` | 86 | `group_storage_writes` (316-322), `recompute_storage_roots` (323-345), `post_account_leaf` (354-395) |
| crates/exec-core/src/anchor/sparse.rs:172 | `SparseTrie::insert_in` | 86 | `split_leaf` (194-213), `split_extension` (221-243), `descend_branch` (245-259) |
| crates/exec-core/src/anchor/sparse.rs:275 | `SparseTrie::remove_in` | 79 | `remove_under_extension` (293-302), `collapse_branch` (320-360), `splice_survivor` (334-359) |
| crates/exec-core/src/executor/write_set.rs:115 | `record_writeset_into_bal_inner` | 63 | `fabricated_account` (118-151), `fabricated_slot` (152-175) |
| crates/footprint/src/grade.rs:87 | `grade_block` | 85 | `predict_and_score_coverage` (106-125), `predicted_pair_set` (131-145), `wave_structure` (154-184) |
| crates/footprint/src/oracle.rs:152 | `analyze` | 75 | `per_block_oracle` (158-173), `split_train_holdout` (176-183), `grade_holdout_blocks` (188-236) |
| crates/footprint/src/oracle.rs:243 | `Report::summary` | 69 | `aggregate_blocks` (246-256), `percentile` (258-263), `format_grading` (288-309) |
| guest/kardamom-zk-host/src/main.rs:25 | `main` | 87 | `parse_single_args`, `run_prove` (59-96), `run_execute` (98-117) |
| guest/kardamom-zk-host/src/main.rs:121 | `batch_main` | 100 | `load_spool_frames` (150-175), `expected_batch_outputs` (176-182), `prove_or_execute_batch` (196-226) |
| guest/kardamom-zk-guest/src/bin/batch.rs:26 | `main` | 83 | `records_and_digest` (58-86, duplicated in `guest/kardamom-zk-guest/src/main.rs:31-57`), `check_root_chain` (46-56) |
| crates/exec-core/src/stateless.rs:311 | `execute_block_anchored` | 22 | not long; listed only because it duplicates the guest-side record fold |

## R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/exec-core/src/executor/scope.rs | 809 (492 non-test + 317 inline test) | Split in three steps. **(1)** Move the `#[cfg(test)] mod tests` block (lines 736-1102) into `crates/exec-core/src/executor/scope_tests.rs`, included with `#[cfg(test)] #[path = "scope_tests.rs"] mod tests;`. That alone drops the file to 492 code lines, under the 500 limit. **(2)** Move the skip machinery into a new `crates/exec-core/src/executor/skip.rs`: `skip_reason_of_tx` (617-641), `Executor::skip_receipt` (643-707), and `Executor::skip` (709-733), about 90 code lines. These share no state with the EVM scope; `skip_receipt` is explicitly a pure associated constructor. Re-export from `executor/mod.rs` next to the other names. **(3)** Move the two derived-transaction paths into `crates/exec-core/src/executor/scope_derived.rs`: `Executor::execute_deposit` (121-243) and `Executor::execute_xchain` (245-344), about 155 code lines. Both run the same "toggle nonce check, transact, commit, build a fee-free receipt" shape and belong beside each other; they also let a shared `transact_without_nonce_check` helper live in one place. What stays in `scope.rs`: `TouchSet`, the `Executor` struct, `new`, `new_with_envs`, `seed_layer`, `execute_tx`, `execute_tx_decoded`, and `execute_once` — about 250 code lines, one cohesive unit. |
| crates/exec-core/src/delta.rs | 329 | KEEP. `WriteSet`, its consensus encoding, and `PendingDelta` are one hashing contract. Splitting the encoder from the type it encodes would let the two drift, and the file already stays under the limit. |
| crates/exec-core/src/anchor/sparse.rs | 369 | KEEP. One data structure with its walk, insert, remove, and re-hash. Every helper at the bottom (`placeholder`, `encode_node`, `rlp_ref`, `wrap_extension`, `merge_extension`) is used only by the `SparseTrie` methods above it. |
| crates/footprint/src/oracle.rs | 259 | KEEP, but move the duplicated predicted-pair-set logic (see R10) into a shared function that `grade.rs` also calls. |

### R3 argument-group struct for the 7 `too_many_arguments` sites

All 7 allows are `scope.rs:261, 346, 418, 576, 584, 656, 710`. They share one argument cluster: where the record sits in the block, and what its receipt's gas fields must carry. Introduce one `Copy` struct next to `TouchSet`:

```rust
/// Where one canonical record sits in its block. Every execute and skip
/// entry point takes this instead of four loose scalars.
#[derive(Debug, Clone, Copy)]
pub struct TxSlot {
    /// Local sanity counter for the executor.
    pub tx_idx: TxIndex,
    /// Canonical wire id, copied into `Receipt.tx_idx`.
    pub position: BPosition,
    /// Zero-based index within the block.
    pub index_in_block: u64,
    /// Running gas sum for txs already executed in this block.
    pub cumulative_gas_before: u64,
}
```

`TxSlot` is `Copy`, so the skip paths reuse the same value the execute path already holds — no borrow juggling, and no extra allocation in `no_std`. New signatures and argument counts (clippy's threshold is 8):

- `execute_deposit(&mut self, slot: TxSlot, deposit: &Deposit, bal: Option<...>)` — 4
- `execute_xchain(&mut self, slot: TxSlot, origin_chain_id: u64, message: &XChainMessage, bal: Option<...>)` — 5
- `execute_tx(&mut self, slot: TxSlot, envelope: &TxEnvelope, bal: Option<...>, touches: Option<&mut TouchSet>)` — 5
- `execute_tx_decoded(&mut self, slot: TxSlot, envelope: &TxEnvelope, alloy_env: &DecodedTx, bal: Option<...>, touches: Option<&mut TouchSet>)` — 6
- `execute_once(snapshot, parent, delta, env, slot: TxSlot, envelope, bal)` — 7
- `skip_receipt(reason, detail, slot: TxSlot, envelope, nonce, to, block_number)` — 7
- `skip(&self, reason, detail, slot: TxSlot, envelope, nonce, to)` — 6

Every count lands under the threshold, so all 7 `#[allow(clippy::too_many_arguments)]` attributes and the four comments that justify them go away. `stateless::execute_record_in_scope` (`crates/exec-core/src/stateless.rs:176`) builds the same four scalars per arm today; it would build one `TxSlot` and pass it to all three arms, which also removes the per-arm repetition there.

## R4 manual drops

None found.

## R5 sync primitives and channels

| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/exec-core/src/witness.rs:125 | `std::sync::Mutex<Recorded>` | JUSTIFIED | `StateDatabase::basic` takes `&self`, and `execute_block_parallel` requires the shared snapshot to be `Sync`. The recorder must memoize first-touch values from several worker threads through a shared reference. No ownership transfer or channel expresses this. | none; the doc at line 114-121 already states the reason |
| crates/exec-core/src/state.rs:51 | `Arc<RwLock<MockInner>>` | JUSTIFIED | `WriterApplyingQueue` applies block deltas from the writer thread while the exec thread reads snapshots. Cross-thread shared mutable state is the point of the fixture. | none |

## R6 dynamic dispatch

None found in non-test code. The only `dyn` in the group is `crates/exec-core/tests/anchor_state.rs:316` (see Tests).

## R7 too many generics

None found. The largest is `apply_block_close_actions<E, F>` (`crates/exec-core/src/features.rs:117`): two type parameters, one bound.

## R8 unnecessary pub

| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/exec-core/src/executor/scope.rs:711 | `pub fn Executor::skip` | no call site anywhere in the workspace | delete it; `skip_receipt` covers every caller |
| crates/exec-core/src/executor/mod.rs:38 | `pub use xchain::XCHAIN_DELIVERY_OVERHEAD` | only `executor/tx_env.rs:141` and `executor/xchain.rs:102` read it; no other crate and no test | drop from the `pub use`; make the constant `pub(super)` |
| crates/footprint/src/lib.rs:49 | `pub args: Vec<U256>` on `TxObs` | written by `engine/src/shadow.rs:150`, `stm/src/schedule.rs:83`, `bench/src/bin/stm-p2.rs`, and both test modules; read by nobody | delete the field and the loop in `decoded_view` (`lib.rs:81-88`) that builds it. This also removes a per-tx `Vec<U256>` allocation of up to 6 words from the STM scheduler's hot path. |
| crates/exec-core/src/anchor/mod.rs:169 | `pub accounts: BTreeMap<...>` on `ProvenPre` | read only inside `anchor/mod.rs` (`recompute_post_root`, line 326 and 355) | make the field private and keep `ProvenPre` an opaque token, or `pub(crate)` |
| crates/footprint/src/classifier.rs:61,63,65 | `SelectorStats::slot_seen`, `slot_obs`, `account_seen` | read only inside `classifier.rs`; the one external consumer (`bench/src/bin/stm-p0.rs:304`) reads `by_selector.len()` and calls `class_shares()` | make the three fields `pub(crate)` |
| crates/reconstruct/src/lib.rs:36 | `pub struct ReconstructError(pub String)` | no crate constructs it from outside; the binary only propagates it through `anyhow` | keep the type `pub`, make the field private, add `Display` only |

## R9 defensive validation

| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/exec-core/src/executor/scope.rs:273 | `if message.value != 0 { return Err(...) }` | the same check runs in the free path (`xchain.rs:69`); a raw `u64` field is validated at two layers | give `XChainMessage` a `ValuelessMessage` newtype with a fallible constructor at the derivation boundary, then neither executor re-checks |
| crates/exec-core/src/executor/xchain.rs:69 | `if message.value != 0 { return Err(...) }` | duplicate of the above, with a duplicated error string | same |
| guest/kardamom-zk-guest/src/bin/batch.rs:31 | `assert!(!input.blocks.is_empty(), "empty batch")` | assert on a decoded input in non-test code | parse into a `NonEmptyBatch` newtype after `rkyv::from_bytes`; a panic is the guest's fail-closed posture, so the win here is one check instead of the three below |
| guest/kardamom-zk-guest/src/bin/batch.rs:47 | `assert_eq!(number, first_block + i as u64, "batch blocks must be contiguous")` | per-iteration structural check on input | a `ContiguousBlocks` constructor validates the whole range once, then the loop needs no check |
| guest/kardamom-zk-guest/src/bin/batch.rs:52 | `assert_eq!(block.witness.pre_state_root, Some(running_root), ...)` | this one is a real inductive invariant, not input validation | keep, but state it as the root-chain rule; it cannot move to a constructor because `running_root` comes from execution |
| guest/kardamom-zk-host/src/main.rs:145 | `anyhow::ensure!(first >= 1 && last >= first, "bad block range")` | raw `u64` pair validated ad hoc after parsing | parse into a `BlockRange` newtype with a fallible `new(first, last)` |

## R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/exec-core/src/bal_ladder.rs:134-153 | `let mut i = 0; while i < changes.len() { ... changes.remove(i - 1); continue; }` | index loop with `Vec::remove` inside, which is O(n^2). Quantize every index in one `for` pass, then collapse with a single reverse-keeping dedup: `changes.reverse(); changes.dedup_by(|a, b| index_of(a) == index_of(b)); changes.reverse();`. Needs `alloc` only, which the crate already imports. |
| crates/exec-core/src/bal_ladder.rs:99-124 | `let mut seen: BTreeMap<u64, usize>; let mut kept: Vec<_>` | the same last-wins collapse as `dedup_changes`, written a second way. Reuse one `dedup_changes`-style helper for storage changes too. |
| crates/exec-core/src/anchor/mod.rs:316-322 | `for ((addr, key), value) in &delta.storage { storage_writes.entry(*addr).or_default().push(...) }` | a group-by fold: `delta.storage.iter().fold(BTreeMap::new(), \|mut m, ((a, k), v)\| { m.entry(*a).or_insert_with(Vec::new).push((*k, *v)); m })`. Needs `alloc`; already available. |
| crates/exec-core/src/anchor/mod.rs:349-352 | `let mut touched: Vec<Address> = ...; touched.extend(...); touched.sort_unstable(); touched.dedup();` | build a `BTreeSet<Address>` directly from the two key iterators. It is sorted and deduped by construction, and `alloc::collections::BTreeSet` is already in use in this module. |
| crates/exec-core/src/anchor/mod.rs:105-116 | `let mut nodes = BTreeMap::new(); let mut prev = None; for raw in &proofs.nodes { ... }` | keep as-is. The canonical-order check needs an early `return Err` on a running comparison, which is the imperative form the rule exempts. |
| crates/exec-core/src/stateless.rs:145-155 | `for (i, rec) in records.iter().enumerate() { ... receipts.push(receipt) }` | keep as-is. Each step mutates `scope` and `delta` and can early-return with `?`. Also the block-driver hot path. |
| crates/exec-core/src/stateless.rs:140 | `-> Result<(BlockExecOutput, ()), ExecutorError>` | the `()` tuple element is dead weight that every caller discards with `.map(\|(out, _)\| out)` or `let (out, _) =`. Return `Result<BlockExecOutput, ExecutorError>` and drop the three destructurings at lines 99, 114, and 130. |
| crates/footprint/src/grade.rs:131-145 | `for i in 0..graded.len() { for j in i+1..graded.len() { ... } }` | index loop over a slice. `graded.iter().enumerate().flat_map(\|(i, a)\| graded[i+1..].iter().map(move \|b\| (a, b)))` reads better. This is the O(n^2) pair grading the doc at line 85 caps on purpose, so measure before changing it; the shape is the cost, not the indexing. |
| crates/footprint/src/grade.rs:107-125 | `let mut predicted: Vec<Option<...>> = Vec::with_capacity(...); for o in &graded { ... }` | the loop mutates three `BlockGrade` counters at once. A `fold` over `(Vec, counters)` expresses it, but the current form is clearer; extract `predict_and_score_coverage` (see R2) instead. |
| crates/footprint/src/grade.rs:172-184 | `let mut level = vec![0; n]; for (lo, hi) in &edges { if level[*lo] + 1 > level[*hi] {...} }` plus `for l in &level { width[*l] += 1 }` | the second loop is a plain histogram: `level.iter().fold(vec![0usize; waves], \|mut w, l\| { w[*l] += 1; w })`, or count with `level.iter().filter(\|l\| **l == k).count()` per wave. The first loop is a real relaxation pass; keep it. |
| crates/footprint/src/oracle.rs:198-225 | `let mut predicted ...; for i in 0..txs.len() { for j in i+1..txs.len() { ... } }` | byte-for-byte the same predicted-pair-set computation as `grade.rs:107-145`. Extract one `predicted_pairs(stats, txs, exclude) -> (HashSet<(u64,u64)>, usize /*cold*/)` and call it from both. The module doc at `grade.rs:8` promises the two match exactly, so a shared function is the only way to keep that promise. |
| crates/footprint/src/oracle.rs:246-256 | `let (mut gas, mut cp, mut pairs, mut txs) = (0, 0, 0, 0); for b in &self.blocks {...}` | four separate iterator sums plus one `filter_map` for `ratios`, or one `fold` over a small accumulator struct. |
| crates/footprint/src/classifier.rs:191-199 | `let (mut fixedish, mut total) = (0, 0); for e in ... { for n in ... } }` | a `fold` over `by_selector.values()` returning the pair, or two independent `map().sum()` passes. |
| crates/footprint/src/lib.rs:81-88 | `let mut args = Vec::new(); if input.len() > 4 { for chunk in ... { args.push(...) } }` | delete entirely; see R8, nothing reads `TxObs.args`. If it must stay, it is `input[4..].chunks(32).take(6).map(...).collect()`. |
| guest/kardamom-zk-host/src/main.rs:150-175 | `let mut blocks = Vec::new(); let mut digests = Vec::new(); let mut pre_root = None; ...` | four accumulators in one `for` loop with `?` inside. Extract `load_spool_frames(spool, first, last) -> Result<Vec<ProverInput>>` and derive the digests with `.iter().map(...).collect()` afterwards. |

## Tests

### R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/exec-core/src/executor/deposit.rs:288 | `THE EQUIVALENCE GATE for the on-scope deposit path (#234 part 2):` | issue number and work-item phase | keep the gate description; drop "(#234 part 2)" |
| crates/exec-core/src/executor/deposit.rs:168 | ``Deposit-execution tests. These mirror the old `execute_deposit``` | names a removed function and file | "Deposit-execution scenarios." |
| crates/exec-core/src/executor/xchain.rs:170 | `Cross-chain (0x7D) execution tests. These are the delivery analogue of` | fine as-is | none |
| crates/exec-core/src/executor/scope.rs:811 | `"typed cause on the wire (#241)"` | issue number inside an assert message | drop "(#241)" |
| crates/exec-core/src/executor/scope.rs:993 | `when a BAL handle is supplied to execute_tx (spec phase 1). An` | phase label in a regression-test doc | drop "(spec phase 1)" |
| crates/exec-core/src/executor/tx_env.rs:149 | `-- tx_env_from_alloy typed-field mapping ---` | restates the module the tests live in | drop |
| crates/exec-core/tests/cfg_pinning.rs:2 | ``docs/agents/l1-client-suite-port-spec.md``).`` | spec doc by path | drop |
| crates/exec-core/tests/eest_state.rs:2 | ``docs/agents/l1-client-suite-port-spec.md``). This is the `consume`` | spec doc by path | drop |

### R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/exec-core/src/executor/scope.rs (tests 736-1102) | 317 | Move to `crates/exec-core/src/executor/scope_tests.rs`. See the main R3 entry: this is step 1 of the split and by itself brings the file under 500. |
| crates/exec-core/tests/eest_state.rs | ~330 | KEEP. A conformance-fixture runner is one unit; splitting it would separate the fixture loader from the assertions it feeds. |
| crates/exec-core/tests/anchor_state.rs | ~370 | KEEP. Cohesive: one witness fixture builder plus the refutation cases that use it. |

### R6 dynamic dispatch

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/exec-core/tests/anchor_state.rs:316 | `let refuted = \|mutate: &dyn Fn(&mut ExecutionWitness)\| {` | `dyn Fn` closure parameter | make the closure generic: `let refuted = \|mutate: impl Fn(&mut ExecutionWitness)\| { ... }`. The call sites all pass distinct closures, so monomorphizing costs nothing here. |

### R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/exec-core/tests/anchor_sparse.rs:110 | `let mut out = Vec::new();` | collect from an iterator |
| crates/exec-core/tests/anchor_sparse.rs:263 | `let mut groups: BTreeMap<u8, Vec<B256>> = BTreeMap::new();` | `.fold` into the map, or `into_group_map`-style helper |
| crates/exec-core/tests/anchor_state.rs:218, 414, 464 | `let mut have: Vec<Bytes> = Vec::new();` | three copies of the same node-set collector; extract one test helper and `collect()` |
| crates/exec-core/tests/eest_state.rs:342-344 | `let mut unexpected_failures: Vec<(String, String)> = Vec::new();` plus two more | three parallel accumulators over one loop; partition the results into an enum, then `partition`/`filter_map` per class |
| crates/exec-core/tests/hash_cost.rs:41 | `let mut acc = 0u64;` | a benchmark accumulator; keep, the imperative form defeats the optimizer on purpose |
| crates/exec-core/tests/anchor_sparse.rs:70 | `let mut rounds = 0;` | a fixed-point loop counter; keep |
