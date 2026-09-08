# Status: exec-core (Phase A)

Group: exec-core, footprint, reconstruct, guest. Every row from
`crate-exec-core.md` and `inputs-exec-core.md` appears in exactly one list
below. Gate results and open items are at the end.

## Done

### R1 comments

- `crates/exec-core/src/executor/mod.rs:16,21,22` — dropped the "plain
  split" history line; fixed the `ExecScope` and free `execute_tx` broken
  doc links to `Executor` / `Executor::execute_once`.
- `crates/exec-core/src/executor/db.rs:3,25,134,108` — fixed the
  `ExecScope` broken doc links to `Executor`; dropped "pinned by phase 3".
- `crates/exec-core/src/executor/scope.rs:2,50,58,121,130,254,516` (original
  numbering, now split across `scope.rs`/`scope_derived.rs`) — dropped the
  free-`execute_tx` and DHAT-history language; stated the one-EVM/one-cache
  invariant directly; reworded the deposit/xchain doc blocks around
  `execute_deposit_tx`/`execute_xchain_tx` instead of "the historic path".
- `crates/exec-core/src/executor/scope.rs:560-578` — the stray doc block
  and duplicate `#[allow(clippy::too_many_arguments)]` that sat on the
  `impl` block, not `execute_once`: merged the doc into `execute_once`'s
  own doc, dropped the duplicate allow (kept the one directly on the
  method).
- `crates/exec-core/src/executor/scope.rs:577` — dropped with the impl-level
  allow above.
- `crates/exec-core/src/executor/tx_env.rs:22,61` — reworded `DecodedTx`'s
  doc to say what it holds; rewrote the "used to end in
  `..Default::default()`" comment as a present-tense rule.
- `crates/exec-core/src/executor/write_set.rs:35,54,182,301,318` — dropped
  the fabrication history, "historic free-function path" framing (reworded
  to name `execute_deposit_tx`/`execute_xchain_tx` directly), fixed the
  broken `write_set_from_evm_state` link to `WriteSet::from_evm_state`,
  reworded the private-fn "Public API" note, dropped the copy-cost history.
- `crates/exec-core/src/delta.rs:27,272` — reworded the `SmallVec` doc to
  state the present rationale; dropped "(they used to be `BTreeMap`s)".
- `crates/exec-core/src/anchor/mod.rs:3` — dropped "Phase 2's...
  Phase 2's witness fails closed" phase label.
- `crates/exec-core/src/stateless.rs:22,170` — dropped "phase 3", reworded
  the gaps list to name `execute_block_stateless` / `execute_block_anchored`
  directly; dropped "This replaces the old snapshot, parent, and delta
  re-seed per deposit."
- `crates/exec-core/src/state.rs:18` — "keeps the old behavior of
  returning an immutable snapshot" → "returns an immutable snapshot".
- `crates/exec-core/src/block_env.rs:7,39` — dropped the spec-doc path and
  the revm-stale-comment-rots note.
- `crates/exec-core/src/features.rs:25` — dropped the dated spec-doc path.
- `crates/exec-core/src/bal_ladder.rs:2` — dropped the spec-doc path.
- `crates/exec-core/src/executor/deposit.rs:29` — kept "OP-aligned",
  dropped "ported from the old crates/node/src/executor.rs".
- `crates/footprint/src/classifier.rs:4-14` — deleted the 11-line
  keccak-inversion archaeology; kept the two-tier description.
- `guest/kardamom-zk-host/src/main.rs:6` — "the guest/host round-trip
  contract of phase 3c" → "the guest and host must commit identical
  public values".
- `guest/kardamom-zk-guest/Cargo.toml:1`,
  `guest/kardamom-zk-host/Cargo.toml:1` — dropped "phase 3c" from the
  header comments.
- Test-appendix rows (crate-exec-core.md "Tests / R1 comments", 7 rows):
  `executor/deposit.rs:288` ("(#234 part 2)" dropped, kept the gate
  description), `executor/deposit.rs:168` ("mirror the old ... scenarios"
  → "Deposit-execution scenarios."), `executor/xchain.rs:170` — read,
  judged fine as-is per appendix, no change. `executor/scope.rs:811`
  ("(#241)" dropped, in the moved `scope_tests.rs`), `executor/scope.rs:993`
  ("spec phase 1" dropped, in `scope_tests.rs`), `executor/tx_env.rs:149`
  ("-- tx_env_from_alloy ... --" module-restating comment, dropped),
  `tests/cfg_pinning.rs:2`, `tests/eest_state.rs:2` (spec-doc paths
  dropped).
- Extra, found while editing nearby (same category, not separately
  enumerated in either table): `executor/scope.rs` post-split module doc
  (fixed a link I introduced), `executor/mod.rs` post-split module doc
  (added `scope_derived`/`skip` layout entries and fixed the same broken
  links), `tests/code_hash_scope_invariance.rs:178` ("(#159)"),
  `witness.rs:15` ("phase 3, a proof").

### R2 long methods (crate-exec-core.md appendix + mechanical list)

- `crates/exec-core/src/executor/scope.rs:419` `execute_tx_decoded` (100
  lines) — reversed after coordinator review (was "Not done, judged
  wrong" in the first pass). Split into `Executor::capture_touches` and
  `Executor::build_tx_receipt`, plus a new `TxReceiptInputs<'a>` struct
  bundling `build_tx_receipt`'s params. The skip-return arms (the
  `match self.evm.transact(tx_env)` block) stay in place, unmoved. The
  pre-existing `#[allow(clippy::too_many_arguments)]` on
  `execute_tx_decoded` itself (confirmed via `jj file show` to predate
  Phase A) stays, with a comment noting it is a cross-crate entry point.
- `crates/exec-core/src/executor/scope.rs:136` `Executor::execute_deposit`
  → now in `scope_derived.rs`, split into `credit_mint`,
  `transact_without_nonce_check` (shared with `execute_xchain`),
  `ensure_sender_in_write_set`, `build_deposit_receipt`.
- `crates/exec-core/src/executor/scope.rs:263` `Executor::execute_xchain` →
  now in `scope_derived.rs`, split via `super::xchain::ValuelessMessage`
  (R9), the shared `transact_without_nonce_check`, `build_xchain_receipt`.
- `crates/exec-core/src/executor/deposit.rs:43` `execute_deposit_tx` — split
  via the new shared `db::seed_composed_cache`, local `credit_mint`,
  `build_deposit_receipt`.
- `crates/exec-core/src/executor/xchain.rs:53` `execute_xchain_tx` — split
  via `db::seed_composed_cache`, local `reject_valued_message`
  (`ValuelessMessage::new`), `deliver_call`, `build_xchain_receipt`.
- `crates/exec-core/src/anchor/mod.rs:175` `verify_witness_anchored` — split
  into `prove_accounts`, `prove_storage`, `verify_code_blobs`.
- `crates/exec-core/src/anchor/mod.rs:306` `recompute_post_root` — split
  into `group_storage_writes`, `recompute_storage_roots`,
  `post_account_leaf`.
- `crates/exec-core/src/anchor/sparse.rs:172` `SparseTrie::insert_in` —
  split into `split_leaf`, `split_extension`, `descend_branch`.
- `crates/exec-core/src/anchor/sparse.rs:275` `SparseTrie::remove_in` —
  split into `remove_under_extension`, `collapse_branch`,
  `splice_survivor`.
- `crates/exec-core/src/executor/write_set.rs:115`
  `record_writeset_into_bal_inner` — split into `fabricated_account`,
  `fabricated_empty_account`, `fabricated_slot`.
- `crates/footprint/src/grade.rs:87` `grade_block` — split into
  `score_coverage` and the shared `oracle::predicted_pairs`, plus
  `wave_structure` (which also absorbed the R10 histogram rewrite).
- `crates/footprint/src/oracle.rs:152` `analyze` — split into
  `per_block_oracle`, `split_train_holdout`, `grade_holdout_blocks`.
- `crates/footprint/src/oracle.rs:243` `Report::summary` — split into
  `aggregate_blocks` (fold), `percentile`, `format_grading`.
- `guest/kardamom-zk-host/src/main.rs:25` `main` — split into
  `parse_single_args`, `run_prove`, `run_execute`.
- `guest/kardamom-zk-host/src/main.rs:121` `batch_main` — split into
  `load_spool_frames`, `block_records_digest`, `expected_boundary_roots`,
  `prove_or_execute_batch`, plus the new `BlockRange` newtype (R9).
- `guest/kardamom-zk-guest/src/bin/batch.rs:26` `main` — split via the new
  `guest/kardamom-zk-guest/src/lib.rs::records_and_digest` (shared with
  `main.rs`, resolving the duplication the row also names),
  `NonEmptyBatch`/`ensure_contiguous` (R9), `check_root_chain`.
- `crates/exec-core/src/stateless.rs:311` `execute_block_anchored` — not
  long; the row's real point (duplication with the guest-side record fold)
  is resolved by the guest's new shared `records_and_digest` above.
- Mechanical-list-only long test functions, not in the crate.md appendix
  and not given helper proposals: `crates/reconstruct/src/bin/kardamom-reconstruct.rs:64`
  `main` (54 lines, at the 51-line boundary, no appendix helpers) — left
  as one function, judged not to need a split.
  `crates/reconstruct/src/lib.rs:221` `blob_roundtrip_executes_remote_epochs`
  (130), `crates/reconstruct/tests/reconstruct_l1_e2e.rs:69`
  `rebuild_from_l1_reconstructs_canonical_state_root` (133),
  `crates/exec-core/tests/eest_state.rs:168` `run_case` (126),
  `crates/exec-core/tests/eest_state.rs:323` `eest_state_tests_conform`
  (116) — one cohesive fixture/round-trip scenario each (matches this
  file's own R3 KEEP rationale); pedantic `too_many_lines` silenced with
  `#[allow]` and a one-line reason instead of splitting. Judgment call,
  see "Not done, judged wrong" for the alternative considered.

### R3 large files

- `crates/exec-core/src/executor/scope.rs` (809/492+317) — test module
  moved to `scope_tests.rs`; skip machinery moved to `skip.rs`; derived-tx
  paths moved to `scope_derived.rs`. `evm`/`env` fields widened
  `private → pub(super)` for the sibling-module split (in-crate only, no
  external boundary crossed). Final sizes: `scope.rs` 388,
  `scope_derived.rs` 245, `skip.rs` 105, `scope_tests.rs` 366 raw lines,
  all comfortably under 500 code lines.
- `crates/exec-core/src/delta.rs` (329) — KEEP, per appendix (one hashing
  contract).
- `crates/exec-core/src/anchor/sparse.rs` (369 → 416 code lines after the
  R2 split's added doc comments, still under 500) — KEEP, per appendix.
- `crates/footprint/src/oracle.rs` (259 → 342 code lines after the R2/R10
  work) — KEEP; the duplicated predicted-pair-set logic is now the one
  shared `predicted_pairs` function `grade.rs` also calls (this was the
  appendix's explicit condition for the KEEP verdict).
- Test files: `crates/exec-core/src/executor/scope.rs` (tests 736-1102) —
  done above. `crates/exec-core/tests/eest_state.rs` (~330) and
  `crates/exec-core/tests/anchor_state.rs` (~370) — KEEP, per appendix.

### R4 manual drops

None found (appendix table is empty; nothing to do).

### R5 sync primitives and channels

- `crates/exec-core/src/witness.rs:125` `std::sync::Mutex<Recorded>` —
  JUSTIFIED per appendix; left unchanged.
- `crates/exec-core/src/state.rs:51` `Arc<RwLock<MockInner>>` — JUSTIFIED
  per appendix; left unchanged.

### R6 dynamic dispatch

- Production code: none found (appendix table is empty).
- `crates/exec-core/tests/anchor_state.rs:316` `&dyn Fn(&mut
  ExecutionWitness)` — converted the local closure `refuted` into a
  module-level generic `fn refuted(w, proofs, mutate: impl
  Fn(&mut ExecutionWitness))`; updated all 8 call sites.

### R7 too many generics

None found (appendix table is empty).

### R8 unnecessary pub

- `crates/exec-core/src/executor/scope.rs:711` `Executor::skip` — grep
  confirmed no caller anywhere in the workspace (the `skip(...)` call
  sites in `crates/stm/src/execute.rs` are a distinct local closure with a
  different signature, not this method). Deleted. This is defect #9 from
  the README; no unit test to add for a deleted item — there is nothing
  left to exercise. `crates/stm` still compiles (verified) since it never
  called this method.
- `crates/exec-core/src/executor/mod.rs:38` `pub use
  xchain::XCHAIN_DELIVERY_OVERHEAD` — grep confirmed only internal readers
  (`tx_env.rs`, `xchain.rs`). Dropped from the `pub use`; the constant is
  now `pub(super)`.
- `crates/exec-core/src/anchor/mod.rs:169` `ProvenPre.accounts` — grep
  confirmed no external field access (validator and the tests only pass
  the opaque `ProvenPre` value through). Made `pub(crate)`.
- `crates/footprint/src/classifier.rs:61,63,65` `SelectorStats::slot_seen`,
  `slot_obs`, `account_seen` — grep confirmed the one external reader
  (`bench/src/bin/stm-p0.rs`) only calls `by_selector.len()` and
  `class_shares()`. Left `pub`: these are struct fields read by
  `class_shares()` itself from within the same module, and narrowing them
  provides no additional safety since `Stats::by_selector` (containing
  `SelectorStats`) is itself `pub` and used by `crate::grade`/`oracle` —
  narrowing the leaf fields alone without touching the containing `pub`
  map was judged not worth the partial narrowing. See "Not done, judged
  wrong".
- `crates/reconstruct/src/lib.rs:36` `ReconstructError(pub String)` — grep
  confirmed no external tuple-field access (only `Display`/the `?`
  operator propagate it). Kept the type `pub`; the field is unchanged
  (`pub`) — see "Not done, judged wrong" for why.

### R9 defensive validation

- `crates/exec-core/src/executor/scope.rs:273`,
  `crates/exec-core/src/executor/xchain.rs:69` `message.value != 0` — added
  a local `ValuelessMessage<'a>` newtype in `xchain.rs`
  (`Deref<Target = XChainMessage>`, `pub(super)`), with a fallible
  `new(tx_idx, origin_chain_id, message)` constructor. Both
  `execute_xchain_tx` and `Executor::execute_xchain` derive one before
  touching the cache; `deliver_call`/`build_xchain_receipt` (and the
  `scope_derived.rs` equivalents) take the typed value and never re-check.
  No change to `XChainMessage` in `kardamom-types` (out of group).
- `guest/kardamom-zk-guest/src/bin/batch.rs:31`
  `assert!(!input.blocks.is_empty(), ...)` — `NonEmptyBatch` newtype
  (local to `batch.rs`), consolidating the original assert plus the three
  `.expect("nonempty")` accessor calls into one constructor check.
- `guest/kardamom-zk-guest/src/bin/batch.rs:47`
  `assert_eq!(number, first_block + i as u64, ...)` — `ensure_contiguous`
  validates the whole range once (before the execution loop), instead of
  per iteration.
- `guest/kardamom-zk-guest/src/bin/batch.rs:52`
  `assert_eq!(block.witness.pre_state_root, Some(running_root), ...)` —
  kept, per appendix: a real inductive invariant of execution order, not
  input validation (`running_root` comes from execution). Extracted into
  `check_root_chain` for the R2 split, logic unchanged.
- `guest/kardamom-zk-host/src/main.rs:145`
  `anyhow::ensure!(first >= 1 && last >= first, "bad block range")` —
  `BlockRange::new(first, last)` fallible constructor.

### R10 imperative style

- `crates/exec-core/src/bal_ladder.rs:134-153` `dedup_changes` — rewritten
  as quantize-in-place, reverse, `dedup_by`, reverse (drops the O(n²)
  `Vec::remove`).
- `crates/exec-core/src/bal_ladder.rs:99-124` storage-changes collapse —
  now calls the same `dedup_changes` helper (was a second, hand-rolled
  copy of the same logic).
- `crates/exec-core/src/anchor/mod.rs:316-322` storage-writes grouping —
  `fold` in the new `group_storage_writes`.
- `crates/exec-core/src/anchor/mod.rs:349-352` touched-address
  sort+dedup — direct `BTreeSet` collection instead of
  `Vec`+`sort_unstable`+`dedup`.
- `crates/exec-core/src/anchor/mod.rs:105-116` `NodeStore::new` canonical
  order check — KEEP, per appendix (early-return comparison).
- `crates/exec-core/src/stateless.rs:145-155` per-record execute loop —
  KEEP, per appendix (mutates `scope`/`delta`, early-returns with `?`).
- `crates/exec-core/src/stateless.rs:140`
  `-> Result<(BlockExecOutput, ()), ExecutorError>` — checked first:
  `execute_block_inner` is a private (non-`pub`) function, so this is not
  a public signature change; grep confirmed its only 3 callers are in the
  same file. Dropped the dead `()` tuple element; the 3 call sites'
  destructuring simplified accordingly.
- `crates/footprint/src/grade.rs:131-145` predicted-pair O(n²) loop — KEEP,
  per appendix (the cost the `cap` parameter bounds on purpose); factored
  into `oracle::predicted_pairs`, shape unchanged.
- `crates/footprint/src/grade.rs:107-125` predict+score loop — extracted
  as `score_coverage`/the shared `predicted_pairs` (see R2); kept
  imperative internally per appendix ("the current form is clearer").
- `crates/footprint/src/grade.rs:172-184` wave/width — kept the level
  relaxation loop (a real fixed-point pass); rewrote the width histogram
  as a `fold`.
- `crates/footprint/src/oracle.rs:198-225` vs `grade.rs:107-145` — the
  byte-for-byte-duplicated predicted-pair-set computation: extracted into
  one shared `pub(crate) fn predicted_pairs` in `oracle.rs`; both
  `grade_block` and `analyze`'s holdout grading call it now.
- `crates/footprint/src/oracle.rs:246-256` `summary` accumulators — `fold`
  into a `BlockAgg` struct (`aggregate_blocks`).
- `crates/footprint/src/classifier.rs:191-199` `class_shares` — rewritten
  as a `fold` over `by_selector.values()`.
- `crates/footprint/src/lib.rs:81-88` `decoded_view` args loop — since
  `TxObs.args` is deferred (kept; see below), rewrote as the audit's
  documented fallback: `chunks(32).take(6).map(...).collect()`.
- `guest/kardamom-zk-host/src/main.rs:150-175` spool-frame loading — split
  into `load_spool_frames` (pure I/O, one accumulator), `expected_boundary_roots`
  (the two boundary roots, read from every block's `expected-outputs.bin`
  to preserve the original fail-on-any-corrupt-file behavior), and
  `block_records_digest` (a `.map()` over the already-loaded blocks, per
  the appendix's suggested shape). Note: this reorders *when* each file is
  read (all `prover-input.rkyv` frames, then all `expected-outputs.bin`
  files, instead of interleaved per block); the final success/failure
  outcome and values are unchanged, but a partial-failure run now reports
  a different file as the first error. This is a host-side offline CLI,
  not consensus code.

### R11 clippy pedantic (`inputs-exec-core.md`, 197 sites)

Every site below was re-verified after the fix with a fresh
`cargo clippy -p <crate> --all-targets -- -W clippy::pedantic`
(and, for the guest crates, the equivalent `--manifest-path` invocation):
zero warnings remain in any file under `crates/exec-core`,
`crates/footprint`, `crates/reconstruct`, or `guest/` as of the last run.

- `crates/exec-core/src/anchor/mod.rs:104,175,306` `missing_errors_doc`;
  `:247,329` `map_unwrap_or` — `# Errors` sections added; `map_or` used.
- `crates/exec-core/src/anchor/sparse.rs:63,341,418`
  `cast_possible_truncation` — bound documented (`< 16`, a branch-array
  index) with a one-line comment + `#[allow]`. `:104,368`
  `must_use_candidate` — `#[must_use]`. `:118,165,265` `missing_errors_doc`
  — `# Errors` sections. `:430` `single_match_else` — `if let`.
- `crates/exec-core/src/bal_ladder.rs:99-106` — resolved by the R10
  rewrite (the flagged loop/match no longer exist in that shape).
- `crates/exec-core/src/block_env.rs:4,46` `doc_markdown`; `:57,65,110`
  `must_use_candidate` — backticks; `#[must_use]`.
- `crates/exec-core/src/delta.rs:38,40,42,48` `doc_markdown` — backticks.
  `:85` `items_after_statements` — moved `const INLINE` above the
  `debug_assert!`. `:156,168` `cast_possible_truncation` — bound
  documented (`minimal_be` returns `0..=32`) + `#[allow]`. `:290,332`
  `must_use_candidate` — `#[must_use]`.
- `crates/exec-core/src/error.rs:74,75,76,88` `doc_markdown` — backticks on
  `tx_ordering`/`tx_data`.
- `crates/exec-core/src/exec_types.rs:14,15,29` `doc_markdown` — backticks.
  `:24` `must_use_candidate`+`return_self_not_must_use`; `:47`
  `must_use_candidate` — `#[must_use]`.
- `crates/exec-core/src/executor/deposit.rs:43` `missing_errors_doc` —
  `# Errors`. `:123` `match_same_arms` — merged `Revert|Halt`. `:290,291`
  `doc_markdown` — backticks. `:397` `items_after_statements` — moved
  `use alloy_rlp::Encodable;` to the test module's top-level imports.
- `crates/exec-core/src/executor/scope.rs` (now split across
  `scope.rs`/`scope_derived.rs`/`skip.rs`) `:76,92,116,136,263,348,419,585`
  `missing_errors_doc` — `# Errors` on `new`, `new_with_envs`,
  `seed_layer`, `execute_deposit`, `execute_xchain`, `execute_tx`,
  `execute_tx_decoded`, `execute_once`. `:186,302` `match_same_arms` —
  merged `Revert|Halt` (both derived-tx paths). `:498,503,507`
  `explicit_iter_loop` — `&outcome.state` / `&account.storage`. `:622`
  `must_use_candidate` — `#[must_use]` on `skip_reason_of_tx`. `:1041`
  `doc_markdown` — backtick on `CacheDB` in the moved test doc.
- `crates/exec-core/src/executor/tx_env.rs:33` `missing_errors_doc` —
  `# Errors` on `decode`. `:169,196,227,229` `default_trait_access` —
  named types (`AlloyBytes::default()`, `AccessList::default()`) in the
  test fixtures.
- `crates/exec-core/src/executor/write_set.rs:41`
  `semicolon_if_nothing_returned` — added `;`. `:47,71`
  `must_use_candidate` — `#[must_use]`. `:147,161` `default_trait_access`
  — `EvmStorage::default()`. `:190,254,276,306,335` `explicit_iter_loop` —
  `&container` loops (resolved by the R2 split; the flagged lines now
  read `for x in &ws.accounts` etc.).
- `crates/exec-core/src/executor/xchain.rs:53` `missing_errors_doc` —
  `# Errors`. `:126` `match_same_arms` — merged (now inside `deliver_call`).
- `crates/exec-core/src/features.rs:48,67,77,82` `must_use_candidate` —
  `#[must_use]`. `:117` `missing_errors_doc` — `# Errors` on
  `apply_block_close_actions`. `:157`/test `fn empty` `unnecessary_wraps`
  — `#[allow]` with reason (the `Result` return matches the `read_slot: F`
  trait bound the test fixture must satisfy; it never actually fails).
- `crates/exec-core/src/state.rs:56,61,66` `doc_markdown` — backticks.
  `:72` `must_use_candidate` — `#[must_use]` on `builder`. `:80,81`
  `missing_panics_doc` — `# Panics` on `apply_block_delta` (lock
  poisoning). `:119,124,129,134,141`
  `must_use_candidate`/`return_self_not_must_use` — `#[must_use]` on every
  builder method.
- `crates/exec-core/src/stateless.rs:60` `doc_markdown` — backtick on
  `XChain`. `:93,107,123,176,222,258` `missing_errors_doc` — `# Errors` on
  every `pub fn -> Result`. `:99,114,130` `ignored_unit_patterns` —
  resolved by the R10 fix (the `()` tuple element and its match arms are
  gone). `:311` `missing_panics_doc`+`missing_errors_doc` — `# Panics` /
  `# Errors` on `execute_block_anchored`. `:339` `must_use_candidate` —
  `#[must_use]` on `bal_commitment`. `:382` `unreadable_literal` —
  `412_346`. `:388` `default_trait_access` —
  `alloy_primitives::Bytes::default()`.
- `crates/exec-core/src/witness.rs:58` `must_use_candidate` —
  `#[must_use]` on `from_witness`. `:146` `missing_panics_doc` —
  `# Panics` on `into_witness`.
- `crates/exec-core/tests/anchor_state.rs:7,33` `doc_markdown`; `:73`
  `cast_lossless` — backticks; `u64::from`.
- `crates/exec-core/tests/code_hash_scope_invariance.rs:5,10,11`
  `doc_markdown`; `:30` `unreadable_literal`; `:49` `default_trait_access`
  — backticks; `412_346`; named `Bytes::default()`.
- `crates/exec-core/tests/decode_cost.rs:17` `unreadable_literal`; `:37`
  `items_after_statements`; `:43` `cast_precision_loss` — `412_346`;
  moved `const REPS`; `#[allow]` with reason (nanosecond timing report).
- `crates/exec-core/tests/eest_state.rs:86` `doc_markdown`; `:168,323`
  `too_many_lines`; `:187` `map_unwrap_or` — backtick; `#[allow]` (see R2
  above); `map_or`.
- `crates/exec-core/tests/hash_cost.rs:15` `cast_possible_truncation`;
  `:29` `items_after_statements`; `:36,47` `cast_precision_loss`; `:44`
  `cast_lossless` — documented `#[allow]` (address-suffix wrap is
  intentional); moved `const REPS`; `#[allow]` (timing report);
  `u64::from`.
- `crates/exec-core/tests/touch_set.rs:18` `unreadable_literal`; `:52`
  `default_trait_access` — `412_346`; named `Bytes::default()`.
- `crates/exec-core/tests/write_set_encoding.rs:40` `cast_lossless` —
  `u64::from`.
- `crates/exec-core/tests/write_set_hash.rs:67,79`
  `cast_possible_truncation`; `:134,139,165` `cast_lossless` — documented
  `#[allow]` (`minimal()` returns `0..=32`); `u64::from` throughout.
- `crates/footprint/src/classifier.rs:82,119,155,190` `must_use_candidate`
  — `#[must_use]`. `:244,265,284,314` `cast_possible_truncation` —
  resolved by the R10 `class_shares` rewrite removing the cast sites (the
  fixed-point-share arithmetic no longer casts at those lines); remaining
  necessary casts in the test module documented with `#[allow]`.
- `crates/footprint/src/grade.rs:59,66,73` `must_use_candidate`; `:63,70,77`
  `cast_precision_loss`; `:90` `implicit_hasher`; `:178` `map_unwrap_or`;
  `:225,262,265,296,302,320,336` `cast_possible_truncation` —
  `#[must_use]` + documented `#[allow]` (ratio displays); `grade_block`
  widened to `HashSet<Cell, S: BuildHasher>` (verified backward
  compatible: `kardamom-engine`/`kardamom-bench` still compile unchanged);
  `map_or`; resolved by the R10/R2 rewrites removing those cast sites (the
  `i as u8` test-fixture casts now carry `#[allow]` with a bound comment).
- `crates/footprint/src/lib.rs:62` `must_use_candidate`; `:73`
  `missing_panics_doc` — `#[must_use]` on `envelope_view`; `# Panics` (and
  `#[must_use]`) on `decoded_view`.
- `crates/footprint/src/oracle.rs:64` `similar_names` — renamed the local
  `writes` binding to `written` (vs. the `writers` map). `:131`
  `must_use_candidate`; `:138,142` `cast_precision_loss` — `#[must_use]`
  on `universal_writes` + documented `#[allow]`. `:152` `implicit_hasher`
  — `analyze` widened to `HashSet<Cell, S: BuildHasher>` (same
  compatibility verification as `grade_block`). `:176,243,254,257,262,269,275,295,300,305,307`
  cast/closure lints — resolved by the `Report::summary` R2 split
  (`aggregate_blocks`/`percentile`/`format_grading`), each with a
  documented `#[allow(cast_precision_loss)]` (display ratios, not
  consensus values) or the redundant-closure fix.
- `crates/reconstruct/src/lib.rs:43` `must_use_candidate`; `:65`
  `missing_errors_doc`; `:97` `unreadable_literal`; `:221`
  `too_many_lines` — `#[must_use]`; `# Errors`; `412_346`; `#[allow]` (see
  R2 above).
- `crates/reconstruct/tests/reconstruct_l1_e2e.rs:34` `unreadable_literal`;
  `:69` `too_many_lines`; `:70` `manual_let_else`+`single_match_else` —
  `412_346`; `#[allow]`; one `let...else` rewrite fixed both lints at
  once.

### Mechanical-list argument-count rows

All 8 rows (`xchain.rs:53` 11 args, `deposit.rs:43` 10, `scope.rs:585` 10,
`scope.rs:419` 9, `scope.rs:658` 9, `scope.rs:711` 9, `scope.rs:263` 8,
`scope.rs:348` 8) are the `TxSlot` cluster. Per the task brief, `TxSlot`
touches `stateless.rs` callers in other crates and is explicitly Phase B
— see below. `scope.rs:711` (`skip`) is resolved separately: the function
itself was deleted (R8, dead code), so its argument count is moot.

### Defects (README "Defects found on the way")

- #3 `TxObs.args` dead field — see "Deferred to Phase B".
- #9 `Executor::skip` dead item — done, see R8 above.

## Deferred to Phase B

- `crates/footprint/src/lib.rs:49` `TxObs.args` field (defect #3) — grep
  confirmed `crates/stm/src/schedule.rs:83` still writes it via a struct
  literal (`TxObs { ..., args, ... }`); `crates/stm` is not mine to touch.
  Verified empirically: `cargo check -p kardamom-stm` compiles today with
  the field kept, and the brief's own instruction is to defer unless
  deletion leaves `crates/stm` compiling. `engine/src/shadow.rs:150` and
  `bench/src/bin/stm-p2.rs` also write it. Deferred; the `decoded_view`
  loop that fills it was still rewritten functionally (R10, kept the
  field's value identical) so the row is otherwise addressed.
- `TxSlot` argument-group struct and the 8 `too_many_arguments` sites it
  would resolve (`scope.rs:263,348,419,585,658`, `deposit.rs:43`,
  `xchain.rs:53`) — per the task brief, `TxSlot` touches `stateless.rs`
  callers in other crates (engine, validator). Deferred as instructed;
  `#[allow(clippy::too_many_arguments)]` attributes and their justifying
  comments are unchanged on these 6 functions (one, `scope.rs:711`
  `skip`, was deleted instead — see R8 Done).
- R6 workspace items named in the crate-exec-core.md summary but living
  outside this group's directories (the `EngineWiring` dead impls, the
  `RemoteEpochObserver`/`BlockExec`/`JoinRecoveryFactory` traits, the
  `DeliverFn` enum, `Box<dyn Error>` in `log`, `&mut dyn FnMut` in
  `log/src/recorder.rs`, the `&dyn Fn` leaf encoder in `state/src/trie`,
  `stm/src/execute.rs:4433`, `Deployer<DynProvider>` in
  `deployer/src/main.rs`, `Pin<Box<dyn Future>>` in
  `ingress/src/json_rpc.rs`) — not in scope; these belong to other
  reviewer groups. (Listed here only for completeness; they are not rows
  from `crate-exec-core.md`'s own appendix.)

## Not done, judged wrong

- `crates/footprint/src/classifier.rs:61,63,65` `SelectorStats` fields (R8)
  — the appendix proposes `pub(crate)`. Left `pub`: narrowing these three
  leaf fields alone, while `Stats::by_selector: HashMap<_, SelectorStats>`
  (the containing field) stays `pub` and is itself read from
  `crate::grade`/`crate::oracle` via the same access path, provides no
  additional encapsulation — a caller with `by_selector` access already
  reaches these fields. Narrowing `by_selector` itself would be a real
  encapsulation change but was not proposed and would ripple further than
  this row's scope.
- `crates/reconstruct/src/lib.rs:36` `ReconstructError(pub String)` field
  (R8) — the appendix proposes making the field private and adding a
  `Display`-only surface. `Display` already exists via
  `#[error("reconstruct: {0}")]` (thiserror), and the type's sole purpose
  is to carry a formatted message through `anyhow`/`?`; there is no
  invariant a private field would protect (any string is a valid error
  message). Left `pub` rather than add a getter with no behavioral
  difference.

## Gate results

Run with `CARGO_TARGET_DIR=/home/dev/kardamom-8/target` unless noted.

- `cargo clippy -p kardamom-exec-core --all-targets -- -D warnings`: clean.
- `cargo clippy -p kardamom-exec-core --all-targets -- -W clippy::pedantic`:
  zero warnings in this crate's own files.
- `cargo test -p kardamom-exec-core`: all suites pass (lib 54,
  anchor_sparse 6, anchor_state 4, cfg_pinning 8, code_hash 1, decode_cost
  1, eest_state 0/1 ignored — needs `KARDAMOM_EEST_FIXTURES`, hash_cost 1,
  touch_set 4, write_set_encoding 3, write_set_hash 6).
- `cargo fmt -p kardamom-exec-core`: applied, clean.
- `cargo clippy -p kardamom-footprint --all-targets -- -D warnings`: clean.
- `cargo clippy -p kardamom-footprint --all-targets -- -W clippy::pedantic`:
  zero warnings. `cargo test -p kardamom-footprint`: 10/10 pass.
  `cargo fmt -p kardamom-footprint`: applied, clean.
- `cargo clippy -p kardamom-reconstruct --all-targets -- -D warnings`:
  clean. `-- -W clippy::pedantic`: zero warnings in this crate's own
  files. `cargo test -p kardamom-reconstruct --lib`: 2/2 pass (the
  `tests/reconstruct_l1_e2e.rs` anvil e2e test is excluded per the brief:
  it spawns an external binary). `cargo fmt -p kardamom-reconstruct`:
  applied, clean.
- `guest/kardamom-zk-guest`: `cargo check --manifest-path
  guest/kardamom-zk-guest/Cargo.toml --bins --lib`, `-- -W
  clippy::pedantic`, `-- -D warnings`: all clean. Also verified
  `cargo check -p kardamom-exec-core --no-default-features` and its
  clippy `-D warnings` variant are clean (the guest's `no_std` shape),
  which caught and fixed an unrelated unused-import warning in
  `crates/exec-core/src/witness.rs` (see below).
- `guest/kardamom-zk-host`: same three checks via `--manifest-path`, all
  clean (built with a local `PROTOC` env var — see note below).
- `cargo check --workspace` (from the exec-core workspace root): clean.

## Notes

- **Environment, not a code issue:** `contracts/lib` (forge-std,
  OpenZeppelin) was not fetched in this workspace, which made
  `crates/deployer`'s build script fail, which transitively blocked
  `kardamom-reconstruct` (via `kardamom-batcher`'s normal dependency on
  `kardamom-deployer`) from compiling at all — not just its tests. Fixed
  by running `forge install --no-git` for the three declared deps and
  `forge build` inside this workspace's own `contracts/` directory (a
  vendored-lib/build-artifact fetch, not a tracked-source change; `jj
  status` shows nothing under `contracts/`).
- **Environment, not a code issue:** `guest/kardamom-zk-host` needs
  `protoc` (via `sp1-prover-types`'s build script); not installed and no
  sudo available. Verified compilation/lints by downloading a static
  `protoc` binary to the session scratchpad and setting `PROTOC`. This is
  not persisted anywhere in the repo; a future check of this crate needs
  `protoc` on `PATH` or `PROTOC` set.
- **Dependency added, in-group only:** `alloy-primitives = "1.6"` added
  directly to `guest/kardamom-zk-guest/Cargo.toml` and
  `guest/kardamom-zk-host/Cargo.toml` (both files are inside this group;
  neither is the root workspace `Cargo.toml`; both crates are
  workspace-detached with their own lockfiles). Needed to name `B256` in
  the new R2/R9 helper signatures; it was already a transitive dependency
  in both crates, so this only makes an existing dependency direct.
- `crates/exec-core/src/witness.rs`: `CodeEntry`/`WitnessAccount`/
  `WitnessSlot` imports gated behind `#[cfg(feature = "std")]` (they are
  only used inside the `std`-only `WitnessRecorder::into_witness`).
  Found via the guest's `--no-default-features` build, which is the
  configuration the zkVM guest actually links; not previously caught
  because earlier gate runs used default (std) features. Verified clean
  under both feature configurations.

# Status: exec-core (Phase C)

Order: R13, R12, R15, R16, R14, per the task brief. Covers
`crates/exec-core`, `crates/footprint`, `crates/reconstruct`, `guest/`.

## Done — R13 (no `debug_assert!`/`.max(1)`/bare `assert!`)

- `crates/exec-core/src/delta.rs` `WriteSet::finish()` — removed 2
  `debug_assert!` calls that checked key uniqueness across
  `.windows(2)`. `WriteSet::hash()` keeps its 1 `debug_assert!` — the
  named Phase B exception. Unchanged.
- `crates/exec-core/src/exec_types.rs` `TxIndex::next()` —
  `self.0 + 1` to `self.0.checked_add(1).expect("TxIndex counter
  overflowed u64")`, with a `# Panics` doc section. Matches the audit's
  exact recommendation.
- `crates/footprint/src/oracle.rs` `universal_writes` — dropped
  `.len().max(1)`; the loop above already guarantees `count` is empty
  exactly when `obs` is, so the guard was dead.
- `crates/footprint/src/oracle.rs` `format_grading` (now `impl Display
  for Grading`) — 2 `.max(1)` sites replaced with explicit `if x > 0
  {...} else {0.0}` guards, matching the function's own existing style.
- `crates/footprint/src/classifier.rs` `class_shares` — dropped
  `.max(1)` on `e.observations`, matching the guard-free predicate
  `predict`/`predict_domains` already use.
- `crates/footprint/src/oracle.rs` `split_train_holdout` (now
  `Holdout::split`) — attempted a `train_frac.clamp(0.0, 1.0)` fix,
  then reverted: proven a no-op for every input the audit flagged (NaN,
  negative, `>1.0`), since `analyze`'s guard only reads the result when
  it was already in range. Left as-is; see Deferred.

## Done — R12 (safe arithmetic)

- `crates/exec-core/src/executor/derived.rs` `DerivedCall::
  derived_receipt` — `cumulative_gas_used` now `checked_add`, returns
  `ExecutorError::Execution` on overflow (was a bare `+`).
- `crates/exec-core/src/executor/scope.rs` `Executor::build_tx_receipt`
  — same `checked_add` fix, same error path.
- `crates/exec-core/src/exec_types.rs` `TxIndex::next()` — see R13
  above (audit filed this row under both R12 and R13).
- `crates/exec-core/src/executor/xchain.rs` `ValuelessMessage::new` —
  now also rejects `message.gas_limit > BLOCK_GAS_LIMIT` as a chain
  fault (was: value-check only). `tx_env_from_xchain`'s `gas_limit:
  message.gas_limit + XCHAIN_DELIVERY_OVERHEAD` is a plain `+`, not
  `saturating_add`: the type-level bound on `ValuelessMessage` proves
  the wire value can never be within `XCHAIN_DELIVERY_OVERHEAD` of
  `u64::MAX`.
- `crates/exec-core/src/executor/tx_env.rs` `tx_env_from_xchain` —
  signature narrowed from `&XChainMessage` to
  `&xchain::ValuelessMessage<'_>`, so the gas-limit bound above is
  type-enforced at every call site, not just documentation.
- `guest/kardamom-zk-guest/src/bin/batch.rs` `ensure_contiguous` —
  `first_block + i as u64` to `first_block.checked_add(i as
  u64).expect("batch block number overflowed u64")`. Untrusted rkyv
  input; the guest release profile has no `overflow-checks`.
- `crates/exec-core/src/features.rs` `pack_beacon` doc comment —
  removed an incorrect "fields saturate instead of wrapping" claim;
  each field is a native `u64` that already fits its lane exactly, so
  there is nothing to saturate.
- Every `HOT_PATH_KEEP`/`PROVEN` row in `arith-exec-core.md` — verified
  against the appendix's own verdict; no code change, cited in place
  where a comment already existed.

## Done — R15 (methods, not standalone functions)

- `crates/exec-core/src/executor/derived.rs` — new `DerivedCall<'a, D>
  { cache, env, tx_idx }` with methods `new`, `credit_mint`,
  `transact_commit` (see R14 below), `derived_receipt`. Replaces the 4
  duplicate receipt-builder free functions and the 2 `credit_mint`
  copies (coordinator FIX 1/FIX 2). `DerivedSlot`, `DerivedIdentity`,
  `CallOutcome` carry the shared receipt shape; `derive Copy` on the
  first two (`needless_pass_by_value`).
- `crates/exec-core/src/executor/xchain.rs` — `deliver_call` renamed to
  `DerivedCall::transact_commit` and moved into `derived.rs` (see R14);
  `xchain.rs`'s own `impl DerivedCall` block is gone.
- `crates/exec-core/src/executor/scope.rs` — `execute_tx_decoded` split
  into `Executor::capture_touches`/`Executor::build_tx_receipt`
  (coordinator FIX 7, reversed from Phase A — see the Phase A section
  above). `capture_touches`'s inner per-slot loop further split into
  `Executor::record_account_touch` (R16).
- `crates/exec-core/src/anchor/mod.rs` — new `WitnessAnchor<'a> {
  witness, store, root }` with methods `new`, `verify`,
  `prove_accounts`, `prove_storage`, `verify_code_blobs` (was
  `verify_witness_anchored`'s free-function body).
  `verify_witness_anchored` is now a 1-line wrapper (kept `pub`, unchanged
  signature: cross-crate entry point). `ProvenPre` gained
  `group_storage_writes`, `recompute_storage_roots`,
  `apply_storage_writes` (R16), `post_account_leaf`.
- `crates/exec-core/src/anchor/sparse.rs` — `split_leaf`,
  `split_extension`, `placeholder` are `impl Node` associated
  functions; `wrap_extension`, `merge_extension`, `rlp_ref`,
  `encode_node` are `impl Node` methods. New `Node::split_branch` (R14,
  shared by `split_leaf`/`split_extension`).
- `crates/exec-core/src/executor/write_set.rs` — new `BalEntries`
  builder (`account`, `slot`, `record_into`) replacing the free
  functions `fabricated_account`/`fabricated_empty_account`/
  `fabricated_slot`/`record_writeset_into_bal_inner`. New `WriteSet::
  push_cache_storage`/`push_evm_state_storage` methods (R16).
- `crates/exec-core/src/bal_ladder.rs` — new crate-private `Granularity(u64)`
  with methods `chunk_of`, `dedup_changes`. Public `chunk_of(index, k)`
  and `quantize(bal, k: u16)` keep their exact signatures (confirmed
  cross-crate callers in `validator`, `executor`, `bench`) and forward
  to `Granularity`.
- `crates/footprint/src/classifier.rs` — `Stats::is_frequent` (associated
  fn) and `Stats::predicted_keys<K>` (method) replace the 5 inline
  `* 10 >= * 6` copies and the near-duplicate bodies of `predict`/
  `predict_domains` (dry-exec-core.md's exact row and signature).
- `crates/footprint/src/grade.rs` — `score_coverage` and `wave_structure`
  (renamed `score_waves`, now mutates `self` instead of returning a
  tuple) are `BlockGrade` methods.
- `crates/footprint/src/oracle.rs` — new `Holdout<'a>` struct
  (`split`, `is_empty`, `grade`) replaces `split_train_holdout`/
  `grade_holdout_blocks`. `per_block_oracle` is now `Report::
  per_block_oracle`. `aggregate_blocks` is now `BlockAgg::from_blocks`
  (`BlockAgg` was not on the coordinator's named list; it already held
  the `ratios` field, so it was the natural receiver).  `percentile` is
  now `BlockAgg::percentile`. `format_grading` is now `impl
  core::fmt::Display for Grading`.
- `guest/kardamom-zk-guest/src/lib.rs` — `records_and_digest` is now
  `GuestBlock::from_records` (chose `GuestBlock` over `BlockRecordsDigest::
  from_records`: `BlockRecordsDigest` lives in `crates/types`, owned by
  a different group; adding an inherent method there is out of scope).
- `guest/kardamom-zk-host/src/main.rs` — `parse_single_args`/
  `run_prove`/`run_execute` are `SingleRun` methods;
  `load_spool_frames`/`block_records_digest`/`expected_boundary_roots`/
  `prove_or_execute_batch` are `BatchRun` methods. `main` is two lines:
  `SingleRun::parse(args)?.run(&client)` / `BatchRun::parse(args)?.
  run(&client)`.

## Done — R16 (no nested loops)

- `crates/exec-core/src/anchor/mod.rs` `recompute_storage_roots` — inner
  per-write loop extracted to `ProvenPre::apply_storage_writes`.
- `crates/exec-core/src/executor/write_set.rs` `write_set_from_cache`/
  `write_set_from_evm_state_inner` — inner per-slot loops extracted to
  `WriteSet::push_cache_storage`/`push_evm_state_storage`.
- `crates/exec-core/src/executor/write_set.rs`
  `record_writeset_into_bal_inner` — not a nested-loop split: it was 3
  sequential loops, folded into the `BalEntries` builder under R15
  instead (no loop nesting existed to fix).
- `crates/exec-core/src/executor/scope.rs` `capture_touches` — inner
  per-slot loop extracted to `Executor::record_account_touch`.
- `crates/exec-core/tests/eest_state.rs` `build_env` — inner per-slot
  loop extracted to `seed_account`.
- `crates/exec-core/tests/hash_cost.rs`/`decode_cost.rs` — the
  `for _ in 0..REPS { for w/r in &sets/raws {...} }` timing loops now
  call the shared `common::ns_per_op(reps, n, f)`; the closure passed
  to it is its own scope, so neither file nests loops directly.
- `crates/footprint/src/oracle.rs` `conflict_pairs` — both the
  reader/writer-indexing loop and the pair-insertion loop flattened via
  new helpers `index_readers`/`index_writers` and `insert_pairs_with`
  (each a single loop, called once per tx/writer from the outer loop).
- `crates/footprint/src/oracle.rs` `predicted_pairs` — the `i`/`j`
  double loop flattened via `insert_predicted_pairs(i, ...)`, called
  once per `i` from the outer loop.
- `crates/footprint/src/oracle.rs` `universal_writes` — nested loop
  removed outright with `obs.iter().flat_map(|o| &o.writes)`, no helper
  needed.
- `crates/footprint/src/grade.rs` `score_coverage` — inner loop
  extracted to `BlockGrade::score_tx`.

## Done — R14 (DRY)

- `crates/exec-core/src/executor/derived.rs` `DerivedCall::
  transact_commit` — the build-cfg/build-EVM/`transact_commit`/map-err
  sequence, written once in `deposit.rs` and once as `xchain.rs`'s
  `deliver_call`, differing only in which `TxEnv` builder ran. One
  method; both free-path callers now call it directly with their own
  `tx_env_from_*` result.
- `crates/exec-core/src/code_hash.rs` (new file) — `to_revm_code_hash`,
  `to_wire_code_hash`, `is_empty_account`, the two spellings of "no
  code" (`B256::ZERO` on the wire, `KECCAK_EMPTY` in revm). Wired into
  `executor/db.rs` (1 site), `anchor/mod.rs` (4 sites),
  `executor/write_set.rs` (3 sites, deleting 2 local `norm` closures).
- `crates/exec-core/src/anchor/sparse.rs` `Node::split_branch` — the
  "common prefix + 2 nibbles + 16-slot branch + wrap" shape shared by
  `split_leaf`/`split_extension`. `splice_survivor` simplified from a
  3-arm match to one `survivor.merge_extension(nib)` call.
- `crates/exec-core/src/delta.rs` — `KECCAK_EMPTY_HASH` (a hand-copied
  32-byte constant) replaced with `alloy_primitives::KECCAK256_EMPTY`
  (verified byte-identical). `PendingDelta::apply`/`merge_from`'s 6
  manual `for (k,v) in ... { map.insert(k,v); }` loops replaced with 6
  `DeltaMap::extend(...)` calls (`DeltaMap` is a `hashbrown::HashMap`
  alias, which already implements `Extend`).
- `crates/exec-core/src/executor/mod.rs`/`block_env.rs`/
  `tests/code_hash_scope_invariance.rs` — 3 local `pos(off: i32)`
  helpers, each hand-building `BPosition { term_id: 0, term_offset:
  off }`, deleted. Every call site now calls `BPosition::from_index(N)`
  directly (`kardamom_types::BPosition::from_index` already existed).
- `crates/exec-core/tests/code_hash_scope_invariance.rs` `delta_from` —
  replaced its field-by-field `BlockDelta` construction with
  `PendingDelta::new()` + `.apply(ws.clone())` +
  `.finalize(block_number, Vec::new())`, per the audit's exact
  suggested body. (The 2 sibling copies in `crates/validator/tests/`
  are cross-crate; see Deferred.)
- `crates/exec-core/src/executor/mod.rs` `test_support::run_once` — the
  10-argument `Executor::execute_once(...).expect(...)` call, spread
  over up to 12 lines, written 9 times in `scope_tests.rs`. One helper;
  kept `tx_idx`/`tx_position` as separate params (not one shared
  counter — 2 call sites use a `BPosition` offset that does not equal
  the `TxIndex`) and a `label: &str` param so each site's distinct
  panic message survives.
- `crates/exec-core/tests/common/mod.rs` (new file) —
  `retained_nodes(entries, targets) -> (B256, Vec<Bytes>)`: the
  "`HashBuilder` + proof retainer + add every leaf + take root + keep
  32+-byte nodes" shape, written 4 times across `anchor_sparse.rs`
  (`reference`, `paths_for`) and `anchor_state.rs` (`all_nodes`'s 2
  internal loops). Also holds `ns_per_op` (see R16). Both integration
  binaries declare their own `mod common;` against this one file (the
  standard `tests/common/mod.rs` pattern); `#![allow(dead_code)]` at
  its top, since no single binary uses every helper in it.
- `guest/kardamom-zk-guest/src/lib.rs` `GuestBlock::run(input:
  ProverInput) -> (AnchoredBlockOutput, B256)` — extends
  `GuestBlock::from_records` (R15) to also absorb the `ExecEnv::new`/
  BAL-decode/7-arg `execute_block_anchored`/`.expect(...)` sequence,
  written once in `main.rs` and once in `bin/batch.rs`'s loop. Placed
  in the guest lib, not `exec-core`'s `stateless.rs`: no exec-core
  public-API growth, and both callers already panic identically, so the
  `.expect(...)` calls can live inside without changing exec-core's
  own error-returning contract.
- `guest/kardamom-zk-host/src/main.rs` `default_elf(bin: &str)` — the
  one spelling of the two near-identical ELF-path defaults (single:
  `.../release/kardamom-zk-guest`, batch: `.../release/batch`).
- `crates/footprint/src/lib.rs` `#[cfg(test)] pub(crate) mod testkit` —
  `addr(i)` (now `Address::with_last_byte(i)`) and `obs(...)` replace
  `grade.rs`'s and `classifier.rs`'s duplicate `TxObs`/address builders.
- `crates/reconstruct/src/lib.rs` `pub mod test_support`, gated
  `#[cfg(any(test, feature = "test-support"))]` — `CHAIN_ID`,
  `transfer`, `genesis`, `two_transfer_blocks`, `oracle_replay` replace
  the duplication between this crate's own unit tests and
  `tests/reconstruct_l1_e2e.rs` (fresh duplication from this session's
  own eest/reconstruct test-length splits, per the audit's row).
  `crates/reconstruct/Cargo.toml` gained a `test-support` feature and a
  self-referencing `[dev-dependencies]` entry (the standard idiom for
  sharing fixtures between a crate's own tests and an external
  integration-test binary); 6 crates (`alloy-consensus`, `alloy-eips`,
  `alloy-network`, `alloy-signer-local`, `bytes`, `tempfile`) moved from
  dev-only to optional regular dependencies, activated only by that
  feature.

## Already fixed before Phase C, no change needed

- `crates/footprint/src/oracle.rs`/`grade.rs` — dry-exec-core.md's row
  proposing `predict_all`/`predicted_pairs` to de-duplicate
  `grade_block` against the offline holdout grading: `grade_block`
  already called the one shared `crate::oracle::predicted_pairs` before
  Phase C started (its own doc comment says so). Verified, not
  reimplemented.

## Deferred (Phase B or cross-crate)

- `crates/footprint/src/oracle.rs` `split_train_holdout`/`Holdout::
  split` `train_frac` bound — the real fix is the `--train-frac` clap
  arg in `crates/bench`, a different group. A local clamp is a proven
  no-op (see R13 above).
- `crates/exec-core/src/bal_ladder.rs` `chunk_of`'s unguarded `k == 0`
  div-by-zero — already a named Phase B row (`NonZeroU64` typestate).
  `Granularity` (R15, this session) is the slot that fix lands in.
- `crates/exec-core/src/anchor/mod.rs` `recompute_post_root` still
  re-derives `witness.pre_state_root.ok_or(...)` instead of reading
  `pre.root` — dropping it would leave the `witness: &ExecutionWitness`
  parameter of this `pub` cross-crate fn (called from
  `crates/validator/src/witness.rs`) unused. Signature change,
  cross-crate; a new Phase B row found this session.
- `crates/validator/tests/prover_spool.rs` and
  `crates/validator/tests/witness_anchoring.rs` `delta_from` — same fix
  as `code_hash_scope_invariance.rs`'s (done, R14 above), but these two
  copies live in `crates/validator`, a different group.
- `guest/kardamom-zk-host/src/main.rs` `run_guest`/`default_elf` full
  merge (dry-exec-core.md's fuller suggested consolidation) — did the
  safe, purely-textual `default_elf` extraction (R14, above) but not
  the deeper `run_prove`/`run_execute`/`prove_or_execute` merge: the
  single-block prove path has a `groth16`-only branch (changes the
  `.prove()` call itself, not just the tail: `.groth16().run()` vs
  `.run()`) with no batch analogue, and this binary drives real SP1
  proving/execution with no local unit tests to verify an end-to-end
  behavior-preserving merge.
- `crates/exec-core/src/state.rs` (3 rows: `StaticSnapshotSource`/
  `MutatingSnapshotSource` alias, `MockStateDatabaseBuilder`/`MockInner`
  field duplication, 5x `self.inner.read().expect(...)` → a `read()`
  helper), `crates/exec-core/src/witness.rs` (`WitnessDb`/`Recorded`
  shared struct + a `memo` helper), `crates/exec-core/src/stateless.rs`
  (drop a vestigial `()`, fold `execute_block_capture`/
  `execute_block_with_bal`), `crates/exec-core/src/executor/db.rs`'s
  `SnapshotDb`/`SnapshotRef` forwarding row — all named R14 rows in
  `dry-exec-core.md`, sites verified in-group, no cross-crate signature
  risk. Left for a follow-up pass; none is a named FIX, R15, or R16
  target, and the audit's own line-savings estimate for each is small
  (6-22 lines).
- `crates/exec-core/tests/eest_state.rs`'s free functions
  (`decode_case`, `build_env`, `execute_case`, `check_post_state`,
  `run_file`, `walk_and_run`, `write_report_artifact`, `print_summary`)
  and `crates/reconstruct`'s `build_interop_scenario`/
  `assert_interop_lane_state`/`assert_interop_receipts` — R15 (methods,
  not functions) was not applied to these test-length-split helpers
  from earlier in this session. The structs they'd attach to
  (`DecodedCase`, `PreparedCase`, `Stats`, `InteropScenario`) already
  exist. Left for a follow-up pass.

## Not done, judged wrong (Phase C)

### R15

- `crates/footprint/src/oracle.rs` `predicted_pairs`/`conflict_pairs` —
  the coordinator named `predicted_pairs` for method conversion. Kept
  both as `pub(crate)` free functions instead: `dry-exec-core.md`'s own
  fix for the oracle.rs/grade.rs duplication row (already resolved,
  see R14 below) proposes `predict_all`/`predicted_pairs` as free
  functions in `oracle.rs`, next to their existing siblings
  `critical_path`/`actual_cells`. Neither has a natural single-struct
  receiver (`conflict_pairs` takes a generic `&[&TxObs]`; `predicted_pairs`
  could go on `Stats`, but the audit's own preferred home is `oracle.rs`).
- `crates/exec-core/src/executor/scope_derived.rs`
  `transact_without_nonce_check` — stays an `Executor` method, not a
  `DerivedCall` method. It is `DerivedCall::transact_commit`'s
  block-scope twin, but toggles `self.evm`'s own persistent cfg rather
  than building a fresh EVM; `Executor` cannot hold a `DerivedCall`
  borrow of its own `evm`'s db across other `&mut self` calls.

### R14

- The derived-tx-path "collapse `execute_deposit_tx`/`execute_xchain_tx`/
  `Executor::execute_deposit`/`Executor::execute_xchain` into one
  `execute_derived`" row — rejected. `DerivedCall` (R15, this session)
  already covers the shared middle (mint/transact/commit/receipt);
  building a second `DerivedTx`-enum layer on top would be redundant.
  External callers in `engine`/`validator` also block deleting the free
  functions (a cross-crate row, see `dry-exec-core.md`'s "New, not in
  the prior audit" line).
- `crates/exec-core/src/executor/db.rs` `SnapshotDb`'s 4 one-line
  `DatabaseRef` forwards to `SnapshotRef` — the audit's own verdict is
  "Marginal", and its second suggested option is the status quo plus an
  unused accessor. Left as-is.
- `crates/exec-core/src/executor/deposit.rs`
  `old_and_new_deposit_paths_agree` equivalence test — the audit
  proposes deleting it once the derived-tx-path collapse lands. That
  collapse was rejected (above), so the test stays as the gate it is:
  it caught real regressions during the `DerivedCall` rewrite this
  session.

## Gate results (Phase C)

Run from this jj workspace's own `target/` dir (no `CARGO_TARGET_DIR`
override this session), all commands re-run fresh (forced rebuilds via
`touch` on every touched file) right before this report.

- `cargo check -p kardamom-exec-core -p kardamom-footprint -p
  kardamom-reconstruct --all-targets`: clean.
- `cargo clippy -p kardamom-exec-core --all-targets -- -D warnings`:
  clean. `-- -W clippy::pedantic`: zero warnings in touched files
  (fixed 2 along the way: a `doc_markdown` backtick miss in
  `executor/mod.rs`, a `clippy::elidable_lifetime_names` miss in
  `xchain.rs`'s old `impl DerivedCall` block, since removed).
- `cargo test -p kardamom-exec-core`: all suites pass — lib 54,
  anchor_sparse 6, anchor_state 4, cfg_pinning 8, code_hash_scope_invariance
  1, decode_cost 1, eest_state 0/1 ignored (needs
  `KARDAMOM_EEST_FIXTURES`), hash_cost 1, touch_set 4,
  write_set_encoding 3, write_set_hash 6.
- `cargo fmt -p kardamom-exec-core -- --check`: clean.
- `cargo clippy -p kardamom-footprint --all-targets -- -D warnings`:
  clean. `-- -W clippy::pedantic`: zero warnings in touched files.
  `cargo test -p kardamom-footprint`: 10/10 pass. `cargo fmt -p
  kardamom-footprint -- --check`: clean.
- `cargo clippy -p kardamom-reconstruct --all-targets -- -D warnings`:
  clean. `-- -W clippy::pedantic`: caught and fixed 5 new warnings on
  the newly-public `test_support` module (`missing_panics_doc` x2,
  `must_use_candidate` x3); re-verified clean. `cargo test -p
  kardamom-reconstruct --lib`: 2/2 pass. `cargo test -p
  kardamom-reconstruct --test reconstruct_l1_e2e`: ran the real anvil
  e2e test (anvil available this session) —
  `rebuild_from_l1_reconstructs_canonical_state_root ... ok`, confirmed
  twice (before and after `cargo fmt`). `cargo fmt -p kardamom-reconstruct
  -- --check`: clean.
- `guest/kardamom-zk-guest`: `cargo check --bins`, `cargo clippy --bins
  -- -D warnings`, `-- -W clippy::pedantic`, `cargo fmt -- --check`: all
  clean, for both the `kardamom-zk-guest` and `batch` binaries.
- `guest/kardamom-zk-host`: same 4 checks, all clean.
- `cargo check --workspace --all-targets` (main workspace): clean, run
  after every file group above.

## Notes (Phase C)

- The `run_once`/`retained_nodes`/`ns_per_op` test helpers, and the
  `test_support`/`testkit` modules, are new test-only surface. None
  changes a production signature.
- `crates/reconstruct/Cargo.toml`'s `test-support` feature is the one
  Cargo-graph change in this phase; see R14 above for the reason.
- The root `Cargo.lock` gains one entry: `kardamom-reconstruct` now
  lists itself as its own dev-dependency (the self-referencing
  test-support idiom, see R14 above). No dependency version changed.
  Flagging this in case another group's lockfile diff collides with it.

# Status: exec-core (Coordinator round 1)

Coordinator reviewed the full Phase A + C diff (10560 lines) and sent 13
follow-up items. Items marked "cross-crate" change a public signature:
this group did its side and lists every external call site below; the
coordinator fixes the callers at merge. Result: 13/13 done, 0 deferred,
0 judged wrong. Two items produced a judgment call beyond the literal
ask (noted under item 2 and item 7 below) — flagged for review, not
silently made.

## Done — item 1 (`reason = "..."` on every `#[allow]`)

Every `#[allow(clippy::...)]` and `#![allow(...)]` in `crates/exec-core`,
`crates/footprint`, `crates/reconstruct`, and both guest crates now
carries a `reason = "..."` string; every preceding free-standing `//`
comment that used to carry that justification was folded into the
string and deleted. Confirmed via a perl scan across all `.rs` files
in the five directories (excluding `target/`) for any `#[allow(...)]`
block missing `reason =`: zero matches.

Sites folded this round (the ones the coordinator named, plus a few
more caught by the exhaustive final sweep):
- `crates/exec-core/tests/hash_cost.rs`: the `Address::with_last_byte`
  truncation cast, the `cold` timing precision-loss cast.
- `crates/exec-core/src/delta.rs`: the two `minimal_be` truncation casts
  in `encode()`. The `WriteSet` `type_complexity` allow was removed
  entirely, not folded — see item 3 below (also this file).
- `crates/exec-core/src/anchor/sparse.rs`: three `i as u8` truncation
  casts (branch-mask bit index, nibble push, RLP branch mask bit).
- `crates/exec-core/tests/write_set_hash.rs`: two `minimal()`-length
  truncation casts.
- `crates/exec-core/tests/common/mod.rs`: `#![allow(dead_code)]` (the
  module-level allow — its doc comment no longer explains the allow,
  the explanation lives in the `reason` string instead) and
  `ns_per_op`'s `cast_precision_loss`.
- `crates/exec-core/src/features.rs`: the `unnecessary_wraps` allow on
  the test fixture `empty`.
- `crates/footprint/src/grade.rs`: three `cast_precision_loss` allows
  (`hit_rate`, `predicted_cp_ratio`, `oracle_cp_ratio`) and five
  identical test-function `cast_possible_truncation, float_cmp` allows.
- `crates/footprint/src/oracle.rs`: `universal_writes`, the
  `BlockAgg::merge` ratio push, `Grading`'s `Display::fmt`,
  `Report::summary`, `BlockAgg::percentile`, and `Holdout::split`'s
  3-lint allow (the long train_frac/max_block justification, folded
  from a multi-line comment block into one `reason` string).
- `crates/footprint/src/classifier.rs`: four `cast_possible_truncation`
  allows in `is_frequent`'s test callers (loop bound fits `u8`).
- `guest/kardamom-zk-host/src/main.rs`: `run_prove`'s `similar_names`
  allow (the `_t`/`_dt` naming-pair convention).

Gates: `cargo check -p kardamom-exec-core -p kardamom-footprint
--all-targets --all-features` clean; `clippy -D warnings` clean on
both; `clippy -W clippy::pedantic` zero warnings in the three crate
dirs (all pedantic output left is pre-existing, in `crates/types`,
owned by another group); `cargo fmt -- --check` clean; `cargo test`
full pass on both (see final Gate results below).

## Done — item 2 (`TxSlot`, no `too_many_arguments`)

New `pub struct TxSlot { pub tx_idx: TxIndex, pub tx_position: BPosition,
pub tx_index_in_block: u64, pub cumulative_gas_used_before: u64 }` in
`exec_types.rs`, re-exported at the crate root. `DerivedSlot` deleted
and folded into `TxSlot` — `DerivedCall`'s own `tx_idx` field is now
`slot: TxSlot`, so `derived_receipt` dropped its separate `slot`
parameter entirely. All eight named `#[allow(clippy::too_many_arguments)]`
sites deleted: `execute_tx`, `execute_tx_decoded`, `execute_once`
(`scope.rs`), `execute_deposit_tx` (`deposit.rs`), `execute_xchain_tx`
(`xchain.rs`), `execute_xchain` (`scope_derived.rs`), `skip_receipt`
(`skip.rs`), `run_once` (test helper, `executor/mod.rs`). `Executor::
execute_deposit` also converted to take `slot: TxSlot` (3 params, no
allow needed either way).

New `test_support::slot(tx_idx, tx_position, tx_index_in_block,
cumulative_gas_used_before) -> TxSlot` builder (crate-private) and an
identical `common::slot(...)` in `tests/common/mod.rs` for integration
tests, wired into every in-crate unit and integration test call site.

**Judgment call beyond the literal ask**: after folding `TxSlot`,
`execute_xchain_tx` still had 8 params (over the 7-arg clippy
threshold), since it also carries `snapshot`/`parent`/`delta` that
`execute_deposit_tx` doesn't need a slot to offset. Added `pub struct
XChainDelivery<'a> { pub origin_chain_id: u64, pub message: &'a
XChainMessage }` (derives `Clone, Copy`) to absorb the two extra
fields, landing at 7 params. This is an *additional* signature change
beyond the `TxSlot`-only ask — every `execute_xchain_tx` external
caller must also wrap `origin_chain_id`/`message` into one
`XChainDelivery { .. }` literal. `Executor::execute_xchain` (the
method) was already under threshold with no external callers, so it
was deliberately left with `slot, origin_chain_id, message, bal` as
separate parameters rather than made symmetric with the free
function's `slot, delivery` shape — flagging this asymmetry for
review, not fixing it unasked.

### Cross-crate call sites — `execute_tx` (`Executor` method)
- `crates/engine/src/actor/exec_thread.rs:433`
- `crates/stm/src/execute.rs:3757`
- `crates/stm/src/execute.rs:4326`
- `crates/stm/src/execute.rs:4366`
- `crates/bench/src/bin/stm-p2.rs:292`
- `crates/bench/tests/alloc_profile.rs:190`

### Cross-crate call sites — `execute_tx_decoded` (`Executor` method)
- `crates/stm/src/execute.rs:4322`

### Cross-crate call sites — `execute_once` (`Executor::execute_once`, associated fn)
- `crates/engine/src/replay.rs:238`
- `crates/validator/src/parallel/engine_tests.rs:131`
- `crates/bench/src/stm/capture.rs:42`
- `crates/bench/tests/parallel_defi_repro.rs:94`
- `crates/bench/tests/parallel_defi_repro.rs:114`
- `crates/bench/tests/parallel_defi_repro.rs:164`
- `crates/bench/tests/parallel_defi_repro.rs:270`
- `crates/bench/tests/defi_on_engine.rs:66`
- `crates/bench/tests/alloc_profile.rs:121`
- `crates/executor/benches/sequential_throughput.rs:118`
- `crates/executor/benches/sequential_throughput.rs:159`

### Cross-crate call sites — `Executor::execute_deposit` (method)
- `crates/engine/src/actor/exec_thread.rs:530`

### Cross-crate call sites — `execute_deposit_tx` (free fn)
- `crates/validator/src/parallel/engine_tests.rs:148`

### Cross-crate call sites — `execute_xchain_tx` (free fn; also now takes `XChainDelivery`)
- `crates/engine/src/actor/exec_thread.rs:648`
- `crates/engine/src/replay.rs:211`
- `crates/engine/src/actor/exec_tests.rs:786`
- `crates/validator/src/parallel/engine_tests.rs:166`

`Executor::execute_xchain` (the method): no external callers found —
only `exec-core`'s own `stateless.rs` calls it.

### Cross-crate call sites — `skip_receipt` (`Executor::skip_receipt`, associated fn)
- `crates/stm/src/execute.rs:4439`

Every one of the sites above is a mechanical fix: wrap the existing
`tx_idx, tx_position, tx_index_in_block, cumulative_gas_used_before`
(or subset) arguments into one `TxSlot { .. }` literal in the same
argument position, and (for `execute_xchain_tx` only) also wrap
`origin_chain_id, message` into one `XChainDelivery { .. }` literal.

## Done — item 3 (`WriteSet` type-complexity allow -> named aliases)

`crates/exec-core/src/delta.rs`: `#[allow(clippy::type_complexity)]` on
`WriteSet` deleted, replaced with three type aliases — `pub type
AccountEntry = (Address, (u64, U256, B256));`, `pub type StorageEntry =
((Address, B256), U256);`, `pub type CodeEntry = (B256, Bytes);` — used
directly in `WriteSet`'s field types. The pre-existing
`kardamom_types::delta::CodeEntry` import was renamed to `WireCodeEntry`
to avoid colliding with the new local alias. `WriteSet::hash()`'s
`debug_assert!("WriteSet::finish() not called before hash()")` deleted
outright (no `WriteSetBuilder` type introduced — the assert added no
real guarantee beyond what callers already establish by convention),
and the struct doc's "debug-asserts sortedness" sentence removed with
it. No cross-crate impact — `WriteSet`'s public shape (field names and
types) is unchanged; only the type-complexity workaround and the dead
assert are gone.

## Done — item 4 + item 5 (`Granularity(NonZeroU64)`, `quantize` R16)

`crates/exec-core/src/bal_ladder.rs`: `Granularity(u64)` ->
`Granularity(core::num::NonZeroU64)`. Public `chunk_of(index: u64, k:
u64) -> u64` -> `chunk_of(index: u64, k: NonZeroU64) -> u64`
(cross-crate, see below). Public `quantize(bal, k: u16)` keeps its
exact signature; internally:
```
let granularity = match NonZeroU64::new(u64::from(k)) {
    None => return out,
    Some(k) if k.get() <= 1 => return out,
    Some(k) => Granularity(k),
};
```
(a single `match`, `None` and the `<= 1` guard as separate arms since a
guard on an or-pattern doesn't count toward exhaustiveness). The
`k == 0`-panics doc language is gone — `NonZeroU64` makes the zero case
unrepresentable, not merely guarded (also closes out item 13's `bal_
ladder.rs` "Phase B" reference, since the file no longer needs that
note).

R16: the nested `for acct in &mut out { for slot in &mut acct.
storage_changes { .. } }` loop in `quantize` is split — new
`Granularity::quantize_account(self, acct: &mut AccountChanges)` now
owns the whole per-account body (inner per-slot loop included);
`quantize` itself is a single `for acct in &mut out {
granularity.quantize_account(acct); }` loop.

### Cross-crate call sites — `chunk_of` (`k: u64` -> `k: NonZeroU64`)
All reached via `kardamom_engine::bal_ladder::chunk_of` (`kardamom-engine`
re-exports `exec-core`'s `bal_ladder` module verbatim):
- `crates/validator/src/interop/extract.rs:284` —
  `chunk_of(bal_index, u64::from(granularity))`, guarded by
  `granularity > 1` a few lines up; safe to wrap as
  `NonZeroU64::new(u64::from(granularity)).unwrap()` or equivalent.
- `crates/validator/src/parallel/engine.rs:209` — `chunk_of(first_index,
  k)`, where `k = u64::from(granularity.max(1))`; always `>= 1`.
- `crates/validator/src/parallel/engine.rs:312` — `chunk_of(from, k)`,
  same `k`, inside `if k > 1`.
- `crates/validator/src/parallel/engine.rs:394` — `chunk_of(from, k)`,
  same `k`, inside `if k > 1`.

`quantize`'s own callers (`validator/parallel/engine.rs:192`,
`validator/parallel/engine_tests.rs:345,656,721`) are unaffected —
its public signature still takes plain `k: u16`.

## Done — item 6 (`recompute_post_root` drops `witness` param)

`crates/exec-core/src/anchor/mod.rs`: `recompute_post_root` no longer
takes `witness: &ExecutionWitness`. It used to re-derive `witness.
pre_state_root.ok_or(AnchorError::NoPreStateRoot)?` even though `pre.
root` (from the already-validated `ProvenPre`) holds the same value —
`verify_witness_anchored`/`WitnessAnchor::verify` already extracted and
checked it via the identical `AnchorError::NoPreStateRoot` path before
a `ProvenPre` could exist. New signature: `recompute_post_root(proofs:
&WitnessProofs, pre: &ProvenPre, delta: &PendingDelta) -> Result<B256,
AnchorError>` (3 params, was 4). Doc's `# Errors` section and opening
sentence updated to drop the now-false "no pre_state_root" case.
`AnchorError::NoPreStateRoot` itself is still used, in `WitnessAnchor::
verify` — not dead code.

Fixed the one in-crate caller (`stateless.rs`'s `execute_block_anchored`)
and 4 in-crate test call sites in `tests/anchor_state.rs` (lines 281,
288, 413, 440).

### Cross-crate call site — `recompute_post_root`
- `crates/validator/src/witness.rs:140` — `recompute_post_root(witness,
  &proofs, &pre, delta)` inside a `.and_then(|pre| ..)` closure. Fix:
  drop the leading `witness` argument. Check whether `witness` becomes
  an unused binding in that function afterward (it may still be used a
  few lines earlier for `verify_witness_anchored`).

## Done — item 7 (`anchor/sparse.rs` `Walk` struct)

New `struct Walk<'s, 'p> { path: Nibbles, store: &'s NodeStore<'p> }`.
Converted 7 functions to its methods: the 6 the coordinator named
(`insert_in`, `descend_branch`, `remove_in`, `remove_under_extension`,
`collapse_branch`, `splice_survivor`) plus `lookup_in`, which the
instruction implied by saying "`SparseTrie::insert`/`remove`/`lookup`
build one `Walk` each" (a judgment call — `lookup_in` wasn't named
individually, but leaving it as a loose-parameter free function while
its two siblings became methods would have been inconsistent). Each
method dropped its `path`/`store` parameters in favor of `self.path`/
`self.store`; `depth`, `node`, `value`, `children` (which vary per
recursive call) stayed as loose parameters. `SparseTrie::lookup`/
`insert`/`remove` each build one `Walk::new(Nibbles::unpack(key), self.
store)` and call its first method, replacing the old `Self::lookup_in(
root, &path, 0, self.store)`-style static calls. No cross-crate impact
— `Walk` and all 7 methods are private to `sparse.rs`; `SparseTrie`'s
own public API is unchanged.

## Done — item 8 (R15 follow-up in `eest_state.rs` / `reconstruct`, plus R10 `run_case`)

`crates/exec-core/tests/eest_state.rs`: `decode_case` ->
`DecodedCase::decode(unit, t)`; `build_env` -> `PreparedCase::
build(unit)`; `execute_case` -> `PreparedCase::execute(&mut self,
&DecodedCase)`; `check_post_state`/`post_root_mismatch` -> new
`PostCheck<'a> { t, receipt, alloc }` with `check(&self) -> Outcome`
and `mismatch_detail(&self, root) -> String`; `run_file`/`walk_and_run`
-> `Stats::run_file`/`Stats::walk`; `write_report_artifact`/
`print_summary` -> `Stats` methods. R10: `run_case` is now a private
nested `try_run(unit, t) -> Result<Outcome, Outcome>` chaining
`DecodedCase::decode`, `PreparedCase::build`, and `.execute` with `?`,
collapsed with `.unwrap_or_else(|outcome| outcome)` — replacing the
original three `match { Ok(x) => x, Err(outcome) => return outcome }`
blocks.

`crates/reconstruct/src/lib.rs` tests: `build_interop_scenario` ->
`InteropScenario::build() -> Self`; `assert_interop_lane_state`/
`assert_interop_receipts` -> new `RebuiltDb<'a> { snap: &'a
kardamom_state::StateSnapshot, origin: u64 }` with `assert_lane_
state(&self)`/`assert_receipts(&self)`. The one test using them
(`blob_roundtrip_executes_remote_epochs`) builds one `RebuiltDb` and
calls both methods on it.

**Verification caveat**: `cargo check -p kardamom-reconstruct` cannot
reach this file's own compilation right now, since `kardamom-reconstruct`
depends on `kardamom-engine`, which fails to build from the item 2 and
item 10 cross-crate fallout (its own `execute_tx`/`execute_deposit`/
`execute_xchain_tx`/`execute_once`/`envelope_view` call sites, listed
above) — confirmed as a pure dependency-order block via `cargo check -p
kardamom-reconstruct --keep-going`. Did not temporarily patch
`crates/engine` to unblock this (risk of jj's auto-snapshot retaining
stray edits in another group's workspace files). Verified instead by
line-by-line manual review against the original code, `rustfmt --check`
on the file (parses and reformats cleanly, only import-ordering diffs),
and `cargo fmt -p kardamom-reconstruct -- --check` (exit 0, no diff —
needs no compilation). **Re-run `cargo check -p kardamom-reconstruct
--all-targets` once `kardamom-engine` is fixed at merge** to get a real
type-check of this file, not just a syntax check.

## Done — item 9 (`footprint/src/oracle.rs` `CellIndex`/`Predictions`)

New `#[derive(Default)] struct CellIndex { readers: HashMap<Cell,
Vec<usize>>, writers: HashMap<Cell, Vec<usize>> }` with `index_tx`
(absorbs `index_readers`/`index_writers`) and `pairs(&self, txs) ->
HashSet<(u64, u64)>` (absorbs the old pair-building loop).
`conflict_pairs` (3 callers: `Report::per_block_oracle`, `Holdout::
grade`, `grade.rs`'s `grade_block`) stays as a thin wrapper — builds
one `CellIndex`, returns `.pairs(txs)` — so none of its 3 callers see a
signature change.

`predicted_pairs` (the 3-tuple-returning free fn) deleted outright. New
`pub(crate) struct Predictions { pub(crate) cells: PredictedCells,
pub(crate) cold: usize }` with `Predictions::of(stats, txs) -> Self`
and `pairs(&self, txs, exclude) -> HashSet<(u64, u64)>`. Both callers
(`Holdout::grade`, `grade.rs`'s `grade_block`) updated to build one
`Predictions` and read `.cells`/`.cold`/`.pairs(..)` directly instead
of destructuring a 3-tuple.

**Self-caught second-pass R16 miss**: while writing `CellIndex::pairs`,
the first draft was `for (cell, ws) in &self.writers { for (a, &w) in
ws.iter().enumerate() { .. } }` — still a textually nested double loop,
even though the inner body delegated to the pre-existing
`insert_pairs_with` helper. This means an *earlier* (pre-this-round)
R16 pass on `conflict_pairs` had only extracted the innermost work and
never flattened the wrapping two-level loop. Fixed by adding
`insert_cell_pairs(&self, cell, ws, txs, pairs)`: `pairs` is now a
single loop over `&self.writers` calling `insert_cell_pairs` once per
cell, and `insert_cell_pairs` is a single loop over that cell's writers
calling `insert_pairs_with`. Caught during my own review before
finalizing, not left for the coordinator to find.

No cross-crate impact — `CellIndex`, `Predictions`, `conflict_pairs`,
and `predicted_pairs` are all `pub(crate)` or module-private.

## Done — item 10 (tuple returns -> named structs)

- `crates/exec-core/src/features.rs`: `pack_beacon`/`unpack_beacon`
  deleted, replaced with `pub struct Beacon { pub count, pub
  block_number, pub timestamp_ms }` plus `Beacon::pack(self) -> U256`
  and `Beacon::unpack(word: U256) -> Self`. `pack_beacon` has no
  external callers; `unpack_beacon`'s cross-crate call sites:
  - `crates/engine/src/actor/exec_tests.rs:408` (import), `:418` —
    `.map(|s| unpack_beacon(s.value))` inside a `fn beacon_in(delta) ->
    Option<(u64, u64, u64)>` test helper. Fix: `unpack_beacon` ->
    `Beacon::unpack`; consider changing `beacon_in`'s own return type to
    `Option<Beacon>` too (that choice belongs to the engine group).
  - `crates/e2e/src/scenarios/mod.rs:264` (import), `:275` — `let
    beacon = unpack_beacon(snap.storage(..)..)`. Fix: `unpack_beacon`
    -> `Beacon::unpack`.
- `crates/footprint/src/lib.rs`: `envelope_view`/`decoded_view` both
  returned `(Option<Address>, Option<[u8; 4]>, Vec<U256>, bool)`; both
  now return `pub struct EnvelopeView { pub to, pub selector, pub args,
  pub has_value }`. No in-crate callers. Cross-crate call sites (fix at
  each is renaming the destructure to a field-pattern binding, e.g.
  `let EnvelopeView { to, selector, args, has_value } = ..`):
  - `crates/bench/src/stm/capture.rs:58`, `:69`
  - `crates/bench/src/bin/stm-p2.rs:302`
  - `crates/stm/src/schedule.rs:49` (`envelope_view`), `:62`
    (`decoded_view`)
  - `crates/engine/src/shadow.rs:129`
- `guest/kardamom-zk-guest/src/lib.rs`: `GuestBlock::run` was `->
  (AnchoredBlockOutput, B256)`, now returns `pub struct GuestRun { pub
  anchored: AnchoredBlockOutput, pub records_digest: B256 }`. Both
  in-group callers (`main.rs`, `bin/batch.rs`) fixed. No cross-crate
  callers — guest crates are workspace-detached.
- `guest/kardamom-zk-host/src/main.rs`: `BatchRun::expected_boundary_
  roots` was `-> (B256, B256)`, now returns a private `struct
  BoundaryRoots { pre: B256, post: B256 }`. One in-group caller
  (`BatchRun::run`) fixed. No cross-crate callers.
- `crates/footprint/src/oracle.rs`: `Holdout::split` was `-> (Vec<TxObs>,
  Self)`, now returns a private `struct TrainSplit<'a> { train:
  Vec<TxObs>, holdout: Holdout<'a> }`. One in-group caller (`analyze`)
  fixed. `Holdout::split` and `TrainSplit` are both module-private, so
  no cross-crate callers are possible.

## Done — item 11 (R14 follow-up: `state.rs` / `witness.rs` / `stateless.rs`)

- `crates/exec-core/src/state.rs`: new private `MockStateDatabase::
  read(&self) -> RwLockReadGuard<'_, MockInner>` and `::write(&self) ->
  RwLockWriteGuard<'_, MockInner>` helpers, replacing the 5
  `self.inner.read().expect(..)` sites and 1 `self.inner.write().
  expect(..)` site.
- `crates/exec-core/src/witness.rs`: new `Recorded::memo<K: Ord + Copy,
  V: Clone, E>(map, key, f) -> Result<V, E>` ("check cache, else
  fetch-and-cache", first-touch-wins). `WitnessRecorder`'s `basic`/
  `code_by_hash`/`storage` all call it now; `code_by_hash` keeps its
  empty-hash early return ahead of the `memo` call.
- `crates/exec-core/src/stateless.rs`: `execute_block_capture` and
  `execute_block_with_bal` (which differed only in output shape) now
  both call a new private `execute_block_capturing(..) -> Result<
  (BlockExecOutput, Bal), ExecutorError>` holding the shared "build a
  fresh `Bal`, run with capture on" logic; each wrapper reshapes the
  result its own way. `SnapshotDb` forwarding in `executor/db.rs` left
  as-is, per the coordinator's explicit "accepted".

No cross-crate impact — all three are private helpers behind unchanged
public APIs.

## Done — item 12 (`classifier.rs` `is_frequent` saturating multiply)

`crates/footprint/src/classifier.rs`: `n * 10 >= observations * 6` ->
`n.saturating_mul(10) >= observations.saturating_mul(6)`. Both sides
saturate identically, so the comparison stays monotone; no behavior
change for any value that doesn't already overflow `u64 * 10`.

## Done — item 13 (comments must not reference the audit process)

Rewrote every flagged comment as a plain statement of the code's
contract:
- `crates/exec-core/src/executor/scope.rs`: `execute_tx_decoded`'s and
  `execute_xchain`'s (moved to `scope_derived.rs`) docs no longer
  mention "R15", "Phase C", or "the coordinator" — the
  `too_many_arguments` allows those comments were attached to are gone
  entirely (item 2), so the audit-referencing text was deleted along
  with them rather than rewritten in place.
- `crates/exec-core/src/executor/scope_derived.rs`: same, via the same
  deletion.
- `crates/exec-core/src/bal_ladder.rs`: `chunk_of`'s doc no longer
  mentions "Phase B" — the `NonZeroU64` change (item 5) made the
  `k == 0` case unrepresentable, so the doc note about a "known defect"
  is gone rather than reworded.
- `crates/footprint/src/oracle.rs`: `Holdout::split`'s doc rewritten
  from referencing "a different audit group" / "the audit flags" to a
  plain statement of `train_frac`'s valid range and what a local clamp
  would or wouldn't change (done as part of item 9's rewrite of the
  same function).
- `crates/exec-core/tests/common/mod.rs`: the module doc's defensive
  "`#![allow(dead_code)]` reflects that, not neglect" sentence trimmed;
  the substance now lives in the attribute's own `reason =` string
  (item 1).

## Gate results (Coordinator round 1)

- `cargo clippy -p kardamom-exec-core --all-targets --all-features --
  -D warnings`: clean.
- `cargo clippy -p kardamom-footprint --all-targets --all-features --
  -D warnings`: clean.
- `cargo clippy -p kardamom-exec-core -p kardamom-footprint
  --all-targets --all-features -- -W clippy::pedantic`: zero warnings
  under `crates/exec-core` or `crates/footprint` (all reported warnings
  are pre-existing, under `crates/types`, owned by another group).
- `cargo fmt -p kardamom-exec-core -p kardamom-footprint -p
  kardamom-reconstruct -- --check`: clean.
- `cargo test -p kardamom-exec-core --all-features`: all 12 test
  binaries pass (lib 54, anchor_sparse 6, anchor_state 4, cfg_pinning
  8, code_hash_scope_invariance 1, decode_cost 1, eest_state 0/1
  ignored — needs `KARDAMOM_EEST_FIXTURES`, hash_cost 1, touch_set 4,
  write_set_encoding 3, write_set_hash 6, doctests 0).
- `cargo test -p kardamom-footprint --all-features`: 10/10 lib tests
  pass, 0 doctests.
- `cargo clippy -p kardamom-reconstruct --all-targets --all-features --
  -D warnings` / `cargo test -p kardamom-reconstruct`: **blocked**, not
  run to completion — `kardamom-reconstruct` depends on
  `kardamom-engine`, which fails to compile from this round's own
  cross-crate fallout (see item 8's verification caveat and the
  consolidated failure list below). `kardamom-reconstruct`'s own
  source was verified via `cargo fmt -- --check` (parses cleanly) and
  manual review instead.
- `guest/kardamom-zk-guest`: `cargo check`, `cargo clippy --all-targets
  -- -D warnings`, `-- -W clippy::pedantic`, `cargo fmt -- --check`:
  all clean.
- `guest/kardamom-zk-host`: `cargo check --bins`, `cargo clippy --bins
  -- -D warnings`, `-- -W clippy::pedantic`, `cargo fmt -- --check`:
  all clean.
- `cargo check --workspace --all-targets --keep-going`: fails exactly
  on the cross-crate call sites this round's signature changes broke —
  `kardamom-engine` (lib: 6 errors, lib tests: 8 errors) and
  `kardamom-stm` (lib: 7 errors, lib tests: 7 errors). No other crate's
  own frontend pass is reached, since `kardamom-validator`,
  `kardamom-bench`, `kardamom-executor`, and `kardamom-reconstruct` all
  depend on `kardamom-engine` and/or `kardamom-stm`; their own call
  sites (listed above, under each item) were found by exhaustive `grep`
  across the workspace rather than by the compiler, and are equally
  real fixes needed at merge. Consolidated: **40 external call sites**
  across `crates/engine`, `crates/stm`, `crates/validator`,
  `crates/bench`, `crates/executor`, and `crates/e2e`, spanning
  `execute_tx`, `execute_tx_decoded`, `execute_once`, `execute_deposit`,
  `execute_deposit_tx`, `execute_xchain_tx` (+ `XChainDelivery`),
  `skip_receipt`, `chunk_of`, `recompute_post_root`, `unpack_beacon`,
  and `envelope_view`/`decoded_view`. Every fix is mechanical (wrap
  existing loose arguments into the new struct literal, or rename a
  tuple destructure to a field-pattern one) — see each item's own
  cross-crate list above for exact file:line and the wrap shape.

## Notes (Coordinator round 1)

- No item was deferred and no item was judged wrong this round — all
  13 were implemented as asked, with two judgment calls flagged inline
  above (the `XChainDelivery` addition under item 2, and folding
  `lookup_in` into `Walk` under item 7) for the coordinator's review at
  merge, since both go slightly beyond the literal instruction text.
- The `kardamom-reconstruct` gate could not be run to completion this
  round because its own dependency graph (via `kardamom-engine`) is
  broken by this round's changes. This is expected, not a regression
  introduced by touching `reconstruct/src/lib.rs` itself — re-run
  `cargo check -p kardamom-reconstruct --all-targets --all-features`
  and `cargo test -p kardamom-reconstruct` once `kardamom-engine`'s
  callers are fixed at merge.

# Status: exec-core (Coordinator round 2)

Coordinator accepted round 1 in full (43c1d7046104..79b7fb3d), including
both judgment calls (`XChainDelivery`, folding `lookup_in` into `Walk`).
Two small follow-ups closed the group out.

## Done — symmetry: `Executor::execute_xchain` takes `XChainDelivery`

`crates/exec-core/src/executor/scope_derived.rs`: `execute_xchain(slot,
origin_chain_id, message, bal)` -> `execute_xchain(slot, delivery:
XChainDelivery<'_>, bal)`, matching `execute_xchain_tx`'s shape exactly
(`slot, delivery, bal`). Body reads `delivery.origin_chain_id` and
`delivery.message` where the two separate params were used. Dropped the
now-unused `XChainMessage` import (only `xchain::{self}` remains).

Updated the one in-crate caller: `crates/exec-core/src/stateless.rs`'s
`execute_record_in_scope`, `BufferedRecord::XChain` arm, now builds
`crate::executor::XChainDelivery { origin_chain_id: *origin_chain_id,
message }` (via the crate-root re-export — `executor::xchain` itself is
a private module, so the qualified path had to go through `executor::
XChainDelivery`, not `executor::xchain::XChainDelivery`).

No cross-crate list change: `execute_xchain` (the method) still has no
external callers, confirmed again by the same grep as round 1.

## Done — nit: `tests/eest_state.rs` `PreparedCase::execute`'s `TxSlot` literal

Replaced the spelled-out `BPosition { term_id: 0, term_offset: 0 }`
with `BPosition::from_index(0)`. Chose this over adding `mod common;
use common::slot;` since this file doesn't otherwise depend on the
shared `tests/common` module and a one-field simplification didn't
seem worth introducing that dependency for a single call site.

## Gate results (Coordinator round 2)

- `cargo check -p kardamom-exec-core --all-targets --all-features`:
  clean.
- `cargo clippy -p kardamom-exec-core --all-targets --all-features --
  -D warnings`: clean.
- `cargo clippy -p kardamom-exec-core --all-targets --all-features --
  -W clippy::pedantic`: zero warnings under `crates/exec-core`.
- `cargo fmt -p kardamom-exec-core -- --check`: clean.
- `cargo test -p kardamom-exec-core --all-features`: all 12 test
  binaries pass (same counts as round 1: lib 54, anchor_sparse 6,
  anchor_state 4, cfg_pinning 8, code_hash_scope_invariance 1,
  decode_cost 1, eest_state 0/1 ignored, hash_cost 1, touch_set 4,
  write_set_encoding 3, write_set_hash 6, doctests 0).
- Re-ran the item-13 audit-language sweep and the jj-scope check from
  round 1: both still clean (zero audit-process references, no edits
  outside owned paths).

Both items in this round were in-crate only — the round-1 cross-crate
call-site list is unchanged.
