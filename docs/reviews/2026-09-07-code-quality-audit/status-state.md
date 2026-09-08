# Status: state (kardamom-state, kardamom-types)

Phase A work for the `state` group. Every row from `crate-state.md` and
`inputs-state.md` appears in one list below. The 237 pedantic rows in
`inputs-state.md` and the pedantic rows the crate-state.md summary counts
are grouped by file: each file's row says what changed, and a fresh
`cargo clippy -p kardamom-state --all-targets -- -W clippy::pedantic` and
`cargo clippy -p kardamom-types --all-targets -- -W clippy::pedantic` both
show zero warnings in this group's own files, confirming every row in that
file is resolved.

The coordinator reviewed the Phase A diff, accepted it with four fixes, and
assigned Phase C: R13, R12, R9 rows 5-7 (reversed out of "Deferred to Phase
B" below), R15, and R16 (two rules new to this review, applied retroactively
to Phase A code too), and R14 (`docs/reviews/2026-09-07-code-quality-audit/
dry-state.md`, and `arith-state.md` for R12/R13). The "## Done" section
below is Phase A; "## Phase C" covers everything after the review. Both the
"## Deferred to Phase B" and "## Not done, judged wrong" sections cover
both phases, with a "Phase C additions" subheading marking where Phase C's
own rows start.

## Done

### Defects (README "Defects found on the way")

- Defect 1, `crates/types/src/xchain/message.rs` (formerly
  `crates/types/src/xchain.rs:388`): `RemoteEpochRecord::last_seq`
  underflowed on an empty batch (`Default` makes one constructible). Fixed
  with `saturating_add`/`saturating_sub` so it never wraps or panics.
  Field types and the method signature are unchanged. Unit test:
  `xchain::tests::last_seq_does_not_underflow_on_an_empty_default_record`.
- Defect 2, `crates/state/src/checkpoint/mod.rs:277` (now inside
  `quarantine_failed_message`): the broken format string ("could not be"
  followed by 26 literal spaces then "quarantined") is fixed, and the
  message-building code is split into its own function so the exact text
  is a plain unit test:
  `checkpoint::tests::quarantine_failed_message_has_a_readable_gap`.
- Defect 14 (state/types half), comments that contradict the code:
  - `crates/state/src/meta.rs:13` "currently 1" → the value is dropped;
    the constant is the source of truth.
  - `crates/state/src/schema.rs:1` "Seven named tables" (11 exist) → now
    says "the tables listed in `ALL_TABLES`", and the doc table lists all
    11.

### R1 comments

Every row in `crate-state.md`'s R1 tables (production and test) is done:
`schema.rs` (1, 8, 41, 118, 127, 137), `meta.rs` (13, 51, 54),
`writer/mod.rs:310` (the "Duplicate code. Fine." comment is gone along
with the redundant match arm it sat on — see R11), `integrity/mod.rs:4`,
`checkpoint/manifest.rs:20`, `checkpoint_transfer.rs` (188, 299),
`types/epoch.rs` (5, 30), `types/xchain` (the dangling
`interop-outbox-messaging-spec.md` reference, dropped in the module doc
during the R3 split), `types/receipt.rs` (17, 129), `types/deposit.rs:61`,
`types/delta.rs` (38, 51, 56), `types/tx_error.rs:43`, `types/witness.rs:12`,
`types/prover.rs` (3, 60), `types/boundary.rs` (7, 26),
`types/tx_ordering.rs` (55, 62), `types/upgrades.rs:22`, and the test-only
rows `types/tests/rkyv_roundtrip.rs:121`,
`state/trie/incremental_tests.rs` (422, 426), `state/genesis.rs:338`,
`types/withdrawals.rs:273`. `state/checkpoint/tests.rs` and
`types/tests/rkyv_roundtrip.rs` KEEP rows needed no change.

### R2 long functions

- `crates/state/src/writer/mod.rs:238` `apply` (142 lines) — moved to a
  new file `crates/state/src/writer/apply.rs` and split into
  `write_storage`, `write_accounts`, `write_code`, `write_header`,
  `write_receipts_and_index`, `write_meta_cursors`, `advance_state_root`,
  and an `ApplyTimings` struct for the `KARDAMOM_WRITER_TIMING`
  stopwatches. `writer/mod.rs` now holds only `WriteBatch`, `WriterHandle`,
  `TrieMode`, `StateWriter`, and the spawn/run path.
- `crates/state/src/integrity/compare.rs:24` `deep_compare` (102 lines) —
  split into `compare_table`, `value_diff_message`, `compare_meta_keys`,
  keeping `receipt_field_diff`.
- `crates/state/src/checkpoint_transfer.rs:217` `fetch_latest_checkpoint`
  (93 lines) — split into `open_peer_stream`, `download_image` (this also
  closes R4/R5's `drop(out)` site — see below), `verify_checkpoint_image`.
- `crates/state/src/checkpoint_transfer.rs:376` `read_response_head`
  (51 lines) — split into `read_head_text`, `parse_status_line`,
  `parse_headers`.
- `crates/state/src/genesis.rs:81` `seed_genesis` (78 lines) — split into
  `check_or_backfill_digest`, `write_allocations`, `seed_trie`.
- `crates/state/src/bin/kardamom-statecheck.rs:60` `main` (74 lines) —
  split into `parse_args`/`Args`, `check_root`, `compare_dirs`.
- `crates/state/src/integrity/checks.rs:153` `check_receipts_index`
  (72 lines) — split into `check_receipt_row`, `check_index_rows`.
- `crates/state/src/recovery/mod.rs:125` `bootstrap_trie_from_state`
  (68 lines) — split into `read_all_accounts`, `read_all_storage`,
  `read_all_code`.
- `crates/state/src/integrity/checks.rs:93` `check_headers` (52 lines) —
  split into a `HeaderChainState` struct, `check_header_row`,
  `check_header_chain_ends`.
- `crates/state/src/trie/mod.rs:187` `update_for_block` (84 lines,
  appendix names helpers though it fell outside my explicit task list) —
  split into `group_storage_by_account`, `update_storage_tries`,
  `write_hashed_accounts`, `update_account_trie`.
- `crates/types/src/xchain.rs:414` `derive_remote_epoch` (56 lines) — see
  "Not done, judged wrong": left as one function.
- Test functions over 50 lines in this group's files (`incremental_tests.rs`
  at 97 and 81 lines, `recovery/tests.rs` at 71/64/58 lines,
  `writer/tests.rs` at 58 lines, `tests/concurrent_readers.rs` at 52 lines):
  KEEP. Each is one linear randomized-model or fixture-setup scenario; the
  reviewer appendix does not name helpers for the state-group test rows,
  and splitting a single scenario across functions would not make any of
  them easier to follow.

### R3 large files

- `crates/types/src/xchain.rs` (541 lines, 259 test) — split into a
  directory: `xchain/mod.rs` (module docs, `OUTBOX`/`INBOX` and the two
  domain/tag constants, re-exports), `xchain/ids.rs` (`remote_source_hash`,
  `alias_remote_address`, `xchain_tx_sender`, `encode_list_two`),
  `xchain/message.rs` (`Callback`, `OutboxMessage`, `XChainMessage`,
  `RemoteEpochRecord`), `xchain/leaf.rs` (`xchain_leaf_domain`, `msg_leaf`,
  `no_callback_hash`), `xchain/abi.rs` (`INBOX_DELIVER_SIGNATURE`,
  `inbox_deliver_selector`, `deliver_calldata`, the `push_word_*`
  helpers), `xchain/derive.rs` (`derive_remote_epoch`, `XChainError`),
  `xchain/tests.rs`, `xchain/abi_tests.rs`. Every public path
  (`kardamom_types::xchain::*`) is unchanged via `pub use` re-exports.
  Largest file after the split is `message.rs` at under 200 lines.
- `crates/state/src/writer/mod.rs` (313 lines) — `apply` and its new
  helpers moved to `writer/apply.rs` (see R2); `mod.rs` now holds only the
  handle/spawn/run path.
- `crates/types/src/epoch.rs` (427 lines, 243 test) — inline
  `#[cfg(test)] mod tests { ... }` moved to a sibling file
  `crates/types/src/epoch_tests.rs`, matching the crate's `#[path = "..."]`
  pattern (`recovery/mod.rs`, `writer/mod.rs` already do this).
  `epoch.rs` is now 401 lines, well under the limit, with one derivation
  rule and its log types (a KEEP per the appendix).
- `crates/state/src/checkpoint_transfer.rs` (431 raw / 309 code lines
  before this pass) — KEEP as one file. After the R2 splits it is still
  well under the 500-line limit; the audit's alternative to a
  `serve.rs`/`fetch.rs` split was explicitly optional ("KEEP... or split").
- `crates/state/src/trie/mod.rs` (361 lines) — KEEP, per the appendix: the
  trie facade (table handles, per-block update, rebuild oracle, pure root
  functions), each part small and sharing `AccountTrieParts`.

### R4 manual drops

- `crates/state/src/writer/mod.rs:96` `drop(std::mem::replace(...))` in
  `WriterHandle::shutdown` — REPLACE_WITH_OWNERSHIP is deferred (see
  Deferred list: `delta_tx` is a public field read directly by five other
  crate groups).
- `crates/state/src/writer/mod.rs:448` (old numbering) `drop(txn)` in
  `ensure_schema_version` — extracted `read_schema_version(env)`, which
  opens and ends its own transaction; the version compare now runs
  outside any transaction scope, and the drop is gone.
- `crates/state/src/genesis.rs:97` `drop(txn)` — this one was genuinely
  redundant (nothing after it touches the transaction before the function
  returns), so it is deleted rather than wrapped in a helper; the borrow
  already ends at the natural end of scope. `seed_genesis`'s later split
  (R2) also moved this branch into `check_or_backfill_digest`, which takes
  the transaction by value and lets it drop implicitly on the "digest
  matches" path, exactly as the original code did.
- `crates/state/src/checkpoint_transfer.rs:295` `drop(out)` — extracted
  `download_image(reader, len, path, peer) -> Result<(), StateError>`; the
  file closes when the helper returns, before `file_keccak` reopens the
  path.
- Test-only drops (`swap.rs` old test, `checkpoint/tests.rs`,
  `integrity/tests.rs`, `tests/env_smoke.rs`): left alone, as instructed —
  each closes an env or handle so a later assertion can observe the
  closed state.

### R5 sync primitives and channels

- `crates/state/src/swap.rs:31,40,47` `Arc<ArcSwapOption<StateSnapshot>>` —
  REPLACE_WITH_CHANNEL done. Deleted `latest`; `current()` now reads
  `self.watch.borrow().clone()`. Traded the lock-free load for one read
  lock per peek, as the finding said to. `swap.rs`'s module doc updated to
  describe the new single-slot design. Removed the now-unused `arc-swap`
  dependency from `crates/state/Cargo.toml` (`Cargo.lock` updated
  automatically by the same `cargo check` that confirmed the crate still
  builds; no other dependency changed).
- `crates/state/src/swap.rs:32,41,48` `crossbeam_channel::bounded(1)`
  notify, `:33,42,49` `tokio::sync::watch::channel` — JUSTIFIED, confirmed,
  no change.
- `crates/state/src/writer/mod.rs:159` `crossbeam_channel::bounded(HORIZON_BLOCKS)`
  — JUSTIFIED, confirmed, no change.
- `crates/state/tests/concurrent_readers.rs:40` `Arc<AtomicBool>` —
  JUSTIFIED, confirmed, no change.
- `crates/state/src/writer/mod.rs:95` `crossbeam_channel::bounded(0)` —
  see Deferred list.

### R6 dynamic dispatch

- `crates/state/src/trie/walker.rs:72,100,251` (old numbering) the
  `account_leaf: &dyn Fn(&AccountTrieParts) -> Vec<u8>` parameter on
  `account_root`, `walk_account`, `walk_account_for_proofs` — deleted.
  Added a free `pub(crate) fn account_leaf_rlp(p: &AccountTrieParts) -> Vec<u8>`
  in `trie/mod.rs`; both callers (`trie/mod.rs`'s `StateRoot` impl and
  `trie/proofs.rs::account_proof_nodes`) now call it directly. This also
  dropped `walk_account`'s argument count from 8 to 7, so its
  `#[allow(clippy::too_many_arguments)]`, which the function never had
  explicitly, stays absent.
- `crates/state/src/trie/incremental_tests.rs:332` (old numbering) the
  test-only `let mine = |pred: &dyn Fn(&[u8; 5]) -> bool| ...` closure —
  `mine` and its sibling `nibs5` are now nested `fn`s, with `mine`
  generic over `P: Fn(&[u8; 5]) -> bool`. The five call sites lost their
  leading `&`.

### R7 supertraits

None found for this group, confirmed (crate-state.md: "no where-clause
holds four bounds"; `trie::cursor::ReadKind` is already the pattern this
rule asks for).

### R8 unreachable/unnecessary `pub`

Workspace-grepped (`grep -rn --include='*.rs' '<name>' crates guest`, plus
a targeted check of `crates/*/tests`, `crates/*/benches`, and
`crates/*/src/bin`, which are separate compilation units even inside this
crate and need their own visibility) for every item below before
demoting. After all demotions, `cargo check -p kardamom-engine -p
kardamom-executor -p kardamom-validator -p kardamom-bench -p
kardamom-sequencer -p e2e -p kardamom-batcher` all succeed (the two
`kardamom-deployer`/`kardamom-batcher` failures seen were pre-existing
missing contract-build artifacts, unrelated to this change, confirmed by
the error text naming `contracts/out/*.json`).

- `crates/state/src/schema.rs`: `TABLE_ACCOUNTS`...`TABLE_HASHED_STORAGE`
  (11 consts), all codec fns (`encode_account_key`... `decode_tx_hash_value`,
  ~19 items), `AccountValue` → `pub(crate)`. `ALL_TABLES` stays `pub`
  (used by `crates/state/tests/env_smoke.rs`, a separate compilation
  unit). `HeaderValue` stays `pub` (return type of `read_all_headers`,
  which `e2e/src/scenarios/derivation.rs` calls).
- `crates/state/src/meta.rs`: all seven `KEY_*` consts, all
  `read_meta_*`/`encode_*`/`decode_*` functions (18 items) →
  `pub(crate)`.
- `crates/state/src/geometry.rs`: `PAGE_SIZE`, `SIZE_UPPER`, `SIZE_LOWER`,
  `GROWTH_STEP`, `SHRINK_STEP`, `MAX_DBS`, `HORIZON_BLOCKS`, `MAX_READERS`
  → `pub(crate)`. The last two are named only in other crates' comments
  (confirmed: no `use` or value reference anywhere outside this crate),
  so this crate's own copy is the only one read.
- `crates/state/src/trie/mod.rs`: `StateRoot`, `apply_trie_updates`,
  `rebuild_root` → `pub(crate)`; `TrieTables` and `update_for_block` stay
  `pub` (used by `validator/src/witness.rs`, `validator/src/prover.rs`,
  `validator/tests/witness_anchoring.rs`).
- `crates/state/src/trie/prefix_set.rs`: `PrefixSet` and its four methods
  → `pub(crate)`; `is_empty` is further gated `#[cfg(test)]`, since it is
  only used by the module's own test (rustc's dead-code pass only sees
  that once the type left the public API).
- `crates/state/src/trie/node.rs`: `encode_branch_node`,
  `decode_branch_node` → `pub(crate)`.
- `crates/state/src/trie/walker.rs`: `TrieUpdates` → `pub(crate)` (its
  three fields stay `pub`, capped at `pub(crate)` by the struct).
- `crates/state/src/checkpoint/mod.rs`: `latest_checkpoint`,
  `restore_checkpoint` → `pub(crate)`. `CheckpointInfo` stays `pub` (its
  `block` field is read by `crates/executor/src/bin/kardamom-executor/state.rs`
  through `create_checkpoint`'s return value — a usage a plain
  identifier grep misses, caught only by re-checking the actual call
  site). `create_checkpoint`, `prune_checkpoints`, `restore_best_checkpoint`,
  `park_state_db`, `has_state_db` stay `pub`, confirmed real external
  callers.
- `crates/state/src/checkpoint/manifest.rs`: `CheckpointManifest` and its
  three helper fns (`manifest_path`, `read_manifest`, `verify_checkpoint`)
  → `pub(crate)`. The `crate::checkpoint` re-export list updated to match;
  `manifest_path`'s re-export is further gated `#[cfg(test)]` (only
  `checkpoint/tests.rs` reaches it through the re-export; production code
  inside `manifest.rs` calls the local name directly).
- `crates/state/src/checkpoint_transfer.rs`: `fetch_latest_checkpoint` →
  `pub(crate)` (confirmed unused outside the crate; the top-level
  `lib.rs` re-export list updated).
- `crates/state/src/writer/mod.rs`: `WriteBatch::approx_size_bytes` →
  `pub(crate)`.
- `crates/state/tests/common/mod.rs`: `open_tmp_writer`, `bpos`,
  `simple_delta`, `slot_key` → `pub(crate)` (the mechanical
  `unreachable_pub` rows). Each of the seven `mod common;` test binaries
  uses only a subset of the four, and the module already carries
  `#![allow(dead_code)]` for exactly that reason, so no new warning.
- `crates/types/src/tx_ordering.rs`: `is_epoch`, `is_remote_epoch`,
  `as_epoch`, `as_remote_epoch` — deleted (confirmed zero callers
  anywhere, including the crate's own tests; `is_tx_ref`, `is_deposit_ref`,
  `is_boundary`, `as_tx_ref`, `as_deposit_ref`, `as_boundary` are used by
  `types/tests/rkyv_roundtrip.rs`, so those six stay).
- `crates/types/src/xchain/message.rs`: `XChainMessage::aliased_sender` —
  deleted (confirmed dead; `xchain_tx_sender` covers every real caller,
  matching the appendix).
- `crates/types/src/receipt.rs`: `Receipt::is_xchain` — deleted (confirmed
  dead, not even in this crate's own tests).
- `crates/types/src/epoch.rs`: `deposit_from_lockbox_log` → private
  (`derive_epoch` is its only caller, confirmed).
- `crates/types/src/withdrawals.rs`: `recompute_root` — kept `pub`, doc
  comment reworded to describe it as the reference verifier a proof
  consumer runs, rather than removing it or gating it to tests (see R9
  section of `crate-state.md`; the constants `OUTPUT_VERSION`,
  `LEAF_DOMAIN`, `NODE_DOMAIN` were already correctly `pub`, no change).
- `crates/types/src/prover.rs`: `BatchProverInput` — NOT dead; see "Not
  done, judged wrong".

### R9 defensive validation (in-group only; cross-crate newtypes listed under Deferred)

- `crates/state/src/writer/mod.rs:198` `assert!(matches!(self.env.raw().env_kind(), ...))`
  in the writer thread's `run()` — the assert would panic the writer
  thread and strand every snapshot consumer. Moved the same check into
  `StateEnvBuilder::open` (`crates/state/src/env.rs`), which now returns
  `Err(StateError::Recovery(..))` for an unexpected env kind before a
  `StateEnv` value can even exist. The writer's `run()` no longer needs
  the check at all (every `StateEnv` it can see already passed it), so
  the assert is deleted outright rather than converted into a second
  check. This is provably a no-behavior-change for every path in this
  codebase today: `StateEnvBuilder::open` is the only constructor of a
  `StateEnv`, and it only ever sets `write_map` or leaves it unset, so
  `env_kind()` could never have been anything but `Default` or
  `WriteMap`; the new check documents and enforces that invariant at the
  boundary instead of asserting on it deep in a background thread.

### R10 imperative style

- `crates/types/src/epoch.rs:348-357` `derive_epoch`'s foreign-log
  collection loop → `.iter().map(...).collect::<Result<_, _>>()?`.
- `crates/types/src/xchain/derive.rs` (formerly `xchain.rs:449-456`)
  the foreign-destination loop → `.iter().find(...)` then one
  `return Err(...)`.
- `crates/types/src/genesis.rs:76-81` the duplicate-alloc loop →
  `.iter().try_for_each(...)`.
- `crates/state/src/trie/node.rs:51-54` the hashes-collection loop →
  `(0..len).map(|_| cur.b256()).collect::<Result<_, _>>()?`.
- `crates/state/src/checkpoint/mod.rs:160-172` (old numbering)
  `latest_checkpoint`'s best-so-far loop → `.filter_map(...).max_by_key(...)`
  (folded into the R2 split above).
- `crates/state/src/checkpoint/mod.rs:183-192` (old numbering)
  `prune_checkpoints`'s counting loop → `rd.try_fold(0usize, ...)`.
- `crates/state/src/writer/apply.rs` (formerly `writer/mod.rs:308-310`)
  the code-write `match` had two identical-body arms
  (`Ok(())` / `Err(KeyExist)`); combined into one `Ok(()) | Err(KeyExist) => {}`
  arm (also fixes the `match_same_arms` pedantic lint and removes the R1
  "Duplicate code. Fine." comment). The same pattern repeats in
  `genesis.rs::write_allocations` and `trie/mod.rs`'s two delete loops —
  all combined the same way.
- `crates/types/src/genesis.rs:46-68` `to_alloc`'s two-loop account/code
  build — NOT changed; see "Not done, judged wrong".
- Kept imperative, confirmed correct to keep, no change:
  `integrity/compare.rs`'s dual-cursor merge (now `compare_table`),
  `trie/cursor.rs` and `checkpoint_transfer.rs`'s reader loops,
  `trie/mod.rs`'s `del_prefix` (must collect keys before deleting, cursor
  borrows the table), `checkpoint_transfer.rs::fetch_best_checkpoint`
  (logs and raises the floor as it goes), every byte-buffer encoder loop
  (`epoch.rs`, `xchain/abi.rs`, `xchain/ids.rs`, `upgrades.rs`,
  `state/genesis.rs`, `state/schema.rs`, `trie/node.rs`),
  `trie/mod.rs::update_storage_tries`'s per-slot write loop (writes to
  mdbx and returns early on error), `state/bin/kardamom-statecheck.rs`'s
  phase-`failed` accumulation (now inside `main`, `check_root`,
  `compare_dirs` — exits the process on hard errors and prints as it
  goes), and the test-code KEEPs listed in `crate-state.md`
  (`trie/incremental_tests.rs`'s mdbx dump and random-walk model harness,
  `writer/tests.rs`'s model-build-plus-send loop).

### R11 clippy pedantic (all sites in `inputs-state.md`, grouped by file)

Ran `cargo clippy --fix --allow-dirty --allow-no-vcs -p kardamom-types
--lib` and `--tests`, then the same for `-p kardamom-state --lib` and
`--all-targets`, to apply every machine-applicable suggestion
(`doc_markdown` backticks, `#[must_use]` additions, `map_or`/`map_or_else`
rewrites, `cast_lossless` → `From`/`u64::from`, `redundant_closure`,
boolean-to-int, etc.), then fixed the rest by hand, file by file:

- `crates/types/src/xchain/*.rs` (all `xchain.rs` pedantic rows, folded
  into the R3 split): `#[must_use]` on every free function; `map_unwrap_or`
  in `XChainMessage::leaf` → `map_or_else`; `missing_panics_doc`/
  `missing_errors_doc` on `derive_remote_epoch`.
- `crates/types/src/genesis.rs`: `map_unwrap_or` (auto-fixed),
  `missing_errors_doc` on `validate` (folded into the R10 rewrite).
- `crates/types/src/epoch.rs`: `missing_errors_doc` on `derive_epoch`
  (folded into the R10 rewrite); the rest auto-fixed.
- `crates/types/src/position.rs`: the two `cast_sign_loss` sites in
  `BPosition::as_index` → `.cast_unsigned()` (clippy's own suggested
  fix), with a one-line comment naming why the reinterpretation is safe
  (the two `i32` halves are bit patterns, not signed magnitudes); kept as
  `as u64` for the widening half, since `u64::from` is not yet usable
  inside a `const fn` on this toolchain, with a comment saying so.
- `crates/types/src/prover.rs`: `BlockRecordsDigest::add_tx`'s
  `cast_possible_truncation` → `#[allow]` with a one-line comment naming
  the bound (the digest format fixes a `u32` length prefix; no raw
  transaction can reach `u32::MAX` bytes).
- `crates/types/src/witness.rs`: the three `cast_possible_truncation`
  sites in `ExecutionWitness::digest` → one function-level `#[allow]`
  with the same style of comment (the format fixes three `u32` count
  prefixes; a block cannot hold anywhere near that many accounts, slots,
  or code entries).
- `crates/types/src/state.rs`: `# Errors` sections on all five
  `StateDatabase` trait methods, naming the real condition (a backend
  read failure) for each, not boilerplate.
- `crates/types/src/tx_ordering.rs`, `txref.rs`, `receipt.rs`,
  `boundary.rs`, `delta.rs`, `deposit.rs`, `withdrawals.rs`,
  `ack_policy.rs`, `upgrades.rs`: `doc_markdown` and `must_use_candidate`,
  all auto-fixed.
- `crates/state/src/checkpoint/manifest.rs`: `# Errors` on `parse`,
  `read_manifest`, `verify_checkpoint`; `must_use_candidate` auto-fixed.
- `crates/state/src/checkpoint/mod.rs`: `case_sensitive_file_extension_comparisons`
  on `is_mdbx_data_name` → `#[allow]` with a one-line reason (mdbx always
  emits a lowercase extension; a case-insensitive match would accept
  names mdbx itself never produces); `# Errors` on `create_checkpoint`,
  `latest_checkpoint`, `prune_checkpoints`, `restore_checkpoint`,
  `restore_best_checkpoint`, `park_state_db`, `has_state_db`.
- `crates/state/src/checkpoint_transfer.rs`: `# Errors` on
  `serve_checkpoints`; the rest folded into the R2 splits.
- `crates/state/src/compaction.rs`: `# Errors` on `compact_to`.
- `crates/state/src/env.rs`: `# Errors` on `open`;
  `must_use_candidate`/`return_self_not_must_use` auto-fixed.
- `crates/state/src/genesis.rs`: `# Errors` on `genesis_applied`,
  `seed_genesis`; `single_match_else` and `match_same_arms` folded into
  the R2/R10 rewrites.
- `crates/state/src/integrity/mod.rs`: `# Errors` on `sweep`;
  `must_use_candidate` auto-fixed.
- `crates/state/src/integrity/tests.rs`: `unreadable_literal` →
  `0x00BE_EF00` (clippy's own suggested grouping).
- `crates/state/src/meta.rs`: `# Errors` on all eight
  `read_meta_*`/`decode_*` functions, each naming the real
  `BadEncoding` condition.
- `crates/state/src/recovery/mod.rs`: `# Errors` on `read_recovery_point`,
  `has_trie`, `bootstrap_trie_from_state`; `# Errors` and `# Panics` on
  `read_all_headers` (the panic is provably unreachable — the length
  check right above the `try_into().expect(..)` makes it so — documented
  as such rather than removed, since removing it would mean threading a
  fallible path through a cursor callback for no behavior change).
- `crates/state/src/schema.rs`: `# Errors` on `decode_account_value`,
  `decode_storage_value`, `decode_receipt_value`, `decode_tx_hash_value`;
  `# Errors` and `# Panics` on `decode_header_value` (same
  provably-unreachable-panic reasoning as above); `# Panics` on
  `encode_receipt_value` (rkyv serialization of owned data has no failure
  path — documented, not restructured into a `Result`, since every
  caller already treats it as infallible); `must_use_candidate` on the
  encoders, auto-fixed.
- `crates/state/src/snapshot.rs`: `# Errors` on `open`, `state_root`;
  `used_underscore_binding` on the `_env` field (it is genuinely read in
  `fork_view`, not merely held for its `Drop`) → renamed to `env`.
- `crates/state/src/trie/mod.rs`: `# Errors` on `storage_root_incremental`,
  `state_root_incremental`, `apply_trie_updates`, `TrieTables::open`,
  `update_for_block`, `rebuild_root`; `match_same_arms` (two sites) →
  combined arms, same as the R10 write-code fix.
- `crates/state/src/trie/node.rs`: `cast_possible_truncation` on the
  `hashes_len` cast → `#[allow]` with a comment naming the bound (a
  branch node has at most 16 children, the nibble fan-out); `# Errors` on
  `decode_branch_node`; folded the `for` loop into the R10 rewrite above.
- `crates/state/src/trie/proofs.rs`: `# Errors` on `account_proof_nodes`,
  `storage_proof_nodes`.
- `crates/state/src/trie/walker.rs`: `unnecessary_wraps` on `finalize` →
  it never actually returned `Err`, so its return type dropped from
  `Result<(B256, TrieUpdates), StateError>` to `(B256, TrieUpdates)`
  (both call sites now wrap with `Ok(..)` themselves);
  `needless_pass_by_value` on `finalize`'s `updated` parameter → taken by
  `&HashMap` reference instead (it was only ever read, never consumed).
- `crates/state/src/trie/incremental_tests.rs`: the three
  `cast_possible_truncation` sites folded into one `pick<T: Copy>(rng,
  items)` helper (also a small R10-style de-duplication of the repeated
  "modulo-index a slice" shape), with a comment naming the bound (the
  modulo bounds the index below `items.len()`); `many_single_char_names`
  on `extension_collapse_regrow_no_stale_orphans` → `#[allow]` with a
  reason (the single-letter names match the geometry the doc comment
  above the test names; renaming them would make the two disagree);
  `map_unwrap_or` auto-fixed.
- `crates/state/src/writer/mod.rs`: `missing_panics_doc` on `shutdown`
  (it can propagate the writer thread's own panic — documented, since
  that is the correct behavior, not a bug to fix); `needless_pass_by_value`
  on `spawn`/`spawn_with_trie`'s `env: StateEnv` parameter →
  `#[allow]` with a reason (by-value matches the shape every one of the
  ~15 call sites across five other crate groups already uses; changing it
  is a Phase B signature change); `manual_let_else` on the delta-channel
  receive → converted to `let Ok(batch) = ... else { .. }`;
  `# Errors` on `spawn`, `spawn_with_trie`, `shutdown`.
- `crates/state/src/integrity/checks.rs`: folded into the R2 splits
  above (`check_header_row`/`check_receipt_row` extraction fixed
  `many_single_char_names` and `unnecessary_wraps` that the extraction
  itself introduced, by renaming `k`/`v` to `key`/`value` and dropping
  `check_header_row`'s unused `Result` wrapper).
- `crates/state/tests/common/mod.rs`, `crates/state/tests/docker_e2e.rs`,
  `crates/state/tests/compaction_smoke.rs`, `crates/state/benches/write_throughput.rs`:
  `doc_markdown`, `map_unwrap_or` auto-fixed; the two `cast_possible_truncation`
  sites (`(block * 1024) as i32`) → `#[allow]` with a one-line bound
  comment (test/bench block numbers never approach `2^32 / 1024`).
- `crates/state/src/bin/kardamom-statecheck.rs`: `map_unwrap_or`,
  `bool_to_int_with_if` auto-fixed.
- `crates/state/src/writer/tests.rs`: `map_unwrap_or` auto-fixed.
- `crates/state/src/checkpoint/tests.rs`: `redundant_closure` auto-fixed.

## Phase C

Coordinator review of the Phase A diff: accepted with fixes. This section covers every change
made after that review: the four review-fix items, R13, R12, R9 rows 5-7 (reversed from
"Deferred to Phase B" to in-scope), R15, R16, and R14, in that order, plus the two new rules
(R15, R16) applied retroactively to Phase A code where a violation existed.

### Review fixes (from the Phase A review)

- `crates/state/src/writer/mod.rs` `spawn`/`spawn_with_trie`: removed the
  `#[allow(clippy::needless_pass_by_value)]` on both. `spawn_inner` now takes `env: StateEnv` by
  value (was `&StateEnv`), and `StateWriter { env, .. }` moves it in directly — no `.clone()`.
  `ensure_schema_version`/`StateSnapshot::open` borrow from the owned `env`.
- `crates/state/src/checkpoint/mod.rs` `latest_checkpoint`: `.max_by_key(|c| c.block)` →
  `.min_by_key(|c| std::cmp::Reverse(c.block))`, so a block-number tie keeps the first entry
  `read_dir` yields, matching the original loop's strict `>` comparison (first-wins), not
  `max_by_key`'s last-wins.
- `crates/types/src/xchain/derive.rs` `derive_remote_epoch`: `ordered.last().expect("non-empty")`
  → `ordered.last().ok_or(XChainError::Empty)?`. Removed the `# Panics` doc section.
- `crates/state/src/checkpoint/mod.rs` `quarantine_failed_message`: doc comment reworded to drop
  refactor-history language ("Its text has a unit test.").

### R13 (NonZero types)

- `crates/state/src/writer/mod.rs` `spawn_with_trie`: added a boundary check —
  `TrieMode::ShadowCheck { every_n: 0 }` is refused with a `StateError::Recovery` naming the
  problem, instead of silently disabling the shadow-check canary. `TrieMode` keeps a plain `u64`
  field (not `NonZeroU64`) because the validator's CLI constructs it from an `Option<u64>`
  argument outside this crate — **Phase B**: parse that argument into `NonZeroU64` and change the
  field's type to match.
- `crates/state/src/writer/apply.rs` `advance_state_root`: removed the now-redundant
  `&& every_n != 0` runtime guard; a comment explains `every_n` is never 0 here because
  `spawn_with_trie` refuses to construct the writer otherwise.
- `crates/types/src/withdrawals.rs` `leaf_level`: removed the dead `.max(1)` after
  `next_power_of_two()` (`0usize.next_power_of_two()` is already 1).
- `crates/types/src/limits.rs`: added `ShardCount(NonZeroU32)` with `new`/`get`. **Phase B**: no
  consumer wiring — the ingress/executor/engine/bench call sites that would parse into this type
  are outside `crates/types`.

### R12 (safe arithmetic)

FIX rows:
- `crates/types/src/xchain/derive.rs` `RemoteEpochRecord::last_seq` — already fixed in Phase A
  (Defect 1, `saturating_add`/`saturating_sub`).
- `crates/types/src/withdrawals.rs` `withdrawal_proof` — added an `assert!(index < leaves.len())`
  bound with a `# Panics` doc explaining the wraparound it prevents (`idx + 1` on
  `index = usize::MAX`). Confirmed via `grep` this function has zero production callers (only
  test/e2e-test sites), so a panic-based fix, not a signature change, is the fully in-scope
  option. New test: `withdrawal_proof_rejects_an_out_of_range_index`.
- `crates/state/src/checkpoint_transfer.rs` `fetch_best_checkpoint` — `b.block + 1` →
  `b.block.saturating_add(1)`.
- `crates/state/src/integrity/checks.rs` `check_header_row`'s gap check — replaced
  `block != p + 1` with a `p.checked_add(1)` match, reporting a distinct "no successor block
  number (u64 overflow)" problem instead of wrapping.
- `crates/types/src/prover.rs` `BlockRecordsDigest::add_tx` — `raw_tx.len() as u32` →
  `u32::try_from(raw_tx.len()).unwrap_or_else(|_| panic!(...))`, with a `# Panics` doc. The
  digest's `u32` length prefix is a wire-format constant this crate owns internally; changing
  `add_tx`'s signature to `Result` would ripple through every batcher/exec-core caller, so a
  panic-on-impossible (no transaction reaches 4 GiB) keeps the signature and turns silent
  wraparound into a loud failure.

Also fixed, the "opposite mistake" (an unchecked cast, not unsafe arithmetic) the coordinator
named:
- `crates/state/tests/common/mod.rs` `bpos`, `crates/state/benches/write_throughput.rs`
  `big_batch` — `(block * 1024) as i32` → `i32::try_from(block * 1024).unwrap()`; the
  `#[allow(clippy::cast_possible_truncation)]` on both is gone.
- `crates/state/src/trie/incremental_tests.rs` `pick` — `splitmix(rng) % items.len() as u64) as
  usize` → `usize::try_from(...).unwrap()`, with a comment explaining the modulo bound makes the
  round trip infallible in practice, so the checked form documents that as a fact instead of an
  assumption.

### R9 rows 5-7 (parse-once newtypes, no defensive re-checks)

- **Row 5**: `crates/state/src/meta.rs` gained `fixed<'b, const N: usize>(table, bytes: &'b [u8])
  -> Result<&'b [u8; N], StateError>`. `decode_u64`/`decode_u32`/`decode_b_position`/`decode_b256`
  (meta.rs), `decode_storage_value`/`decode_header_value` (schema.rs), `decode_account_leaf`
  (trie/cursor.rs), and `read_all_headers` (recovery/mod.rs) all route their length check through
  it. `decode_header_value`'s two fixed-width branches (24-byte and pre-origin 20-byte) use array
  patterns (`let &[t0, ..., ] = fixed::<24>(...)?;`) instead of a manual length check plus
  `try_into().expect(...)`.
- **Row 6**: `crates/state/src/checkpoint_transfer.rs`'s `ResponseHead` (four independent
  `Option` fields, each checked separately after `read_response_head` returns) is replaced by
  `PeerResponse` (`NotFound` | `Image(CheckpointHead)`), parsed once by
  `PeerResponse::parse`/`parse_headers` via a new `ParsedHeaders` accumulator and a
  `CheckpointFetch<'a>` struct (peer, reader, expected genesis) that owns the connect/response/
  download/verify sequence as methods. `fetch_latest_checkpoint` now matches
  `PeerResponse::NotFound | PeerResponse::Image(head)` once instead of four `if let`/`else`
  presence checks.
- **Row 7**: `crates/state/src/checkpoint/mod.rs` gained `CheckpointEntry` (`Checkpoint(u64)` |
  `Tmp` | `MdbxData` | `Other`), replacing the separate `parse_checkpoint_block` and
  `is_mdbx_data_name` functions. `CheckpointEntry::parse` is the one place a directory-entry name
  is classified; `sweep_stale_tmp`, `latest_checkpoint`, `prune_checkpoints`, `find_mdbx_data`, and
  `has_state_db` all match on the enum instead of calling two different string-testing functions.

### R15 (methods, not standalone functions)

- `crates/state/src/writer/apply.rs`: `write_storage`/`write_accounts`/`write_code`/
  `write_header`/`write_receipts_and_index`/`write_meta_cursors`/`advance_state_root` became
  methods on a new `BatchWriter<'a>` (the transaction plus the seven table handles, plus
  `ApplyTimings` as mutable state). `apply` builds one with `BatchWriter::open(&txn,
  self.trie_mode)?` then chains `.storage(delta)?.accounts(delta)?.code(delta)?.header(boundary)?
  .receipts_and_index(delta)?.meta_cursors(boundary)?.advance_state_root(boundary, delta)?`,
  `.finish()` to get the timing breakdown, then commits the transaction and reports timing.
  `report_timing` became `ApplyTimings::report`.
- `crates/state/src/integrity/checks.rs`: `check_meta`/`check_headers`/`check_header_row`/
  `check_header_chain_ends`/`check_receipts_index`/`check_receipt_row`/`check_index_rows`/
  `check_accounts`/`check_storage`/`check_trie` became methods on a new `Sweep<'a>` (the
  transaction and the `IntegrityReport` being built). `integrity::sweep` builds one and calls
  `s.meta()?; s.headers(meta_end_tx)?; ...; s.finish()`.
- `crates/state/src/integrity/compare.rs`: `compare_table`/`value_diff_message`/
  `compare_meta_keys` became methods (the last an associated function not using `self`) on a new
  `TableCompare<'a>` (the two transactions, the table name, and the accumulated diffs).
- `crates/state/src/trie/mod.rs`: `group_storage_by_account`/`update_storage_tries`/
  `write_hashed_accounts`/`update_account_trie`/`apply_trie_updates`/`rebuild_root` became methods
  on `TrieTables` (already the table-handle bundle). `apply_trie_updates` → `apply_updates`, and
  its redundant `db: Database` parameter is gone: the table is now derived from whether
  `account_hash` is `Some` (storage trie) or `None` (account trie), the same information the
  caller was already passing twice.
- `crates/state/src/trie/walker.rs`: `walk_account`/`walk_storage` became `AccountWalk`/
  `StorageCtx::walk` (unified further under R14 — see below).
- `crates/state/src/genesis.rs`: `check_or_backfill_digest`/`write_allocations`/`seed_trie` became
  methods on a new `GenesisSeed<'a>` (the transaction and the three opened tables).
  `check_or_backfill_digest` no longer commits the transaction internally — `seed_genesis` commits
  once, in the same place, after either branch.
- `crates/state/src/bin/kardamom-statecheck.rs`: `check_root`/`compare_dirs` became methods on
  `Args`.
- `crates/state/src/recovery/mod.rs`: `read_all_accounts`/`read_all_storage`/`read_all_code`
  became methods on a new `TrieBootstrap<'a>` (the transaction and the three table handles).
- `crates/types/src/xchain/abi.rs`: the `push_word_u64`/`push_word_u128`/`push_word_address`
  free functions became an `AbiWords(Vec<u8>)` builder with `u64`/`u128`/`address`/`bytes` methods
  returning `Self`; `deliver_calldata` is now one chained expression.

Left as public free functions, per the exception the coordinator listed: `derive_epoch`,
`derive_remote_epoch` (`epoch.rs`/`xchain/derive.rs`), `msg_leaf` (`xchain/leaf.rs`), and
`source_hash`/`source_hash_system` (`epoch.rs`) — moving these onto a type changes call sites in
`crates/validator`, `crates/e2e`, `crates/da_watcher`, and others outside this group. **Phase B**.

### R16 (no nested loops)

Found and fixed, beyond the coordinator's list:
- `crates/state/src/trie/walker.rs`: both `AccountWalk::walk` and `StorageCtx::walk` had a
  `for i in 0..16u8 { ... for (k, v) in collect_..._under(...)? { ... } ... }` nested loop (the
  16-nibble scan's leaf-or-empty-child branch, and its exact-miss/full-rebuild branch, shared the
  same inner loop body verbatim). Extracted to `emit_under`/`emit_leaves_under` methods (also an
  R14 dedup — the two call sites in each walk had identical bodies).
- `crates/types/src/withdrawals.rs` test `proofs_recompute_root_all_sizes`:
  `for n in 1..=9 { ... for (i, &l) in leaves.iter().enumerate() { ... } }` — extracted the inner
  loop to `assert_proofs_recompute_root`.
- `crates/types/src/xchain/abi_tests.rs` test `hand_encoding_is_byte_identical_to_sol_types`:
  `for data in [...] { for callback in [None, Some(cb)] { ... } }` — extracted to
  `assert_hand_encoding_matches_sol_types`.

Coordinator-listed targets:
- `crates/state/src/trie/mod.rs` `update_storage_tries` (per account, then per slot): the
  per-slot body extracted to `TrieTables::write_account_slots`.
- `rebuild_root`'s closures: `storage_root_for` (a closure containing its own `while let` cursor
  loop, called from inside `for_each_row`'s closure) extracted to a `storage_root_for` method,
  so the per-account loop calls a method, not an inline closure holding a second loop.
- `write_receipts_and_index` (now `BatchWriter::receipts_and_index`): this was two *sequential*
  loops (not nested), but merged into one pass anyway — the receipt-cursor loop now also collects
  the hash-index entries `hk`, replacing a separate `.iter().map(...).collect()` walk over the
  same `delta.receipts`.
- `Genesis::to_alloc`: already a single loop; no nested-loop violation found, no change made.
- Random-walk loops, `crates/state/src/trie/incremental_tests.rs`
  `incremental_equals_full_rebuild_over_random_blocks`: the `for block in 0..80u64 { ... }` outer
  loop directly contained the op-generation loops (`for _ in 0..n_acct`, `for _ in 0..n_stor`) and
  the model-update loops. Extracted the whole per-block body to `run_random_block`, so the outer
  loop's body is one function call plus a periodic assertion, not five loops.
- `crates/state/src/writer/tests.rs`: `trie_writer_root_matches_model_and_persists`'s
  `for delta in &blocks { for s in &delta.storage {...} for a in &delta.accounts {...} ... }` —
  extracted the two inner model-update loops to `record_delta_in_model`.

Considered and left: `crates/state/tests/concurrent_readers.rs`'s
`for (i, snap) in snapshots... { thread::spawn(move || { while !stop... { ... } }); }` — the
`while` runs in a spawned background thread, not as nested iteration within the outer `for`;
it's concurrency, not the loop-in-loop shape R16 targets.

### R14 (DRY)

Production code, in the order in `dry-state.md`:

- **`wire.rs` macro** (130 lines): the five `ArchiveWith`/`SerializeWith`/`DeserializeWith` trios
  (`AddressBytes`, `B256Bytes`, `U256Bytes`, `BytesVec`, `VecB256`) are now one
  `wire_adapter!($name, $ty, $pod, $doc, |f| $to, |v| $from)` macro plus five invocations.
- **`trie/walker.rs` `LeafSource` trait** (85 lines): `AccountWalk` and `StorageCtx` both
  implement a new `LeafSource` trait (`trie_db`, `namespace`, `emit_under`), whose default `walk`
  method is the shared match, 16-nibble loop, and `tree_mask`/`hash_mask` skip rule, written once.
  A new `root_with<K, L: LeafSource>` replaces the duplicated `HashBuilder`/`WalkLog`/`finalize`
  setup in `account_root`/`storage_root`, and the two `*_for_proofs` functions collapsed the same
  way.
- **`tx_ordering.rs` macro**: only 3 `is_*`/`as_*` pairs remain after Phase A's deletion of the
  `Epoch`/`RemoteEpoch` accessors (the audit's estimate assumed 5 pairs). Still converted to a
  `variant_accessor!` macro; each invocation lists the variant's siblings explicitly (not a
  wildcard `_` arm), so a new `TxOrderingMessage` variant fails to compile at every invocation
  until updated, preserving the "keep the variant list explicit" requirement.
- **`abi.rs` new module** (`crates/types/src/abi.rs`, private): `word_u64`/`word_u128`/
  `word_u256`/`word_address` (each `[u8; 32]`) plus `push_word_u64`/`push_word_u128`/
  `push_word_address` (append to a `Vec<u8>`). Wired into: `xchain/abi.rs`'s `AbiWords` (its
  `u64`/`u128`/`address` methods now call the `push_word_*` primitives); `withdrawals.rs`
  `withdrawal_leaf`; `epoch.rs` `alias_l1_address`; `xchain/leaf.rs` `msg_leaf`; `xchain/message.rs`
  `Callback::commitment`. (`word_b256` was written but never needed by a call site — removed
  rather than left as dead code.)
- **`meta.rs` `get_decoded`**: `read_meta_u64`/`read_meta_u32`/`read_meta_b_position`/
  `read_meta_b256` are now one-line wrappers over `get_decoded(txn, db, key, decode_fn)`.
  `snapshot.rs`'s `basic`/`storage`/`get_receipt`/`get_tx_position` (4 of 5) route through it too.
  `code_by_hash` is **not** wired — `get_decoded`'s `decode: fn(&[u8]) -> Result<T, E>` signature
  can only borrow the fetched bytes, but `code_by_hash`'s original body moves the owned
  `Vec<u8>` into `Bytes::from(v)` with no copy; forcing it through `get_decoded` would copy every
  contract's bytecode on every read.
- **`meta.rs`/`schema.rs` `fixed<N>`**: already done — see R9 row 5 above.
- **`integrity/checks.rs` `decoded_meta`**: added as a `Sweep` method (read a key, decode it,
  record a problem naming it on failure). Wired into `meta()`'s
  `last_committed_end_tx_position` read and `trie()`'s `state_root` read. Not wired into the
  `schema_version` or `last_committed_block` reads — both have extra branch-specific logic (a
  version-mismatch check; an inline `unwrap_or_else` with a different message shape) that doesn't
  fit the shared shape.
- **`prover.rs` `Words160`**: a private `Words160([u8; 160])` with `put_b256`/`put_u64`/`b256`/
  `u64_checked` (the last returns `None` if the word's top 24 bytes are non-zero, the same check
  `decode` used to do inline via `U256` comparison). `PublicOutputs::encode`/`decode` and
  `BatchPublicOutputs::encode`/`decode` are rewritten over it — verified byte-identical via the
  existing round-trip behavior (no dedicated prover test exists in this crate; the encode/decode
  logic is exercised indirectly by `crates/batcher`/`crates/validator`, outside this group, so the
  rewrite was double-checked by hand against the original byte offsets).
- **`schema.rs` `for_each_prefix`**: added next to `for_each_row`. Wired into `trie/mod.rs`
  `del_prefix` and the new `TrieTables::storage_root_for` (both are byte-level prefix scans on
  `node_key`-encoded or full-account-hash keys). **Not** wired into `trie/cursor.rs`
  `collect_hashed_accounts_under`/`collect_hashed_storage_under` — judged wrong: those two scan
  `hashed_accounts`/`hashed_storage`, whose keys pack two nibbles per byte, and their existing
  `key_starts_with` check is a *nibble*-level prefix test. `for_each_prefix`'s `k.starts_with(...)`
  is a *byte*-level test; for an odd-length nibble prefix the two are not equivalent (a 1-nibble
  prefix `0xA` byte-packs as `0xA0`, and `starts_with([0xA0])` would wrongly exclude a key whose
  first byte is `0xA5`, which does share the 1-nibble prefix). Routing these two through
  `for_each_prefix` would be a silent correctness bug in the trie walker, not a refactor.
- **`checkpoint/mod.rs` `entry_names`**: added (`Result<Vec<(String, PathBuf)>, StateError>`, a
  missing directory gives an empty list). Wired into `sweep_stale_tmp`, `latest_checkpoint`,
  `prune_checkpoints`, `find_mdbx_data`, `has_state_db` — all five now map through
  `CheckpointEntry::parse`, so the R9-row-7 enum and this row are complementary, not competing.
- **`schema.rs` `del_if_present`/`put_if_absent`**: added. Wired into `trie/mod.rs`'s three
  `match txn.del(...) { Ok | NotFound => {}, Err => return Err }` sites (`apply_updates`'s removal
  loop, `write_account_slots`'s zero-value delete, `write_hashed_accounts`'s empty-account
  delete) and `del_prefix`'s own delete loop; `put_if_absent` backs the new `schema::write_code`
  (see below).
- **`schema.rs` `write_accounts`/`write_code`**: added, with the same `AccountValue`/
  `storage_root: B256::ZERO` convention and write flags both prior copies used, and a cursor for
  `write_accounts` (load-bearing for the writer's per-block hot path — sorted `BTreeMap` iteration
  order matches cursor-upsert locality). `writer/apply.rs`'s `BatchWriter::accounts`/`code` and
  `genesis.rs`'s `GenesisSeed::write_allocations` both call these instead of duplicating the loop.
- **`trie/mod.rs` `commit_trie_root`**: added (`open TrieTables, update_for_block, put
  KEY_STATE_ROOT`). Wired into `genesis.rs`'s `GenesisSeed::seed_trie` and `recovery/mod.rs`'s
  `bootstrap_trie_from_state` — **2 of 3 sites**. The third, `writer/apply.rs`'s
  `advance_state_root`, keeps its own version: the shadow-check oracle rebuild must run between
  `update_for_block` and the meta put, which `commit_trie_root`'s fixed three-step body has no
  room for.
- **`types/rlp.rs` (new file)**: `encode_list_two` (was duplicated byte-for-byte in `epoch.rs` and
  `xchain/ids.rs`, formerly `xchain.rs`) is now defined once, `pub(crate)`, and both
  `source_hash_in_domain` (epoch.rs) and `remote_source_hash` (xchain/ids.rs) call it.
- **`integrity/mod.rs` `head`**: added (`&b[..b.len().min(8)]`). Wired into every listed site in
  `compare.rs` (`run`'s three diff messages, `value_diff_message`, `meta_keys`) and `checks.rs`
  (the `tx_hash_index`-entry-missing-receipt message, the missing-code message, the two
  undecodable-value messages) — the `checks.rs` sites previously used `&k[..4]`, now standardized
  on the same up-to-8-byte convention as `compare.rs`.
- **`checkpoint/manifest.rs` `load_manifest`**: added (build the manifest path, read it, parse
  it; the caller supplies the read-failure message via a closure, since `read_manifest`'s and
  `verify_checkpoint`'s wording differ). Both now call it.
- **`kardamom-statecheck.rs` `sweep_or_exit`**: added (run `sweep`, print+report, exit on error).
  `main`'s primary-directory sweep and `Args::compare_dirs`'s `--compare` sweep both call it.
- **`trie/mod.rs` `account_leaf_rlp`**: already done in Phase A (R6, `dyn` removal); confirmed no
  further action needed.
- **`withdrawals.rs` `tagged_hash2`**: added (`keccak256(tag ++ a ++ b)`, the shared 65-byte
  one-tag-two-words shape). `hash_pair` and `output_root` (formerly two independent `[u8; 65]`
  buffers under different domain tags) both call it.
- **`checkpoint_transfer.rs` `mod framing`**: added (`HDR_BLOCK`/`HDR_KECCAK`/`HDR_GENESIS`
  constants). `prepare_response`'s header-writing `format!` and `PeerResponse::parse_headers`'s
  match arms both reference the same constants, closing the "a rename on one side silently drops
  a header" gap the row describes — this reconciles cleanly with the R9-row-6 `PeerResponse`
  rewrite, since `parse_headers` already existed as a method by the time this row was applied.
- **KEEP, no action** (per the audit's own note, confirmed unchanged): `withdrawals.rs`'s
  `withdrawal_proof`/`withdrawals_root` fold shape (the sibling-read differs per level);
  `checkpoint/manifest.rs` vs. `checkpoint_transfer.rs`'s field-splitting (different separator and
  case rule).
- **Not done, judged wrong**: `crates/types/tests/rkyv_roundtrip.rs`'s three `Receipt` literals
  (`receipt_roundtrip`, `receipt_roundtrip_contract_creation`, `receipt_roundtrip_invalid_skip`)
  — each deliberately varies nearly every field to pin one distinct scenario (a normal receipt
  with logs; a CREATE transaction's `to: None`/`contract_address: Some` inversion; an invalid-skip
  `Receipt` with `skip_reason: Some(...)`). A single `receipt(idx) -> Receipt` with
  `..Default::default()` would hide which fields each test is actually asserting behind a
  default, for about 30 lines — the state-crate sites the same row mentions already use
  `..Default::default()` and needed no change, per the audit's own note.

Tests:

- **New `crates/state/src/testing.rs`**, `#[cfg(test)] pub(crate) mod testing;` in `lib.rs`:
  `temp_env`, `parts`, `model_state_root`, `put_plain_accounts`. This is **not** gated behind a
  `test-util` feature and self-dependency (the audit's suggested `#[cfg(any(test, feature =
  "test-util"))]` shape) — grepping the workspace found no existing self-dev-dependency pattern
  to follow, and adding one would touch the root `Cargo.lock`, which is outside
  `crates/state`/`crates/types`. So this module reaches only `#[cfg(test)]` unit tests compiled
  as part of `crates/state`'s own lib (`genesis.rs`, `trie/incremental_tests.rs`,
  `recovery/tests.rs`, `writer/tests.rs`) — not integration tests or benches, which are separate
  crates. Two homes, not one, as a result:
  - `temp_env`: replaced identical local copies in `genesis.rs` and `trie/incremental_tests.rs`.
  - `parts(nonce, balance) -> AccountTrieParts` (code_hash and storage_root both `B256::ZERO`):
    wired into all 6 `recovery/tests.rs` sites, including the one with a non-zero `storage_root`
    override, via struct-update syntax (`AccountTrieParts { storage_root, ..testing::parts(1,
    55) }`). `trie/mod.rs`'s own private test `parts(nonce, balance, storage_root)` (which uses
    `KECCAK_EMPTY`, not `B256::ZERO`, because its tests exercise the pure `state_root`/
    `storage_root` oracle functions directly, bypassing the ZERO-sentinel normalization) is a
    different, deliberately un-unified shape — left as is.
  - `model_state_root`: replaced `writer/tests.rs`'s `model_root` and
    `trie/incremental_tests.rs`'s `oracle_root` (the latter adapts its `Basic`-keyed map to the
    `(u64, U256, B256)` tuple shape first, then delegates).
  - `put_plain_accounts(env, rows: &[(Address, u64, u64)])`: wired into 2 of 3
    `recovery/tests.rs` sites. The third interleaves a storage-table write in the same
    transaction before commit; forcing it through the self-committing helper would split one
    atomic test-fixture transaction into two.
  - `trie/incremental_tests.rs` `dump_table`: rewritten over `schema::for_each_row` (no new
    helper, per the audit's own note).
  - `trie/incremental_tests.rs`'s random-walk per-block body: extracted to `run_random_block`
    (counted under R16 above, since the motivating problem was the nested loop, not duplication).
- **`crates/state/tests/common/mod.rs`** (the integration-test/bench home — see above): gained
  `temp_env`/`open_env` (used by `compaction_smoke.rs`, `env_smoke.rs`, `recovery_midblock.rs`,
  and `docker_e2e.rs`'s writer-setup site) and `commit_block`/`commit_range` (used by
  `concurrent_readers.rs`, `fork_view.rs`, `snapshot_mvcc.rs`, `snapshot_swap.rs`).
  `write_replay.rs` and `recovery_midblock.rs`'s block-commit loops, and `docker_e2e.rs`'s
  `.expect(...)`-annotated commit, are **not** wired to `commit_block`/`commit_range`: both send
  every delta first and drain all the resulting snapshots afterward (a burst/pipelining pattern),
  which is a different shape from `commit_block`'s strict send-then-immediately-recv lockstep —
  forcing it would silently narrow what those tests exercise. `benches/snapshot_open.rs` and
  `benches/write_throughput.rs` pull `tests/common/mod.rs` in via `#[path = "../tests/common/
  mod.rs"]`; `write_throughput.rs`'s `big_batch` also now calls `common::bpos` instead of
  duplicating its `term_offset` computation.
- **`crates/state/src/checkpoint/tests.rs` `seeded_env_with_blocks`**: added (open, seed, commit
  `1..=upto`). Wired into all 8 listed sites, including the two that commit a second, larger
  range afterward (`commit_blocks(&env, addr, 5)` right after `seeded_env_with_blocks(..., 2)`,
  for example) — the helper only replaces the shared open+seed+first-commit prefix.
- **`crates/types/tests/rkyv_roundtrip.rs` `pos(term_id, term_offset) -> BPosition`**: added.
  All 17 `BPosition { term_id: N, term_offset: M }` literals in the file converted.
- **`receipt()` helper**: not added — see the production-code "Not done, judged wrong" row above
  (same three `Receipt` literals).

## Deferred to Phase B

- R4/R5, `crates/state/src/writer/mod.rs:95` `crossbeam_channel::bounded(0)`
  in `WriterHandle::shutdown`, and the `drop(std::mem::replace(...))` at
  line 96 it exists to support — the audit's fix
  (`delta_tx: Option<Sender<WriteBatch>>`, `.take()`) requires changing
  the type of `WriterHandle::delta_tx`, a **public field** read or cloned
  directly (`handle.delta_tx.clone()`, `.send(..)`) by
  `crates/engine/src/persist.rs`, `crates/executor/src/bin/kardamom-executor/main.rs`,
  `crates/validator/src/bin/kardamom-validator/main.rs`, and
  `crates/bench/src/bin/stm-p2.rs` — four other crate groups. (A
  same-named `delta_tx` field in `crates/stm/src/execute.rs` is an
  unrelated `std::sync::mpsc::Sender<DeltaRelease>`, not this one.) Needs
  a signature change coordinated with those groups.
- R11, `crates/types/src/xchain/abi.rs` (formerly `xchain.rs:268`)
  `msg_leaf`'s 9 arguments — an argument-group struct would change a
  signature `crates/e2e/src/scenarios/xchain.rs` and
  `crates/validator/src/interop/extract.rs` call directly. Needs
  coordination with the e2e and validator groups.
- R9, `crates/state/src/checkpoint_transfer.rs:223-225` `peer.parse()`
  re-parsed on every `fetch_latest_checkpoint` retry — the audit's fix
  (parse into `Vec<SocketAddr>`, or a `PeerAddr` newtype, at config load)
  means the peer list's owner, `fetch_best_checkpoint`'s caller, needs to
  hand this crate typed addresses instead of `&[String]`. That caller is
  outside this group.
- R9, `crates/types/src/epoch.rs:146` / `crates/da_watcher/src/rpc_source.rs:201,226`
  a `Mint(u128)`/`U128Amount` newtype for `DepositLog.mint` — the
  narrowing check today lives in `da_watcher`, a different crate group;
  the newtype would move validation to `kardamom-types`, which is exactly
  the kind of cross-crate newtype the task brief asks to list rather than
  build.
- R9, `crates/types/src/xchain` (`RemoteEpochRecord`/`OutboxMessage`) and
  `crates/types/src/genesis.rs` (`Genesis::chain_id`) — a
  `ChainId(NonZeroU64)` newtype for `dest_chain_id`/`origin_chain_id`/
  `self_chain_id`/`Genesis::chain_id`. Grepping `dest_chain_id`,
  `origin_chain_id`, and `self_chain_id` across the workspace (outside
  `crates/types/src`) finds real, non-comment usage in over 25 files
  across `da_watcher`, `engine`, `validator`, `batcher`, `sequencer`,
  `cluster-adapter`, `exec-core`, `interop-feed`, `e2e`, and
  `reconstruct` — construction and field reads, not just references.
- R9, `crates/types/src/receipt.rs` (`TX_TYPE_LEGACY`/`DEPOSIT`/`XCHAIN`
  as raw `u8`s) — a `TxType` enum for the receipt/envelope type byte.
  Grepping the three constants (outside `crates/types/src`) finds real
  usage in `exec-core` (`executor/scope.rs`, `executor/deposit.rs`,
  `executor/xchain.rs`), `reconstruct`, `ingress/src/json_rpc.rs`,
  `e2e/src/scenarios/xchain_da_parity.rs`, and test code in `engine`,
  `sequencer`, and `validator`. Both newtypes are listed per the brief
  ("new newtypes in `types` that other crates would parse into") rather
  than built, given the blast radius.
- R9, `crates/types/src/prover.rs:91-98,185-194`
  `PublicOutputs`/`BatchPublicOutputs::decode`'s length-and-range checks
  — narrowing `&[u8]` to `&[u8; 160]` moves the length check to each
  caller's `try_into()`; `crates/validator` and the `guest/` crates call
  `decode` directly, so this also crosses the group boundary.

(R9 rows 5-7 — `fixed<N>`, the `ResponseHead`→`PeerResponse` rework, and the
`CheckpointEntry` enum — moved to **Done** in Phase C below; the coordinator
reversed these three from this list after reviewing the Phase A diff.)

**Phase C additions:**

- R13, `crates/types/src/limits.rs` `ShardCount(NonZeroU32)` — added the
  newtype (`new`/`get`), but did not wire it into a consumer. The
  ingress/executor/engine/bench call sites that would parse a shard-count
  config value into it are outside `crates/types`.
- R15, `crates/types/src/epoch.rs` `derive_epoch`/`source_hash`/
  `source_hash_system`, `crates/types/src/xchain/derive.rs`
  `derive_remote_epoch`, `crates/types/src/xchain/leaf.rs` `msg_leaf` — left
  as public free functions, per the coordinator's explicit exception: moving
  them onto a type changes call sites in `crates/validator`, `crates/e2e`,
  and `crates/da_watcher`.

## Not done, judged wrong

- R2, `crates/types/src/xchain/derive.rs` (formerly `xchain.rs:414`)
  `derive_remote_epoch` (56 lines) — the appendix names four helpers
  (`sorted_unique_by_seq`, `check_dense_from`, `check_destination`,
  `to_wire_messages`), but the function is a single linear sequence of
  guard clauses (empty → sort → dup → start → gap → destination →
  construct), each already a one-line check with its own error variant.
  Splitting it would scatter one coherent validate-then-construct rule
  across five functions for no readability gain; this matches the
  exception the rule itself states ("a function with one linear body and
  no repeated shape can stay").
- R10, `crates/types/src/genesis.rs:46-68` `Genesis::to_alloc`'s
  account/code build — the appendix's suggested rewrite
  (pre-compute `(entry, code_hash)` pairs, then two separate
  `.map()`/`.filter_map()` passes) computes `code_hash_of(entry)` once
  per entry and caches it in an intermediate `Vec`, which is what the
  *current* single loop already does for free by computing the hash once
  and using it for both the account push and the code push in the same
  iteration. The suggested form is not simpler and would not avoid
  recomputation without the same intermediate collection the current
  code doesn't need.
- R8, `crates/types/src/prover.rs:155` `BatchProverInput` — the appendix
  called it dead ("no hit outside `crates/types/`"), but
  `guest/kardamom-zk-host/src/main.rs` and
  `guest/kardamom-zk-guest/src/bin/batch.rs` both construct and
  `rkyv::from_bytes` it directly. The `guest/` crates are outside the
  main workspace scan the appendix's tooling ran over, which is why the
  grep it used missed them. Confirmed live; left `pub`, no change.

**Phase C additions:**

- R14, `crates/state/src/trie/cursor.rs` `collect_hashed_accounts_under`/
  `collect_hashed_storage_under` — `dry-state.md` groups these with
  `trie/mod.rs`'s byte-level prefix scans under one `for_each_prefix`
  helper, but these two scan `hashed_accounts`/`hashed_storage`, whose
  keys pack two nibbles per byte, and their existing `key_starts_with`
  check is a *nibble*-level prefix test. A byte-level `starts_with` is not
  equivalent for an odd-length nibble prefix (a 1-nibble prefix `0xA`
  byte-packs to `0xA0`, and `starts_with([0xA0])` would wrongly exclude a
  key whose first byte is `0xA5`, which does share that 1-nibble prefix).
  Routing these through `for_each_prefix` would be a silent trie-walker
  correctness bug, not a refactor. `for_each_prefix` was still added and
  used for the two byte-level-prefix call sites the row also names
  (`trie/mod.rs`'s `del_prefix` and the new `storage_root_for`).
- R14, `crates/types/tests/rkyv_roundtrip.rs`'s three `Receipt` literals
  (`receipt_roundtrip`, `receipt_roundtrip_contract_creation`,
  `receipt_roundtrip_invalid_skip`) — each deliberately varies nearly
  every field to pin one distinct scenario (a normal receipt with logs; a
  CREATE transaction's `to`/`contract_address` inversion; an
  invalid-skip receipt). A single `receipt(idx) -> Receipt` built on
  `..Default::default()` would hide which fields each test actually
  asserts behind a default, for about 30 lines saved.

## Gates

Run with `CARGO_TARGET_DIR=/home/dev/kardamom-8/target`, from this
workspace's root (`/home/dev/kardamom-8-impl/state`):

1. `cargo clippy -p kardamom-state --all-targets -- -D warnings` — clean.
   `cargo clippy -p kardamom-types --all-targets -- -D warnings` — clean.
2. `cargo clippy -p kardamom-state --all-targets -- -W clippy::pedantic` —
   zero warnings in `crates/state/**`. `cargo clippy -p kardamom-types
   --all-targets -- -W clippy::pedantic` — zero warnings in
   `crates/types/**`. (Both commands also compile `kardamom-log`, a
   dependency owned by another group; its pedantic warnings are not
   this group's and are left alone, per the brief.)
3. `cargo test -p kardamom-state --lib --bins` and `cargo test -p
   kardamom-types --lib --bins` — all pass (64 tests in `kardamom-state`,
   54 + 17 in `kardamom-types`). No test in this group spawns a workspace
   binary, Docker, or testcontainers, so none was excluded. `e2e` and
   `guest/` are not in this group.
4. `cargo fmt -p kardamom-state -p kardamom-types` — applied; a follow-up
   `-- --check` shows no diff.

Also ran, beyond the required gates, to validate the R8 visibility
narrowing: `cargo check -p kardamom-engine -p kardamom-executor -p
kardamom-validator -p kardamom-bench -p kardamom-sequencer -p e2e -p
kardamom-batcher`. All succeed except two pre-existing failures in
`kardamom-deployer`/`kardamom-batcher` from missing `contracts/out/*.json`
build artifacts (unrelated to this change — the same failure would occur
on an unmodified checkout without a contracts build).

## Phase C gates

Rerun after all Phase C changes above, same commands, same target dir:

1. `cargo clippy -p kardamom-state -p kardamom-types --all-targets --
   -D warnings` — clean.
2. `cargo clippy -p kardamom-state -p kardamom-types --all-targets --
   -W clippy::pedantic` — clean in `crates/state/**` and `crates/types/**`.
   Two new pedantic findings surfaced during this pass and were fixed:
   `trie/mod.rs`'s `write_account_slots` had a `changed`/`changes`
   `similar_names` warning (renamed to `changed_hashes`), and
   `checkpoint/mod.rs`'s new `CheckpointEntry::parse`'s `.tmp` suffix
   check triggered `case_sensitive_file_extension_comparisons` (added an
   `#[allow]` with the same justification as the adjacent `.dat` check:
   this crate is the only writer of that suffix, always lowercase).
3. `cargo test -p kardamom-state --lib --bins` — 64 passed, 0 failed (plus
   0 in the `kardamom-statecheck` bin). `cargo test -p kardamom-types
   --lib --bins` — 55 passed, 0 failed. `cargo test -p kardamom-state
   --tests` (every integration test file, including the ones touched by
   the R14 test-helper consolidation) — 64 lib + 11 across
   `compaction_smoke`/`concurrent_readers`/`env_smoke`/`fork_view`/
   `recovery_midblock`/`snapshot_mvcc`/`snapshot_swap`/`write_replay`, all
   passed, 0 failed (`docker_e2e`'s one test is `#[ignore]`d, unaffected).
   `cargo test -p kardamom-types --test rkyv_roundtrip` — 17 passed
   (confirms the `wire.rs` macro and the `pos()` helper are byte-identical
   to the code they replaced).
4. `cargo fmt -p kardamom-state -p kardamom-types` — applied (reformatted
   3 files this pass touched); a follow-up `-- --check` shows no diff.

Also reran `cargo check --workspace --all-targets` from this workspace
root — passes cleanly (exit 0) across every crate in the workspace,
confirming the coordinator's note that the `contracts/out` artifacts are
now seeded and the deployer/batcher failures are gone.

## Fix round

Coordinator review of the Phase C diff: both phases reviewed, clippy/pedantic/fmt confirmed on
their own run, and the listed structs/traits confirmed good. Eight fix items, in order.

1. **R12, `xchain/derive.rs` unchecked seq adds**: `w[0].seq + 1` (the gap-detection window) and
   the error's `gap[0].seq + 1` were unchecked `u64` adds. Rewrote the gap check as an explicit
   loop over `ordered.windows(2)`, computing `expected_next = w[0].seq.checked_add(1).ok_or(
   XChainError::SeqOverflow { seq: w[0].seq })?` before comparing or using it in an error. Added
   the `SeqOverflow { seq: u64 }` variant (checked: no exhaustive match on `XChainError` exists
   outside `xchain/tests.rs`'s `matches!`-style checks, so this is not a breaking addition).
2. **R9, `derive_remote_epoch` double-checked non-emptiness**: replaced the top-of-function
   `if msgs.is_empty()` check plus the later `ordered.last().ok_or(Empty)?` with one
   `let (first_msg, _) = ordered.split_first().ok_or(XChainError::Empty)?;` right after sorting,
   and `ordered.last().expect(...)` at the end (an `expect`, not a second `ok_or`, since
   `split_first` already proved `ordered` non-empty and it is never truncated in between).
3. **R12, raw casts without `try_from`**: `xchain/abi.rs` `deliver_calldata`'s
   `(HEAD_WORDS * 32) as u64` and `data.len() as u64` → `u64::try_from(..).expect(..)`;
   `xchain/message.rs` `RemoteEpochRecord::last_seq`'s `self.messages.len() as u64` →
   `u64::try_from(..).unwrap_or(u64::MAX)` (kept saturating end to end, matching the method's
   "never wraps or panics, even on a default-constructed record" contract);
   `trie/incremental_tests.rs` `pick`'s `items.len() as u64` → `u64::try_from(..).expect(..)`;
   `tests/common/mod.rs` `bpos`'s `block * 1024` → `block.checked_mul(1024).expect(..)` before the
   existing `i32::try_from(..)`.
4. **R11, every `#[allow]` needs `reason = "..."`**: converted all 6 —
   `trie/node.rs`'s `cast_possible_truncation` (comment above → `reason`),
   `witness.rs`'s `digest`'s `cast_possible_truncation` (doc paragraph → `reason`),
   `checkpoint/mod.rs`'s two `case_sensitive_file_extension_comparisons` (comments above → each's
   own `reason`), `trie/incremental_tests.rs`'s `many_single_char_names` (comment above →
   `reason`), and `tests/common/mod.rs`'s `#![allow(dead_code)]` (module doc → `reason`). Grepped
   every `#[allow(` and `#![allow(` in both crates afterward — the only one left without
   `reason =` was `xchain/leaf.rs`'s `too_many_arguments`, item 5 below.
5. **R11, `msg_leaf`'s 9-argument `#[allow(too_many_arguments)]` is not allowed**: added
   `pub struct MsgLeaf { origin_chain_id, dest_chain_id, seq, sender, target, value, gas_limit,
   data_hash, cb_hash }` with `fn hash(&self) -> B256` — the real rule, moved off nine positional
   parameters onto one receiver. `XChainMessage::leaf` (the one in-crate caller) now builds a
   `MsgLeaf` and calls `.hash()`. `msg_leaf(..)` stays as a thin wrapper
   (`MsgLeaf { .. }.hash()`) with `#[allow(clippy::too_many_arguments, reason = "...")]`, kept
   only for the two external callers below — **Phase B**, the coordinator fixes them at merge:
   - `crates/e2e/src/scenarios/xchain.rs:364`
   - `crates/validator/src/interop/extract.rs:229,337`

   `xchain/tests.rs`'s two `msg_leaf(..)` call sites (`leaf_recomputable_from_wire_record`,
   `leaf_known_vector_is_pinned`) were left calling the free function deliberately: `msg_leaf` is
   still public API that `e2e`/`validator` depend on, so a test that pins its exact byte output
   (the cross-language vector) should keep exercising that function directly, not the internal
   `MsgLeaf::hash` it now forwards to.
6. **R15, loose-struct-parameter functions**:
   - `trie/mod.rs` `update_for_block(txn, t: &TrieTables, delta)` → `TrieTables::update_for_block(
     &self, txn, delta)`. Every in-crate call site (`commit_trie_root`, `writer/apply.rs`'s
     `advance_state_root`, `trie/incremental_tests.rs`, `recovery/tests.rs`) now calls
     `tables.update_for_block(txn, delta)`. The free function `update_for_block(txn, t, delta)`
     stays as a compatibility wrapper (`t.update_for_block(txn, delta)`), because
     `crates/validator/tests/witness_anchoring.rs:157,326` calls it directly and is outside this
     group — **Phase B**, move it onto the method at merge, then delete the wrapper.
   - `del_prefix(txn, db, prefix)` → moved to `schema.rs`, next to `for_each_prefix` (my call, per
     the coordinator's offer): it never needed `&self` — every call site already passed a `db:
     Database` selected from outside (`self.hashed_storage`, `self.storage_trie`, or a `db`
     already resolved inside `apply_updates`), so a `Table` newtype wrapping one `Database` would
     have added a type with no behavior beyond what `schema.rs`'s free-function siblings
     (`for_each_row`, `for_each_prefix`, `del_if_present`) already provide. `trie/mod.rs`'s three
     call sites now say `crate::schema::del_prefix(..)`.
   - `writer/mod.rs`'s `read_schema_version(env)`/`ensure_schema_version(env)` →
     `StateEnv::read_schema_version(&self)`/`StateEnv::ensure_schema_version(&self)` in `env.rs`
     (both `pub(crate)`, no external callers). `StateWriter::spawn_inner` now calls
     `env.ensure_schema_version()?`.
   - **Phase B, recorded, not changed**: the public free functions on `&StateEnv` —
     `read_recovery_point`, `has_trie`, `bootstrap_trie_from_state`, `read_all_headers`,
     `genesis_applied`, `seed_genesis`, `compact_to`, `sweep`, `deep_compare` — with their
     external call sites, grepped fresh:
     - `read_recovery_point`: `crates/executor/src/bin/kardamom-executor/state.rs`,
       `crates/engine/src/persist.rs`, `crates/validator/src/bin/kardamom-validator/main.rs`
     - `has_trie`: `crates/validator/src/bin/kardamom-validator/adoption.rs`
     - `bootstrap_trie_from_state`: `crates/e2e/src/scenarios/xchain_da_parity.rs`,
       `crates/validator/src/bin/kardamom-validator/adoption.rs`
     - `read_all_headers`: `crates/e2e/src/scenarios/derivation.rs`
     - `genesis_applied`: no external call site found (grepped the whole workspace) — listed per
       the coordinator's instruction regardless
     - `seed_genesis`: `crates/engine/src/replay.rs`,
       `crates/executor/src/bin/kardamom-executor/main.rs`, `crates/bench/src/bin/stm-p2.rs`,
       `crates/validator/src/parallel/engine_tests.rs`,
       `crates/validator/src/bin/kardamom-validator/main.rs`
     - `compact_to`: no external call site found (only a doc-comment mention in
       `crates/executor/src/bin/kardamom-executor/state.rs`) — listed per the coordinator's
       instruction regardless
     - `sweep`: `crates/e2e/src/scenarios/consistency.rs`
     - `deep_compare`: `crates/e2e/src/scenarios/xchain_da_parity.rs`,
       `crates/e2e/src/scenarios/consistency.rs`
7. **R14, `TrieBootstrap`'s three "walk a table, decode each row, push" copies**: added
   `fn collect_rows<T>(&self, db, decode: impl FnMut(&[u8], &[u8]) -> Result<T, StateError>) ->
   Result<Vec<T>, StateError>`. `read_all_accounts`/`read_all_storage`/`read_all_code` now each
   call it with only their decode closure. `read_all_code`'s `code: v.into()` (a zero-copy move
   from the owned `Vec<u8>` `for_each_row` handed it) became `code:
   bytes::Bytes::copy_from_slice(v)` (`v` is now `&[u8]`, shared across all three closures) — a
   copy this one-time, whole-table trie-bootstrap read can afford, unlike the per-read hot path in
   `snapshot.rs::code_by_hash` that the R14 `get_decoded` row explicitly left alone for the same
   reason.
8. **Merge note, no action taken**: `crates/types/src/xchain.rs` was changed on `main` by #263
   while this split was in flight. Confirmed no conflicting edits from this side — the added
   items land in the split as:
   - `XCHAIN_ANCHOR_TAG`, `xchain_anchor_hash` → `xchain/ids.rs` (alongside the other
     alias/hash derivations)
   - `check_anchor`, `XChainError::{MultiBlockBatch, AnchorMismatch}`, and the multi-block-batch
     check inside `derive_remote_epoch` → `xchain/derive.rs` (alongside `XChainError` and the
     derivation rule)
   - `canonical_id` (now committing to `anchor_number`) → `xchain/message.rs` (alongside
     `RemoteEpochRecord`, which owns `anchor_number`)

   No code change made for this item — the coordinator ports #263's additions into these modules
   at merge.

### Fix-round gates

Rerun after every fix above, same commands, same target dir:

1. `cargo clippy -p kardamom-state -p kardamom-types --all-targets -- -D warnings` — clean.
2. `cargo clippy -p kardamom-state -p kardamom-types --all-targets -- -W clippy::pedantic` — clean
   in `crates/state/**` and `crates/types/**`. Three new pedantic findings surfaced and were
   fixed: `xchain/abi.rs::deliver_calldata` and `xchain/derive.rs::derive_remote_epoch` were
   missing `# Panics` sections (both now `.expect()`/checked-arithmetic panic sites, documented);
   `trie/mod.rs`'s new `update_for_block` compatibility wrapper was missing `# Errors`.
3. `cargo test -p kardamom-state -p kardamom-types --lib --bins`, `cargo test -p kardamom-state
   --tests`, and `cargo test -p kardamom-types --test rkyv_roundtrip` — all pass, same counts as
   the Phase C gates above (64 + 55 lib, 11 integration, 17 rkyv roundtrip).
4. `cargo fmt -p kardamom-state -p kardamom-types` — no changes this round; `-- --check` was
   already clean.

Also reran `cargo check --workspace --all-targets` — passes cleanly (exit 0), confirming
`crates/validator/tests/witness_anchoring.rs`'s external `update_for_block` call site still
compiles against the new compatibility wrapper.

## Final fixes

Coordinator review of the fix round (`42e9ecd669ef..f0244312`): accepted. Three small items,
then the group is complete.

1. `crates/state/src/trie/mod.rs`: `del_prefix`'s old doc comment, and the "Apply one block's
   `BlockDelta` ..." paragraph, were left orphaned directly above `impl TrieTables {` after
   `del_prefix` moved to `schema.rs` and `update_for_block` became a method — both doc blocks had
   drifted onto the `impl` block itself instead of the item they described. Deleted the stale
   `del_prefix` doc lines, and moved the "Apply one block's `BlockDelta` to the hashed-state
   mirror and the stored tries. Returns the new canonical world-state root." paragraph into
   `update_for_block`'s own doc comment, above the "This runs inside the writer's block-commit
   transaction" sentence.
2. `crates/types/src/xchain/derive.rs`: the trailing `ordered.last().expect(...)` re-proved
   non-emptiness `split_first` had already established. Changed
   `let (first_msg, _) = ordered.split_first()...` to
   `let (first_msg, rest) = ordered.split_first()...`, and the tail to
   `let last = rest.last().unwrap_or(first_msg);` — no `expect`, using the same split's two
   halves instead of a second proof. Dropped the now-unneeded `# Panics: Never` doc section.
3. `crates/types/src/xchain/abi.rs` `deliver_calldata`: `u64::try_from(HEAD_WORDS *
   32).expect(...)` converted a compile-time constant at runtime. Replaced with
   `const HEAD_BYTES: u64 = 320;` (a one-line comment ties it to `HEAD_WORDS * 32`) plus
   `const _: () = assert!(HEAD_BYTES == HEAD_WORDS as u64 * 32);` to enforce the tie at compile
   time, and `.u64(HEAD_BYTES)` at the call site. The assert compares in `u64` (`HEAD_WORDS as
   u64 * 32`, a widening cast), not `usize` (`HEAD_BYTES as usize`, which pedantic clippy flagged
   as a possible-truncation cast on the first attempt).

**Merge-time items the coordinator holds** (unchanged from the fix round, restated for a single
place to check at merge): move the `msg_leaf` callers in `crates/e2e/src/scenarios/xchain.rs` and
`crates/validator/src/interop/extract.rs` onto `MsgLeaf::hash`; move
`crates/validator/tests/witness_anchoring.rs` onto `TrieTables::update_for_block`; delete both
compatibility wrappers (`msg_leaf`, the free-function `update_for_block`) once those callers move;
port #263's `xchain.rs` additions into `ids.rs` (`XCHAIN_ANCHOR_TAG`, `xchain_anchor_hash`),
`derive.rs` (`check_anchor`, `XChainError::{MultiBlockBatch, AnchorMismatch}`, the multi-block-batch
check inside `derive_remote_epoch`), and `message.rs` (`canonical_id`, now committing to
`anchor_number`).

### Final-fix gates

Rerun after all three fixes above, same commands, same target dir:

1. `cargo clippy -p kardamom-state -p kardamom-types --all-targets -- -D warnings` — clean.
2. `cargo clippy -p kardamom-state -p kardamom-types --all-targets -- -W clippy::pedantic` —
   clean in `crates/state/**` and `crates/types/**`. One new pedantic finding surfaced and was
   fixed: the first `const _: () = assert!(...)` draft cast `u64 as usize`, which pedantic
   clippy flagged as a possible-truncation cast; rewritten to cast the other direction
   (`usize as u64`, widening) instead.
3. `cargo test -p kardamom-state -p kardamom-types --lib --bins`, `cargo test -p kardamom-state
   --tests`, and `cargo test -p kardamom-types --test rkyv_roundtrip` — all pass, same counts as
   every prior gate run (64 + 55 lib, 11 integration, 17 rkyv roundtrip), 0 failures.
4. `cargo fmt -p kardamom-state -p kardamom-types` — no changes; `-- --check` clean.

Also reran `cargo check --workspace --all-targets` — passes cleanly (exit 0).

## Round B (re-audit of the main delta, `crates/types` only)

Scope: `reaudit-main.md`'s `types` section, plus the cross-crate items the
round B brief assigned to the types owner. `crates/state` is untouched in
this round (the merged delta added nothing to `crates/state`).

### Done

- R3, `crates/types/src/xchain.rs` split. The merge had already split it
  into `xchain/{abi,derive,ids,layout,leaf,message,mod}.rs` plus test
  siblings; this round finishes the split the reaudit still found lacking:
  `derive.rs` split into a private `Batch` newtype (one method per verdict:
  `check_no_duplicates`, `check_starts_at_expected`, `check_dense`,
  `check_range_fits`, `check_destination`, `check_one_block`) called from a
  five-line `derive_remote_epoch` entry point. Every file in `xchain/` is
  under 500 code lines (largest: `derive.rs` at 184, `tests.rs` at 401).
- R2, `crates/types/src/xchain/derive.rs` `derive_remote_epoch`: the `Batch`
  split above also fixes the 101-line function; each method is under 20
  lines.
- R15, `crates/types/src/xchain/layout.rs`: the eight standalone layout fns
  become two unit structs, `Outbox` and `Inbox`, with associated fns and
  consts (`Outbox::nonces_slot`, `Outbox::sent_messages_slot`,
  `Outbox::send_message_selector`, `Outbox::message_sent_topic0`,
  `Inbox::next_seq_slot`, `Inbox::delivered_slot`); `mapping_slot` and
  `u64_word` are now private helpers, not exported.
- R15, `crates/types/src/xchain/derive.rs` / `ids.rs`: `check_anchor(origin,
  msg)` is now `OutboxMessage::check_anchor(&self, origin)`;
  `xchain_anchor_hash(origin, block)` is now `Anchor { origin_chain_id,
  block_number }.hash()`.
- R14, `crates/types/src/xchain/mod.rs`: one `keccak_concat(&[&[u8]])`
  helper, used by `Anchor::hash`, `alias_remote_address`,
  `RemoteEpochRecord::canonical_id`, `layout::mapping_slot`, and
  `Inbox::delivered_slot` (the two call sites that built the same 64-byte
  buffer twice).
- R14 (cross-crate, item 3 of the brief): one
  `XChainMessage::check_bounds(&self) -> Result<(), BoundsFault>` in
  `crates/types/src/xchain/message.rs`, with a `BoundsFault` enum
  (`ValueNotAllowed`, `GasLimitAboveCap`, `DataAboveCap`). `derive_remote_epoch`
  calls it via `messages.iter().try_for_each(XChainMessage::check_bounds)?`
  after building the `Vec<XChainMessage>` (so it now runs after the
  `MultiBlockBatch` check, not before — no test in `tests.rs` combines the
  two in one batch, so this reorder changes no observable behavior).
  `XChainError` replaces its three duplicate variants with one
  `Bounds(#[from] BoundsFault)`. The validator's `interop::verify::check_messages`
  calls the same method and maps the fault into `RemoteEpochFault::Bounds`
  (see `status-validator.md`).
- R10, `crates/types/src/xchain/derive.rs`: the imperative `for w in
  ordered.windows(2)` gap-check loop and the `ordered.iter().find(..)`
  bounds loop are `Batch::check_dense`'s and `Batch::check_destination`'s
  `try_for_each`/`find` bodies now; no manual loop remains in the batch
  checks.
- R9, `crates/types/src/xchain/derive.rs`: `split_first` was already in
  place from the earlier round; unchanged.
- R1: deleted every audit id and phase reference in `crates/types`:
  `check_anchor`'s "audit M4" doc line (now `xchain_anchor_hash`'s replacement,
  `Anchor::hash`'s doc), `canonical_id`'s "audit H3", `xchain_anchor_hash`'s
  "the job of spec §10" (now "a separate, later check, never this field's
  job" on `Anchor::hash`), and `tests.rs`/`layout_tests.rs`'s "Value pins
  (audit 2026-09-03, L3)" / "(audit L4)" comment headers (the `forge
  inspect` / `cast index` regeneration commands stay, as the brief
  instructs). `crates/types/src/limits.rs`'s `ShardCount` doc lost its
  "Phase B" heading and its literal `debug_assert!` mention (reworded so
  the grep gate does not fire on prose).
- R11/R14, `crates/types/src/xchain/leaf.rs`: deleted the `msg_leaf`
  compatibility wrapper and its `#[allow(clippy::too_many_arguments)]`
  (the grep-forbidden allow). `crates/validator/src/interop/extract.rs`
  (ours) now calls `MsgLeaf { .. }.hash()` directly at both call sites
  (`check_leaf`, and the `tests_support::honest_sent_log_full` fixture).
- Item 4 (cross-crate), `crates/types/src/delta.rs`: `BalFrame.granularity`
  is now `NonZeroU16`, parsed once by rkyv at the wire boundary rather than
  re-checked by every reader. Verified safe before making the change: the
  wire codec (`kardamom_log::codec`) uses rkyv's checked (`bytecheck`)
  archive access, so a zeroed granularity byte pattern is a clean decode
  `Err`, never a bytecheck panic or unchecked/UB read; a decode failure
  drops the fragment and logs, which lands the frame in the existing,
  already-tested `bal_missing` soft path (`validator_bal_missing_total`,
  "not a proven divergence: log it and count it" — confirmed by reading
  `crates/validator/src/seams.rs` and `crates/validator/src/metrics.rs`).
  Added `delta::tests::a_zeroed_granularity_byte_pattern_fails_to_decode`,
  which serializes two otherwise-identical frames differing only in
  `granularity`, diffs the byte buffers to find the granularity bytes with
  no assumption about rkyv's endianness, zeroes them in a third buffer,
  and asserts `rkyv::from_bytes` returns `Err`. This is the one intended
  behavior change in this round with its test, per the brief's
  behavior-preservation rule.
  `crates/validator/src/bin/kardamom-validator/pumps.rs::index_claims`
  (already the boundary read site from an earlier round) no longer needs
  its own `NonZeroU16::new(..) else { warn; return }` guard — the type now
  carries the guarantee.

### Callers outside this group, needed for the workspace to build again

- `Outbox`/`Inbox`/`u64_word` (all eight renamed layout items):
  `crates/e2e/src/scenarios/xchain.rs` (imports the old free-fn names:
  `inbox_delivered_slot`, `inbox_next_seq_slot`, `message_sent_topic0`,
  `outbox_nonces_slot`, `sent_messages_slot`, `u64_word`),
  `crates/e2e/src/scenarios/xchain_two_stacks.rs` (all eight),
  `crates/e2e/src/scenarios/xchain_da_parity.rs` (`inbox_next_seq_slot`,
  `inbox_delivered_slot`). `crates/da_watcher` and `crates/engine` already
  self-migrated to `Inbox::next_seq_slot` / `OutboxMessage::check_anchor`
  during this round (confirmed by reading `da_watcher/src/interop/{reconcile,watcher}.rs`
  and `engine/src/actor/exec_tests/interop.rs`); `crates/da_watcher/tests/interop_watcher.rs`
  (an integration test, not a `src/` file) still imports the old
  `xchain_anchor_hash` name and needs `Anchor { .. }.hash()`.
- `msg_leaf` → `MsgLeaf { .. }.hash()`: `crates/e2e/src/scenarios/xchain.rs:297`.
- `BalFrame.granularity: u16 → NonZeroU16`: `crates/executor/src/bal.rs`
  (`configured_granularity() -> u16` at line 53, and `encode_frame`'s
  `granularity: u16` parameter and the `BalFrame { granularity, .. }`
  literal at line 102 — confirmed by `cargo check -p kardamom-executor`,
  the one failure attributable to this change, distinct from an unrelated
  concurrent `NonZero<usize>` migration in `crates/executor/src/parallel.rs`)
  and `crates/e2e/src/harness/inject.rs:100` (`granularity: 1` literal).
  No `crates/engine`, `crates/bench`, or `crates/exec-core` call site reads
  `BalFrame.granularity` directly (grepped `\.granularity\b` across all
  three; the only exec-core `granularity: u16` parameters, in
  `stateless.rs`, are independent function arguments, not a `BalFrame`
  field read).

### Gates

- `cargo check -p kardamom-types --all-targets --all-features`: clean.
- `cargo test -p kardamom-types --all-features`: 68 lib + 19 integration
  (`rkyv_roundtrip`), 0 failures, including the new granularity-decode
  test.
- `cargo fmt -p kardamom-types -- --check`: clean.
- `cargo clippy -p kardamom-types --all-targets --all-features -- -D
  warnings -W clippy::pedantic -D unreachable_pub`: zero warnings.
- Forbidden-pattern grep (`debug_assert!`, `.max(1)`, `Box<dyn`,
  `allow(clippy::too_many_arguments)`) over `crates/types`: no hits outside
  test paths.
- No file in `crates/types/src/xchain/` or `crates/types/src/limits.rs`/`delta.rs`
  exceeds 500 code lines.

## Round B, follow-up

Three items from a mid-round coordinator message, plus a broadened R16
sweep, addressed after the original Round B section above was written.

### 1. `NonEmptyVec<XChainMessage>` (R9: no defensive check for a
structurally-enforceable invariant)

`RemoteEpochRecord::messages` was `Vec<XChainMessage>`, documented
"non-empty by construction" but not enforced by the type — three
downstream crates re-checked `is_empty()`/re-cast `.len()` at runtime
instead.

- Added `NonEmptyVec<T>` in `crates/types/src/xchain/message.rs`: a
  single-field wrapper around `Vec<T>`, archived with rkyv's
  `#[rkyv(bytecheck(verify))]` plus a hand-written `Verify` impl that
  rejects a decoded archive with zero elements (mirrors rkyv's own
  `ArchivedVec`/`ArchivedDuration` verify pattern). API: `new(first: T,
  rest: Vec<T>)`, `len() -> NonZeroUsize`, `first()`/`last() -> &T`,
  `iter()`, `as_slice()`, `as_mut_slice()`, `impl IntoIterator for
  &NonEmptyVec<T>`. Deliberately no `Default`, no `Index`, no `Deref` —
  nothing lets a caller reach for `[0]` or `.is_empty()` again.
- `RemoteEpochRecord.messages` is now `NonEmptyVec<XChainMessage>`;
  `Default` dropped from the struct's derive (a `NonEmptyVec` cannot be
  built empty, so there is no default value); `last_seq()` simplified to
  `first_seq.saturating_add(len().get() - 1)` — no underflow guard
  needed, since `len().get() >= 1` always.
- `crates/types/src/xchain/derive.rs::derive_remote_epoch`: builds the
  record via `NonEmptyVec::new(first, rest)` from the batch's own
  proven-non-empty `(first, rest)` split, instead of collecting into a
  plain `Vec` and never re-checking it.
- Test changes in `crates/types/src/xchain/tests.rs`: three struct
  literals updated to `NonEmptyVec::new`; `[0]` indexing replaced with
  `.first()`; deleted
  `last_seq_does_not_underflow_on_an_empty_default_record` (the empty
  case it guarded against is now unrepresentable); added
  `an_archive_with_zero_messages_fails_to_decode`, which serializes a
  1- and a 2-message record, confirms empirically that the archive's
  trailing 4 bytes are exactly `messages`' length word (not an assumed
  offset), zeroes that word in the 1-message archive, and asserts
  `rkyv::from_bytes` rejects it — proof the `Verify` wiring is live, not
  just present. `crates/types/tests/rkyv_roundtrip.rs`'s
  `golden_remote_epoch_record` fixture updated the same way; its pinned
  hex bytes are UNCHANGED (`NonEmptyVec<T>` archives identically to
  `Vec<T>` — a single-field tuple struct adds no wire bytes), confirmed
  by `remote_epoch_record_golden_bytes_are_pinned` still passing.

**Callers outside this group, needing the same `NonEmptyVec::new(first,
rest)` / `.len().get()` / `.first()`/`.last()` treatment** (checked
against the tree at the time of this report; several were already fixed
by their owning groups before this report was written):

- `crates/cluster-adapter/src/wire/tests.rs:119` (struct literal,
  `messages: vec![...]`) and `:357` (`rec.messages[0].callback = ...`) —
  still pending as of this report; `wire/ingress.rs` and `wire/mod.rs`
  (the production code) were already fixed by the owning group.
- No other pending callers found: `crates/da_watcher/src/interop/watcher.rs`,
  `crates/engine/src/reader/tests.rs`, `crates/engine/src/actor/test_support.rs`,
  `crates/engine/src/reader/threads.rs`, `crates/sequencer/src/remote_epoch.rs`,
  `crates/sequencer/src/outbound/cluster.rs`, and `crates/batcher/src/frame.rs`'s
  own wire decoder (`decode_remote_epoch`, the second "wire boundary" a
  coordinator message asked for) were already updated by their owning
  groups by the time this report was written.

### 2. R16, broadened form, across every owned production file

A later coordinator message confirmed `docs/STYLE.md`'s R16 was
deliberately broadened (by the repository owner) to: no loop containing
an `if`/`else`/`match`/`let-else` in its body, and no loop inside a
branch, in either direction — not just "no nested loops." Applied across
every file this group owns. One production site, `crates/types` only
(the validator-side sites are in the validator status doc):

- `crates/types/src/genesis.rs::AllocEntry::to_alloc`: the `for entry in
  &self.alloc { ...; if let Some(c) = entry.code.as_ref() { ... } }` loop
  had an `if let` in its body. Split into `AllocEntry::code_hash()` and
  `AllocEntry::code_entry() -> Option<CodeEntry>`, then two iterator
  chains: `.iter().map(...)` for `accounts`, `.iter().filter_map(AllocEntry::code_entry)`
  for `code`. No loop left.
- `crates/types/src/withdrawals.rs::withdrawal_proof`: the `while
  level.len() > 1 { let sibling = if ... else ...; ... }` loop had an
  `if`/`else` in its body. Extracted to `fn sibling_of(level: &[B256],
  idx: usize) -> B256`; the loop's body is now three plain statements
  (push the helper's result, requantize `level`, halve `idx`).
- `crates/types/src/withdrawals.rs::recompute_root`: same pattern, `for
  sibling in proof { node = if ... else ...; ... }`. Extracted to `fn
  combine_with_sibling(node: B256, sibling: B256, idx: usize) -> B256`.
- Everything else already had branch-free loop bodies (verified by
  grepping every `for`/`while`/`loop` site in `crates/types/src` and
  reading each): `ack_policy.rs`, `position.rs`, `prover.rs`,
  `xchain/mod.rs::keccak_concat`, `witness.rs::digest`'s three hashing
  loops, and every test-file loop except none found needing a fix in
  `crates/types` test files (the one test-side fix, `serve/tests.rs`, is
  in the validator group).

Behavior preservation: both `withdrawals.rs` extractions keep the exact
same left/right ordering logic, just as named functions instead of
inline branches; `proofs_recompute_root_all_sizes` (which specifically
exercises `withdrawal_proof`+`recompute_root` together, sizes 1 through
9) still passes.

### Gates (this follow-up)

- `cargo check -p kardamom-types --all-targets`: clean.
- `cargo test -p kardamom-types --all-targets`: 69 lib + 19 integration
  tests, all pass (one fewer lib test than the original Round B count:
  the deleted underflow test; one more: the new zero-messages-archive
  test — net even, but the coverage moved from an unrepresentable
  in-memory case to the real wire-decode boundary).
- `cargo fmt -p kardamom-types -- --check`: clean.
- `cargo clippy -p kardamom-types --all-targets --no-deps -- -D warnings
  -W clippy::pedantic -D unreachable_pub`: clean (required adding `#
  Panics` doc sections to `NonEmptyVec::len`/`first`/`last`, since
  clippy's `missing_panics_doc` cannot see that the panic is
  unreachable by construction).
- Forbidden-pattern grep: unchanged, clean.

## Round B, follow-up 2 (`crates/types` only)

Reversed the follow-up round's `NonEmptyVec` restructure ruling (that
round's `(first, rest)` field-split plan was never implemented; a
coordinator ruling before implementation kept `Vec<T>` storage instead,
since the golden `RemoteEpochRecord` vector in `xchain/tests.rs` pins the
archived bytes byte-for-byte and a field split would change them).
Consolidated the three `.expect("non-empty by construction")` sites
(`len`, `first`, `last`) into one private `fn split(&self) -> (&T, &[T])`
in `message.rs`; the other three derive from it without their own panic.

Restored `RemoteEpochRecord::last_seq() -> u64` to its original
`saturating_add`/`saturating_sub` form (a prior round in this same pass
had made it return `Option<u64>`, which broke three non-owned call sites
in `cluster-adapter`/`da_watcher` — all reverted to their original form).
The overflow guard instead moved to the wire boundary: `#[rkyv(bytecheck
(verify))]` on `RemoteEpochRecord`'s own derive, with a hand-written
`Verify` impl (`mod remote_epoch_verify`, mirroring `NonEmptyVec`'s own
`non_empty_verify`) rejecting an archive whose `first_seq + (messages.len()
- 1)` does not fit `u64`.

Fixed a real behavior regression from the same earlier round:
`derive_remote_epoch`'s bounds check had moved to run AFTER
`check_one_block` and after copying every message into `Bytes`, instead of
before (so a batch that spans two blocks AND carries an over-cap message
wasted the copy, then reported `MultiBlockBatch` instead of `Bounds`).
Added `OutboxMessage::check_bounds` (mirrors `XChainMessage::check_bounds`)
and moved the check back to running on the borrowed messages before
`check_one_block`; added
`a_multi_block_batch_with_an_over_cap_message_faults_on_bounds_first` to
`xchain/tests.rs` to pin the restored order.

`withdrawals.rs`'s `sibling_of`/`combine_with_sibling` (extracted the
prior Round B follow-up, see above) still used unchecked `level[idx + 1]`
/ `level[idx - 1]` and a bare `assert!` at `withdrawal_proof`'s boundary.
Replaced with a `Level(Vec<B256>)` newtype (`Level::leaves`,
`Level::sibling` using `idx ^ 1` instead of `+1`/`-1`, `Level::up`) and a
`LeafIndex::new(index, leaf_count) -> Option<Self>` parsed once at
`withdrawal_proof`'s boundary instead of the `assert!`; the function's
public signature and panic contract are unchanged (two non-owned callers,
`crates/e2e/src/scenarios/bridge.rs` and
`crates/validator/tests/withdrawal_e2e.rs`, needed no change).

`genesis.rs::Genesis::to_alloc` walked `self.alloc` twice, hashing each
code-carrying entry's code twice (`AccountChange::code_hash` from one
pass, `AllocEntry::code_entry()`'s own `code_hash()` call from the other).
Now one `.map(...).unzip()` pass computes `code_hash` once per entry and
reuses it for both outputs; `AllocEntry::code_entry()` is deleted.

`xchain/mod.rs::keccak_concat`'s manual `for p in parts { buf
.extend_from_slice(p) }` loop became `keccak256(parts.concat())`.
`NonEmptyVec::new`'s `Vec::with_capacity(rest.len() + 1)` became
`rest.len().saturating_add(1)`.

### Gates (follow-up 2)

- `cargo check -p kardamom-types --all-features --all-targets`: clean.
- `cargo test -p kardamom-types --lib`: 72 passed (up from 69: the new
  `word_u64`/`u64_word` round-trip test from the prior addendum, the new
  multi-block-plus-over-cap test above, and a self-review-pass addition
  below; `withdrawals::tests` all pass, including the panic test for
  `withdrawal_proof`'s now-`LeafIndex`-backed bound).
- `cargo fmt -p kardamom-types -- --check`: clean.
- `cargo clippy -p kardamom-types --all-features --all-targets -- -D
  warnings -D clippy::pedantic -D unreachable_pub`: clean.
- Forbidden-pattern grep: unchanged, clean.

A self-review pass before reporting done caught a real defect in the
`Verify` impl: it checked `first_seq + (len - 1)` (room for `last_seq`)
instead of `first_seq + len` (room for `next_cursor`, one more — the bound
the ruling specifies and the producer's `check_range_fits`/the validator's
`SeqRange::new` both already use). Fixed to `first_seq.checked_add(len)
.is_none()`; added `an_archive_whose_seq_range_overflows_u64_fails_to_
decode` (positive and negative control at the exact `u64::MAX` boundary)
to `xchain/tests.rs`, which passes against the fix and would have caught
the original off-by-one. Also simplified `NonEmptyVec::len`'s
`unwrap_or(NonZeroUsize::MIN)` fallback (a `.max(1)`-shaped sentinel) to
`NonZeroUsize::MIN.saturating_add(rest.len())`.

Full detail on the validator-side half of this round (SeqRange, the R15
method conversions, the R1 comment fixes, the slots.rs lock-poisoning fix,
and the workspace-build state) is in `status-validator.md`'s "Round B,
follow-up 2" section.
