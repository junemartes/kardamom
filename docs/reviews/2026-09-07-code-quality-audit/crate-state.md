# state

## Summary

The two crates are clean and well documented. The biggest problems are size and staleness, not correctness.
Only one file passes 500 code lines: `crates/types/src/xchain.rs` (541, of which 259 are tests).
Thirteen long functions exceed 50 code lines; `writer::apply` (144), `integrity::deep_compare` (104) and
`checkpoint_transfer::fetch_latest_checkpoint` (100) are the worst.
Comments carry a lot of history: 10 doc-file references (one of them, `docs/specs/interop-outbox-messaging-spec.md`,
does not exist), a PR reference, phase markers, and three comments that now contradict the code
(`schema.rs` "Seven named tables" for 11 tables, `meta.rs` "currently 1" for `SCHEMA_VERSION = 2`,
`deposit.rs` "always `false`" for a field `upgrade_from_log` sets to `true`).
`kardamom-state` exports a very wide surface: 80 `pub` items have no user outside the crate, mostly the
`schema`, `meta` and `trie` codecs.
Counts: R1 24, R2 13, R3 1, R4 4, R5 6, R6 3, R7 0, R8 15 rows (80 items), R9 12, R10 11.

## R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/state/src/schema.rs:1 | `libmdbx schema. Seven named tables, each with a fixed key/value encoding.` | Stale. `ALL_TABLES` holds 11 tables. | Say "the tables listed in `ALL_TABLES`". Drop the count. |
| crates/state/src/meta.rs:13 | `| schema_version | u32 BE (currently 1) |` | Stale. `SCHEMA_VERSION` is 2. | Remove the value. The constant is the truth. |
| crates/state/src/meta.rs:51 | `v2 adds the incremental state-trie tables: account_trie, storage_trie,` | Migration history plus a doc-file name. | State the present rule: "the DB refuses a version other than `SCHEMA_VERSION`". |
| crates/state/src/meta.rs:54 | `docs/specs/2026-06-23-incremental-trie-design.md, section 8.` | Dated spec reference. | Drop it. |
| crates/state/src/schema.rs:41 | `docs/specs/2026-06-23-incremental-trie-design.md.` | Dated spec reference. | Drop it. |
| crates/state/src/schema.rs:127 | `The origin field added 4 bytes to the 4 reserved bytes, so rows grew` | Change history ("added", "grew"). | Say: "a 24-byte row carries `l1_origin`; a 20-byte row decodes as `l1_origin = 0`". |
| crates/state/src/schema.rs:8 | `encoded (BPosition end_tx_idx, u64 l2_timestamp), no state root` | Stale. The row also carries `l1_origin`. | Add `l1_origin` to the table row. |
| crates/state/src/schema.rs:118 | `code value = raw bytes; no codec needed` | Restates the absence of code. | Delete. |
| crates/state/src/schema.rs:137 | `docs/agents/l1-origin-deposit-derivation-spec.md.` | Spec-doc reference. | Drop it. |
| crates/state/src/writer/mod.rs:310 | `Err(..KeyExist) => {} // Duplicate code. Fine.` | Restates the arm. | Delete. The block comment above already explains it. |
| crates/state/src/integrity/mod.rs:4 | `This module supports the chain-semantics end-to-end test suite. See` | Names a spec doc. | Say what the checks prove. Drop the doc name. |
| crates/state/src/checkpoint/manifest.rs:20 | `Without a manifest, three failures were silent: bytes corrupted...` | Explains the past, not the rule. | Say: "the manifest binds the image to a chain and to its bytes". |
| crates/state/src/checkpoint_transfer.rs:188 | `(the std version used socket timeouts)` | Refers to a removed implementation. | Delete the parenthesis. |
| crates/state/src/checkpoint_transfer.rs:299 | `Without this check, this transfer was plain HTTP with only a length` | Past-tense justification. | Say: "the fetch verifies the image hash and the chain identity". |
| crates/types/src/epoch.rs:30 | `This is ported verbatim from crates/node/src/deposit.rs.` | Migration note. The path no longer exists. | Delete. Keep the "OP-compatible, CI-pinned" sentence. |
| crates/types/src/epoch.rs:5 | `docs/agents/l1-origin-deposit-derivation-spec.md, two parties run the` | Spec-doc reference. | Drop the name. Keep the producer/verifier rule. |
| crates/types/src/xchain.rs:6 | `docs/specs/interop-outbox-messaging-spec.md the same rule is run by the` | The named file does not exist in `docs/`. | Delete the reference. |
| crates/types/src/receipt.rs:17 | `(docs/specs/interop-outbox-messaging-spec.md). One below the deposit` | Same dangling reference. | Delete. |
| crates/types/src/receipt.rs:129 | `Before this field existed, a deposit and a genuine nonce-0` | Five lines of bug history. | Keep only "a consumer must branch on `tx_type`, not on `nonce == 0`". |
| crates/types/src/deposit.rs:61 | `v0 only canonicalizes user deposits, so this is always false.` | Wrong now. `epoch::upgrade_from_log` sets it `true`. | Say: "`true` for an upgrade system transaction, `false` for a user deposit". |
| crates/types/src/delta.rs:38 | `This is its own struct, not a (B256, Bytes) tuple as in the original plan.` | Refers to a plan. | Keep only "a struct lets the rkyv `with` adapters apply". |
| crates/types/src/delta.rs:56 | `It was once a V1/V2 enum. V1 (the delta alone...) had only one producer` | Eight lines of version history. | Keep "the wire format may still change while the chain is at v0". |
| crates/types/src/tx_error.rs:43 | `drop used to be silent: the parked submit waited for a receipt that` | Bug history. | Keep "the sequencer never sequences this transaction; the client must resubmit". |
| crates/types/src/witness.rs:12 | `This is the fail-closed rule a prover needs. Phase 2` | Phase markers (Phase 2, Phase 3). | State what the type carries today. Move the plan to the spec. |
| crates/types/src/prover.rs:60 | `The single-block proof's public outputs (v2, spec PR 5 slice 0).` | PR reference. | Delete the parenthesis. |
| crates/types/src/prover.rs:3 | `docs/agents/no-std-exec-core-spec.md.` | Spec-doc reference. | Drop it. |
| crates/types/src/boundary.rs:26 | `...is ordered, and on older chains from before such records.` | Legacy-chain history. | Say: "`0` means no origin record is ordered yet". |
| crates/types/src/boundary.rs:7 | `docs/agents/l1-origin-deposit-derivation-spec.md.` | Spec-doc reference. | Drop it. |
| crates/types/src/tx_ordering.rs:55 | `docs/agents/l1-origin-deposit-derivation-spec.md.` | Spec-doc reference. | Drop it. |
| crates/types/src/tx_ordering.rs:62 | `peer count. See docs/specs/interop-outbox-messaging-spec.md.` | Dangling spec reference. | Drop it. |
| crates/types/src/delta.rs:51 | `docs/agents/bal-attribution-parallel-validation-spec.md. It carries the` | Spec-doc reference. | Drop it. |
| crates/types/src/upgrades.rs:22 | `See docs/specs/2026-08-16-l1-upgrade-feature-flags-design.md.` | Dated spec reference. | Drop it. |

Not flagged, on purpose: `walker.rs:297` ("no longer present") states the `removals` invariant in present tense;
`env.rs:71` and `snapshot.rs:120` carry measured performance arguments; `error.rs:33` ("Do not change the Display
string") is a live contract with the tests.

## R2 long methods

| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/state/src/writer/mod.rs:238 | `StateWriter::apply` | 144 | `write_storage(&txn, db, &delta)`, `write_accounts(..)`, `write_receipts_and_index(..)`, `write_meta_cursors(..)`, `advance_state_root(&txn, mode, &delta)`. The `KARDAMOM_WRITER_TIMING` stopwatches then move into one `ApplyTimings` struct. |
| crates/state/src/integrity/compare.rs:24 | `deep_compare` | 104 | `compare_table(ta, tb, name) -> Vec<String>` (the whole dual-cursor merge), `compare_meta_keys(ta, tb) -> Vec<String>`, `value_diff_message(table, key, va, vb)`. |
| crates/state/src/checkpoint_transfer.rs:217 | `fetch_latest_checkpoint` | 100 | `open_peer_stream(peer) -> BufReader<TcpStream>`, `download_image(reader, len, path) -> Result<()>`, `verify_and_publish(tmp, dest, block, head)`. |
| crates/state/src/trie/mod.rs:187 | `update_for_block` | 90 | `group_storage_by_account(&delta)`, `update_storage_tries(txn, t, &stor_by) -> BTreeMap<Address,B256>`, `write_hashed_accounts(txn, t, &touched, &basics, &new_sroot)`, `update_account_trie(txn, t, &touched)`. |
| crates/state/src/genesis.rs:81 | `seed_genesis` | 84 | `check_or_backfill_digest(&txn, meta, digest) -> Result<bool>`, `write_allocations(&txn, accounts, code)`, `seed_trie(&txn, accounts, code) -> B256`. |
| crates/state/src/integrity/checks.rs:153 | `check_receipts_index` | 78 | `check_receipt_row(txn, tx_hash_db, k, v, meta_end_tx, r)`, `check_index_rows(txn, receipts_db, tx_hash_db, r) -> u64`. |
| crates/state/src/bin/kardamom-statecheck.rs:60 | `main` | 75 | `parse_args() -> Args`, `check_root(&report, expect) -> bool`, `compare_dirs(&env, other) -> bool`. |
| crates/state/src/recovery/mod.rs:125 | `bootstrap_trie_from_state` | 70 | `read_all_accounts(&txn)`, `read_all_storage(&txn)`, `read_all_code(&txn)`. Each is a table scan with one key-length check. |
| crates/types/src/xchain.rs:414 | `derive_remote_epoch` | 63 | `sorted_unique_by_seq(msgs) -> Result<Vec<&OutboxMessage>, XChainError>`, `check_dense_from(ordered, expected_first_seq)`, `check_destination(ordered, self_chain_id)`, `to_wire_messages(origin_chain_id, ordered)`. |
| crates/state/src/integrity/checks.rs:93 | `check_headers` | 58 | `check_header_row(k, v, &mut state, r)`, `check_header_chain_ends(state, meta_end_tx, r)`. |
| crates/state/src/trie/walker.rs:92 | `walk_account` | 55 | `emit_leaves_under(tx, db, path, hb, leaf)` (the block appears twice inside the function), `descend_or_skip(node, i, child, prefix_set, hb)`. |
| crates/state/src/checkpoint_transfer.rs:376 | `read_response_head` | 53 | `read_head_text(reader) -> Result<String>`, `parse_status_line(line) -> Result<u16>`, `parse_headers(lines) -> ResponseHead`. |
| crates/state/src/trie/walker.rs:186 | `walk_storage` | 51 | Same two helpers as `walk_account`. The two walkers differ only in the leaf source and the key namespace; one generic walker over a `LeafSource` trait removes the duplicate. |

## R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/types/src/xchain.rs | 541 (282 non-test, 259 test) | Split into a module directory: `xchain/mod.rs` (module docs, `OUTBOX`, `INBOX`, constants, re-exports); `xchain/ids.rs` (`remote_source_hash`, `alias_remote_address`, `xchain_tx_sender`, `encode_list_two`); `xchain/message.rs` (`Callback`, `OutboxMessage`, `XChainMessage`, `RemoteEpochRecord`); `xchain/leaf.rs` (`xchain_leaf_domain`, `msg_leaf`); `xchain/abi.rs` (`INBOX_DELIVER_SIGNATURE`, `inbox_deliver_selector`, `deliver_calldata`, the three `push_word_*` helpers); `xchain/derive.rs` (`derive_remote_epoch`, `XChainError`). Move the two test modules to `xchain/tests.rs` and `xchain/abi_tests.rs`. That alone takes the file under 300 code lines. |
| crates/types/src/epoch.rs | 427 (184 non-test) | KEEP. One derivation rule with its log types. Move the 243 test lines to `epoch/tests.rs` if the raw length matters. |
| crates/state/src/checkpoint_transfer.rs | 431 (309 non-test) | KEEP as one file, but split by direction inside it, or into `checkpoint_transfer/serve.rs` and `checkpoint_transfer/fetch.rs`. The server half and the client half share only `IO_TIMEOUT` and `MAX_HEAD`. |
| crates/state/src/writer/mod.rs | 313 | KEEP the module, but move `apply` and its new helpers into `writer/apply.rs`. `mod.rs` then holds `WriteBatch`, `WriterHandle`, `TrieMode` and the spawn path. |
| crates/state/src/trie/mod.rs | 361 | KEEP. It is the trie facade: table handles, the per-block update, the rebuild oracle, and the pure root functions. Each part is small and they share `AccountTrieParts`. |

## R4 manual drops

| file:line | snippet | class | fix |
|---|---|---|---|
| crates/state/src/writer/mod.rs:96 | `drop(std::mem::replace(&mut self.delta_tx, closed_tx));` | channel sender close | Hard to remove as written, because `Drop` also calls `shutdown`. Make the field `delta_tx: Option<Sender<WriteBatch>>` and use `self.delta_tx.take()`; the sender then closes when the `Option`'s value drops at the end of the statement. This also removes the dummy `bounded(0)` channel on line 95. |
| crates/state/src/writer/mod.rs:448 | `drop(txn);` | resource release (mdbx write transaction) | Extract `fn read_schema_version(env) -> Result<Option<u32>>`, which opens and ends its own transaction. `ensure_schema_version` then compares outside any transaction scope. |
| crates/state/src/genesis.rs:97 | `drop(txn);` | resource release (mdbx write transaction) | Extract `fn stored_digest(env) -> Result<Option<B256>>`. The borrow ends with the helper, and the mismatch check runs after it returns. |
| crates/state/src/checkpoint_transfer.rs:295 | `drop(out);` | resource release (file handle) | Extract `fn write_image(reader, len, path) -> Result<u64>`; the file closes when the helper returns, before `file_keccak` reopens the path. |

Test-only drops (not fixed here): `swap.rs:114`, `checkpoint/tests.rs:108,146,177,205`, `integrity/tests.rs:139`,
`tests/env_smoke.rs:20`. Each closes an env or handle so a later assertion can observe the closed state.

## R5 sync primitives and channels

| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/state/src/swap.rs:31,40,47 | `Arc<ArcSwapOption<StateSnapshot>>` | REPLACE_WITH_CHANNEL | The `tokio::sync::watch` slot on line 33 already holds the same value and already receives every publish. `watch::Receiver::borrow()` reads it with no runtime, so the `ArcSwapOption` is a second copy of one fact. | Delete `latest`. Implement `current()` as `self.watch.borrow().clone()`. Trade-off to state in the doc: `borrow` takes a read lock, so the load is no longer lock-free. |
| crates/state/src/swap.rs:32,41,48 | `crossbeam_channel::bounded(1)` notify | JUSTIFIED | The sync consumer (the exec thread) needs a blocking wake, and `watch` gives none without a runtime. Depth 1 coalesces pending wakes. | None. |
| crates/state/src/swap.rs:33,42,49 | `tokio::sync::watch::channel` | JUSTIFIED | Async consumers (prover spool, commit poller) park on `changed()`. The writer thread sends without a runtime. | None. |
| crates/state/src/writer/mod.rs:159 | `crossbeam_channel::bounded(HORIZON_BLOCKS)` delta channel | JUSTIFIED | Hands whole blocks from the exec thread to the writer thread, with backpressure at the MVCC horizon. This is the textbook channel case. | None. |
| crates/state/src/writer/mod.rs:95 | `crossbeam_channel::bounded(0)` | REPLACE_WITH_OWNERSHIP | The channel is never used. It exists only to give `mem::replace` a value so the real sender drops. | Make `delta_tx` an `Option<Sender<..>>` and `take()` it (see R4). |
| crates/state/tests/concurrent_readers.rs:40 | `Arc<AtomicBool>` stop flag | JUSTIFIED | Test-only. Four reader threads poll one shutdown flag; a `Relaxed` bool is the right shape. | None. |

`kardamom-types` has no synchronization primitive and no channel. That is correct for a pure data crate.

## R6 dynamic dispatch

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/state/src/trie/walker.rs:72 | `account_leaf: &dyn Fn(&super::AccountTrieParts) -> Vec<u8>` (`account_root`) | stored `dyn Fn` closure | The only two callers (`trie/mod.rs:81` and `trie/proofs.rs:42`) pass the same closure body: RLP-encode `to_trie_account()`. Delete the parameter and call a free `fn account_leaf_rlp(p: &AccountTrieParts) -> Vec<u8>` in `trie/mod.rs`. If a second leaf shape ever appears, take `F: Fn(&AccountTrieParts) -> Vec<u8>` instead. |
| crates/state/src/trie/walker.rs:100 | same, on `walk_account` | stored `dyn Fn` closure | Same fix. `walk_account` recurses, but a generic `F` monomorphizes once, so recursion is fine. |
| crates/state/src/trie/walker.rs:251 | same, on `walk_account_for_proofs` | stored `dyn Fn` closure | Same fix. Removing this parameter also removes the duplicate closure in `proofs.rs`. |

No `Box<dyn Error>` and no trait-object dispatch in either crate.

## R7 too many generics

None found. Every generic function in both crates carries one or two type parameters
(`for_each_row<K>`, `walk_account<K>`, `encode_list_two<A, B>`), and no where-clause holds four bounds.
`trie::cursor::ReadKind` (cursor.rs:25) is already the supertrait pattern this rule asks for: it groups
`TransactionKind + SyncKind<Access = Arc<PtrSync>>` behind one name.

## R8 unnecessary pub

Verified by grepping every other crate in the workspace for each name.

| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/state/src/schema.rs:25-59 | `TABLE_ACCOUNTS`, `TABLE_STORAGE`, `TABLE_CODE`, `TABLE_HEADERS`, `TABLE_RECEIPTS`, `TABLE_TX_HASH_INDEX`, `TABLE_META`, `TABLE_ACCOUNT_TRIE`, `TABLE_STORAGE_TRIE`, `TABLE_HASHED_ACCOUNTS`, `TABLE_HASHED_STORAGE`, `ALL_TABLES` | No hit outside `crates/state/`. | `pub(crate)`. |
| crates/state/src/schema.rs:71-227 | `encode_account_key/value`, `decode_account_value`, `encode_storage_key/value`, `decode_storage_value`, `encode_code_key`, `encode_block_key`, `encode_header_value`, `decode_header_value`, `encode_receipt_value`, `decode_receipt_value`, `encode_tx_hash_key/value`, `decode_tx_hash_value` | No hit outside the crate. The codecs serve `writer`, `snapshot`, `integrity` and `recovery` only. | `pub(crate)`. |
| crates/state/src/schema.rs:64 | `AccountValue` and its four `pub` fields | Read by `writer`, `snapshot`, `genesis`, `integrity` only. | `pub(crate) struct`. |
| crates/state/src/schema.rs:133 | `HeaderValue` and its `pub` fields | `read_all_headers` returns it, and `e2e/src/scenarios/derivation.rs:74` consumes that. | Keep `pub`. Not a finding. |
| crates/state/src/meta.rs:22-49 | `KEY_LAST_COMMITTED_BLOCK`, `KEY_LAST_COMMITTED_END_TX_POSITION`, `KEY_LAST_FSYNCED_B_POSITION`, `KEY_SCHEMA_VERSION`, `KEY_GENESIS_APPLIED`, `KEY_GENESIS_DIGEST`, `KEY_STATE_ROOT` | No hit outside the crate. | `pub(crate)`. |
| crates/state/src/meta.rs:68-184 | `read_meta_u64/u32/b_position/b256`, `encode_u64/u32/b_position/b256`, `decode_u64/u32/b_position/b256` | No hit outside the crate. | `pub(crate)`. |
| crates/state/src/geometry.rs:29-63 | `PAGE_SIZE`, `SIZE_UPPER`, `SIZE_LOWER`, `GROWTH_STEP`, `SHRINK_STEP`, `MAX_DBS` | No hit outside the crate. | `pub(crate)`. |
| crates/state/src/geometry.rs:23,57 | `HORIZON_BLOCKS`, `MAX_READERS` | Other crates name them only in comments (`engine/src/actor/exec_settle.rs:26`, `executor/.../wiring.rs:54`). No code path reads them. | Either make them `pub(crate)` and drop the comments, or keep them `pub` and let the other crates import them instead of duplicating the number. |
| crates/state/src/trie/mod.rs:57,104,293 | `StateRoot` (and its two methods), `apply_trie_updates`, `rebuild_root` | No hit outside the crate. Only `TrieTables` and `update_for_block` cross the boundary (`validator/src/prover.rs:28`, `validator/tests/witness_anchoring.rs:28`). | `pub(crate)`. |
| crates/state/src/trie/prefix_set.rs:11-48 | `PrefixSet`, `from_b256s`, `from_nibbles`, `is_empty`, `contains_prefix` | No hit outside the crate. `proofs::account_proof_nodes` builds the set itself. | `pub(crate)`, and make `trie::prefix_set` a private module. |
| crates/state/src/trie/walker.rs:44 | `TrieUpdates` and its three `pub` fields | Produced and consumed inside `trie` only. | `pub(crate)`. |
| crates/state/src/trie/node.rs:21,40 | `encode_branch_node`, `decode_branch_node` | No hit outside the crate. | `pub(crate)`, and make `trie::node` a private module. |
| crates/state/src/checkpoint/mod.rs:53,156,179,200 | `CheckpointInfo` fields, `latest_checkpoint`, `restore_checkpoint`, `genesis_applied` (genesis.rs:56) | No hit outside the crate. Other crates call `restore_best_checkpoint`, `create_checkpoint`, `prune_checkpoints` and `has_state_db` only. | Keep `CheckpointInfo` `pub` (it is a return type), demote the four functions to `pub(crate)`. |
| crates/state/src/checkpoint/manifest.rs:35,91,127,194 | `CheckpointManifest` + fields, `manifest_path`, `read_manifest`, `verify_checkpoint` | No hit outside the crate. `restore_checkpoint` and the serve path are the only callers. | `pub(crate)`. |
| crates/state/src/checkpoint_transfer.rs:77,217 | `CheckpointServer` (`addr`, `task`), `fetch_latest_checkpoint` | `executor/.../state.rs:40` calls `serve_checkpoints` and discards the handle. `engine/bin_support.rs` calls `fetch_best_checkpoint`, never `fetch_latest_checkpoint`. | Demote `fetch_latest_checkpoint` to `pub(crate)`. Keep `CheckpointServer`, since `serve_checkpoints` returns it. |
| crates/state/src/writer/mod.rs:66 | `WriteBatch::approx_size_bytes` | Called only by `StateWriter::run`. | `pub(crate)` or private. |
| crates/state/src/integrity/mod.rs:48-57 | `IntegrityReport.headers/receipts/accounts/storage_slots/rebuilt_root` | Only the in-crate `kardamom-statecheck` binary and `integrity/tests.rs` read them. Outside the crate, `e2e` reads `problems`, `state_root`, `last_committed_block` only. | Keep the struct `pub`. The five fields could become `pub(crate)` with accessors, but a report type with `pub` fields is a fair exception. Low priority. |
| crates/types/src/xchain.rs:47,51,75,118,236,301 | `XCHAIN_SOURCE_DOMAIN`, `XCHAIN_ALIAS_TAG`, `xchain_leaf_domain`, `alias_remote_address`, `XChainMessage::aliased_sender`, `inbox_deliver_selector`, `INBOX_DELIVER_SIGNATURE` | Other crates use `xchain_tx_sender`, `deliver_calldata`, `msg_leaf`, `remote_source_hash`, `OUTBOX`, `INBOX`, `Callback`, `OutboxMessage`, `RemoteEpochRecord`, `derive_remote_epoch` only. | `types` is the shared crate, so keep the constants `pub` as protocol documentation. Demote `aliased_sender` (dead: `xchain_tx_sender` covers every caller). |
| crates/types/src/tx_ordering.rs:68-136 | `is_tx_ref`, `is_deposit_ref`, `is_boundary`, `is_epoch`, `is_remote_epoch`, `as_tx_ref`, `as_deposit_ref`, `as_epoch`, `as_remote_epoch`, `as_boundary` | No caller anywhere in the workspace. Consumers match on the enum directly. | Delete the ten accessors, or keep two and delete the rest. This is the largest block of dead `pub` API in `types`. |
| crates/types/src/epoch.rs:45,54,321 | `DOMAIN_USER_DEPOSIT`, `DOMAIN_SYSTEM_TX`, `deposit_from_lockbox_log` | No hit outside `crates/types/`. | Keep the two domain constants (protocol values). Demote `deposit_from_lockbox_log` to private; `derive_epoch` is its only caller. |
| crates/types/src/withdrawals.rs:20,26,29,151 | `OUTPUT_VERSION`, `LEAF_DOMAIN`, `NODE_DOMAIN`, `recompute_root` | No hit outside `crates/types/`. `recompute_root` is used only by this file's own tests. | Keep the three constants (they must match the L1 contracts). Move `recompute_root` behind `#[cfg(test)]`, or document it as the reference verifier. |
| crates/types/src/receipt.rs:204 | `Receipt::is_xchain` | No caller. Other crates compare `tx_type == TX_TYPE_XCHAIN` directly. | Either delete it, or make the direct comparisons use it. |
| crates/types/src/genesis.rs:87 | `GenesisError` | Callers use `.context(..)` on the `Result` and never name the type. | Keep `pub` — it is the error of a `pub fn`. Not a finding. |
| crates/types/src/prover.rs:155 | `BatchProverInput` | No hit outside `crates/types/`. | Keep if the batch guest is planned; otherwise delete. Confirm with the prover owner. |
| crates/types/src/wire.rs:27,69,111,158,200 | `AddressBytes`, `B256Bytes`, `U256Bytes`, `BytesVec`, `VecB256` | Used only by `#[rkyv(with = ...)]` attributes inside `types`. | Keep `pub`. Any crate that adds a wire type needs them, and the module doc says so. |

## R9 defensive validation

| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/state/src/checkpoint_transfer.rs:368-374 | `struct ResponseHead { status: u16, block: Option<u64>, content_length: Option<u64>, keccak: Option<B256>, genesis: Option<B256> }` | Four `Option` fields, each re-checked by the caller at lines 248, 253, 303 and 309 with `ok_or_else`/`let else`. The parse and the check sit in different functions. | Return an enum: `PeerResponse::NotFound` or `PeerResponse::Image(CheckpointHead { block: u64, len: u64, keccak: B256, genesis: B256 })`. The parser fails once; the fetch path then has no `Option` and no re-check. |
| crates/state/src/checkpoint_transfer.rs:223-225 | `let addr: SocketAddr = peer.parse().map_err(...)` | Every element of `peers: &[String]` is re-parsed on each call of `fetch_best_checkpoint`, once per retry. | Parse at config load into `Vec<SocketAddr>` (or a `PeerAddr` newtype in `types`) and pass typed values down. |
| crates/state/src/writer/mod.rs:198 | `assert!(matches!(self.env.raw().env_kind(), EnvironmentKind::Default \| EnvironmentKind::WriteMap));` | An assert on a runtime value in production code. It panics the writer thread, which strands every snapshot consumer. | Check the env kind in `StateEnvBuilder::open` and return `StateError::Recovery`. Better: have `StateEnv` carry a validated `EnvKind` field, so `run` cannot see a bad value. |
| crates/state/src/schema.rs:99-110, 154-175 and crates/state/src/trie/cursor.rs:63-70, 177-183 and crates/state/src/meta.rs:116-184 | `if bytes.len() != N { return Err(BadEncoding {..}) }` | The same fixed-width length checks appear in seven decoders. | Acceptable as boundary parses. Reduce the repetition with one `fn fixed<const N: usize>(table, bytes) -> Result<&[u8; N]>` helper that every decoder calls. |
| crates/state/src/integrity/checks.rs:103, 271 and crates/state/src/recovery/mod.rs:137, 156, 215 | `if k.len() != 8 {..}`, `if k.len() != 52 {..}`, `if k.len() != 20 {..}` | The SAME key-shape checks that `schema.rs` already owns, repeated in two more layers (the sweep and the bootstrap). Three copies of one rule. | Add typed key parsers in `schema`: `AccountKey::parse(&[u8]) -> Result<Address>`, `StorageKey::parse(&[u8]) -> Result<(Address, B256)>`, `BlockKey::parse(&[u8]) -> Result<u64>`. `checks` and `recovery` then call them and never see a raw length. |
| crates/state/src/checkpoint/mod.rs:75, 63-70 | `is_mdbx_data_name(name)`, `parse_checkpoint_block(name)` | Directory names are re-parsed by `latest_checkpoint`, `prune_checkpoints`, `sweep_stale_tmp`, `find_mdbx_data` and `has_state_db`. | One `enum CheckpointEntry { Checkpoint(u64), Tmp, Rejected, Other }` with a single `parse(&OsStr)`. Every scanner then matches on the enum. |
| crates/types/src/genesis.rs:72-83 | `pub fn validate(&self) -> Result<(), GenesisError>` | A separate step the caller must remember. Three call sites do (`engine/bin_support.rs:52`, `reconstruct/.../main.rs:70`, `deployer` tests); anything that forgets gets an unchecked `Genesis`. | Parse, do not validate: give `Genesis` a private constructor and a `ChainId(NonZeroU64)` field, and implement `Deserialize` through a raw shadow struct that runs the checks. `validate` then cannot be skipped, and `chain_id == 0` becomes unrepresentable. |
| crates/types/src/epoch.rs:146 (doc) with crates/da_watcher/src/rpc_source.rs:201, 226 | `mint: u128` / `if v > U256::from(u128::MAX) { .. }` | The narrowing rule lives in the watcher, but the type it protects lives in `types`. | Add `Mint(u128)` (or `U128Amount`) in `types` with `TryFrom<U256>`. `DepositLog.mint` takes it, and the watcher's two checks become one `try_into()`. |
| crates/types/src/xchain.rs:449-456 | `for m in &ordered { if m.dest_chain_id != self_chain_id { return Err(ForeignDestination{..}) } }` | Each message repeats a chain-id comparison that `da_watcher/src/interop/source.rs:123` also applies as a server-side filter, and `da_watcher/src/bin/kardamom-da-watcher.rs:167` applies again. | Add `ChainId(NonZeroU64)` in `types` and use it for `dest_chain_id`, `origin_chain_id`, `self_chain_id` and `Genesis::chain_id`. The equality check stays (it is a protocol rule), but the `u64 == u64` confusion between the three ids goes away. |
| crates/types/src/xchain.rs:388-390 | `self.first_seq + self.messages.len() as u64 - 1` | `last_seq` underflows when `messages` is empty. The doc says "non-empty by construction", but the struct derives `Default` and every field is `pub`. | Wrap the batch in a `NonEmpty<XChainMessage>` newtype with a fallible constructor, or drop the `Default` derive and make the field private behind `RemoteEpochRecord::new`. |
| crates/types/src/prover.rs:91-98, 185-194 | `if bytes.len() != Self::ENCODED_LEN { return None }` plus `if n > U256::from(u64::MAX) { return None }`, twice | The same length-and-range check in `PublicOutputs::decode` and `BatchPublicOutputs::decode`. | Take `&[u8; 160]` instead of `&[u8]`; the length check then belongs to the caller's `try_into()`. Share the "u256 word to u64" narrowing in one `fn word_u64(bytes, off) -> Option<u64>`. |
| crates/types/src/receipt.rs:13-19, 24-29, 134 | `pub const TX_TYPE_LEGACY: u8 = 0x00; ...` and `pub tx_type: u8` | A raw `u8` that six crates compare against three constants. Nothing stops an out-of-range value on the wire. | An `enum TxType { Legacy, Deposit, XChain, Other(u8) }` with `from_byte`/`as_byte`, kept rkyv-compatible. `is_deposit`, `is_xchain` and `tx_type_of` then return the enum, and the constant comparisons in `exec-core`, `ingress`, `reconstruct` and `e2e` disappear. |

Not flagged: `derive_epoch` and `derive_remote_epoch` return errors on gaps, duplicates and foreign logs.
Those are protocol rules a verifier must enforce, not defensive input checking.
Asserts inside `#[cfg(test)]` and `tests/` were skipped, as instructed.

## R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/types/src/epoch.rs:348-357 | `let mut ordered = Vec::with_capacity(logs.len()); for log in logs { if log.block_hash() != l1_hash { return Err(..) } ordered.push(log) }` | `let mut ordered: Vec<&LockboxLog> = logs.iter().map(\|l\| if l.block_hash() == l1_hash { Ok(l) } else { Err(EpochError::ForeignLog { expected: l1_hash, found: l.block_hash() }) }).collect::<Result<_, _>>()?;` |
| crates/types/src/xchain.rs:449-456 | `for m in &ordered { if m.dest_chain_id != self_chain_id { return Err(..) } }` | `if let Some(m) = ordered.iter().find(\|m\| m.dest_chain_id != self_chain_id) { return Err(XChainError::ForeignDestination { expected: self_chain_id, found: m.dest_chain_id }); }` — this also matches the `windows(2).find(..)` style already used at lines 426 and 443. |
| crates/types/src/genesis.rs:76-81 | `let mut seen = BTreeSet::new(); for entry in &self.alloc { if !seen.insert(entry.address) { return Err(..) } }` | `let mut seen = BTreeSet::new(); self.alloc.iter().try_for_each(\|e\| if seen.insert(e.address) { Ok(()) } else { Err(GenesisError::DuplicateAlloc(e.address)) })?` |
| crates/types/src/genesis.rs:46-68 | `let mut accounts = ..; let mut code = ..; for entry in &self.alloc { .. accounts.push(..); if let Some(c) = .. { code.push(..) } }` | Two chains over one pre-computed `code_hash`: `let hashed: Vec<_> = self.alloc.iter().map(\|e\| (e, code_hash_of(e))).collect();` then `hashed.iter().map(..).collect()` for accounts and `hashed.iter().filter_map(..).collect()` for code. |
| crates/state/src/trie/node.rs:51-54 | `let mut hashes = Vec::with_capacity(len); for _ in 0..len { hashes.push(cur.b256()?) }` | `let hashes: Vec<B256> = (0..len).map(\|_\| cur.b256()).collect::<Result<_, _>>()?;` |
| crates/state/src/checkpoint/mod.rs:160-172 | `let mut best: Option<CheckpointInfo> = None; for entry in rd { .. if .. best = Some(..) }` | `rd.map(\|e\| e.map_err(StateError::from)).collect::<Result<Vec<_>, _>>()?.into_iter().filter_map(\|e\| parse_checkpoint_block(..).map(\|b\| CheckpointInfo { block: b, path: e.path() })).max_by_key(\|c\| c.block)` |
| crates/state/src/checkpoint/mod.rs:183-192 | `let mut removed = 0; for entry in rd { .. removed += 1 }` | `rd.try_fold(0usize, \|n, entry\| { let entry = entry?; .. Ok(n + 1) })` — the `?` keeps working inside `try_fold`. |
| crates/state/src/recovery/mod.rs:135, 154, 172, 211 and crates/state/src/trie/mod.rs:158, 309 and crates/state/src/integrity/checks.rs:202 | `let mut out = Vec::new(); for_each_row(&txn, db, \|k, v\| { out.push(..); Ok(Continue(())) })?;` | The visitor API forces the mutable accumulator in seven places. Add `fn rows(txn, db) -> Result<impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), StateError>>>` next to `for_each_row`. Each site then becomes `rows(&txn, db)?.map(decode).collect::<Result<Vec<_>, _>>()?`, and `for_each_row` stays for the early-break callers. |
| crates/state/src/trie/mod.rs:200-203 | `let mut stor_by = BTreeMap::new(); for s in &delta.storage { stor_by.entry(s.address).or_default().push((s.key, s.value)) }` | `let stor_by = delta.storage.iter().fold(BTreeMap::<Address, Vec<_>>::new(), \|mut m, s\| { m.entry(s.address).or_default().push((s.key, s.value)); m });` — or keep the loop and extract it as `group_storage_by_account` (see R2). |
| crates/state/src/trie/mod.rs:206-227 | `let mut changed = Vec::with_capacity(changes.len()); for (slot, val) in changes { .. }` | Keep imperative. The loop writes to mdbx and returns early on error; only the `changed` vector is pure. Split it: `let changed: Vec<B256> = changes.iter().map(\|(slot, _)\| keccak256(slot)).collect();` then loop for the writes. |
| crates/state/src/bin/kardamom-statecheck.rs:78, 88, 121, 130 | `let mut failed = false; .. failed \|= !r.is_clean();` | Keep. The phases exit the process on hard errors and print as they go, so a fold would not read better. Worth extracting the phases into functions that return `bool` (see R2). |

Kept imperative, on purpose: `integrity/compare.rs:37-113` (a dual-cursor merge with resynchronization);
`trie/cursor.rs:137, 166` and `checkpoint_transfer.rs:377-391` (cursor and reader loops with `?` on each step);
`trie/mod.rs:158-176` (`del_prefix` must collect keys before deleting, because the cursor borrows the table);
`checkpoint_transfer.rs:355-363` (`fetch_best_checkpoint` logs each failure and raises the floor as it goes);
every `Vec<u8>` byte-buffer builder (`epoch.rs:86`, `xchain.rs:98, 119, 320, 397`, `upgrades.rs:63`,
`state/genesis.rs:41`, `state/schema.rs:76`, `trie/node.rs:22`) — these are encoders, and the push sequence is
the specification.

## Tests

### R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/types/tests/rkyv_roundtrip.rs:121 | `// #92 skip marker + the typed cause (#241): the reason must survive` | Two issue numbers. | Say what the test pins: "the skip reason must survive the wire, and the discriminant is append-only". |
| crates/state/src/trie/incremental_tests.rs:422 | `"stale orphan left at [n0,n1] after the subtree collapsed (F09.1)"` | Ticket id in an assertion message. | Drop `(F09.1)`. The message already names the fault. |
| crates/state/src/trie/incremental_tests.rs:426 | `//    the formerly orphaned region.` | "formerly" refers to an earlier step of the same test, so it reads as history. | Say "the region cleared in step 2". |
| crates/state/src/genesis.rs:338 | `// Simulate a legacy env by removing the stored digest.` | "legacy" names a past state of the code. | Say "simulate an env seeded before the digest key existed" — or better, "an env with no `genesis_digest` row". |
| crates/types/src/withdrawals.rs:273 | `// A tampered field (value) also no longer matches the carried hash.` | Reads as a change note. | Say "a tampered value does not match the carried hash either". |

### R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/state/src/trie/incremental_tests.rs | 382 | KEEP. Under the limit, and the model-versus-trie harness (`step`, `model_root`) is shared by every case in it. |
| crates/types/tests/rkyv_roundtrip.rs | 324 | KEEP. One test per wire type. Splitting would scatter the shared fixtures. |
| crates/state/src/checkpoint/tests.rs | 260 | KEEP. Cohesive around one `commit_blocks` + `create_checkpoint` harness. |

### R6 dynamic dispatch

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/state/src/trie/incremental_tests.rs:332 | `let mine = \|pred: &dyn Fn(&[u8; 5]) -> bool\| -> Address {` | stored `dyn Fn` closure | Make `mine` a `fn mine<P: Fn(&[u8; 5]) -> bool>(pred: P) -> Address`. A test helper has no reason to box a predicate. |

### R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/state/src/trie/incremental_tests.rs:127-133 | `let mut out = Vec::new(); while let Some((k, v)) = item { out.push((k, v)); .. }` | Keep. It is a raw mdbx cursor walk in `dump_table`; the iterator does not exist yet. Worth reusing the `rows(..)` helper proposed in R10 above. |
| crates/state/tests/concurrent_readers.rs:25-37 | `let mut snapshots = Vec::new(); for block in 1..=4u64 { .. snapshots.push(..) }` | `let snapshots: Vec<_> = (1..=4u64).map(\|block\| { writer.delta_tx.send(..).unwrap(); writer.snapshot_rx.recv().unwrap() }).collect();` |
| crates/state/tests/concurrent_readers.rs:41-54 | `let mut handles = Vec::new(); for (i, snap) in .. { handles.push(thread::spawn(..)) }` | `let handles: Vec<_> = snapshots.into_iter().enumerate().map(\|(i, snap)\| thread::spawn(move \|\| { .. })).collect();` |
| crates/state/src/writer/tests.rs:91-113 | `let mut model_accts = BTreeMap::new(); .. for delta in &blocks { for s in &delta.storage { .. } for a in &delta.accounts { .. } }` | Keep. The loop also sends each batch to the writer, so the model build and the side effect share one pass. |
| crates/state/src/trie/incremental_tests.rs:198-236 | `let mut m_accts .. let mut ops .. let mut stor_ops ..` | Keep. This is the random-walk model harness; the mutation sequence is the test. |
