# engine

## Summary

The engine crate has no integer division and no `%` operator, so it holds 0 divisions by a
possibly-zero value. The two cursor advances in `reader/cluster/mod.rs` hold the most risk;
both take wire-decoded operands. Two block-number advances in `actor/exec_thread.rs` and the
batch cursor in `actor/commit_thread.rs` trust the persisted cursor, the wire, and an
external trait. R12 gives 15 FIX, 5 HOT_PATH_KEEP, and 17 PROVEN verdicts. R13 lists 11
sites; 3 are defects today, and depth K stays non-zero only by a literal. The join buffer
has no size bound at all; see the note after the tables.

## R12 safe arithmetic

| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| reader/cluster/mod.rs:133 | `ni + slot_width(&msg)` | wire: `1 + messages.len()` of a decoded epoch | FIX | `checked_add` -> `ExecutorError::State`. A wrap rewinds the cursor and re-delivers records |
| reader/cluster/mod.rs:108 | `self.cursor.next_block.store(nb + 1, ...)` | block cursor, advanced per boundary | FIX | `saturating_add` |
| actor/exec_thread.rs:878 | `self.current_block = block_number + 1;` | `BlockBoundaryStart.block_number`, from the wire | FIX | `saturating_add`, or `checked_add` -> fail-stop |
| actor/exec_thread.rs:236 | `current_block: start.block + 1,` | `ResumePoint.block`, read from the state DB | FIX | `saturating_add` |
| bin_support.rs:355 | `ReplayCursor::new(start.record_count, start.block + 1)` | the same persisted cursor | FIX | `saturating_add` |
| actor/commit_thread.rs:118 | `from += published;` | `published`, returned by a `TxReceiptsPublication` impl | FIX | `from = from.saturating_add(published).min(receipts.len())`. An over-large count drops the unpublished suffix |
| bin_support.rs:236 | `self.tx_data_stream_base + shard_id as i32` | `ChannelsConfig` base plus a shard index | FIX | `checked_add` -> `Err(String)`. A base near `i32::MAX` wraps to a negative stream id. The `shard_id as i32` cast widens from `u8`: PROVEN |
| replay.rs:225 | `cumulative_gas += receipt.gas_used;` | execution result; replay applies no block gas limit | FIX | `checked_add` -> `ReplayError`. The value goes into every receipt |
| replay.rs:251 | `cumulative_gas += receipt.gas_used;` | the same | FIX | the same |
| reader.rs:744 | `let deadline = Instant::now() + cfg.join_timeout;` | config `Duration` | FIX | `Instant::checked_add`. `Instant + Duration` gives a panic instead of a typed error |
| reader.rs:820 | `let deadline = Instant::now() + timeout;` | derived from the same config | FIX | `Instant::checked_add` |
| reader.rs:535 | `timeout_ms = cfg.join_timeout.as_millis() as u64` | config `Duration`, `u128` -> `u64` | FIX | `u64::try_from(..).unwrap_or(u64::MAX)`. A log field only |
| reader.rs:541 | `timeout_ms: cfg.join_timeout.as_millis() as u64` | the same, into `ExecutorError::JoinTimeout` | FIX | the same |
| reader.rs:777 | `recovered += 1;` | one per envelope fed by an archive refetch | FIX | `saturating_add`. A cold diagnostic counter |
| actor/commit_thread.rs:51 | `*attempts += 1;` | retry counter, one per 50 ms sleep | FIX | `saturating_add`. A wrap to 0 re-fires the "attempt 1" log |
| actor/exec_thread.rs:376 | `self.tx_index_in_block += 1;` | per-block record counter | HOT_PATH_KEEP | reset to 0 at exec_thread.rs:880; bounded by one block's records |
| actor/exec_thread.rs:441 | `(&mut self.block_bal, self.tx_index_in_block + 1)` | the same counter | HOT_PATH_KEEP | the same bound |
| actor/exec_thread.rs:538 | `(&mut self.block_bal, self.tx_index_in_block + 1)` | the same counter | HOT_PATH_KEEP | the same bound |
| actor/exec_thread.rs:661 | `(&mut self.block_bal, self.tx_index_in_block + 1)` | the same counter | HOT_PATH_KEEP | the same bound |
| replay.rs:227,253 | `tx_index_in_block += 1;` | per-block counter | HOT_PATH_KEEP | reset at replay.rs:196 for each block |
| reader.rs:561,590,606,653,669 | `next_tx_idx = next_tx_idx.next()` | `TxIndex(self.0 + 1)`, exec-core/src/exec_types.rs:25 | PROVEN | `u64` at 100k records/s needs 5.8 million years. The `+ 1` sits out of group; a `checked_add` there is still cheap insurance |
| actor/exec_thread.rs:419,522 | `self.current_block.saturating_sub(1)` | block counter | PROVEN | The opposite-mistake check: `current_block >= 1` from exec_thread.rs:236 and :878, so the clamp hides nothing |
| reader.rs:794 | `deadline.saturating_duration_since(Instant::now())` | join deadline | PROVEN | The opposite-mistake check: a past deadline must give zero, and the caller tests `slice.is_zero()` at reader.rs:795 |
| actor/exec_thread.rs:543 | `self.shadow_serial += 1;` | one per deposit, shadow on only | PROVEN | `u32`, reset by `std::mem::take` at exec_thread.rs:849 each boundary |
| actor/exec_thread.rs:378 | `*..get_or_insert(Duration::ZERO) += apply_start.elapsed()` | per-tx wall time | PROVEN | `.take()` at exec_thread.rs:757 resets it each boundary |
| actor/exec_thread.rs:726 | the same expression, whole-block arm | per-block wall time | PROVEN | the same reset |
| replay.rs:207,208,228,229,230,254,255,256,290 | `counters.* += 1;` | one per replayed record or block | PROVEN | `u64` range against one increment per record |
| reader.rs:550 | `cur > last_warn_len * 2` | `cur = buffer.len()`, a DashMap entry count | PROVEN | every entry holds at least one word, so `len < usize::MAX / 2` always |
| shadow.rs:74 | `Vec::with_capacity(ws.accounts.len() + ws.storage.len())` | two live map lengths | PROVEN | both maps stay resident at once, so the sum fits `usize` |
| shadow.rs:145,164,165,166,168 | `i as u64`, `g.missed_pairs as u64`, ... | `usize` counts | PROVEN | `usize` -> `u64` widens on the 64-bit build targets |
| shadow.rs:169,170,171 | `g.predicted_waves as f64`, ... | `usize` per-block counts | PROVEN | exact below 2^53; metrics gauges only |
| actor/exec_settle.rs:58 | `.set(b.block_number as f64)` | `u64` block number | PROVEN | exact below 2^53 |
| reader/cluster/mod.rs:115 | `.set(b.block_number as f64)` | `u64` block number | PROVEN | exact below 2^53 |
| reader.rs:1009,1011,1067,1122,1186,1196 | `0xD0 + i`, `1_000 + i as u128`, `1 + i as u64` | test loop index | PROVEN | loop ranges `0..3` and small literal counts (test code) |
| reader/cluster/tests.rs:11,36,106 | `off as u8`, `i as i32` | test loop index | PROVEN | loop range `0..5` (test code) |
| actor/commit_tests.rs:162,233 | `receipt(i as u8, i * 64)` | test loop index | PROVEN | loop ranges `0..5` and `0..6` (test code) |
| persist.rs:163 | `l2_timestamp: 1_700_000_000 + block_number` | test block number | PROVEN | callers pass 1 and 2 (test code) |

Seed note: `bin_support.rs:293` (`as Box<dyn JoinRecovery>`) is a trait-object coercion, not
a numeric cast. It is out of scope for R12.

## R13 non-zero types

| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| reader.rs:253 | `pub dedup_window: usize` | dedup window capacity | `NonZeroUsize`; the boundary is `ReaderConfig::default()` at reader.rs:263 and every struct literal | DEFECT: the field is `pub` and has no guard. With 0, `DedupWindow::first_seen` evicts the id it just inserted (reader.rs:432), so dedup goes silently OFF. Each duplicate TxRef then misses the join, because the first take removed the envelope. Without archive recovery the reader dies with `JoinTimeout`; with recovery the refetch re-inserts, the duplicate executes, and the next boundary fails with `BoundaryMisaligned` |
| reader.rs:414 | `capacity: usize` in `DedupWindow` | window capacity | `NonZeroUsize`; the boundary is `DedupWindow::new` at reader.rs:418 | The same value, one layer down. Type it here and the config field's type follows |
| actor/types.rs:74 | `pub receipt_queue_depth: usize` | channel capacity | `NonZeroUsize`; the boundary is `ExecutorConfig::default()` at actor/types.rs:105 and the binaries' struct literals | Feeds `bounded(n)` at actor.rs:139 and actor.rs:140. `bounded(0)` builds a rendezvous channel, so the reader-to-exec hop turns lock-step. No guard, no doc |
| actor/exec_settle.rs:29 | `const COMMIT_PIPELINE_DEPTH: usize = 4;` | commit pipeline depth K | `NonZeroUsize` (`NonZeroUsize::new(4).unwrap()`); no parse boundary today | The literal keeps it non-zero, so it is safe now. With 0 the guard at exec_settle.rs:93 (`inflight.len() >= 0`) always holds. The exec thread then waits in `wait_committed` at every boundary, and the pipeline drops to depth 1 with no signal. Type it before it moves into config |
| bin_support.rs:196 | `shards: u8` in `open_tx_data_subs` | tx_data shard count | `NonZeroU8`; the boundary is the clap arg `--shards` (executor args.rs:41, `default_value_t = 8`; also the validator and batcher) | DEFECT: `--shards 0` passes. `(0..shards)` at bin_support.rs:198 yields no reader thread, so the join buffer never fills. Without archive recovery every TxRef dies with `JoinTimeout` after the full 60 s budget; with recovery every join limps through a refetch |
| bin_support.rs:70 | `.unwrap_or(chain_id_flag)` in `resolve_genesis` | L2 chain id | `NonZeroU64`; the boundary is `resolve_genesis` at bin_support.rs:59 | DEFECT: `Genesis::validate` rejects `chain_id == 0` (types/src/genesis.rs:73). The no-genesis branch returns the flag unchecked, so `--chain-id 0` reaches every `CfgEnv` |
| actor/types.rs:71 | `pub chain_id: u64` | L2 chain id | `NonZeroU64`; the same boundary as above | Type the field and the missing branch check above becomes impossible to write |
| reader.rs:243 | `pub join_poll_interval: Duration` | poll period | a validated newtype, or a clamp at the `ReaderConfig` boundary | `Duration::ZERO` turns the wait loop at reader.rs:822 into an unthrottled spin for the whole join budget |
| reader.rs:246 | `pub buffer_warn_threshold: usize` | warn threshold | none needed | Zero only widens the warn window. Accounted for, not a candidate |
| actor/types.rs:59 | `self.block > 0` | resume cursor | none needed | A genesis test, not a zero guard before a division. Zero carries meaning here. Accounted for, not a candidate |
| actor/exec_thread.rs:844 | `self.shadow_serial > 0` | serial-lane count | none needed | An emptiness test that skips an empty block's handoff. Accounted for, not a candidate |

## Tests

None found. The crate has no integer division and no `%` operator in any file.

## Join buffer bounds (focus note)

`JoinBuffer` (reader.rs:132) wraps a plain `DashMap` with no capacity. Its doc claims the
in-flight window bounds its size. Nothing in this crate enforces that.
`buffer_warn_threshold` (reader.rs:550) only writes a log line. The doc at reader.rs:245
says so: "This applies no back-pressure". Two paths grow the map with no matching take.

1. The archive refetch at reader.rs:775 inserts every envelope the recovery yields from
   `from` onward. The caller takes exactly one. The rest stay until a later TxRef claims
   them. The refetch repeats on each `join_refetch_after` slice inside the join budget, so
   one persistent miss can insert the same range many times.
2. A tx_data reader inserts an envelope for every fragment it sees. An envelope whose TxRef
   never arrives is never removed.

The map has no eviction, no TTL, and no cap. A sustained producer-consumer skew is therefore
an unbounded-memory path. It ends in an OOM kill, not a bounded failure. The cluster reader
shows the right shape: `MAX_PENDING` (reader/cluster/mod.rs:54) with a hard error at
reader/cluster/mod.rs:224. Give `JoinBuffer` the same treatment.
