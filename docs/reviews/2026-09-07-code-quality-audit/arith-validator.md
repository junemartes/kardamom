# validator

## Summary
The crate does no division at all, so there are 0 divisions by a possibly-zero value in
production code and 0 in tests. The arithmetic is almost all `+ 1` on a u64 cursor: a block
number, an L1 origin number, an outbox `seq`, or a bal index. Three cursor families take their
operand straight from the wire and are the real hot spots: the attester cadence
(`last_attested` comes from the L1 oracle, `interval` from a CLI flag, added plainly at
attester.rs:306), the remote-epoch density check (`first_seq` comes from the canonical stream,
added to an index at verify.rs:145), and the outbox serve cursors (serve.rs:85, store.rs:75).
The parallel chunk ranges are safe: `batch_ranges` derives both ends from `txs.len()`, so
every slice index and every `- 1` in engine.rs is bounded by a Vec length. R12 gives 36 sites:
17 FIX, 2 HOT_PATH_KEEP, 17 PROVEN. R13 gives 13 sites; the strongest is the wire granularity
K, a `u16` that three call sites clamp with `.max(1)` and that feeds `chunk_of`, which divides
by it.

## R12 safe arithmetic
| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| src/parallel/engine.rs:165 | `let bal_index = first_index + i as u64;` | batch range, loop index | HOT_PATH_KEEP | Per-record loop. `first_index <= txs.len()` and `i < records.len()`, both Vec lengths. |
| src/parallel/engine.rs:166 | `let global_index_in_block = bal_index - 1;` | derived index | PROVEN | `batch_ranges` emits `start + 1`, so `first_index >= 1` and `bal_index >= 1`. |
| src/parallel/engine.rs:195 | `let last_index = first_index + records.len() as u64 - 1;` | batch range, slice len | PROVEN | Callers pass `first_index >= 1` and a non-empty slice from `batch_ranges`. Note: `execute_batch` is `pub`, so an outside caller with `first_index == 0` and an empty slice wraps. |
| src/parallel/engine.rs:279 | `granularity as usize` (also 381) | wire BalFrame | PROVEN | u16 to usize is a widening cast on every supported target. |
| src/parallel/engine.rs:296 | `crate::metrics::counter_fork_fallback(refused as u64);` | worker count | PROVEN | usize to u64 widening; `refused <= pool.workers() <= 40`. |
| src/parallel/engine.rs:307 | `let slice = &txs[(from as usize - 1)..(to as usize)];` (also 390) | batch range | PROVEN | `batch_ranges(txs.len(), ..)` built `from`/`to` from the same `txs.len()`; `from >= 1`. |
| src/parallel/engine.rs:344 | `cumulative += r.gas_used;` (also 423) | own re-execution receipts | HOT_PATH_KEEP | Per-receipt fold over one block. The executor bounds the block's total gas by the block gas limit. |
| src/parallel/claims.rs:271 | `let end = (start + bs).min(n);` | CLI batch size, slice len | PROVEN | `start > 0` implies `start` is a multiple of `bs` below `n`, so `bs < n <= isize::MAX`. |
| src/parallel/claims.rs:272 | `((start + 1) as u64, end as u64)` | slice len | PROVEN | `start < n <= isize::MAX`; usize to u64 is widening. |
| src/attester.rs:150 | `Ok(n.try_into().unwrap_or(u64::MAX))` | L1 `outputCount()` | FIX (checked) | Opposite mistake. A U256 count clamps to `u64::MAX`; `latest_attested_block` then runs `(0..u64::MAX).rev()` with one RPC per index. Return an `AttesterError` instead. |
| src/attester.rs:290 | `let carry = self.last_attested + 1;` | L1 oracle block number | FIX (saturating) | `last_attested` comes from `o.l2BlockNumber` on L1. Use `saturating_add(1)`. |
| src/attester.rs:306 | `block >= self.last_attested + self.interval` | L1 oracle + CLI flag | FIX (saturating) | Both operands are external. A wrap makes `due()` true and posts an output out of cadence. Use `saturating_add`. |
| src/attester.rs:325 | `self.own_attest_floor.get_or_insert(self.last_attested + 1);` | L1 oracle block number | FIX (saturating) | Same operand as line 290. |
| src/attester.rs:327 | `self.pending = self.pending.split_off(&(block + 1));` (also 328) | attested block number | FIX (saturating) | A wrap to 0 keeps every pending leaf instead of clearing it. |
| src/attester.rs:450 | `let tail = self.pending.split_off(&(block + 1));` | receipt-stream block number | FIX (saturating) | A wrap to 0 flushes nothing and leaks the pending map. |
| src/epoch_verify.rs:231 | `if got != previous + 1 {` | L1 origin numbers | PROVEN | Line 228 already returned when `got <= previous`, so `previous < u64::MAX`. |
| src/epoch_verify.rs:277 | `attempt += 1;` | local retry counter | PROVEN | The loop breaks at `attempt >= VERIFY_ATTEMPTS` (8). |
| src/epoch_verify.rs:377 | `&& prev_number + 1 == epoch.l1_number` | verified epoch's L1 number | FIX (checked) | `prev_number` is a wire value carried in the anchor. Use `checked_add(1)` and skip the parent check on `None`. |
| src/interop/extract.rs:282 | `let bal_index = tx_index + 1;` | own receipt index | PROVEN | `receipt.transaction_index` is a position in the block's record vector. |
| src/interop/serve.rs:70 | `skipped: floor - next,` (also 113) | store floor, subscriber cursor | PROVEN | The enclosing `if next < floor` guards it. |
| src/interop/serve.rs:85 | `next = m.seq + 1;` | outbox message seq | FIX (saturating) | `seq` comes from the Outbox event through the store. Use `saturating_add(1)`. |
| src/interop/serve.rs:134 | `next = block + 1;` | attestation ring block | FIX (saturating) | Same cursor shape as line 85. |
| src/interop/sink.rs:80 | `let tail = self.pending.split_off(&(block + 1));` | boundary block number | FIX (saturating) | A wrap to 0 flushes nothing, so no block reaches the feed. |
| src/interop/store.rs:75 | `lane.floor = front.seq + 1;` | outbox message seq | FIX (saturating) | `seq` is wire-derived. A wrap sets the retention floor to 0 and hides the loss. |
| src/interop/store.rs:80 | `self.items.send_modify(\|v\| *v += 1);` (also 133) | local version counter | FIX (wrapping) | Wrap is the meaning for a wake-up version. Say so with `wrapping_add(1)`. |
| src/interop/verify.rs:145 | `let want_seq = rec.first_seq + i as u64;` | canonical-stream record | FIX (checked) | `first_seq` is wire data. A wrap lets a crafted record pass the density check. Return `RemoteEpochFault::NonDense` on `checked_add`. |
| src/interop/verify.rs:204 | `.insert(rec.origin_chain_id, rec.last_seq() + 1);` | canonical-stream record | FIX (checked) | `last_seq()` is itself `first_seq + len - 1` on wire data. Use `checked_add` and treat `None` as a fault. |
| src/prover.rs:210 | `let next = *pending.get_or_insert(at + 1);` (also 223, 262) | committed block number | PROVEN | `at` is this validator's own committed block number, monotone from genesis. |
| src/prover.rs:211 | `h.block_number() != next - 1` (also 212) | derived cursor | PROVEN | `pending` is only ever set from `at + 1` or `next + 1`, so `next >= 1`. |
| src/prover.rs:217 | `counter_prover_skipped((at + 1) - next);` | committed block numbers | PROVEN | Reached only under `at >= next`, so the difference is non-negative. |
| src/prover.rs:234 | `if at >= next + 2 {` | committed block number | FIX (saturating) | Cheap; the comparison is off the hot path. Use `next.saturating_add(2)`. |
| src/buffers.rs:112 | `let deadline = std::time::Instant::now() + timeout;` | const Duration | PROVEN | Every caller passes a compile-time constant (250 ms or 2 s). |
| src/buffers.rs:131 | `head.index() > key.index() + self.lookbehind` | block number, const 16 | FIX (saturating) | `key.index()` is a requested block or `tx_idx`. Use `saturating_add`. |
| src/bin/kardamom-validator/adoption.rs:92 | `elapsed_ms = started.elapsed().as_millis() as u64,` | clock | FIX (try_from) | u128 to u64 narrowing on a clock value. Use `u64::try_from(..).unwrap_or(u64::MAX)`. |
| src/metrics.rs:103 | `.increment(n as u64);` | message count | PROVEN | usize to u64 widening. |
| src/metrics.rs:113 | `.record(batches as f64);` (also 144, 152) | counts, block number | PROVEN | Telemetry only. f64 holds every value below 2^53. |

## R13 non-zero types
| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| src/parallel/engine.rs:194 | `let k = u64::from(granularity.max(1));` | wire attribution granularity K | `NonZeroU16` in `kardamom_types::BalFrame::granularity`, parsed at src/bin/kardamom-validator/pumps.rs:88-110 | Strongest finding. `k` feeds `bal_ladder::chunk_of`, which does `index.div_ceil(k)`. Only `.max(1)` keeps that from dividing by zero. A K of 0 on the wire silently becomes per-tx verification against a frame the executor collapsed differently, which breaks the module's stated same-view invariant. Reject K = 0 at the decode. |
| src/parallel/engine.rs:277 | `let k = u64::from(granularity.max(1));` | wire granularity K | same as above | Second of three clamps of the same wire field. |
| src/parallel/engine.rs:379 | `let k = u64::from(granularity.max(1));` | wire granularity K | same as above | Third clamp, in the scoped reference path. |
| src/parallel/engine.rs:474 | `WorkerPool::new(workers.max(1), Vec::new())` | pool worker count | `NonZeroUsize` parameter, resolved at src/bin/kardamom-validator/main.rs:378-383 | The binary already maps 0 to auto and caps at 40, so this `.max(1)` is dead defence. Make `parallel_block_exec` take `NonZeroUsize`. |
| src/parallel/claims.rs:267 | `let bs = batch_size.max(1);` | txs per parallel batch | `NonZeroUsize`, clap `--validation-batch-size` at src/bin/kardamom-validator/args.rs:147-148 | Nothing rejects `--validation-batch-size 0`; it silently becomes 1, so batching is off with no log line. |
| src/attester.rs:231 | `interval: interval.max(1),` | post cadence in blocks | `NonZeroU64`, clap `--attester-post-interval` at src/bin/kardamom-validator/args.rs:156-157 | `--attester-post-interval 0` silently becomes 1 and posts an L1 output every block. |
| src/attester.rs:190 | `pub post_interval_blocks: u64,` | post cadence in blocks | `NonZeroU64` in `AttesterConfig`, filled at src/bin/kardamom-validator/main.rs:266 | The doc on line 189 says "(>= 1)". The type should say it instead. |
| src/interop/store.rs:49 | `retention_blocks: retention_blocks.max(1),` | feed retention window | `NonZeroU64`, clap `--feed-retention-blocks` at src/bin/kardamom-validator/args.rs:170-171 | A 0 window would prune every message on arrival; the clamp hides the misconfiguration. |
| src/interop/store.rs:118 | `retention_blocks: retention_blocks.max(1),` | attestation retention window | same as above | Second store built from the same flag (main.rs:190 and 193). |
| src/buffers.rs:75 | `fn new(cap: usize, lookbehind: u64) -> Self` | buffer capacity | `NonZeroUsize` parameter | `while g.map.len() > self.cap` at line 96 drains the map to empty when `cap` is 0. Every production caller passes a const (1024 or 1 << 16), so this is latent, not live. |
| src/bin/kardamom-validator/args.rs:79 | `pub trie_shadow_check: Option<u64>,` | shadow-check period | `Option<NonZeroU64>`, clap `--trie-shadow-check` | `--trie-shadow-check 0` passes `Some(0)` through main.rs:229-231 into `TrieMode::ShadowCheck`, where crates/state/src/writer/mod.rs:393 guards it with `every_n != 0` and silently disables the canary the operator asked for. |
| src/bin/kardamom-validator/args.rs:38 | `pub shards: u8,` | tx_data shard count | `NonZeroU8`, clap `--shards` | `--shards 0` opens no tx_data subscription (main.rs:137), so the validator sees no transactions and reports no error. |
| src/bin/kardamom-validator/args.rs:46 | `pub chain_id: u64,` | L2 chain id | `NonZeroU64`, clap `--chain-id` | A chain id of 0 is not a valid chain; it reaches `resolve_genesis` at main.rs:104 unchecked. |

## Tests
None found. The crate contains no division operator in either production or test code, so no
test divides by a possibly-zero value.
