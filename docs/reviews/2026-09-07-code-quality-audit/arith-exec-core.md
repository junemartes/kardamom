# exec-core

## Summary

The group has four hot spots. The first is `crates/exec-core/src/bal_ladder.rs:87`: a public
`div_ceil(k)` with no guard on `k == 0`. It is the one unguarded division by a possibly-zero
value in the group, and its `k` comes from a wire field (`granularity: u16`) that reaches
`crates/exec-core/src/stateless.rs:275` straight from the untrusted prover input. The second
is `guest/kardamom-zk-guest/src/bin/batch.rs:49`: the batch guest proves block contiguity with
`first_block + i as u64` on two untrusted u64 fields, and the guest release profile sets no
`overflow-checks`, so a wrapped range passes the assertion. The third is footprint's ratio and
percentage math: nine f64 divisions, four `.max(1)` clamps on divisors, and one saturating
float-to-integer cast at `crates/footprint/src/oracle.rs:176` that turns any out-of-range
`--train-frac` into a silent all-train or all-holdout split. The fourth is the five
`cumulative_gas_used_before + gas_used` sites; exec-core enforces no cross-tx block gas cap,
so the only bound is the per-tx EIP-7825 cap times the tx count. The sparse trie's nibble
arithmetic is clean: every `depth + n` and `common + 1` is bounded by the 64-nibble key width,
and every `nibble as usize` is bounded by the `Nibbles` type. `crates/reconstruct` contains no
integer arithmetic at all. Counts: R12 lists 34 rows over about 90 sites (7 FIX, 8 HOT_PATH_KEEP,
19 PROVEN); R13 lists 14 rows with 1 defect; Tests finds no division by a possibly-zero value.

## R12 safe arithmetic

| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| exec-core/src/executor/scope.rs:218 | `cumulative_gas_used_before + gas_used` | running block counter + EVM gas | FIX | `checked_add` -> `ExecutorError::Execution`. No block gas cap runs across txs here. |
| exec-core/src/executor/scope.rs:318 | `cumulative_gas_used_before + gas_used` | same (xchain path) | FIX | checked_add, same reason |
| exec-core/src/executor/scope.rs:525 | `cumulative_gas_used_before + gas_used` | same (tx path) | FIX | checked_add, same reason |
| exec-core/src/executor/deposit.rs:139 | `cumulative_gas_used_before + gas_used` | same (free deposit fn) | FIX | checked_add, same reason |
| exec-core/src/executor/xchain.rs:141 | `cumulative_gas_used_before + gas_used` | same (free xchain fn) | FIX | checked_add, same reason |
| exec-core/src/executor/xchain.rs:102 | `message.gas_limit.saturating_add(XCHAIN_DELIVERY_OVERHEAD)` | wire XChainMessage | FIX | Wrong clamp. Reject a `gas_limit` above `BLOCK_GAS_LIMIT` at derivation, then plain add. |
| exec-core/src/executor/tx_env.rs:139 | `message.gas_limit.saturating_add(...)` | wire XChainMessage | FIX | same clamp, second copy; keep the two in step |
| exec-core/src/exec_types.rs:25 | `TxIndex(self.0 + 1)` | monotone global tx counter | FIX | `checked_add(1).expect(...)`. Saturating is wrong: it would repeat an id. |
| guest/kardamom-zk-guest/src/bin/batch.rs:49 | `first_block + i as u64` | untrusted rkyv `BatchProverInput` | FIX | `checked_add` + assert. Guest release profile has no `overflow-checks`; a wrapped range passes the contiguity assert and yields `first_block > last_block`. |
| footprint/src/oracle.rs:176 | `((max_block as f64) * train_frac).ceil() as u64` | clap `--train-frac`, unvalidated f64 | FIX | Validate `train_frac` in `0.0..=1.0` at the clap boundary. The `as u64` cast saturates, so NaN and negatives silently give split 0. |
| footprint/src/classifier.rs:136 | `if *n * 10 >= e.observations * 6` | monotone per-selector u64 counters | HOT_PATH_KEEP | `n <= observations`; `observations` grows one per tx, so `*6` needs 3.07e18 txs. u128 widening removes the argument for free. |
| footprint/src/classifier.rs:142 | `if *n * 10 >= e.observations * 6` | same | HOT_PATH_KEEP | same bound |
| footprint/src/classifier.rs:172 | `if *n * 10 >= e.observations * 6` | same; `predict_domains` per-tx admission | HOT_PATH_KEEP | same bound; this one is the true hot path |
| footprint/src/classifier.rs:177 | `if *n * 10 >= e.observations * 6` | same | HOT_PATH_KEEP | same bound |
| footprint/src/classifier.rs:195 | `if *n * 10 >= e.observations.max(1) * 6` | same, report path | HOT_PATH_KEEP | same bound. See R13: the `.max(1)` also makes this predicate differ from lines 136 and 142. |
| exec-core/src/delta.rs:87-92 | `1 + 10 + accounts.len() * 95 + 10 + storage.len() * 85 + 10` | per-tx write set sizes | HOT_PATH_KEEP | A tx write set holds a few entries; usize overflow needs 2e17 entries. Overflow would panic in `BufSink::put`, not corrupt. |
| exec-core/src/delta.rs:226-227 | `self.buf[self.n..self.n + bytes.len()]`; `self.n += bytes.len()` | encoder sink | HOT_PATH_KEEP | The `need <= INLINE` test at line 93 bounds the total at 1024 bytes. |
| exec-core/src/features.rs:78 | `U256::from(bn) << 64 \| U256::from(ts) << 128` | block number, timestamp | PROVEN | Each field is a u64 in a 64-bit lane of a U256; 64+128 < 256. The doc's "fields saturate" claim is wrong; the u64 type is what bounds them. |
| exec-core/src/features.rs:84 | `((word >> shift) & mask).to::<u64>()` | packed beacon word | PROVEN | `shift` is a const 0, 64 or 128; the mask is `u64::MAX`. |
| exec-core/src/features.rs:140 | `beats.saturating_add(1)` | health beat counter | PROVEN | Correct use. A monotone health counter must freeze, not wrap. |
| exec-core/src/anchor/sparse.rs:147 | `Self::lookup_in(child, path, depth + key.len(), store)` | trie walk depth | PROVEN | `rest.starts_with(key)` at line 146 gives `key.len() <= 64 - depth`. |
| exec-core/src/anchor/sparse.rs:218 | `Self::insert_in(Some(taken), path, depth + klen, ...)` | same | PROVEN | `rest.starts_with(key)` at line 215 |
| exec-core/src/anchor/sparse.rs:298 | `Self::remove_in(*child, path, depth + klen, store)` | same | PROVEN | `rest.starts_with(&key)` at line 294 |
| exec-core/src/anchor/sparse.rs:157,254,314 | `depth + 1` | trie walk depth | PROVEN | `rest.first()` is `Some`, so `depth < 64`. |
| exec-core/src/anchor/sparse.rs:201,205,225,236 | `key.slice(common + 1..)` | common prefix length | PROVEN | Keys are 32-byte secure-trie words, so `common < key.len() <= 64`. |
| exec-core/src/anchor/sparse.rs:156,209,210,240,241,249,307 | `children[nibble as usize]` | trie nibble | PROVEN | `Nibbles` yields 0..=15 only; the array is 16 wide. |
| exec-core/src/anchor/sparse.rs:63,341,418 | `i as u8` | 0..16 loop index | PROVEN | The loop runs over a 16-element array. |
| exec-core/src/delta.rs:158,173,263 | `&bal[32 - blen..]`; `(bytes, 32 - lead)` | minimal-width encoder | PROVEN | `lead <= 32` from a `take_while` over 32 bytes, so `blen <= 32`. |
| exec-core/src/delta.rs:156,168 | `blen as u8 \| (code_tag << 6)` | balance byte length | PROVEN | `blen <= 32` fits the low 6 bits; `code_tag <= 2`. |
| exec-core/src/delta.rs:244,248,252 | `(v & 0x7f) as u8`; `n += 1` | LEB128 varint | PROVEN | `v >>= 7` on a u64 ends within 10 rounds; the buffer is 10 bytes. |
| exec-core/src/delta.rs:145,163,176,179 | `self.accounts.len() as u64` and 3 more | collection lengths | PROVEN | usize to u64 widens on every supported target. |
| exec-core/src/stateless.rs:146,149 | `i as u64`; `idx_in_block + 1` | record index | PROVEN | `i < records.len() <= usize::MAX`; the cast widens. |
| exec-core/src/bal_ladder.rs:145,147 | `changes[i - 1]`; `changes.remove(i - 1)` | dedup cursor | PROVEN | `if i > 0` guards both at line 144. |
| footprint/src/oracle.rs:189 | `for b in (split + 1)..=max_block` | derived split point | PROVEN | Reached only inside `!holdout.is_empty()` (line 179), which needs some `block > split`, so `split < u64::MAX`. |
| footprint/src/oracle.rs:262 | `ratios[((ratios.len() - 1) as f64 * p) as usize]` | percentile index | PROVEN | `ratios.is_empty()` returns above; `p` is a literal 0.10, 0.50 or 0.90. A caller passing `p > 1.0` would panic. |
| footprint/src/oracle.rs:118 | `cp[i] = best + txs[i].gas` | per-tx gas along a path | PROVEN | Path gas is bounded by the block's total gas, at most 30M per block. |
| footprint/src/oracle.rs:80,212; grade.rs:133 | `for b in a + 1..ws.len()` and 2 more | slice indices | PROVEN | Bounded by the slice length. |
| footprint/src/grade.rs:174,175,178,181 | `level[*lo] + 1`; `m + 1`; `width[*l] += 1` | DAG level counters | PROVEN | Levels are bounded by `graded.len()`; `width` is sized `waves`. |
| footprint/src/oracle.rs:135,249-252; grade.rs:97,111,118,120; classifier.rs:102-110 | `+= 1` and `+= b.gas` accumulators | observation counters | PROVEN | usize and u64 counters bounded by the input record count and the 30M per-block gas cap. |
| footprint/src/oracle.rs:142,254,269,275,284,295,300 | `k as f64 / n`; `gas as f64 / cp as f64` and 5 more | u64 and usize counters | PROVEN | f64 division, guarded by an explicit non-zero test at each site; no panic and no wrap. |
| footprint/src/grade.rs:63,70,77 | `self.cells_hit as f64 / self.cells_actual as f64` and 2 more | grade counters | PROVEN | Each has an `== 0` early return directly above it (lines 60, 67, 74). |
| footprint/src/classifier.rs:244,265,284,314; grade.rs:225,262,265,296,302,320,336 | `addr(i as u8 + 1)` (test helpers) | small loop indices | PROVEN | Every loop runs over a literal range of at most 13, so the u8 cast cannot truncate. |

## R13 non-zero types

| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| exec-core/src/bal_ladder.rs:87 | `index.div_ceil(k)` | BAL granularity divisor | `NonZeroU64` in the `chunk_of` signature | DEFECT. A public function divides by `k` with no zero guard. `k == 0` panics. Today only `quantize` calls it, behind `if k <= 1`, but nothing in the type stops a new caller. |
| exec-core/src/bal_ladder.rs:93-95 | `pub fn quantize(bal, k: u16)`; `if k <= 1 { return bal; }` | granularity | `NonZeroU16`, parsed at the `BalFrame` wire decode | The guard conflates `k == 0` with `k == 1`. Both mean "do not quantize", so an invalid 0 is accepted as valid. |
| exec-core/src/stateless.rs:264 | `granularity: u16` in `execute_block_stateless` | proof-input granularity | `NonZeroU16`, parsed at the guest's rkyv `ProverInput` decode | The value reaches `quantize` (line 275) unvalidated from an untrusted prover input. It selects which quantization the BAL equality check uses. |
| exec-core/src/stateless.rs:318 | `granularity: u16` in `execute_block_anchored` | same | same | Same parameter, one frame out; fix both together. |
|  exec-core/src/block_env.rs:51 | `pub chain_id: u64` in `ExecEnv` | chain id | `NonZeroU64`, parsed in `Genesis::validate` | `ExecEnv::new` accepts 0. Chain id 0 is invalid under EIP-155 and reaches `cfg.chain_id` unchecked. |
| footprint/src/oracle.rs:138 | `let n = obs.len().max(1) as f64;` | division denominator | `NonZeroUsize`, or an early return on empty | The clamp is dead: when `obs` is empty the `count` map is empty too, so `n` is never used. Return `Vec::new()` on empty instead. |
| footprint/src/oracle.rs:305 | `g.holdout_txs.max(1) as f64` | division denominator | `NonZeroUsize` built at line 185 | `holdout_txs` is already non-zero, from the `!holdout.is_empty()` guard at line 179. The clamp hides that proof. |
| footprint/src/oracle.rs:307 | `g.predicted_pairs.max(1) as f64 * 100.0` | division denominator | keep the clamp, add a note | `predicted_pairs` can be a real 0. The numerator is then 0 too, so 0/1 gives the right 0.0%. State that in a comment. |
| footprint/src/classifier.rs:195 | `e.observations.max(1) * 6` | multiplicand, not a divisor | none needed; delete the `.max(1)` | The clamp guards no division. It makes this predicate differ from the identical one at lines 136 and 142 when `observations == 0`. One predicate, one form. |
| footprint/src/grade.rs:60,67,74 | `if self.cells_actual == 0 { return 1.0; }` and 2 more | division denominators | none; keep the guards | These are accumulator fields that are legitimately 0 on an empty block. A NonZero type does not fit. The guards are the right shape. |
| footprint/src/oracle.rs:253,275,294,299 | `if b.critical_path_gas > 0` and 3 more | division denominators | none; keep the guards | Same reasoning as the grade guards. |
|  footprint/src/grade.rs:91 | `cap: usize` in `grade_block` | grading batch size | `NonZeroUsize`, at the `GRADE_CAP` constant | `cap == 0` silently returns an empty grade with no truncation report. Both call sites pass the literal 2_048. |
| footprint/src/classifier.rs:71 | `const MAX_SELECTORS: usize = 16_384;` | entry cap | `NonZeroUsize` | Non-zero by literal today. A `NonZeroUsize` const documents the requirement. |
| guest/kardamom-zk-host/src/main.rs:145 | `ensure!(first >= 1 && last >= first, "bad block range")` | first block of a batch | `NonZeroU64` via a clap `value_parser` | A "must be at least one" check written by hand after parsing, not at the boundary. |

## Tests

None found. Every division in test code has a literal non-zero divisor:
`crates/exec-core/tests/hash_cost.rs:36` divides by `REPS * sets.len()` (50 * 1000),
line 44 takes `% junk.len()` (16 MiB), line 47 divides by `sets.len()` (1000), and
`crates/exec-core/tests/decode_cost.rs:43` divides by `REPS * raws.len()`.
