# stm

## Summary

The crate holds 4 production files and 7,197 lines. `execute.rs` (4,620 lines) holds
almost every arithmetic site. Most of the arithmetic is safe by a proven bound: the
block arena is capped at `MAX_BLOCK_TXS = 4_096` and `push_prepared` returns an error
above it (execute.rs:2813), so every index, counter and `as u32` narrowing in the
scheduler is bounded by a check you can point at. The real exposure is the money
arithmetic in the commit tail. `alloy_primitives::U256` maps `+` and `-` to
`wrapping_add` and `wrapping_sub` in every profile, debug included, so
execute.rs:4573 (`*b - sink_start_balance`) wraps silently on an underflow and writes
a garbage `fee_delta` into a receipt. That is the one defect of substance. The crate
uses no `NonZero*` type at all, and only 2 `saturating_*` calls. It carries 6
`.max(1)` calls on a worker, shard or batch count, plus 4 more zero guards, spread
over 5 different files and functions; one of them (execute.rs:2146) can still produce
a `TouchTable` of capacity 1, whose probe loop never ends. Divisions by a
possibly-zero value: 1 latent (execute.rs:616 and 2864 divide by `workers`, which only
`with_pool` normalizes), 0 unguarded in tests.

## R12 safe arithmetic

| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| execute.rs:4573 | `*b - sink_start_balance` | EVM write set (input) | FIX | `checked_sub`, return `ExecutorError`. `U256::sub` is `wrapping_sub` in every profile, so an underflow writes a huge `fee_delta` into a receipt with no panic. |
| execute.rs:3355 | `fee_sum += r.fee_delta` | per-tx fee deltas | FIX | `checked_add` with an error. Same wrapping `U256` semantics; the sum feeds `MvRelease::sink_final`. |
| execute.rs:3361 | `a.balance = b0.sink_start_balance + fee_sum` | block-start sink plus sum | FIX | `checked_add` with an error. Wrapping here corrupts the released sink balance. |
| execute.rs:3424, 3772 | `sink_running += r.fee_delta` | per-tx fee deltas | FIX | `checked_add` with an error. This value is written into the write set at 3428 and re-hashed. |
| execute.rs:3422, 3770 | `cumulative += r.receipt.gas_used` | receipt gas (wire-derived) | FIX | `checked_add` with an error. The bound is the block gas limit, which this crate never checks; `gas_used` is a plain `u64` here. |
| execute.rs:3754 | `Some((f, b + i as u64 + 1))` | `bal_base` (caller-supplied) | FIX | `checked_add`. `bal_base` is a public parameter of `begin_block_layered_bal`. |
| execute.rs:4537 | `let idx = b + local_idx as u64 + 1` | `bal_base` plus local index | FIX | `checked_add` or `saturating_add`. Same caller-supplied `bal_base`. |
| execute.rs:2146 | `((MAX_BLOCK_TXS * 4) / cfg_admit_shards.max(1)).next_power_of_two()` | `PoolConfig::admit_shards` | FIX | Clamp the result: `.max(64)`. With `admit_shards > 16_384` the quotient is 0, `next_power_of_two()` gives 1, and `TouchTable::upsert` (1140) then spins forever on a full 1-slot table. |
| execute.rs:616 | `(h % workers as u64) as usize` | `PoolConfig::workers` | FIX (R13) | Make `workers` a `NonZeroUsize`. `domain_hash` takes the count as a bare `usize` and divides by it with no guard. |
| execute.rs:2864 | `None => i % self.workers` | `PoolConfig::workers` | FIX (R13) | Same. `self.workers` is copied from `st.cfg.workers` at 2306 with no `.max(1)`; only `with_pool` (1881) makes it non-zero. |
| execute.rs:2713 | `if (*h % k as u64) as usize != sh` | `admit_shards` | PROVEN | `k = admit_shards.max(1)` at 2699. Per-cell, hot; the `as u64` widens a `usize`, and the `as usize` result is `< k`. |
| execute.rs:1104 | `mask: capacity_pow2 - 1` | `TouchTable::new` argument | PROVEN | The `assert!(capacity_pow2.is_power_of_two())` at 1092 rejects 0. |
| execute.rs:1126 | `let mut i = hash as usize & self.mask` | domain hash | HOT_PATH_KEEP | One probe per predicted cell per tx. The `as usize` is lossless on 64-bit, and the mask bounds the index to `slots.len()`. |
| execute.rs:1140 | `i = (i + 1) & self.mask` | probe walk | HOT_PATH_KEEP | `i <= mask < usize::MAX`, so `i + 1` cannot wrap; the mask re-bounds it. |
| execute.rs:1111 | `self.stamp = self.stamp.wrapping_add(1)` | block counter | PROVEN | Wrap is the meaning, and 1112 hard-clears on wrap to 0. Correct as written. |
| execute.rs:192 | `(b[19] as usize) % BASE_SHARDS` | address byte | PROVEN | `BASE_SHARDS` is `const usize = 64`. `b[19]` is in range because `Address` is 20 bytes. |
| execute.rs:2818 | `let idx = i as u32` | admitted count | PROVEN | 2813 returns an error when `i >= MAX_BLOCK_TXS` (4,096). |
| execute.rs:1334, 1405, 1431, 1450, 1513, 1527, 2720, 2723, 2949, 2963, 3060, 4185, 4229 | `nodes[idx as usize]`, `results[job as usize]` | node index `u32` | PROVEN | Every index comes from the session counter bounded at 2813, and the arenas hold `MAX_BLOCK_TXS` entries. |
| execute.rs:1503 | `.fetch_sub(b.len() as u32, ...)` | completion buffer length | PROVEN | The buffer holds at most one entry per admitted tx, so at most 4,096. |
| execute.rs:1536, 1537, 1548 | `applied as u64`, `applied as u32` | completions applied | PROVEN | Same 4,096 bound. |
| execute.rs:1454 | `n_ready += 1` | ready-child count | PROVEN | Guarded by `n_ready < ready_buf.len()` at 1452. |
| execute.rs:1507 | `applied += 1` | drained completions | PROVEN | Bounded by `MAX_BLOCK_TXS`. |
| execute.rs:2064, 2439 | `st.generation += 1` | block counter | PROVEN | One increment per block on a `u64`. |
| execute.rs:2023, 2075, 2423 | `std::time::Instant::now() + STALL_TIMEOUT` | clock plus a 30s const | PROVEN | `Instant + Duration` can panic on overflow only near the monotonic-clock end; 30 seconds ahead of now never reaches it. |
| execute.rs:2915, 3017 | `(t - t_feed).as_nanos() as u64` | two `Instant` reads | PROVEN | `t` is read after `t_feed` on the same thread, so the `Instant` subtraction cannot panic; a `u64` holds 584 years of nanoseconds. |
| execute.rs:463, 470, 481, 1541, 2793, 2981, 3118, 3125, 3516, 3593, 3596, 3636, 3692, 4182, 4188, 4208, 4331, 4370, 4513, 4568 | `t0.elapsed().as_nanos() as u64` | monotonic clock | HOT_PATH_KEEP | Two clock reads per state access (463-481) and per tx elsewhere. `Duration::as_nanos` returns `u128`; the `u64` range is 584 years of uptime. |
| execute.rs:3900 | `commit_us: t_commit.as_micros() as u64` | clock | PROVEN | Same 584-year bound, in microseconds. |
| execute.rs:282, 284, 306, 328, 348, 351, 403, 405, 412, 424, 434, 437 | `self.n_reads += 1` and siblings | per-read counters | HOT_PATH_KEEP | Worker-local `u64` counters, flushed once per block (`flush_counters`, 250). At most a few reads per tx over 4,096 txs. |
| execute.rs:2743, 2954, 2968, 3103 | `self.edges += ...` | edge count | PROVEN | `usize`, at most `MAX_BLOCK_TXS` squared per block. |
| execute.rs:2827, 2905, 2906, 3082, 3090 | `self.cold += 1`, `n_txs += 1`, `dispatch[worker] += 1`, `covered += 1`, `deg += 1` | per-tx counters | PROVEN | All bounded by the 4,096 admission cap at 2813. |
| execute.rs:2884 | `self.pool.assign_load.borrow_mut()[worker] += 1` | pool-lifetime dispatch count | PROVEN | `u64`; one increment per transaction over the pool's life. |
| execute.rs:3465 | `d.accounts.reserve(results.len() * 2)` | result count | PROVEN | `results.len() <= MAX_BLOCK_TXS`. |
| execute.rs:3548 | `let chunk = n_res.div_ceil(n_ch)` | lane count | PROVEN | `n_ch >= 1` from the two `.max(1)` calls at 3546 and 3547. See R13. |
| execute.rs:3572, 3573 | `let base = ci * chunk;` `(base + chunk).min(n_res)` | chunk index | PROVEN | `ci < n_ch` and `n_ch * chunk <= n_res + n_ch`, with `n_res <= 4_096`. |
| execute.rs:3513, 3588 | `mv.validate(i as u32, rec)` | result index | PROVEN | `i < n_res <= MAX_BLOCK_TXS`. |
| execute.rs:3893 | `(l - f) / 1_000` | two metric timestamps | PROVEN | 3890 returns 0 when `l < f` or `f == u64::MAX`. |
| execute.rs:2624 | `elapsed.as_nanos() as u64 / txs as u64` | tx count | PROVEN | Guarded by `if txs > 0` at 2622. |
| execute.rs:2650 | `... / txs.len() as u64` | tx count | PROVEN | Guarded by `if !txs.is_empty()` at 2648. |
| execute.rs:3846 | `m.busy_ns.load(...) / n as u64` | tx count | PROVEN | Guarded by `if n > 0` at 3844. |
| execute.rs:3867-3918 (18 sites) | `m.<x>_ns.load(...) / 1_000` | metric counters | PROVEN | The divisor is the literal 1,000. |
| execute.rs:3916 | `completions as f64 / prune_calls as f64` | metric counters | PROVEN | Guarded by `if prune_calls == 0` at 3913. |
| execute.rs:4209, 4210 | `done_at.saturating_sub(t_busy_at)`, `local_busy_ns += busy_ns` | clock offsets | PROVEN | `saturating_sub` is already explicit; the sum is worker-busy nanoseconds inside one block. |
| execute.rs:4253 | `ctx.pending.fetch_add(1, SeqCst) + 1` | outstanding completions | PROVEN | At most one per admitted tx, so at most 4,096. |
| execute.rs:4259 | `if owed as usize >= ctx.prune_batch` | same counter | PROVEN | Same bound; `u64` to `usize` is lossless on the 64-bit targets this crate pins threads on. |
| execute.rs:4414 | `let at = g.len() - n` | pool length | PROVEN | `n = g.len().min(64)` at 4413, so `n <= g.len()`. |
| execute.rs:4447, 4607, 3758, 3761, 4323, 4327, 4367 | `local_idx as u64`, `i as u64` | local index | PROVEN | Widening cast, always lossless. |
| execute.rs:613-614 | `h ^= *b as u64; h = h.wrapping_mul(...)` | address bytes | PROVEN | `wrapping_mul` is explicit and wrap is the meaning for hash mixing. |
| execute.rs:1959 | `MAX_BLOCK_TXS.saturating_sub(g.len())` | recycle pool length | PROVEN | Already explicit, and clamping is the right meaning for a capacity headroom. |
| mv.rs:64-67 | `h ^= *b as u64; ...; (h % SHARDS as u64) as usize` | address or slot bytes | PROVEN | `SHARDS` is `const usize = 64`; the result is `< 64`, so the `as usize` is lossless. |
| mv.rs:180, 190 | `(p > 0).then(\|\| list[p - 1])` | partition point | PROVEN | The `p > 0` test guards the subtraction and the index. |
| pool.rs:134 | `pins[li % pins.len()]` | pin-core list | PROVEN | Guarded by `if !pins.is_empty()` at 132. |
| pool.rs:245 | `spins += 1` | spin counter | PROVEN | The loop breaks at 259 once `spins` reaches 256, so the `u32` never wraps. |
| lib.rs:60, 76, 82 | `h.wrapping_mul(K).rotate_left(23)` | hash mixing | PROVEN | `wrapping_mul` is explicit and correct for a hash. |
| lib.rs:81 | `u64::from_le_bytes(buf) ^ ((rem.len() as u64) << 56)` | tail length | PROVEN | `as_chunks::<8>` bounds `rem.len()` to 0..7, so the shift by 56 never loses a bit. |
| schedule.rs:181, 182 | `scheduling_view_decoded(i as u32, ...)`, `dag.admit(i as u32, ...)` | `envelopes.len()` | FIX | `u32::try_from(i)` with an error. `build` is a public function and applies no `MAX_BLOCK_TXS` cap of its own. |
| schedule.rs:184, 185 | `s.children[p as usize].push(i as u32)`, `s.indegree[i] += 1` | predecessor list | PROVEN | `p < i < n` because `admit` only returns earlier indices, and `preds` is deduplicated at 160. |
| schedule.rs:147, 161 | `self.cold += 1`, `self.edges += preds.len()` | admission counters | PROVEN | `usize` diagnostics, one increment per admitted transaction. |
| schedule.rs:78 | `index: local_idx as u64` | local index | PROVEN | Widening cast. |
| tests/equivalence.rs:35, 117, 233, 656, 778, 959, 1136, 1336 | `i as u8 + 1`, `i as u8 + 0x10` | loop index | FIX | Test code. `u8::try_from(i)` and `checked_add`. Every caller uses a small signer group today, but a grown group truncates or wraps and silently reuses an address. |
| tests/equivalence.rs:383 | `n as u32` | tx count | PROVEN | Test code. `n` is a small literal-derived count. |
| pool.rs:317 | `out[i].store(i * round + 1, ...)` | test loop indices | PROVEN | Test code. `i < 64` and `round < 200`. |

## R13 non-zero types

| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| execute.rs:939 | `pub workers: usize` | worker count | `NonZeroUsize`; parsed in `PoolConfig` (built by `crates/validator/src/parallel/engine.rs:474`, the benches, and `execute_block_stm`) | This one field drives 6 of the crate's zero guards. Making it `NonZeroUsize` removes them all. |
| execute.rs:1881 | `let workers = cfg.workers.max(1);` | worker count | same | The single normalization point today. Every later guard exists only because this one is not in the type. |
| execute.rs:1914 | `workers.max(1),` | worker count | same | Dead: `workers` is already `>= 1` from 1881. |
| execute.rs:2220 | `st.cfg.workers.max(1)` | worker count | same | Third normalization of the same field. |
| execute.rs:2306 | `(st.cfg.workers, st.cfg.prune_batch, ...)` | worker count | same | Read with no `.max(1)`. It reaches `BlockSession::workers` and then the `%` at 2864 and 616. Correct only because `with_pool` normalized at 1881. |
| execute.rs:2309 | `snapshots.len() >= workers.max(1)` | worker count | same | An `assert!` that a type would make unnecessary. |
| execute.rs:616 | `(h % workers as u64) as usize` | worker count | same | Division by a parameter. `domain_hash` is a free function; nothing in its signature says the count is non-zero. Callers: 2856, 2861, 4221. |
| execute.rs:2864 | `None => i % self.workers` | worker count | same | Second division by the same value, again with no local guard. |
| execute.rs:2875 | `(0..self.workers).min_by_key(...).unwrap_or(hashed)` | worker count | same | The `unwrap_or` is the empty-range fallback, that is, the zero case again. |
| execute.rs:4275, 4284 | `workers: usize`, `vec![snapshot.clone(); workers.max(1)]` | worker count | `NonZeroUsize` at the `execute_block_stm` boundary | A public entry point takes a bare `usize` and clamps it. |
| execute.rs:945 | `pub prune_batch: usize` | batch size | `NonZeroUsize`; `PoolConfig` | The doc says "`1` updates the graph on every completion", so 0 has no meaning. |
| execute.rs:1884, 2387 | `prune_batch: cfg.prune_batch.max(1)` | batch size | same | Clamped twice. The value is only compared (`owed >= prune_batch` at 4259), so 0 would force a prune per completion rather than fault. |
| execute.rs:1035 | `pub const DEFAULT_PRUNE_BATCH: usize = 8;` | batch size | `NonZeroUsize` const | Parse the default once, in the type. |
| execute.rs:999 | `pub admit_shards: usize` | shard count | `Option<NonZeroUsize>`; `PoolConfig` | 0 is a real mode here ("0 means the serial feed"), so the honest type is an `Option`, not a clamped `usize`. |
| execute.rs:2142 | `cfg_admit_shards.max(1),` | shard count | same | Clamped for `ShardTables::new`, so shard 0 exists even in serial mode. |
| execute.rs:2146 | `((MAX_BLOCK_TXS * 4) / cfg_admit_shards.max(1))` | shard count as divisor | same | The `.max(1)` stops the division by zero, but not the zero *result*. `admit_shards > 16_384` gives capacity 1, and `TouchTable::upsert` (1140) then loops forever. Clamp the capacity, or cap `admit_shards`. |
| execute.rs:2148, 2334, 2891 | `(cfg_admit_shards > 0).then(...)`, `if self.admit_shards > 0` | shard count | same | Three `> 0` tests that an `Option<NonZeroUsize>` turns into `if let Some(k)`. |
| execute.rs:2699 | `let k = self.pool.admit_shards.max(1);` | shard count | same | Fourth clamp, and the divisor of 2713. |
| execute.rs:3546 | `let n_lanes = lanes.workers().max(1);` | lane count | `NonZeroUsize` returned by `WorkerPool::workers()` | The pool already knows its own thread count. Return it non-zero and this clamp disappears. |
| execute.rs:3547 | `let n_ch = n_lanes.min(n_res.max(1));` | chunk count | `NonZeroUsize` | Both `.max(1)` calls exist only to make `div_ceil` at 3548 safe. |
| execute.rs:1092 | `assert!(capacity_pow2.is_power_of_two(), ...)` | table capacity | A `Pow2Capacity(NonZeroUsize)` newtype, built once at 2140 and 2146 | The comment says the assert is deliberate. A newtype checked at construction says the same thing in the type, and 1104 (`capacity_pow2 - 1`) then needs no reasoning. |
| pool.rs:110 | `pub fn new(workers: usize, pin_cores: Vec<usize>)` | worker count | `NonZeroUsize` at the `WorkerPool` constructor | **Defect.** `WorkerPool::new(0, ..)` spawns no thread. `run(n, f)` then stores `active = 0`, the wait loop at 244 exits at once, and `run` returns `Ok(())` **without running a single chunk**. Every caller clamps (`execute.rs:1914`, `engine.rs:474`), so nothing hits it today, but the type invites it. |
| pool.rs:214 | `if n_chunks == 0 { return Ok(()); }` | chunk count | `NonZeroUsize` parameter | This early return is correct and intended. Keep it, or take `NonZeroUsize` and drop it. |
| mv.rs:56 | `const SHARDS: usize = 64;` | shard count and divisor of mv.rs:67 | `NonZeroUsize` const | Safe today as a literal const, but it is the same shape as the configurable counts above. |
| execute.rs:175 | `const BASE_SHARDS: usize = 64;` | shard count and divisor of execute.rs:192 | `NonZeroUsize` const | Same. |
| execute.rs:1112 | `if self.stamp == 0 { ... self.stamp = 1; }` | stamp sentinel | `NonZeroU32` for the live stamp | 0 is the "never written" sentinel for a slot, so the live stamp is exactly a `NonZeroU32`. The hard clear at 1115 stays. |
| execute.rs:2120 | `id: pin[w % pin.len()]` | pin-core list length | `NonZeroUsize` length, or keep the guard | Guarded by `if !pin.is_empty()` at 2118. Correct, not a defect. |
| execute.rs:3998 | `&ctx.snapshots[worker % ctx.snapshots.len()]` | snapshot count | `NonZeroUsize` | Non-zero only through the assert at 2309. |
| pool.rs:134 | `id: pins[li % pins.len()]` | pin-core list length | same as 2120 | Guarded by `if !pins.is_empty()` at 132. Correct, not a defect. |
| execute.rs:2622, 2648, 3844, 3890, 3913 | `if txs > 0`, `if !txs.is_empty()`, `if n > 0`, `l < f`, `prune_calls == 0` | divisors | none needed | Five correct zero guards ahead of a division. Listed for completeness; each one is right. |

## Tests

None found. No test in `tests/equivalence.rs` or in the `#[cfg(test)]` modules of
`pool.rs`, `mv.rs` and `schedule.rs` divides by a value that can be zero. Every
divisor in the test code is a literal (`% 4`, `% 37`). The test-code R12 findings are
listed in the R12 table above.
