# bench

## Summary

The hot spots are three. First, `crates/bench/src/load/mod.rs` sizes the run from raw clap
values. So `sender_offset + senders`, `target_tps * total_secs`, and
`per_sender * signers.len()` all wrap in a release build. Second, the STM scenario
generators (`stm/uniswap.rs`, `bin/stm-p0.rs`, `bin/stm-p2.rs`, `bin/stm-contention.rs`)
divide and take the remainder of clap counts with no lower bound. Third, two text parsers
(`harness/flame.rs`, `perf/report.rs`) add file-supplied `u64` values with plain `+=`.
About 60 of the 230 arithmetic sites act on input-derived values. The rest are per-run
accumulators over engine metrics, which the run length bounds. There are 11 divisions by a
possibly-zero value across 9 production lines, all reachable from a CLI argument, plus 3 in
tests. Counts: R12 has 62 rows, which are 29 FIX, 2 HOT_PATH_KEEP, and 31 PROVEN. A further
5 rows flag clamping that hides a bug. R13 has 31 rows, of which 8 are hard defects.

Coverage note: this audit read every non-test source file in both crates.
`executor/src/lib.rs` and `executor/src/bin/kardamom-executor/main.rs` hold no integer
arithmetic and no casts.

## R12 safe arithmetic

| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| load/mod.rs:89 | `ramp_steps * cfg.ramp_step_secs + cfg.duration.as_secs()` | clap `--ramp-step-secs`, `--duration` | FIX | saturating_mul, then saturating_add |
| load/mod.rs:90 | `u64::from(cfg.target_tps) * total_secs` | clap `--target-tps` | FIX | saturating_mul |
| load/mod.rs:91 | `((est_total as f64 * 1.2 / ...).ceil() as usize) + 64` | line 90 | FIX | the float cast saturates to `usize::MAX`; use saturating_add |
| load/mod.rs:114 | `cfg.sender_offset + cfg.senders` | two clap u32 args | FIX | checked_add with an error |
| load/mod.rs:115 | `&signers[cfg.sender_offset as usize..]` | clap `--sender-offset` | FIX | use `get(..)` with an error; a wrap at line 114 makes this index panic |
| load/mod.rs:124 | `per_sender * signers.len()` | config estimate | FIX | saturating_mul |
| load/mod.rs:404,405,419 (3) | `after.offered - before.offered` | tracker counters | PROVEN | each is a monotonic `AtomicU64`; `before` is read first |
| load/accounting.rs:197 | `Some(r - a - rj - queued)` | scraped Prometheus deltas | FIX | saturating_sub chain; the i64 deltas can each be near `i64::MIN` |
| load/accounting.rs:119,150 (2) | `i64::try_from(b).unwrap_or(i64::MAX) - i64::try_from(a)...` | scraped gauges | PROVEN | both operands lie in `[0, i64::MAX]`; see the opposite-mistake note |
| load/accounting.rs:234 | `(c.accepted / 100).max(50)` | tracker counter | PROVEN | the divisor is the literal 100 |
| load/accounting.rs:329-331,368-371; load/mod.rs:435-437; perf/report.rs:136-139,158-160 (13) | `lat_p50_us / 1000` | histogram values | PROVEN | the divisor is the literal 1000 |
| load/scrape.rs:127,155,201 (3) | `sum_metric(b, m).unwrap_or(0.0) as u64` | Prometheus text from the wire | FIX | a negative or NaN gauge becomes 0 in silence; use a checked conversion |
| load/scrape.rs:191,192,193 (3) | `d += sum_metric(&body, M_SEQ_DROPPED)... as u64` | wire | FIX | saturating_add plus the checked cast above |
| load/plan.rs:61; signers.rs:50; load/defi.rs:189,305,351 (5) | `nonce_start + i as u64` | clap `--nonce-start` plus a loop index | FIX | checked_add with an error |
| load/defi.rs:59,60,299,345 (4) | `deployer.create(nonce_start + 1)` | clap `--nonce-start` | FIX | checked_add with an error |
| load/defi.rs:86-88 | `.wrapping_mul(0x9E37..).wrapping_add(seq.wrapping_mul(..))` | sender and sequence | PROVEN | wrap is the meaning; this is a mixer |
| load/defi.rs:92,100,103,116,117,384 (6) | `10u128.pow(17) + u128::from(h % 100) * 10u128.pow(15)` | mixer output | PROVEN | `h % 100` bounds the product at 1e17; u128 has headroom |
| load/defi.rs:70; stm/uniswap.rs:31 (2) | `Vec::with_capacity(4 + 32 * args.len())` | fixed call sites | PROVEN | every caller passes 5 words or fewer |
| load/engine.rs:76 | `self.rr = (self.rr + 1) % n` | round-robin cursor | FIX | wrapping_add; wrap is the meaning for a ring index |
| load/engine.rs:137 | `Duration::from_millis(200 * (u64::from(attempt) + 1))` | clap `--retry-submit` | PROVEN | a u32 attempt widened into u64 cannot overflow |
| harness/flame.rs:113 | `*merged.entry(rest.to_string()).or_insert(0) += count` | u64 parsed from a folded-text file | FIX | saturating_add |
| harness/flame.rs:40,42 (2) | `kept_count += *count` | pprof sample counts | FIX | saturating_add on isize |
| perf/report.rs:58,60,64 (3) | `total += n` | u64 parsed from a collapsed-stack file | FIX | saturating_add |
| harness/inprocess.rs:51 | `term_id: shard as i32` | shard index | FIX | `i32::try_from`; this truncates and can change sign |
| harness/inprocess.rs:47 | `idx = idx.wrapping_add(1)` | receipt counter | PROVEN | wrap is the meaning for a term offset |
| load/mod.rs:105; harness/inprocess.rs:82; harness.rs:184; main.rs:82 (4) | `cfg.max_in_flight as usize + MAX_IN_FLIGHT_SLACK` | clap `--max-in-flight` | PROVEN | u32 widens into usize on a 64-bit target; use saturating_add for 32-bit |
| workflows/mixed.rs:137,145; transfers.rs:69; calls.rs:100 (4) | `WARMUP_PER_TASK * signers.len()` | clap `--concurrency` | FIX | saturating_mul |
| workflows/mixed.rs:120-125 | `(txs_per_task as usize).saturating_mul(..) / cycle_total as usize` | clap args | PROVEN | the `cycle_total == 0` bail at line 96 guards the divisor |
| workflows/mixed.rs:231,232 (2) | `MixedItem::Transfer(_) => (t + 1, c)` | fold over one vector | PROVEN | the vector length bounds both counters |
| benchmark.rs:230,231,242 (3) | `counters.ok += accum.ok.load(..)`; `sent = ok + err` | per-task counters | PROVEN | bounded by `concurrency * txs_per_task`, two u32 values |
| stm/uniswap.rs:71,72 (2) | `amount_in * U256::from(997u64)`; `(with_fee * reserve_out) / ..` | simulated reserves | FIX | checked_mul; the reserves grow with each simulated swap |
| stm/uniswap.rs:147 | `for _ in 0..pairs * 2` | clap `--pairs` | FIX | saturating_mul |
| stm/uniswap.rs:263 | `let si = (b * 131 + op_i * 7) % n_send` | block and op index | FIX | wrapping_mul and wrapping_add; the result feeds `% n_send` |
| stm/uniswap.rs:271 | `(home + 1 + (r as usize / 10_000) % pair_states.len().max(1))` | mixer output | PROVEN | the sum stays below `2 * len`, and line 272 takes it modulo `len` |
| stm/uniswap.rs:281,282,286,287 (4) | `ps.reserve0 += amount_in; ps.reserve1 -= out` | simulated reserves | PROVEN | the constant-product formula keeps `out < reserve_out`; this bound needs the fix on lines 71 and 72 |
| stm/uniswap.rs:311 | `(si + 1 + (r as usize >> 8) % (n_send - 1)) % n_send` | `--senders` | FIX | checked_sub; `n_send == 0` wraps to `usize::MAX` |
| stm/uniswap.rs:91 | `self.nonce += 1` | per-signer counter | PROVEN | the counter starts at 0 and the loop count bounds it |
| stm/capture.rs:33,35,100; stm-p2.rs:404,406,411,412,933,934,1722,1723 (11) | `block_number: bi as u64 + 1`; `1_700_000_000 + bi as u64 * 2` | block index | PROVEN | `bi` is bounded by `--blocks`, a small usize |
| bin/stm-contention.rs:90 | `let idx = (i * threads + w) as u32` | thread count | PROVEN | `i * threads` stays at or below `BLOCK_TXS`, which is 4000 |
| bin/stm-contention.rs:93,107 (2) | `(w * 7 + i) % ACCOUNTS`; `(BLOCKS * per_thread * threads) as f64` | constants | PROVEN | all three constants are compile-time literals |
| bin/stm-p0.rs:128; bin/stm-p2.rs:1327 (2) | `derive_signers(ANVIL_MNEMONIC, (a.senders + 1) as u32)` | clap `--senders` (usize) | FIX | `u32::try_from` with an error; this truncates |
| bin/stm-p0.rs:160; bin/stm-p2.rs:1359 (2) | `(a.blocks * a.block_size) / a.senders + 2` | three clap args | FIX | saturating_mul; the divisor is a defect, see R13 |
| bin/stm-p0.rs:196; bin/stm-p2.rs:1394 (2) | `if si > a.block_size * queues.len() * 2` | clap `--block-size` | FIX | saturating_mul |
| bin/stm-p0.rs:261 | `o.block -= n_setup as u64` | capture output | PROVEN | the `o.block > n_setup` filter on line 259 guards it |
| bin/stm-p0.rs:96-105; bin/stm-p2.rs:1110-1147,1873-1901 (about 60) | `sum_gas += g.gas`; `row.busy_us += out.busy_us` | per-block engine metrics | PROVEN | bounded by the run length times a per-block metric |
| bin/stm-p2.rs:1040-1052 (12) | `seq_allocs += a1.0 - a0.0` | global allocator counters | PROVEN | monotonic `AtomicU64` values, snapshotted in order |
| bin/stm-p2.rs:191,194,205,210,213 (5) | `ALLOC_BYTES.fetch_add(layout.size() as u64, ..)` | allocator hook | HOT_PATH_KEEP | usize widens into u64 with no loss; this runs on every allocation |
| bin/stm-p2.rs:1015,1754 (2) | `t_snap.elapsed().as_micros() as u64` | clock | FIX | `u64::try_from`; u128 to u64 truncates |
| bin/stm-p2.rs:1247 | `(span * w as u64) as f64 / 1000.0` | measured span, worker count | FIX | saturating_mul |
| bin/stm-p2.rs:1475 | `let n = a.call_work.max(1) as u16` | clap `--call-work` (usize) | FIX | `u16::try_from`; a value of 65536 truncates to 0 and empties the loop |
| bin/stm-p2.rs:1560 | `60_000 + a.call_work.max(1) as u64 * 400` | clap `--call-work` | FIX | saturating_mul, then saturating_add |
| bin/stm-p2.rs:1504,1511 (2) | `runtime.len() as u8` | a 22-element literal vector | PROVEN | the literal fixes the length |
| bin/stm-p2.rs:681,710 (2) | `(rel.block as usize).checked_sub(warm + 1)` | engine release | PROVEN | already checked; `warm + 1` is a small block index |
| bin/stm-p2.rs:708,724 (2) | `engine_mvs[fi - 1]` | fragment index | PROVEN | the `if fi > 0` guard on line 707 covers both |
| bin/stm-p2.rs:1122 | `bpw[wi] += *b / 1000` | per-worker busy time | PROVEN | the divisor is the literal 1000 |
| executor/parallel.rs:171,177 (2) | `start: i as u64` | record index | PROVEN | usize widens into u64 with no loss |
| executor/parallel.rs:254 | `r.transaction_index = start + j as u64` | record indices | HOT_PATH_KEEP | both indices lie inside one block's record list |
| executor/parallel.rs:255 | `cumulative += r.gas_used` | executed gas | FIX | saturating_add; this value becomes the `cumulative_gas_used` receipt field |
| executor/parallel.rs:282 | `Some((&mut frag, at + 1))` | record index | PROVEN | `at` is below `records.len()` |
| executor/bal.rs:165,217 (2) | `.record(bytes.len() as f64)` | frame length | PROVEN | display metric only |
| executor/bal.rs:189 | `let deadline = Instant::now() + PUBLISH_DEADLINE` | constant | PROVEN | `PUBLISH_DEADLINE` is a small const `Duration` |
| executor/bin/kardamom-executor/state.rs:141 | `prune_checkpoints(ckpt_dir, info.block - keep + 1)` | block height, clap `--checkpoint-keep` | PROVEN | the `info.block > keep` guard on line 140 covers the subtraction |

### Opposite mistake: clamping that hides a bug

| file:line | snippet | why a checked form is right |
|---|---|---|
| load/accounting.rs:119,150 | `i64::try_from(b).unwrap_or(i64::MAX)` | a gauge above `i64::MAX` clamps, so the delta reads 0 and the keep-pace gate passes |
| load/scrape.rs:127,155,201 | `sum_metric(..).unwrap_or(0.0) as u64` | a negative or NaN gauge becomes 0, which reads as "no drops" in the drop-accounting gate |
| load/mod.rs:243 | `.clamp(1, cfg.target_tps.max(1))` | a `--target-tps` of 0 silently becomes a soak rate of 1 |
| workflows/mixed.rs:120-125 | `.saturating_mul(..)` then integer divide | a saturated product yields a nonsense `transfers_per_task`; checked_mul with an error is right |
| bin/stm-p2.rs:1287 | `let w_all = (w_own + w_foreign).max(1)` | this hides "no account writes observed", which means the measurement failed |

## R13 non-zero types

| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| load/mod.rs:171 | `Semaphore::new(cfg.max_in_flight.max(1) as usize)` | in-flight limit | `NonZeroU32`, clap `--max-in-flight` at bin/load.rs:73 | parse once into `LoadConfig` |
| load/mod.rs:87,383,460,462 (4) | `cfg.ramp_step_tps.max(1)` | ramp increment | `NonZeroU32`, bin/load.rs:104 | it is a divisor at line 87 |
| load/mod.rs:376,429 (2) | `cfg.ramp_step_secs.max(1)` | ramp step period | `NonZeroU64`, clap `--ramp-step-secs` at bin/load.rs:108 | it is a divisor at line 429 |
| load/mod.rs:91 | `f64::from(cfg.senders.max(1))` | sender count | `NonZeroU32`, clap `--senders` at bin/load.rs:44 | it is a divisor |
| load/mod.rs:243 | `.clamp(1, cfg.target_tps.max(1))` | ramp ceiling | `NonZeroU32`, clap `--target-tps` at bin/load.rs:40 | see the opposite-mistake note |
| bin/load.rs:195,196 | `if args.ramp_step_tps == 0 { (args.target_tps / 8).max(1) }` | ramp increment | `NonZeroU32` | the one real boundary; make it build a `NonZeroU32` and drop the 4 downstream `.max(1)` calls |
| bin/perf.rs:216 | `((f64::from(..) * a.soak_fraction).round() as u32).max(1)` | soak rate | `NonZeroU32`, computed in `perf` phase 1 | it feeds `cfg.target_tps` |
| load/engine.rs:214 | `if rate == 0 { return; }` | pacer rate | `NonZeroU32` parameter | a silent early return hides a misconfigured rate |
| load/engine.rs:71 | `if n == 0 { return None; }` | queue count | a non-empty `Vec` newtype in `Queues::new` | `% n` at lines 75 and 76 |
| signers.rs:38 | `if n == 0 { bail!(..) }` | signer count | `NonZeroUsize` | `count.div_ceil(n)` at line 41 |
| benchmark.rs:120,123 (2) | `if self.concurrency == 0 { bail!(..) }` | task count, queue depth | `NonZeroU32` each, clap at main.rs:41 and main.rs:45 | the doc already says "must be > 0" |
| workflows/mixed.rs:96 | `if cycle_total == 0 { bail!(..) }` | mix cycle length | `NonZeroU32` | divisor at lines 122 and 125 |
| stm/uniswap.rs:244 | `.chunks(txs_per_block.max(1))` | block size | `NonZeroUsize`, clap `--block-size` | `chunks(0)` panics |
| stm/uniswap.rs:263 | `let si = (b * 131 + op_i * 7) % n_send` | sender count | `NonZeroUsize` | **DEFECT: `--senders 0` divides by zero, with no guard** |
| stm/uniswap.rs:269,272 (2) | `let home = si % pair_states.len()` | pair count | `NonZeroUsize`, clap `--pairs` | **DEFECT: `--pairs 0` divides by zero; the `.max(1)` on line 271 covers only the inner remainder** |
| stm/uniswap.rs:311 | `(si + 1 + (r as usize >> 8) % (n_send - 1)) % n_send` | sender count | `NonZeroUsize` | **DEFECT: `--senders 1` gives `% 0`; `--senders 0` wraps `n_send - 1`** |
| stm/uniswap.rs:313 | `tokens[(r as usize >> 16) % tokens.len()]` | token count | `NonZeroUsize` | **DEFECT: `--pairs 0` leaves `tokens` empty** |
| bin/stm-p0.rs:160; bin/stm-p2.rs:1359 (2) | `(a.blocks * a.block_size) / a.senders + 2` | sender count | `NonZeroUsize`, clap `--senders` at stm-p0.rs:23 and stm-p2.rs:45 | **DEFECT: `--senders 0` divides by zero** |
| bin/stm-contention.rs:78 | `let per_thread = BLOCK_TXS / threads` | thread count | `NonZeroUsize`, parsed from the argv CSV at line 123 | **DEFECT: a `0` in the CSV divides by zero** |
| bin/stm-contention.rs:97 | `let own = \|k: usize\| (k % (ACCOUNTS / threads)) * threads + w` | thread count | `NonZeroUsize`, argv CSV | **DEFECT: more than 96 threads makes `ACCOUNTS / threads` zero, so `% 0` panics** |
| bin/stm-p2.rs:1287 | `let w_all = (w_own + w_foreign).max(1)` | write count | display divisor | clamping hides an empty measurement |
| bin/stm-p2.rs:1073 | `/ seq_receipts.len().max(1) as u64` | receipt count | display divisor | report "no receipts" instead |
| bin/stm-p0.rs:115,116,117,119,121,306,307 (7) | `sum_hit as f64 / sum_actual.max(1) as f64` | metric totals | display divisors | clamping hides an empty capture |
| bin/stm-p2.rs:1253 | `let n_tx = (rt / rt.max(1)).max(1); // placeholder` | dead value | none | delete it; `let _ = n_tx;` follows on line 1254 |
| bin/stm-p2.rs:1257-1260,1264-1267,1277-1280 (12) | `seq_allocs as f64 / 8000.0` | transaction count | derive it as `a.blocks * a.block_size` | **DEFECT: the per-transaction divisor is hard-coded. A non-default `--blocks` or `--block-size` gives a wrong allocs/tx figure. Line 1234 repeats the count as `8 * 1000usize`** |
| bin/stm-p2.rs:1475,1560 (2) | `a.call_work.max(1)` | loop-iteration count | `NonZeroUsize`, clap `--call-work` at stm-p2.rs:108 | see the truncation row in R12 |
| load/defi.rs:112,404 (2) | `U256::from((seq.saturating_sub(1)).max(1))` | synthetic order id | keep as is | clamping is the meaning for a churn id |
| executor/parallel.rs:84,120 (2) | `let workers = cfg.workers.max(1)` | worker count | `NonZeroUsize` in `StmExecConfig`, set at wiring.rs:55-60 | the boundary already yields 1 or more; the type should say so |
| executor/bin/kardamom-executor/state.rs:110 | `args.checkpoint_interval_secs > 0` | checkpoint period | `Option<NonZeroU64>`, clap `--checkpoint-interval-secs` at args.rs:119 | `tokio::time::interval` panics on a zero period |
| executor/bin/kardamom-executor/args.rs:123 | `checkpoint_keep: u64` | retention count | `NonZeroU64` | keeping 0 checkpoints defeats the feature |
| executor/bin/kardamom-executor/args.rs:41 | `shards: u8` | shard count | `NonZeroU8` | `MockChannels::new(0)` in harness/inprocess.rs:34 makes an empty shard set |

## Tests

| file:line | snippet | why |
|---|---|---|
| crates/bench/tests/defi_on_engine.rs:118 | `let avg = op_gas.iter().sum::<u64>() / op_gas.len() as u64;` | `op_gas` is empty if no receipt lands, so this divides by zero |
| crates/bench/tests/alloc_profile.rs:223 | `println!("gas/tx: {}", gas / n);` | `n` is `measured.len()`, which is 0 when `txs.len()` is at or below `SENDERS * 4` |
| crates/bench/tests/parallel_defi_repro.rs:146 | `let si = (xs(&mut rng) % queues.len() as u64) as usize;` | nothing checks that `queues` is non-empty before the remainder |
