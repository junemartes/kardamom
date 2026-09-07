# e2e

## Summary
The e2e crate holds two kinds of code. The harness (`src/harness`, `src/bin`) is production
code: it parses a clap argument, a Content-Length header, and a Prometheus body. The scenarios
(`src/scenarios`) are test code, but they do arithmetic on values that come from L1, from the
state DB, and from a metrics scrape. The worst site is `harness/l1_verified.rs:159`, where a
wire Content-Length adds to a buffer offset with no guard. The runner binary adds a clap
`--account-base` to eight small literals with no guard. Scenario arithmetic on input-derived
values covers about 45 sites, of which 13 are the same `dev_signers(idx as u32 + 1)` shape.
There are ZERO divisions by a possibly-zero value in the whole crate: every `/` and `%` uses a
literal or a const divisor. R12 gives 12 FIX rows and 18 PROVEN rows. R13 gives 11 rows, one of
which is a defect: the `seeded_shuffle` seed guard is a `debug_assert!`, so it vanishes in a
release build.

## R12 safe arithmetic

### Production (`src/harness`, `src/bin`)
| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
| --- | --- | --- | --- | --- |
| harness/l1_verified.rs:159 | `while buf.len() < header_end + len` | Content-Length header, wire | FIX | `checked_add`; reject a bad length with an error |
| harness/l1_verified.rs:149 | `break i + 4;` | `find_subslice` position | PROVEN | `i + 4 <= buf.len()`; the needle is inside `buf` |
| harness/l2.rs:294-296 | `seed ^= seed << 13` (also `>> 7`, `<< 17`) | seed parameter | PROVEN | wrap is the meaning (xorshift); shift counts are const < 64 |
| harness/l2.rs:300 | `let j = (next() % (i as u64 + 1)) as usize;` | slice index `i` | PROVEN | `i < items.len()`, so the modulo bounds `j` |
| harness/metrics.rs:85; harness/proc.rs:96,129,168,195; harness/sealer.rs:140; harness/mod.rs:738 | `Instant::now() + <timeout>` | caller Duration | PROVEN | every caller passes a 5 s to 90 s literal |
| harness/proc.rs:94,113,121 | `libc::kill(self.child.id() as i32, ...)` | OS pid | PROVEN | Linux `pid_max` is far below `i32::MAX`; `i32::try_from` reads better |
| harness/mod.rs:330 | `Vec::with_capacity(cfg.shards as usize)` | StackConfig | PROVEN | `u32` to `usize` widens on a 64-bit host |
| harness/l1/mod.rs:213 | `(FINALIZATION_WINDOW + 10,)` | two consts | PROVEN | both are compile-time consts (60 + 10) |
| bin/kardamom-semantics.rs:108 | `park * 3 + Duration::from_secs(5)` | clap `--pending-receipt-timeout-ms` | FIX | `Duration::saturating_mul` then `saturating_add`; `Mul` panics today |
| bin/kardamom-semantics.rs:180,181,182,196,197,212,234,235 | `base + 5` … `base + 14` | clap `--account-base` (usize) | FIX | `checked_add` and bail; the sum then narrows with `as u32` |

### Scenarios (`src/scenarios`, grouped by shape)
| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
| --- | --- | --- | --- | --- |
| bridge.rs:53,142; consistency.rs:73; crash_recovery.rs:65,91; da_parity.rs:76; nonce_gap.rs:52; nonce_unordered.rs:41; rpc_liveness.rs:92,211,271; rpc_vectors.rs:107; upgrade.rs:368 | `dev_signers(<idx> as u32 + 1)` (13 sites) | Params usize index | FIX | `u32::try_from` on the index, then `checked_add(1)` |
| crash_recovery.rs:81,106,123; divergence.rs:79 | `b as u64` on a scraped gauge | Prometheus f64 | FIX | Rust saturates float to int, so NaN and a negative both read 0; check the range and return an error |
| l1_batch.rs:55,97; divergence.rs:159,279; upgrade.rs:264; derivation.rs:128; bridge.rs:394; xchain_da_parity.rs:140 | `<wire value> + <literal>` (8 sites) | L1 contract, state DB, RPC receipt | FIX | `saturating_add`, or `checked_add` with an error |
| divergence.rs:81 | `(committed + 1..=committed + 8).collect()` | validator gauge | FIX | `checked_add`; a saturated `committed` gives an EMPTY range, so the drill injects nothing and still passes |
| nonce_unordered.rs:51,72 | `p.senders * p.txs_per_sender` | Params | FIX | `checked_mul`; line 51 sizes an allocation |
| nonce_unordered.rs:56 | `l2::seeded_shuffle(&mut run, p.shuffle_seed + i as u64 + 1)` | Params seed | FIX | wrap is acceptable, so use `wrapping_add`, but guard the 0 result (see R13) |
| consistency.rs:165 | `let nonce = p.transfers_per_sender as u64 + 1 + i;` | Params | FIX | `u64::try_from` then `checked_add` |
| nonce_gap.rs:120,124; rpc_liveness.rs:218 | `park * 3` | Target::pending_receipt_timeout | FIX | `Duration::saturating_mul`; `Mul` panics on overflow |
| rpc_liveness.rs:299 | `t.pending_receipt_timeout + Duration::from_secs(10)` | Target config | FIX | `Duration::saturating_add` |
| da_parity.rs:146; xchain_da_parity.rs:209 | `l2_timestamp: 1_700_000_000 + block_number` | L2 block number | FIX | `saturating_add` |
| da_parity.rs:181 | `prev_index as usize == blocks.len()` | L1 batch index | FIX | `usize::try_from`; this narrows on a 32-bit host |
| l1_batch.rs:83 | `expect_start - 1` | derived from L1 `l2_block_end` | FIX | fix line 97 first; `expect_start >= 1` then holds |
| nonce_gap.rs:93,120,123 | `park / 2` | Target config | PROVEN | the divisor is the literal 2 |
| da_parity.rs:142; xchain_da_parity.rs:205; xchain_two_stacks.rs:78; consistency.rs:104; crash_recovery.rs:52 | `recorded.len() as u64`, `n as u64` | collection length, Params | PROVEN | `usize` to `u64` widens on a 64-bit host |
| da_parity.rs:274; l1_batch.rs:73; derivation.rs:304; rpc_vectors.rs:207,209,213,216,219; xchain.rs:280; xchain_da_parity.rs:290 | `i as u64 + 1`, `ln + 1` | `enumerate` index | PROVEN | the index is bounded by the collection length |
| xchain.rs:180,323,483; xchain_two_stacks.rs:123,215,309,423; nonce_unordered.rs:91; derivation.rs:424 | `nonce += 1`, `landed += 1` | local counter | PROVEN | bounded by the loop trip count |
| da_parity.rs:175 | `next == prev_index + 1` | local loop counter | PROVEN | `prev_index` counts the blocks in this run |
| upgrade.rs:86 | `.map(\|d\| d + 1)` | state-DB block number | PROVEN | the `checked_sub` on line 85 bounds `d` |
| upgrade.rs:321 | `b.block_number == first_active - 1` | state-DB header | PROVEN | the ensure at line 315 gives `first_active > scheduled_block` |
| xchain.rs:133 | `B256::repeat_byte(origin_block as u8)` | scenario literal | PROVEN | truncation is the intent; this builds a synthetic hash |
| xchain_two_stacks.rs:69,80 | `4 + 8 * 32 + data.len().div_ceil(32) * 32` | literal byte slices | PROVEN | every call site passes a 3-byte literal |
| bridge.rs:146 | `U256::from(DEPOSIT_WEI / 4)` | const | PROVEN | the divisor is the literal 4 |

### The opposite mistake: silent clamping
| file:line | expression | why a checked form is right |
| --- | --- | --- |
| nonce_gap.rs:58; nonce_unordered.rs:47; bridge.rs:59; consistency.rs:79,87,91; upgrade.rs:134,144; divergence.rs:51,59,138,174; derivation.rs:337; rpc_liveness.rs:276 (14 sites) | `.executor_metric(...).await.unwrap_or(0.0)` | An absent or unreachable metric reads as 0. The later checks (for example `applied == applied_start + 6.0` at nonce_gap.rs:129) then run against a wrong baseline and pass quietly. A scrape failure must be an error; only a genuinely absent counter should read 0. |

Correct use, for contrast: `harness/proc.rs:149` uses `lines.len().saturating_sub(n)` for a log
tail. Clamping is the intended meaning there. It is the crate's only `saturating_*` call.

## R13 non-zero types
| file:line | snippet | value | NonZero type and boundary | note |
| --- | --- | --- | --- | --- |
| harness/sealer.rs:54 | `anyhow::ensure!(members >= 1, "cluster needs at least one member")` | Raft member count | `NonZeroUsize`; boundary is `StackConfig::sealer_members` (harness/mod.rs:46, default 1 at :126) | The type removes the runtime check. The value passes through harness/mod.rs:287 unchanged. |
| harness/mod.rs:45 | `pub shards: u32` | shard / partition count | `NonZeroU32`; boundary is the `StackConfig` default 2 (harness/mod.rs:125) | No guard exists today. With 0, the loop at harness/mod.rs:331 spawns no sequencer, and every service gets `--partition-count 0`. |
| harness/services.rs:110 | `pub shards: u32` | the same count on the spec | `NonZeroU32`; same boundary, copied at harness/mod.rs:324 | Reaches services.rs:218,230,279,356,440 as a CLI string. Use one type end to end. |
| harness/mod.rs:47 | `pub cluster_tick_ms: u64` | boundary-tick period | `NonZeroU64`; boundary is the `StackConfig` default 250 (harness/mod.rs:127) | A 0 reaches the JVM as `-Dkardamom.cluster.tickMs=0` (harness/sealer.rs:112). |
| harness/mod.rs:73 | `pub chain_id: u64` | L2 chain id | `NonZeroU64`; boundary is `DEV_CHAIN_ID` (harness/mod.rs:39) and clap `--chain-id` (bin/kardamom-semantics.rs:50) | Chain id 0 is invalid under EIP-155. `materialise_genesis` (harness/mod.rs:211) would write it into a genesis file. |
| harness/l2.rs:292 | `debug_assert!(seed != 0, "xorshift seed must be non-zero")` | shuffle seed | `NonZeroU64` parameter of `seeded_shuffle`; boundary is `Params::shuffle_seed` (nonce_unordered.rs:26) | DEFECT. `debug_assert!` compiles out in release. The only caller passes `p.shuffle_seed + i as u64 + 1` (nonce_unordered.rs:56), which can wrap to 0. A zero seed makes the shuffle a no-op, so the scenario stops testing disorder and still passes. |
| harness/l2.rs:166 | `pub fn dev_signers(count: u32) -> Result<Vec<DerivedSigner>>` | signer count | `NonZeroU32`; boundary is each scenario's `Params` index | With 0, this returns an empty Vec and all 13 callers then index it and panic. |
| harness/services.rs:333 | `pub trie_shadow_check: Option<u64>` | shadow-check cadence | `Option<NonZeroU64>`; boundary is `StackConfig::trie_shadow_check` (harness/mod.rs:65, default `Some(1)`) | A cadence is a period. A 0 reaches `--trie-shadow-check 0` at services.rs:371. |
| harness/services.rs:411 | `pub rpc_max_connections: u32` | connection capacity | `NonZeroU32`; boundary is `IngressOptions::default` (services.rs:418, value 8192) | A 0 makes the ingress refuse every connection, and rpc_liveness reads that as a pass. |
| harness/services.rs:410 | `pub pending_receipt_timeout: Duration` | park period | A newtype over `NonZeroU64` millis, or an ensure in `IngressOptions` | A 0 park makes the lower bound `out.elapsed >= park / 2` at nonce_gap.rs:120 trivially true. The latency check goes vacuous instead of loud. |
| nonce_unordered.rs:21,22; consistency.rs:44,45; crash_recovery.rs:37; rpc_liveness.rs:207,270 | `pub senders: usize`, `txs_per_sender`, `transfers_per_sender`, `txs_each`, `cap`, `n` | workload batch counts | `NonZeroUsize`; boundary is each `Params` literal and bin/kardamom-semantics.rs:164-235 | With 0, the workload is empty and every assertion still passes. `landed == total` at nonce_unordered.rs:93 becomes `0 == 0`. |

## Tests
None found. No division or modulo in `src`, `tests`, or `benches` uses a possibly-zero divisor.
Every one of the 8 sites uses a literal (2, 4, 32) or a const.
