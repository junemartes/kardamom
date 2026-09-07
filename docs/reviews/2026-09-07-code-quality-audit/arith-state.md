# state

## Summary

The two crates use no `checked_*`, no `saturating_*`, and no `NonZero*` at all. They hold one
production `wrapping_add` (`types/src/epoch.rs:114`), which is correct: mod-2^160 aliasing is the
spec. Most arithmetic is a fixed-offset write into a fixed-size buffer (52 such index sites in
`types`, 32 in `state`, plus 13 more in state test modules); those are proven by the array type
and are not a risk. The risk sits in the small set of expressions that mix a wire-decoded length
or counter with a plain `+` or `-`. The worst is `RemoteEpochRecord::last_seq`
(`types/src/xchain.rs:389`), which underflows to `u64::MAX` on an empty batch; the
cluster-adapter decode path calls it before any emptiness check. `withdrawal_proof`
(`types/src/withdrawals.rs:137`) takes an unvalidated `index` and can wrap `idx + 1` into a
silently wrong Merkle sibling. `checkpoint_transfer.rs:357` adds 1 to a block number taken from a
peer's HTTP header. Divisions by a possibly-zero value: 0 in these two crates; 1 in the workspace
outside them (`ingress/src/routing.rs:14`, guarded only by a `debug_assert!`), which is the case
that motivates the `ShardCount` newtype. Counts: R12 30 rows (5 FIX, 25 PROVEN, 0 HOT_PATH_KEEP);
R13 7 rows (3 defects, 3 NonZero types to add, 1 dead `.max(1)`, 3 constants already proven by
`const`).

Two notes outside both rules, kept short because they are real: `integrity/checks.rs:259` slices
`&k[..4]` on an `accounts` key with no length check, so a corrupt short key panics the very tool
that exists to find corruption; and `checkpoint_transfer.rs:288` streams a peer's whole
`content-length` to disk with no cap.

## R12 safe arithmetic

| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| types/src/xchain.rs:389 | `self.first_seq + self.messages.len() as u64 - 1` | wire (rkyv `RemoteEpochRecord`) | FIX (checked) | Empty batch gives `0 + 0 - 1` = `u64::MAX`. Return `Option<u64>`, or `messages.len().checked_sub(1)` then `first_seq.checked_add`. Reached from `cluster-adapter/src/wire/egress.rs:186` (`canonical_id()`) before any emptiness check, and `validator/src/interop/verify.rs:204` then computes `last_seq() + 1`, wrapping the pair cursor to 0. |
| types/src/withdrawals.rs:137 | `level[idx + 1]` | caller `index` param | FIX (checked) | `withdrawal_proof(leaves, usize::MAX)` wraps to `level[0]` and returns a wrong sibling with no panic. Validate `index < leaves.len()` and return an error. |
| types/src/withdrawals.rs:139 | `level[idx - 1]` | caller `index` param | PROVEN | Reached only when `idx` is odd, so `idx >= 1`. |
| types/src/prover.rs:131 | `(raw_tx.len() as u32).to_le_bytes()` | wire (raw tx bytes) | FIX (try_from) | A byte length; 4 GiB is allocatable, so the cast can truncate and two txs can share a `BlockRecordsDigest`. Use `u64`, or `u32::try_from` with an error. |
| state/src/checkpoint_transfer.rs:357 | `best.as_ref().map_or(min_block, \|b\| b.block + 1)` | wire (`x-checkpoint-block` header) | FIX (saturating) | A peer advertising `u64::MAX` wraps `floor` to 0 and disables the "newer only" filter for every later peer. Use `saturating_add(1)`. |
| state/src/integrity/checks.rs:113 | `block != p + 1` | `headers` table key | FIX (checked) | `p == u64::MAX` wraps to 0 and reports a false gap. Use `p.checked_add(1)` and report a problem on `None`. |
| types/src/xchain.rs:319 | `data.len().div_ceil(32) * 32` | wire (`msg.input`) | PROVEN | `len <= isize::MAX`, so the product stays below `usize::MAX`. |
| types/src/xchain.rs:320 | `4 + (HEAD_WORDS + 1) * 32 + padded_len` | const + above | PROVEN | Same allocation bound; `HEAD_WORDS` is `const 10`. |
| types/src/xchain.rs:335 | `out.len() + (padded_len - data.len())` | wire | PROVEN | `padded_len >= data.len()` by construction at :319. |
| types/src/xchain.rs:443 | `w[1].seq != w[0].seq + 1` | wire (`OutboxMessage.seq`) | PROVEN | `sort_by_key` at :424 plus duplicate rejection at :426 give `w[0].seq < w[1].seq`, so `w[0].seq <= u64::MAX - 1`. |
| types/src/xchain.rs:445 | `expected: gap[0].seq + 1` | wire | PROVEN | Same bound as :443. |
| types/src/xchain.rs:99 | `let payload_len = a.length() + b.length()` | in-memory u64 + B256 | PROVEN | RLP lengths of two fixed-width values. |
| types/src/epoch.rs:87 | `let payload_len = a.length() + b.length()` | in-memory B256 + u64 | PROVEN | Same. |
| types/src/xchain.rs:119 | `Vec::with_capacity(XCHAIN_ALIAS_TAG.len() + 8 + 20)` | consts | PROVEN | All three terms are compile-time constants. |
| types/src/xchain.rs:328 | `(HEAD_WORDS * 32) as u64` | const | PROVEN | Constant 320. |
| types/src/xchain.rs:333 | `data.len() as u64` | wire | PROVEN | Widening cast on 32-bit and 64-bit targets. |
| types/src/epoch.rs:114 | `U256::from_be_bytes(a).wrapping_add(...)` | L1 log sender | PROVEN | Wrap is the meaning (OP alias, mod 2^160). Pinned by `alias_wraps_at_uint160`. Correct use, not the opposite mistake. |
| types/src/witness.rs:125 | `(self.accounts.len() as u32).to_be_bytes()` | in-memory Vec | PROVEN | `WitnessAccount` is ~89 B, so 2^32 entries exceed addressable memory. Same for :134 and :141. |
| types/src/witness.rs:144 | `(c.code.len() as u64).to_le_bytes()` | in-memory Bytes | PROVEN | Widening cast. |
| types/src/position.rs:52 | `term_id: (idx >> 32) as i32` | logical record count | PROVEN | Deliberate bit split; `as_index` at :59 reverses it with `as u32 as u64`, and `index_round_trips` pins `u64::MAX`. |
| state/src/trie/node.rs:67 | `if self.pos + n > self.b.len()` | mdbx value bytes | PROVEN | `pos <= b.len() <= isize::MAX` (invariant kept at :75) and `n` is 1, 2, or 32. Same for :70, :74. |
| state/src/trie/node.rs:33 | `(n.hashes.len() as u16).to_be_bytes()` | in-memory node | PROVEN | alloy-trie holds `hashes.len()` equal to the popcount of a 16-bit mask, so at most 16. |
| state/src/trie/node.rs:50 | `let len = cur.u16()? as usize` | mdbx value bytes | PROVEN | Widening cast; `len <= 65535`, so the `with_capacity` at :51 pre-allocates at most 2 MB before the loop errors on truncation. |
| state/src/trie/cursor.rs:115 | `out[i / 2] \|= n << 4` | `Nibbles` path | PROVEN | The `i >= 64` break at :111 bounds `i / 2 < 32`, and `Nibbles` holds each nibble in `0..=15`. Same for :117. |
| state/src/writer/mod.rs:67 | `self.delta.accounts.len() * (20 + 96)` | block delta | PROVEN | Every `len` is bounded by `isize::MAX / size_of::<T>()`, and the sum at :73 only feeds a debug log. If hardened, use `saturating_mul` / `saturating_add`. Same for :68 to :73. |
| state/src/writer/mod.rs:160 | `crate::geometry::HORIZON_BLOCKS as usize` | const `u32` = 4 | PROVEN | Widening cast on every supported target. |
| state/src/genesis.rs:41 | `Vec::with_capacity(accs.len() * 92 + codes.len() * 32)` | genesis TOML | PROVEN | Capacity hint; both lengths are bounded by the allocation limit. |
| state/src/checkpoint_transfer.rs:169 | `.take(MAX_HEAD as u64)` | const `usize` = 4096 | PROVEN | Widening cast on a constant. |
| state/src/checkpoint/mod.rs:190 | `removed += 1` | directory entries | PROVEN | Bounded by the directory entry count; the counter is `usize`. Same shape at integrity/checks.rs:119, :196, :204, :261, :279 and integrity/compare.rs:57, :87, :97, :105. |
| state/src/geometry.rs:35 | `256 * 1024 * 1024 * 1024` (usize) | const | PROVEN | Fits `usize` on 64-bit. On a 32-bit target this fails const evaluation at compile time, so it cannot wrap silently. |

## R13 non-zero types

| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| state/src/writer/mod.rs:123 | `ShadowCheck { every_n: u64 }` | shadow-check period | `NonZeroU64`; parsed at `validator/src/bin/kardamom-validator/args.rs:79` (`Option<u64>` clap arg, env `KARDAMOM_TRIE_SHADOW_CHECK`) | Defect: `--trie-shadow-check 0` silently disables the canary instead of failing, because of the `every_n != 0` guard at writer/mod.rs:393. clap parses `NonZeroU64` natively, so `Option<NonZeroU64>` removes the guard and rejects 0 at the boundary. |
| types/src/genesis.rs:73 | `if self.chain_id == 0 { return Err(ZeroChainId) }` | chain id | New `ChainId(NonZeroU64)` in `kardamom-types`; parsed by the serde `Genesis` decoder (`types/src/genesis.rs:17`, `deny_unknown_fields` TOML) | Today the zero check is a runtime branch that only `validate()` runs, so a `Genesis` built in code skips it. With a `deserialize_with` on the field, `GenesisError::ZeroChainId` (:88) disappears. Consumers to change: `types` itself (`xchain.rs` `origin_chain_id`/`dest_chain_id` on :180, :369 and the params of `remote_source_hash`:90, `alias_remote_address`:118, `msg_leaf`:269, `deliver_calldata`:316), then `exec-core` (block_env.rs:51, executor/xchain.rs:60, executor/tx_env.rs:133), `engine` (actor/types.rs:71, reader.rs:314, replay.rs:119), `validator` (prover.rs:101, interop/extract.rs:150, interop/store.rs:38), `ingress` (config.rs:42), `da_watcher` (interop/watcher.rs:47), `interop-feed` (lib.rs:105, :114, :282), `executor` (bin args.rs:63), `reconstruct` (lib.rs:67). |
| types/src/withdrawals.rs:109 | `level.len().next_power_of_two().max(1)` | leaf count | none needed | The `.max(1)` is dead: `0usize.next_power_of_two()` already returns 1. Remove it. The real missing guard is on `withdrawal_proof` (:131), which accepts an empty `leaves` and any `index`; document `leaves` non-empty and `index < leaves.len()`, and return an error. |
| ingress/src/routing.rs:14 | `(leading % m as u64) as u32` | shard count M | New `ShardCount(NonZeroU32)` in `kardamom-types`; parsed at `ingress/src/bin/kardamom-ingress/main.rs:75` (clap `shards: u32`) and `executor/src/bin/kardamom-executor/args.rs:41` (clap `shards: u8`) | Defect: a division by a possibly-zero value. The only guard is `debug_assert!(m > 0)` at routing.rs:11, which is compiled out in release, so `partition_count_m = 0` divides by zero at runtime. Field to change: `ingress/src/config.rs:22`. Other consumers: `ingress/src/proxy/mod.rs:253`, `ingress/src/proxy/submit.rs:222`, `engine/src/bin_support.rs:196` (`(0..shards)`), `bench/src/harness/inprocess.rs:72`. Keep `TxRef::shard_id` (types/src/txref.rs:49) a plain `u8`: it is an index, not a count. |
| state/src/geometry.rs:23 | `pub const HORIZON_BLOCKS: u32 = 4;` | writer channel depth | `NonZeroU32` only if it ever becomes configurable | PROVEN today: a `const` literal. It becomes a channel capacity at writer/mod.rs:160, where 0 would turn the executor handoff into a rendezvous channel. |
| state/src/geometry.rs:29 | `pub const PAGE_SIZE: usize = 16 * 1024;` | mdbx page size, and a modulus | `NonZeroUsize` only if configurable | PROVEN today: a `const` literal. It is the divisor at geometry.rs:71 and :72. `MAX_DBS` (:63) and `MAX_READERS` (:57) are the same case; `MAX_DBS = 16` is asserted `>= ALL_TABLES.len()` (11 tables) by the test at :86. |
| types/src/receipt.rs:128 | `must branch on this field, not on nonce == 0` | nonce-0 sentinel | none needed | Correct as written: `tx_type` (:134) carries the deposit/xchain distinction, so no nonce-0 sentinel exists. `is_invalid_skip` (:193) uses `gas_used == 0` as a documented marker, not as a divisor. |

No block-time or epoch-length integer exists anywhere in the workspace to wrap in a newtype. The
250 ms cadence is prose only, in `state/src/geometry.rs:8`; the only `block_time` values are Anvil
test-harness settings (`e2e/src/harness/l1/mod.rs:67`).

## Tests

| file:line | snippet | why |
|---|---|---|
| state/src/trie/incremental_tests.rs:211 | `addrs[(splitmix(&mut rng) % addrs.len() as u64) as usize]` | Division by a length. Safe: `addrs` comes from the literal range `(1u16..=160)` at :189. Same at :233. |
| state/src/trie/incremental_tests.rs:244 | `slots[(splitmix(&mut rng) % slots.len() as u64) as usize]` | Division by a length. Safe: `slots` comes from the literal range `(1u8..=10)` at :196. |
| state/src/checkpoint/tests.rs:116 | `let mid = bytes.len() / 2;` | Division by the constant 2, not by a possibly-zero value. |
