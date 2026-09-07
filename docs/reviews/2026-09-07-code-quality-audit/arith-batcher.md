# batcher

## Summary

The hot spot is batch-index arithmetic. Every "next batch" step is a plain `+ 1` on a
`u64` that comes from an L1 contract call: `l1.rs:118`, `live.rs:259`, `live.rs:287`,
`optimistic.rs:70`, `optimistic.rs:157`, `prover_submit.rs:59`. The second hot spot is
`optimistic.rs:47`, where `(end - start + 1)` on an on-chain block range underflows if
the settlement entry has `l2BlockEnd < l2BlockStart`, and the result becomes a
`Vec::with_capacity`. The third is the KAR1 decoder: `frame.rs` turns four wire `u32`
counts straight into `Vec::with_capacity`, with no check against the bytes left. There
are zero divisions by a possibly-zero value: every `/` and `%` in the group divides by
a compile-time constant (`blob.rs:29`, `blob.rs:110`, `archive_reader.rs:154`,
`deployer/build.rs:170`). R12 lists 31 sites: 15 FIX, 4 HOT_PATH_KEEP, 12 PROVEN. R13
lists 12 sites, of which two are real defects: a zero `--poll-interval-secs` panics the
da-watcher inside `tokio::time::interval`, and a zero `challenge_window_secs` deploys a
proof oracle with no dispute period at all.

## R12 safe arithmetic

| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| batcher/src/l1.rs:118 | `Ok(prev_batch_index + 1)` | L1 `lastBatchIndex` | FIX | checked_add, `BatcherError::L1` |
| batcher/src/live.rs:259 | `cursor.last_batch_index = self.prev_index + 1;` | L1 CAS counter | FIX | checked_add, bail |
| batcher/src/live.rs:281 | `t.last_batch_index == self.prev_index + 1` | L1 CAS counter | FIX | checked_add; wrap makes a false match |
| batcher/src/live.rs:287 | `self.prev_index += 1;` | L1 CAS counter | FIX | checked_add, bail |
| batcher/src/live.rs:299 | `t.last_batch_index > self.prev_index + 1` | L1 CAS counter | FIX | checked_add |
| batcher/src/live.rs:308 | `attempt += 1;` | counter vs `--l1-retries` | FIX | saturating_add; `max_retries` is CLI input |
| batcher/src/live.rs:318 | `Duration::from_secs(1 << attempt.min(4))` | attempt counter | PROVEN | `.min(4)` bounds the shift to 16 |
| batcher/src/live.rs:439 | `next_block: last.block_number + 1,` | stream block number | FIX | checked_add, bail |
| batcher/src/optimistic.rs:47 | `Vec::with_capacity((end - start + 1) as usize)` | settlement entry range | FIX | checked_sub + checked_add + try_into; underflow gives a huge capacity |
| batcher/src/optimistic.rs:65-70 | `oracle.highestClaimedBatch().call().await? + 1` | oracle counter | FIX | checked_add |
| batcher/src/optimistic.rs:157 | `let batch_index = last_finalized + 1;` | oracle counter | FIX | checked_add |
| batcher/src/optimistic.rs:222 | `offset = Some(i as u64);` | enumerate index | PROVEN | usize to u64 is lossless on 64-bit |
| batcher/src/optimistic.rs:232 | `entry.l2BlockStart + block_offset` | chain entry + calldata index | FIX | checked_add |
| batcher/src/prover_submit.rs:59 | `let next = last_finalized + 1;` | oracle counter | FIX | checked_add |
| batcher/src/frame.rs:203 | `Vec::with_capacity(msg_count as usize)` | wire u32 | FIX | cap against `r.buf.len() - r.pos`; 4e9 aborts |
| batcher/src/frame.rs:264 | `Vec::with_capacity(block_count as usize)` | wire u32 | FIX | same cap against remaining bytes |
| batcher/src/frame.rs:269 | `Vec::with_capacity(remote_epoch_count as usize)` | wire u32 | FIX | same cap against remaining bytes |
| batcher/src/frame.rs:274 | `Vec::with_capacity(tx_count as usize)` | wire u32 | FIX | same cap against remaining bytes |
| batcher/src/frame.rs:310 | `if self.pos + n > self.buf.len()` | wire len, reader pos | HOT_PATH_KEEP | per-field decode loop; `pos <= buf.len()` and `n <= u32::MAX` |
| batcher/src/frame.rs:316-317 | `&self.buf[self.pos..self.pos + n]; self.pos += n;` | wire len | HOT_PATH_KEEP | line 310 already proved the sum |
| batcher/src/archive_reader.rs:133 | `if self.pos + len > self.bytes.len()` | segment file u32 | PROVEN | `pos <= bytes.len()`, `len <= u32::MAX`, 64-bit |
| batcher/src/archive_reader.rs:154 | `len.div_ceil(FRAME_ALIGN) * FRAME_ALIGN` | segment file u32 | PROVEN | line 133 bounds `len` by `bytes.len()` |
| batcher/src/archive_reader.rs:155 | `self.bytes.len() - self.pos` | reader pos | PROVEN | line 105 guarantees `pos <= bytes.len()` |
| batcher/src/archive_reader.rs:194 | `let total: u32 = (FRAME_HEADER_LEN + payload.len()) as u32;` | rkyv payload len | FIX | try_from; a >4 GiB payload writes a wrong frame length |
| batcher/src/blob.rs:78 | `if LENGTH_HEADER_BYTES + len > raw.len()` | wire u32 length header | PROVEN | u32 widened to usize; 4 + 4e9 fits 64-bit usize |
| batcher/src/blob.rs:94-99 | `take`, `dst + 1 + take`, `src += take` | payload chunk | HOT_PATH_KEEP | per-field-element pack loop; `chunk.len() <= USABLE_BYTES_PER_BLOB` |
| batcher/src/blob.rs:112 | `&raw[dst + 1..dst + FIELD_ELEMENT_BYTES_USIZE]` | const field index | HOT_PATH_KEEP | `field_idx < BYTES_PER_BLOB / 32`, both consts |
| batcher/src/batcher.rs:123 | `let blob_count = batch.blobs.len() as u64;` | packed blobs | PROVEN | `pack_blocks` rejects more than 6 blobs at line 150 |
| batcher/src/rereplicate.rs:83 | `report.bytes_copied += bytes;` | file sizes | FIX | saturating_add; the sum is a report field, not a control value |
| da_watcher/src/watcher.rs:121 | `Some(c) => *c + 1,` | L1 cursor | PROVEN | line 120 returns early when `tip <= *c`, so `c < tip` |
| da_watcher/src/watcher.rs:112,160 | `.set(tip as f64)`, `.set(number as f64)` | L1 block number | PROVEN | gauge only; lossy above 2^53, not a control value |
| da_watcher/src/interop/watcher.rs:115 | `*cursor = last_seq + 1;` | peer feed seq | FIX | checked_add; a wrap rewinds the lane cursor to 0 |
| da_watcher/src/interop/source.rs:223 | `.max().map(\|s\| s + 1)` | peer feed seq | FIX | checked_add, `RemoteSourceError::Decode` |
| da_watcher/src/interop/publisher.rs:90,97 | `term_offset: (len as i32) * 64,` | published count | FIX | try_from; i32 wraps past 33.5 M records (test fake, `test-support`) |
| da_watcher/src/publisher.rs:63 | `term_offset: (v.len() as i32) * 64,` | published count | FIX | try_from; same wrap (test fake, `testing` feature) |
| deployer/src/deployer.rs:273-274 | `count.to(); Vec::with_capacity(count as usize)` | factory `l2ChainIdCount` | FIX | try_from with an error; `.to()` panics and a huge count aborts |
| deployer/src/deployer.rs:285-286 | `let count: u64 = count.to(); for i in 0..count` | factory `idCount` | FIX | try_from with an error |
| deployer/src/main.rs:344 | `.map(\|e\| e.version + 1)` | on-chain registry version | FIX | checked_add |
| deployer/build.rs:174 | `out.push((hi << 4) \| lo);` | hex nibble | PROVEN | `hex_nibble` returns 0..=15 |
| deployer/build.rs:181-183 | `c - b'0'`, `c - b'a' + 10` | hex char | PROVEN | the match arms bound `c` to each range |
| deployer/src/deployer.rs:414 | `action: s.action as u8,` | enum discriminant | PROVEN | a fieldless enum with fewer than 256 variants |

## R13 non-zero types

| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| da_watcher/src/bin/kardamom-da-watcher.rs:64 | `poll_interval_secs: u64` | poll period | NonZeroU64, clap arg | DEFECT: 0 reaches `tokio::time::interval` at watcher.rs:214, which panics on a zero period |
| deployer/src/spec.rs:144 | `challenge_window_secs: u64` | dispute period | NonZeroU64, `encode_proof_oracle_init_args` | DEFECT: `KardamomProofOracle.initialize` does not reject 0 either, so a claim finalizes at once |
| deployer/src/main.rs:87 | `finalization_window: u64` | withdrawal window | NonZeroU64, clap arg | the contract reverts with `ZeroWindow()`; check it before the deploy tx spends gas |
| da_watcher/src/interop/source.rs:143 | `max_attempts.max(1)` | reconnect count | NonZeroU32, `with_reconnect` | the clamp hides a caller that asked for zero attempts |
| batcher/src/rereplicate.rs:123 | `for _ in 1..attempts.max(2)` | read attempts | NonZeroUsize, `read_stable` | the clamp also silently turns 1 into 2 |
| batcher/src/bin/kardamom-batcher.rs:60 | `blocks_per_batch: usize` | batch size | NonZeroUsize, clap arg | used at batcher.rs:118 and live.rs:411; 0 makes every comparison true |
| batcher/src/bin/kardamom-batcher.rs:111 | `shards: u8` | tx_data subscription count | NonZeroU8, clap arg | 0 opens no tx_data readers, so every join misses |
| batcher/src/bin/kardamom-batcher.rs:137 | `flush_ms: u64` | flush period | NonZeroU64, clap arg | 0 makes `tokio::time::timeout` at live.rs:380 expire at once, a busy loop |
| da_watcher/src/bin/kardamom-da-watcher.rs:94 | `interop_retry_interval_secs: u64` | retry period | NonZeroU64, clap arg | 0 turns the failure path at interop/watcher.rs:257 into a tight reconnect loop |
| da_watcher/src/bin/kardamom-da-watcher.rs:68,77 | `interop_peer_chain_id`, `self_chain_id` | chain ids | NonZeroU64, clap args | only the "must differ" check exists at line 167; chain id 0 is not valid |
| deployer/src/main.rs:62,100 | `l2_chain_ids: Vec<u64>` | chain ids | NonZeroU64, clap arg | feeds `ContractId::proxy_salt`, so a 0 gives a valid but wrong CREATE2 salt |
| batcher/src/bin/kardamom-batch-claimer.rs:61 | `if args.interval_secs == 0 { return Ok(()) }` | poll period | keep u64 | INTENTIONAL sentinel ("0 = run once and exit"), documented; same at kardamom-proof-submitter.rs:65 and kardamom-batch-watcher.rs:76 |

## Tests

None found. Every division in the group's test and bench code uses a compile-time
constant divisor (`multi_archive_reader.rs:144` uses `i % 2`).
