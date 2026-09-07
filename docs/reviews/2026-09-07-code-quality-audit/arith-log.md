# log

## Summary
The hot spots are the Aeron position decode and the config-derived stream ids and ports.
`decode_position` unpacks the Aeron offer result with a fixed 32-bit shift, but Aeron packs a
position with `log2(term_length)` bits, so the decode is wrong for every term length that is not
4 GiB. The in-memory fake in `testing.rs` divides by a real 16 MiB term length, so the fake hides
the defect from the unit tests. Config-derived arithmetic is the second hot spot: 8 sites add to
an `i32` stream id or an `i32` base port that no code range-checks, and `ChannelsConfig::validate`
runs only through `from_toml_path`. Only 2 divisions exist in the group, and both have a
non-zero divisor (a constant, and a length checked one line above), so there are 0 divisions by a
possibly-zero value. Counts: R12 43 rows (16 FIX, 3 HOT_PATH_KEEP, 24 PROVEN), R13 7 rows,
Tests 0 rows.

## R12 safe arithmetic
| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
| --- | --- | --- | --- | --- |
| aeron_live/thread.rs:312 | `let term_id = (p >> 32) as i32;` | Aeron offer return (C client i64) | FIX | Shift by the publication `position_bits_to_shift` (log2 term length), and add `initial_term_id`. With a 16 MiB term this always gives 0. |
| aeron_live/thread.rs:313 | `let term_offset = (p & 0xFFFF_FFFF) as i32;` | Aeron offer return | FIX | `i32::try_from` after a correct term split. The mask keeps the whole stream position, so above 2 GiB the cast wraps to a negative offset. |
| publisher.rs:242 | `let term_id = (p >> 32) as i32;` | Aeron offer return | FIX | Same defect as thread.rs:312. Two copies of the same decode. |
| publisher.rs:243 | `let term_offset = (p & 0xFFFF_FFFF) as i32;` | Aeron offer return | FIX | Same defect as thread.rs:313. |
| publisher.rs:236 | `Ok(decode_position(r - len as i64))` | rkyv frame length (usize) | FIX | `i64::try_from(len)`. The result is also the payload start plus the frame header and its padding, not the frame start. |
| config/mod.rs:306 | `self.tx_receipts_endpoint_base_port as u32 + 2 * replica_idx` | TOML `i32` plus caller `u32` | FIX | `u16::try_from(base)` then `checked_add`. `validate` bounds only `replica_idx < executor_count`, and only through `from_toml_path`. |
| config/mod.rs:333 | `...base_port as u32 + 2 * replica_idx + 1` | TOML `i32` plus caller `u32` | FIX | Same as above. A struct literal (config/tests.rs:163) skips `validate` completely. |
| config/mod.rs:338 | `self.tx_data_stream_id_base + sequencer_id as i32` | TOML `i32` plus `u8` | FIX | `checked_add` with a `LogError::Config`, or range-check the base in `validate`. Nothing bounds the base today. |
| config/mod.rs:355 | `self.fsync_watermark_tx_data_stream_id_base + sequencer_id as i32` | TOML `i32` plus `u8` | FIX | Same as config/mod.rs:338. |
| aeron_live/handles/tx_receipts.rs:126 | `ch.tx_receipts_stream_id + 1` | TOML `i32` | FIX | `checked_add`, or reject `i32::MAX` in `validate`. The side-stream id is also never checked against the other stream ids. |
| aeron_live/handles/tx_receipts.rs:161 | `ch.tx_receipts_stream_id + 1` | TOML `i32` | FIX | Same as line 126. |
| aeron_live/handles/tx_receipts.rs:355 | `ch.tx_receipts_stream_id + 1,` | TOML `i32` | FIX | Same as line 126. |
| aeron_live/handles/tx_receipts.rs:371 | `ch.tx_receipts_stream_id + 1,` | TOML `i32` | FIX | Same as line 126. |
| subscriber.rs:81 | `n.unwrap_or(0).max(0) as usize` | Aeron poll status | FIX | Opposite mistake: `.max(0)` hides a negative poll status. Log or return the code, then cast. |
| subscriber.rs:99 | `n.unwrap_or(0).max(0) as usize` | Aeron poll status | FIX | Same as subscriber.rs:81. |
| refetch.rs:314 | `let idx = (self.next_endpoint + attempt) % endpoints.len();` | rotation counter | FIX | `wrapping_add`. Lines 340 and 347 advance `next_endpoint` with `wrapping_add`, so it can reach `usize::MAX`. |
| config/mod.rs:279 | `let highest = i64::from(base) + 2 * i64::from(self.tx_receipts_executor_count) + 1;` | TOML `i32` and `u32` | PROVEN | Both operands widen to `i64` first, so the result stays below 2^34. |
| refetch.rs:409 | `Ok((term_count << bits) + pos.term_offset as i64)` | wire term id plus archive term length | PROVEN | `bits <= 30`, because a positive power-of-two `i32` term length is at most 2^30. `term_count < 2^32`, so the shift stays below 2^62. |
| refetch.rs:402 | `let term_count = (pos.term_id as i64) - (rec.initial_term_id as i64);` | wire plus archive `i32` | PROVEN | Both operands widen to `i64` first, so the subtraction cannot overflow. |
| refetch.rs:394 | `let term_len = rec.term_buffer_length as i64;` | archive descriptor `i32` | PROVEN | Widening cast. Line 395 rejects a value that is not positive. |
| refetch.rs:395 | `(term_len & (term_len - 1)) != 0` | archive descriptor | PROVEN | The same condition rejects `term_len <= 0` first, so `term_len >= 1`. |
| refetch.rs:385 | `Ok((bound - from_raw, self.cfg.replay_endpoint.clone()))` | archive positions | PROVEN | Both are non-negative `i64` archive positions. The caller treats a result `<= 0` as "nothing to replay". |
| refetch.rs:458 | `Some(id) => from_record_id = id + 1,` | archive recording id | PROVEN | `i64` catalog id that grows by one per recording. |
| recorder.rs:593 | `Some(max_id) => from_record_id = max_id + 1,` | archive recording id | PROVEN | Same bound as refetch.rs:458. |
| replay.rs:505 | `Some((max_id, _, _)) => from_record_id = max_id + 1,` | archive recording id | PROVEN | Same bound as refetch.rs:458. |
| replay.rs:460 | `self.found.matches.set(self.found.matches.get() + 1);` | catalog descriptor count | PROVEN | `usize` counter, one step per catalog descriptor in one paged scan. |
| recorder.rs:154 | `(connect_timeout.as_nanos() as u64).max(1_000_000_000)` | caller `Duration` | PROVEN | The `if connect_timeout >= 30 s` branch above sends every larger value elsewhere, so the value is below 3e10 ns. |
| offer_retry.rs:91 | `elapsed_ms = start.elapsed().as_millis() as u64,` | monotonic clock | PROVEN | Line 75 leaves the loop once `elapsed >= timeout`, and `OFFER_TIMEOUT` is seconds. |
| offer_retry.rs:82 | `attempt += 1;` | retry counter | HOT_PATH_KEEP | The same deadline check bounds the loop to about one step per millisecond. |
| obs/src/lib.rs:126 | `attempt += 1;` | retry counter | PROVEN | The loop condition `attempt < bind_retries` bounds the `u32`. |
| aeron_live/thread.rs:198 | `(pubs.len() - 1) as u32` | publication table length | PROVEN | The push on the line above makes the length at least 1. The table holds far fewer than 2^32 rows. |
| aeron_live/thread.rs:219 | `Ok((subs.len() - 1) as u32)` | subscription table length | PROVEN | Same bound as thread.rs:198. |
| aeron_live/pending.rs:65 | `let doublings = (self.streak - self.grace).min(10);` | idle streak counter | PROVEN | Line 62 returns early while `streak <= grace`. Line 61 uses `saturating_add`. |
| aeron_live/pending.rs:66 | `self.base.saturating_mul(1u32 << doublings).min(self.cap)` | idle streak counter | PROVEN | `doublings <= 10`, so the shift stays in `u32`. |
| aeron_live/pending.rs:115 | `match pubs.get(item.pub_id as usize)` | publication id | PROVEN | `u32` to `usize` is a widening cast on the target platforms, and `get` bounds-checks. |
| refetch.rs:199 | `delivered += 1;` | replay fragment count | HOT_PATH_KEEP | Per-fragment drain loop. The `u64` counter is bounded by the fragments of one bounded replay. |
| refetch.rs:279 | `delivered += 1;` | replay fragment count | HOT_PATH_KEEP | Same bound as refetch.rs:199. |
| refetch.rs:552 | `std::thread::park_timeout(deadline - now);` | monotonic clock | PROVEN | Line 549 returns first when `now >= deadline`. |
| thread.rs:180, thread.rs:188, refetch.rs:498, refetch.rs:543, supervisor.rs:112 | `Instant::now() + <const Duration>` | monotonic clock | PROVEN | The addend is a compile-time constant, and `Instant + Duration` panics on overflow in every profile. |
| testing.rs:81 | `g.next_offset += bytes.len() as i64;` | fake stream offset | PROVEN | Test support. The offset grows by one frame per publish inside one test process. |
| testing.rs:103 | `let frag_start = off - bytes.len() as i64;` | fake stream offset | PROVEN | Test support. `off` is the post-message offset, so the result stays non-negative. |
| testing.rs:127 | `term_id: (off / TERM_LEN) as i32,` | fake stream offset | PROVEN | Test support. `TERM_LEN` is a non-zero constant, and the quotient needs 2^55 bytes to leave `i32`. |
| testing.rs:128 | `term_offset: (off % TERM_LEN) as i32,` | fake stream offset | PROVEN | Test support. The remainder is below 16 MiB. |

## R13 non-zero types
| file:line | snippet | value | NonZero type and boundary | note |
| --- | --- | --- | --- | --- |
| config/mod.rs:199 | `pub tx_receipts_executor_count: u32,` | executor replica count | `NonZeroU32`, parsed in `ChannelsConfig` from `[channels]` TOML and from the clap `--executor-count` flag | `validate` (line 276) checks only the port range. A zero count with MDS enabled loads and then receives nothing. |
| aeron_live/handles/tx_receipts.rs:33 | `if executor_count == 0 {` | executor replica count | Same as above | The zero case only warns. Make the type carry the invariant, and fail at load. |
| config/mod.rs:173 | `pub tx_receipts_endpoint_base_port: i32,` | UDP base port | `NonZeroU16` newtype, parsed in `from_toml_path` | `validate` line 280 rejects `base <= 0`, but only on the `from_toml_path` path. config/tests.rs:163 builds the struct directly and skips the check. |
| config/mod.rs:363 | `pub n: usize,` | total recorders | `NonZeroUsize`, parsed in `from_toml_path` | No validation at all. `n = 0` loads. |
| config/mod.rs:365 | `pub q: usize,` | required quorum | `NonZeroUsize`, parsed in `from_toml_path` | No validation. `q = 0`, and `q > n`, both load. Add `q <= n` to `validate`. |
| refetch.rs:395 | `if term_len <= 0 \|\| (term_len & (term_len - 1)) != 0 {` | archive term length | `NonZeroU32` newtype (a power of two), parsed where the recording descriptor is read (refetch.rs:431) | The guard is right, but it repeats per call. The same invariant is unchecked in `decode_position`. |
| testing.rs:566 | `pub async fn multi_node(n: usize)` | node count | `NonZeroUsize` at the call site | Test support. `n = 0` builds an empty harness, and the later `self.nodes[i]` panics. |

## Tests
None found. No test file in `crates/log` or `crates/obs` divides by a value that can be zero.

## Notes
- Considered and rejected as R13 candidates: `replay.rs:297` (`start_position != 0` is a position
  check), `replay.rs:393` and `thread.rs:115` (poll fragment counts), `refetch.rs:201`
  (`delivered == 0` is an error path), `pending.rs:331` (a test assertion).
- `ChannelsConfig` validates no stream id. The default block at config/mod.rs:416 documents a past
  collision between `tx_errors_stream_id` and `tx_receipts_stream_id + 1`. A `validate` check that
  every derived stream id is unique would catch that class at load time.
- `ChannelsConfig::validate` is called only from `LogConfig::from_toml_path`. Every other
  construction path, including `Default` plus field assignment, skips it.
