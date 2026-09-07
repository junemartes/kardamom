# sequencer

## Summary

The group has one live arithmetic defect and one live non-zero defect. In
`nonce_decode.rs`, `skip_rlp_item` adds an attacker-supplied RLP length to an
offset with no check. A 12-byte frame makes the sum wrap in a release build,
and `peek_nonce` then returns a wrong nonce instead of falling back to the
full decode. I built and ran the exact function with `-C overflow-checks=off`
and got `Some(10)` for garbage input. The second defect is
`partition::partition_for`, which computes `leading % m` behind a
`debug_assert!` only, so a zero `m` panics in release. The SBE codec in
`cluster-client/protocol.rs` is sound: every offset add is preceded by a
`get`, a `need`, or a widening from `u16`, so wire input cannot drive it out
of range on a 64-bit target. Most of the 39 `as` casts in the group are
widening (`u16`/`u32` to `usize`) or clock-to-`u64` casts that a wall clock
cannot reach; the narrowing ones that matter are `partition_index as u8`
(two sites) and three `len() as u16`/`as u32` length prefixes. Divisions by a
possibly-zero value: 1 (`partition.rs:19`). Counts: R12 60 rows (16 FIX, of
which 2 are the opposite mistake — a clamp that should be a checked error —
plus 42 PROVEN and 2 HOT_PATH_KEEP), R13 15 rows, Tests 2 rows.
Note on the brief: `PartitionCount` and `PartitionIndex` do not exist. A grep
over the whole workspace finds no such types. `partition_count` and
`partition_index` are plain `u32` fields on `SequencerConfig`. The only
`NonZero` use in the workspace is in `crates/ingress`.

## R12 safe arithmetic

| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
|---|---|---|---|---|
| crates/sequencer/src/nonce_decode.rs:91 | `i + 1 + ll + l` | RLP length bytes from tx_data | FIX | `checked_add` chain, `None` on overflow. `l` reaches `usize::MAX`; the sum wraps to a live index. |
| crates/sequencer/src/nonce_decode.rs:100 | `i + 1 + ll + l` | RLP length bytes from tx_data | FIX | Same site in the list arm. Same `checked_add` fix. |
| crates/sequencer/src/partition.rs:19 | `(leading % m as u64) as u32` | `m` from config | FIX | Take `NonZeroU32`. See R13. The `as u32` is lossless: result `< m`. |
| crates/sequencer/src/config.rs:113 | `(self.partition_index + offset) % m` | clap `--partition-offset` | FIX | `checked_add` with a config error. Both operands are raw CLI `u32`. |
| crates/sequencer/src/config.rs:114 | `self.sequencer_id = self.partition_index as u8` | config `partition_index` | FIX | `u8::try_from` with an error. A count above 256 truncates silently. |
| crates/sequencer/src/bin/kardamom-sequencer/main.rs:147 | `cfg.sequencer_id = cfg.partition_index as u8` | config `partition_index` | FIX | Same truncation. Same `try_from`. |
| crates/cluster-adapter/src/watermark.rs:26 | `self.durable_count.max(index + 1)` | `index` from cluster egress | FIX | `index.saturating_add(1)`. `index` is decoded wire data (`ingress/src/cluster.rs:51`). |
| crates/cluster-adapter/src/wire/ingress.rs:119 | `(entries.len() as u16).to_le_bytes()` | caller slice length | FIX | `u16::try_from` with a `WireError`. Over 65535 entries writes a wrong count. Today's only caller sends at most 16 (`sequencer.rs:182`), but the function is `pub`. |
| crates/cluster-adapter/src/wire/ingress.rs:121 | `(e.len() as u32).to_le_bytes()` | entry length | FIX | `u32::try_from` with a `WireError`. |
| crates/cluster-adapter/src/wire/egress.rs:208 | `(payload.len() as u32).to_le_bytes()` | payload length | FIX | `u32::try_from` with a `WireError`. |
| crates/cluster-client/src/protocol.rs:147 | `(bytes.len() as u32).to_le_bytes()` | var-field length | FIX | `u32::try_from`. A field over 4 GiB writes a wrong SBE length. |
| crates/cluster-client/src/session/mod.rs:190 | `self.next_correlation_id += 1` | connect counter | FIX | `wrapping_add(1)`, or `saturating_add`. The counter grows for the process lifetime. |
| crates/cluster-client/src/session/mod.rs:202 | `self.connect_attempts += 1` | connect counter | FIX | `saturating_add(1)`. Only compared against 0 at line 179. |
| crates/cluster-adapter/src/live/endpoints.rs:80 | `d.as_millis() as u64` | `SystemTime` since epoch | FIX | `u64::try_from(..).unwrap_or(u64::MAX)`. `u128` to `u64` narrowing on a clock value. |
| crates/sequencer/src/resync/mod.rs:156 | `dedup_capacity.saturating_mul(enter_percent) / 100` | `[resync]` TOML, clap | FIX | Opposite mistake: `saturating_mul` clamps a mis-set capacity or percent to a silent wrong threshold. Use `checked_mul` with a config error. The `/ 100` divisor is a literal and is safe. |
| crates/sequencer/src/resync/mod.rs:388 | `w.saturating_sub(self.last_watermark)` | cluster boundary `end_tx_idx` | FIX | Opposite mistake: a watermark that goes backwards is a sealer fault, and this clamps it to `jump = 0`, so no trigger fires and nothing logs. Compare first and log the regression. |
| crates/cluster-client/src/bytes.rs:16 | `buf.get(at..at + N)?` | `at` from callers | PROVEN | `N <= 8` and `at` never exceeds `body.len()` plus about 12 GiB on a 64-bit target. Would overflow on a 32-bit target. |
| crates/cluster-client/src/protocol.rs:98 | `buf.get(at..at + n)` | `at`, `n` from wire | PROVEN | Same 64-bit bound. `n` is a `u32` widened to `usize`. |
| crates/cluster-client/src/protocol.rs:133 | `rd_u32(body, at)? as usize` | wire `u32` | PROVEN | Widening cast on a 64-bit target. |
| crates/cluster-client/src/protocol.rs:134 | `let start = at + 4;` | `at` from wire | PROVEN | `rd_u32` at line 133 succeeded, so `at + 4 <= body.len()`. |
| crates/cluster-client/src/protocol.rs:136 | `Ok((bytes, start + len))` | wire length | PROVEN | `need(body, start, len)` at line 135 already proved `start + len <= body.len()`. |
| crates/cluster-client/src/protocol.rs:196 | `rd_var(body, h.block_length as usize)?` | wire `blockLength` | PROVEN | `u16` to `usize` widening. An out-of-range value fails in `rd_u32`. |
| crates/cluster-client/src/protocol.rs:316 | `rd_var(body, h.block_length as usize)?` | wire `blockLength` | PROVEN | Same. |
| crates/cluster-client/src/protocol.rs:352 | `rd_var(body, h.block_length as usize)?` | wire `blockLength` | PROVEN | Same. |
| crates/cluster-client/src/protocol.rs:388 | `.get(h.block_length as usize..)` | wire `blockLength` | PROVEN | Same, and `get` returns `None` when out of range. |
| crates/cluster-client/src/protocol.rs:258 | `HEADER_LEN + BLOCK_... as usize + payload.len()` | payload length | PROVEN | A capacity hint only. Two constants plus a real slice length. |
| crates/cluster-client/src/protocol.rs:192 | `let body = &buf[HEADER_LEN..];` | wire buffer | PROVEN | `MessageHeader::decode` read offset 6 as `u16`, so `buf.len() >= 8`. Same for lines 244 and 379. |
| crates/cluster-adapter/src/wire/ingress.rs:116 | `entries.iter().map(\|e\| 4 + e.len()).sum()` | entry lengths | PROVEN | A capacity hint over real slice lengths already resident in memory. |
| crates/cluster-adapter/src/wire/ingress.rs:138 | `u16::from_le_bytes([hdr[0], hdr[1]]) as usize` | wire count | PROVEN | Widening. `hdr` is a checked 2-byte slice. |
| crates/cluster-adapter/src/wire/ingress.rs:142 | `buf.get(pos..pos + 4)` | loop cursor | PROVEN | The line 149 `get` bounds `pos <= buf.len()` after every iteration. |
| crates/cluster-adapter/src/wire/ingress.rs:147 | `u32::from_le_bytes(..) as usize` | wire length | PROVEN | Widening on a 64-bit target. |
| crates/cluster-adapter/src/wire/ingress.rs:149 | `buf.get(pos..pos + len)` | wire length | PROVEN | `pos <= buf.len()` and `len <= u32::MAX` on a 64-bit target. Would overflow on 32-bit. |
| crates/cluster-adapter/src/wire/ingress.rs:154 | `pos += len` | wire length | PROVEN | The line 149 `get` already proved `pos + len <= buf.len()`. |
| crates/cluster-adapter/src/wire/egress.rs:53 | `rd_u32(buf, 9)? as usize` | wire `payload_len` | PROVEN | Widening. |
| crates/cluster-adapter/src/wire/egress.rs:56 | `buf.get(start..start + payload_len)` | wire `payload_len` | PROVEN | `start` is the literal 13 and `payload_len <= u32::MAX`. |
| crates/cluster-adapter/src/wire/egress.rs:118 | `&p[CANONICAL_ID_LEN + 1..]` | relayed payload | PROVEN | The line 113 `get(32)` proved `p.len() >= 33`. |
| crates/cluster-adapter/src/wire/mod.rs:178 | `1 + epoch.deposits.len() as u64` | in-memory record | PROVEN | A real `Vec` length. The `+ 1` cannot overflow `u64`. Line 187 is the same. |
| crates/cluster-adapter/src/live/endpoints.rs:63 | `ids[(start + step) % ids.len()]` | parsed member list | PROVEN | `start < ids.len()` and `step <= ids.len()`, so the sum is at most twice the list length. `ids.is_empty()` is checked at line 58. |
| crates/cluster-adapter/src/live/session_loop.rs:358 | `(self.egress_silence_reset_ms * 2).min(MAX)` | own backoff state | PROVEN | The field is seeded at 10000 and only ever set to `next_window_ms`, which is capped at `EGRESS_SILENCE_RESET_MAX_MS` (60000). The product is at most 120000. |
| crates/cluster-adapter/src/live/session_loop.rs:384 | `self.driver.wrap_app(&req, now as i64)` | wall clock ms | PROVEN | `now_ms()` is milliseconds since the epoch, far below `i64::MAX`. Lines 419 and 484 are the same. |
| crates/cluster-adapter/src/live/session_loop.rs:540 | `live += 1` | local counter | HOT_PATH_KEEP | At most two increments per call, both in straight-line code. Line 544 is the same. |
| crates/sequencer/src/nonce_decode.rs:48 | `i += 1` | RLP cursor | HOT_PATH_KEEP | Per-transaction decode. `i` is at most 1 here, after a successful `b.get(i)`. |
| crates/sequencer/src/nonce_decode.rs:50 | `let ll = (first - 0xf7) as usize;` | RLP prefix byte | PROVEN | Guarded by `first >= 0xf8` at line 49, so the difference is 1 to 8. |
| crates/sequencer/src/nonce_decode.rs:51 | `i += ll` | RLP prefix | PROVEN | `i <= 1` and `ll <= 8`. |
| crates/sequencer/src/nonce_decode.rs:64 | `let l = (p - 0x80) as usize;` | RLP prefix byte | PROVEN | Guarded by `p >= 0x80` at line 60 and `p <= 0x88` at line 63. |
| crates/sequencer/src/nonce_decode.rs:65 | `b.get(i + 1..i + 1 + l)?` | RLP cursor | PROVEN | The line 59 `get(i)` proved `i < b.len()`, and `l <= 8`. |
| crates/sequencer/src/nonce_decode.rs:71 | `v = (v << 8) \| u64::from(x)` | nonce bytes | PROVEN | At most 8 bytes, so the shifts fill a `u64` exactly. |
| crates/sequencer/src/nonce_decode.rs:83 | `0x00..=0x7f => i + 1` | RLP cursor | PROVEN | `i < b.len()` from the line 81 `get`. |
| crates/sequencer/src/nonce_decode.rs:84 | `0x80..=0xb7 => i + 1 + (p - 0x80) as usize` | RLP prefix | PROVEN | The match arm bounds the difference at 55. |
| crates/sequencer/src/nonce_decode.rs:86 | `let ll = (p - 0xb7) as usize;` | RLP prefix | PROVEN | The `0xb8..=0xbf` arm bounds the difference at 1 to 8. Line 95 is the same for `0xf8..=0xff`. |
| crates/sequencer/src/nonce_decode.rs:89 | `l = (l << 8) \| x as usize` | RLP length bytes | PROVEN | At most 8 iterations from a zero start, so the shifts fill a `usize` exactly. Line 98 is the same. The result feeds the FIX at line 91. |
| crates/sequencer/src/nonce_decode.rs:93 | `0xc0..=0xf7 => i + 1 + (p - 0xc0) as usize` | RLP prefix | PROVEN | The match arm bounds the difference at 55. |
| crates/sequencer/src/state/mod.rs:126 | `let mut advanced = nonce.saturating_add(1);` | tx nonce | PROVEN | Correct form. Lines 133 and 189 are the same. |
| crates/sequencer/src/resync/mod.rs:289 | `let floor = u.executed_nonce.saturating_add(1);` | receipt nonce | PROVEN | Correct form. |
| crates/sequencer/src/pending.rs:159 | `self.next = self.next.checked_add(1)?;` | drain cursor | PROVEN | Correct form. |
| crates/sequencer/src/resync/mod.rs:402 | `now.duration_since(since).as_millis() as u64` | `Instant` delta | PROVEN | A process-lifetime delta in milliseconds. Lines 404 and 449 are the same. `Instant::duration_since` saturates to zero, so no panic. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:84 | `now.duration_since(prev).as_millis() as u64` | `Instant` delta | PROVEN | Same bound. |
| crates/sequencer/src/metrics.rs:156 | `.increment(n as u64)` | `usize` count | PROVEN | Widening on a 64-bit target. Line 162 is the same. |
| crates/sequencer/src/metrics.rs:140 | `.set(senders as f64)` | `usize` count | PROVEN | A gauge. Lossy only above 2^53 senders. Lines 148 and 152 are the same. |
| crates/sequencer/src/sequencer.rs:185 | `let chunk = BATCH_MAX.min(rest.len());` | drained batch | PROVEN | A `min` against a constant 16. |

## R13 non-zero types

| file:line | snippet | value | NonZero type and boundary | note |
|---|---|---|---|---|
| crates/sequencer/src/partition.rs:16 | `debug_assert!(m >= 1, ...)` | partition count | `NonZeroU32` parameter | The assert is compiled out in release. Line 19 then divides by it. |
| crates/sequencer/src/partition.rs:19 | `(leading % m as u64) as u32` | partition count | `NonZeroU32` parameter | Division by a possibly-zero value with no release guard. This is a defect. Callers (`sequencer.rs:415`, `feeds.rs:224`) pass `cfg.partition_count`, whose invariant lives only in `validate()`. |
| crates/sequencer/src/partition.rs:29 | `if m == 0 { Err(..Zero) }` | partition count | `NonZeroU32` | `validate_partition_count` disappears once the type carries the invariant. |
| crates/sequencer/src/config.rs:9 | `pub partition_count: u32` | partition count | `NonZeroU32`, boundary: serde TOML plus `--partition-count` | The field is the root cause of every row above. |
| crates/sequencer/src/config.rs:83 | `if self.partition_count == 0` | partition count | `NonZeroU32` | Runtime check that a type would make unnecessary. |
| crates/sequencer/src/config.rs:112 | `let m = self.partition_count.max(1);` | partition count | `NonZeroU32` | `.max(1)` hides a zero count. `rotate_partition` (main.rs:136) runs BEFORE `validate()` (main.rs:179), so a zero count really can reach here. |
| crates/sequencer/src/config.rs:11 | `pub partition_index: u32` | partition index | Newtype over `u32`, boundary: serde plus `--partition-index` | No `PartitionIndex` type exists. The `< partition_count` invariant lives only in `validate()`. |
| crates/sequencer/src/resync/mod.rs:117 | `pub dedup_capacity: u64` | dedup capacity | `NonZeroU64`, boundary: `[resync]` TOML plus `--cluster-dedup-capacity` | A capacity that must match the cluster's `dedupCapacity`. |
| crates/sequencer/src/resync/mod.rs:156 | `(..).max(1)` | enter threshold | `NonZeroU64` | The `.max(1)` exists only because `dedup_capacity` or `enter_percent` may be 0. |
| crates/sequencer/src/resync/mod.rs:121 | `pub enter_percent: u64` | percent | `NonZeroU64`, boundary: TOML plus `--resync-enter-percent` | Also unbounded above. A value over 100 makes the threshold exceed the capacity it protects. Neither end is checked. |
| crates/sequencer/src/pending.rs:74 | `if self.capacity == 0 { DroppedBufferDisabled }` | buffer capacity | `Option<NonZeroUsize>`, boundary: `max_pending_per_sender` in TOML | Zero is a deliberate "disabled" sentinel, so `Option<NonZeroUsize>` states it, not a bare `usize`. |
| crates/cluster-adapter/src/config.rs:45 | `if self.keep_alive_interval_ms == 0 { .. = 1000 }` | keep-alive period | `Option<NonZeroU64>`, boundary: `[cluster]` TOML | Zero means "omitted". `SessionDriver::new` takes the value raw (`session/mod.rs:125`); a zero would emit a keep-alive on every poll. |
| crates/cluster-adapter/src/config.rs:39 | `if self.ingress_stream_id == 0 { .. = 101 }` | stream id | `Option<i32>`, boundary: `[cluster]` TOML | Zero is the "omitted" sentinel, so a real stream id 0 cannot be configured. Line 42 is the same for `egress_stream_id`. |
| crates/cluster-adapter/src/live/session_loop.rs:93 | `if self.last_ms == 0 \|\| ..` | last-send timestamp | `Option<u64>` | Zero is the "never sent or re-armed" sentinel. `rearm()` (line 87) writes it and line 412 reads it back. A clock that legitimately returns 0 re-fires every tick. |
| crates/cluster-adapter/src/live/endpoints.rs:58 | `if ids.is_empty() { return None; }` | member count | Guarded, no change needed | The `% ids.len()` at line 63 is protected by this early return. |

## Tests

| file:line | snippet | why |
|---|---|---|
| crates/sequencer/tests/alloc_profile.rs:169 | `n as f64 / wall.as_secs_f64() / 1e3` | `wall.as_secs_f64()` can be 0.0 on a fast run. Float division prints `inf` instead of a throughput. |
| crates/sequencer/tests/alloc_profile.rs:160 | `allocs as f64 / n as f64` | `n` is `SENDERS * MEASURED_NONCES` (line 108). It is a constant product, so it is non-zero today, but nothing in the type says so. Lines 161 and 165 divide by the same `n`. |
