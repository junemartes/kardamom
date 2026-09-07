# ingress

## Summary
The ingress crate does very little arithmetic, so the risk is concentrated in three places.
The shard path is the hot spot: `--shards` is a plain `u32` clap arg, it becomes the modulus in
`partition_for`, and the only zero guard is a `debug_assert!`. So `--shards 0` divides by zero in a
release build, and `--shards 256` truncates to 0 in the `as u8` cast that opens the publisher
handles. That is 1 division by a possibly-zero value and 2 truncating casts on one config value.
The rate limiter does no arithmetic in this crate; it already takes `NonZeroU32` for rate and burst
and hands both to `governor`, which is the model the rest of the crate should copy. Nonce values in
`pending/` are only map keys, so there is no nonce arithmetic; the one counter there, `park_seq`,
is proven by the `split_off` sentinel comment. `interop-feed` has no arithmetic at all in production
code: `value` already narrows through `u128::try_from`, and the 11 index sites the seed lists are
`serde_json::Value` lookups in tests. Counts: R12 16 sites (4 FIX, 1 opposite-mistake flag, 11
PROVEN), R13 15 sites (1 defect, 11 FIX, 3 keep).

## R12 safe arithmetic
| file:line | expression (max 80 chars) | operand source | verdict | fix or bound |
| --- | --- | --- | --- | --- |
| crates/ingress/src/routing.rs:14 | `(leading % m as u64) as u32` | `m` = `--shards` CLI arg via `cfg.partition_count_m` | FIX | Make `m` a `NonZeroU32`. The `%` panics when `m == 0`; only a `debug_assert!` guards it (routing.rs:11). The outer `as u32` is safe: the result is `< m <= u32::MAX`. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:228 | `args.shards as u8` | `--shards` clap arg, `u32` | FIX | `u8::try_from(args.shards)` with an error. `--shards 256` truncates to 0, so no recorder starts. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:242 | `LiveIngressPublication::open(&rt, &channels, args.shards as u8)` | `--shards` clap arg, `u32` | FIX | `u8::try_from` with an error. `--shards 260` opens 4 handles while routing uses 260 partitions, so most publishes fail with "shard out of range". |
| crates/ingress/src/binary.rs:124 | `sock.write_all(&(payload.len() as u32).to_be_bytes())` | reply payload length | FIX | `u32::try_from(payload.len())`. Narrowing cast on a length. |
| crates/ingress/src/sig_verify.rs:105 | `let deadline = Instant::now() + flush_window;` | clock plus `cfg.sig_verify_flush_window` | FIX | `Instant::now().checked_add(flush_window)`, or call `sleep(flush_window)`. `Add` panics on overflow. Only producer today is the 50µs default. |
| crates/ingress/src/sig_verify.rs:89 | `let parallelism = parallelism.max(1);` | caller parameter | FIX (opposite mistake) | Silent clamping hides a caller that passes 0. Take `NonZeroUsize` instead. |
| crates/ingress/src/binary.rs:97 | `let len = u32::from_be_bytes(len_buf) as usize;` | wire frame header | PROVEN | Widening cast; `usize` is at least 32 bits. `binary.rs:98` then bounds `len` by `MAX_FRAME_BYTES`. |
| crates/ingress/src/proxy/mod.rs:55 | `((ingress_id as u64) << 48) \| (seq & 0x0000_FFFF_FFFF_FFFF)` | config `ingress_id`, atomic counter | PROVEN | `u16` widened to `u64`, so `<< 48` cannot overflow. The mask bounds `seq` to 48 bits. |
| crates/ingress/src/proxy/mod.rs:61 | `(correlation_id >> 48) as u16` | packed correlation id | PROVEN | The shift leaves 16 significant bits, so the cast loses nothing. |
| crates/ingress/src/proxy/mod.rs:223 | `self.correlation_seq.fetch_add(1, Ordering::Relaxed)` | per-process counter | PROVEN | `fetch_add` wraps by definition, and the doc at proxy/mod.rs:210-220 states a wrap is harmless: the value is opaque and nothing orders on it. |
| crates/ingress/src/proxy/submit.rs:222 | `partition_for(v.sender, self.cfg.partition_count_m) as usize` | routed partition index | PROVEN | `u32` to `usize` is widening. The adapter also range-checks the shard. |
| crates/ingress/src/cluster.rs:83 | `BPosition::from_index(count - 1)` | cluster egress durable count | PROVEN | `cluster.rs:80` returns early when `count == 0`. |
| crates/ingress/src/pending/mod.rs:88 | `metrics::gauge!(QUEUE_DEPTH).set(map.len() as f64)` | registry size | PROVEN | `cfg.pending_shed_depth` (default 16,384) bounds the depth, far below the 2^53 an `f64` holds exactly. |
| crates/ingress/src/pending/mod.rs:216 | `.fetch_add(1, std::sync::atomic::Ordering::Relaxed)` | park counter | PROVEN | The comment at pending/mod.rs:348 states the seq never reaches `u64::MAX`, which is what makes the `split_off(&(eff, u64::MAX))` bound at line 354 exact. |
| crates/ingress/src/json_rpc.rs:287 | `log_index: Some(log_index as u64)` | log enumerate index | PROVEN | `usize` to `u64` is widening on every supported target. |
| crates/ingress/src/aeron_adapters.rs:45 | `Vec::with_capacity(shards as usize)` | shard count | PROVEN | `u8` to `usize` is widening. |

## R13 non-zero types
| file:line | snippet | value | NonZero type and boundary | note |
| --- | --- | --- | --- | --- |
| crates/ingress/src/routing.rs:11 | `debug_assert!(m > 0, "partition count must be positive")` | partition count M | `NonZeroU32`, parsed at the clap `--shards` arg | DEFECT. A `debug_assert!` is a no-op in release, and routing.rs:14 divides by `m`. `--shards 0` panics every submit handler. |
| crates/ingress/src/config.rs:22 | `pub partition_count_m: u32` | partition count M | `NonZeroU32` | The field carries the divisor. Nothing between the CLI and `partition_for` checks it. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:75 | `shards: u32` | shard count | `NonZeroU8` at the clap arg | One type fixes both problems: the zero divisor and the `as u8` truncation at main.rs:228 and :242. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:181 | `partition_count_m: args.shards` | shard count | `NonZeroU32` | The only place the CLI value reaches the config. Parse it once here. |
| crates/ingress/src/aeron_adapters.rs:43 | `shards: u8,` | shard count | `NonZeroU8`, constructor parameter | With `shards == 0` the handle vector is empty and every publish returns "shard out of range". |
| crates/ingress/src/bin/kardamom-ingress/recorders.rs:42 | `shards: u8,` | shard count | `NonZeroU8`, constructor parameter | With `shards == 0` no recorder thread starts, and the ready barrier passes at once. |
| crates/ingress/src/sig_verify.rs:88 | `assert!(depth > 0);` | sig-verify ring depth | `NonZeroUsize`, from `cfg.sig_verify_batch_depth` (config.rs:35) | A runtime `assert!` in a constructor. The field has no CLI flag, so the default 64 is the only producer today. |
| crates/ingress/src/sig_verify.rs:89 | `let parallelism = parallelism.max(1);` | worker count | `NonZeroUsize`, constructor parameter | Clamping hides a zero caller. See the R12 row. |
| crates/ingress/src/sig_verify.rs:82 | `.map(\|n\| n.get().clamp(1, 8))` | worker count | `NonZeroUsize` | `available_parallelism()` already returns `NonZeroUsize`. The `.get()` throws that away and the `clamp(1, ..)` rebuilds it. Keep the type. |
| crates/ingress/src/receipt_cache.rs:35 | `assert!(capacity > 0);` | cache capacity | `NonZeroUsize`, from `cfg.receipt_cache_capacity` (config.rs:45) | A bare `assert!` with no message. |
| crates/ingress/src/seen_receipts.rs:46 | `assert!(capacity > 0, "SeenReceipts capacity must be > 0")` | dedup set capacity | `NonZeroUsize`, constructor parameter | Only producer is the `DEFAULT_CAPACITY` const (seen_receipts.rs:29). |
| crates/ingress/src/tx_error_dedup.rs:97 | `assert!(capacity > 0, "TxErrorDedup capacity must be > 0")` | dedup map capacity | `NonZeroUsize`, constructor parameter | Only producer is the `DEFAULT_CAPACITY` const (tx_error_dedup.rs:59). |
| crates/ingress/src/config.rs:42 | `pub chain_id: u64` | L2 chain id | `NonZeroU64`, parsed at the clap `--chain-id` arg (main.rs:131) | EIP-155 forbids chain id 0, and nothing rejects it. |
| crates/interop-feed/src/lib.rs:105 | `pub origin_chain_id: u64` | wire chain id | `NonZeroU64` at the wire decode | Also `dest_chain_id` (lib.rs:114) and `AttestationDto::chain_id` (lib.rs:282). `into_outbox_message` compares `origin_chain_id` but never rejects 0. |
| crates/ingress/src/cluster.rs:80 | `if count == 0 {` | durable record count | Keep as is | Not a divisor. It is a "no durable position yet" sentinel that guards the `count - 1` on line 83. Correct as written. |
| crates/ingress/src/config.rs:64 | `pub pending_shed_depth: usize` | shed threshold | Keep as is | 0 is a documented test hook: it sheds every submission. Not a non-zero value. |
| crates/ingress/src/config.rs:31 | `pub rate_limit_per_ip_per_sec: NonZeroU32` | token rate and burst | Already `NonZeroU32` | Correct model, with one gap: neither field has a CLI flag or a TOML field, so the values come only from `IngressConfig::default()`. |

## Tests
None found. Every division in `tests/`, `benches/`, and the in-file test modules divides by a
literal or by a `len()` that a literal range fills. Examples: `envs.len() / threads` with `threads`
from `[1, 2, 4]` (tests/stage_costs.rs:92), and `idx % pre.len()` with `pre` built from `0..1000`
(benches/latency.rs:99).
