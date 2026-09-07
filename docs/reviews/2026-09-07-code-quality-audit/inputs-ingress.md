# Implementation inputs: ingress

Directories: crates/ingress, crates/interop-feed

## Clippy pedantic sites (153)

| file:line | lint | test | message |
|---|---|---|---|
| `crates/ingress/benches/latency.rs:25` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/benches/latency.rs:67` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/ingress/benches/latency.rs:67` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/ingress/benches/throughput.rs:26` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/benches/throughput.rs:69` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/ingress/benches/throughput.rs:69` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/ingress/src/aeron_adapters.rs:40` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/aeron_adapters.rs:87` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/aeron_adapters.rs:95` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/aeron_adapters.rs:106` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/aeron_adapters.rs:149` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/aeron_adapters.rs:232` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:10` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:60` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:73` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:76` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:80` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:228` | cast_possible_truncation |  | casting `u32` to `u8` may truncate the value |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:242` | cast_possible_truncation |  | casting `u32` to `u8` may truncate the value |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:6` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:28` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:30` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:32` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:39` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:40` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:41` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/ingress/src/binary.rs:59` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/binary.rs:82` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/binary.rs:124` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/ingress/src/channels.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:24` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:36` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:37` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:39` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:41` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:52` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:70` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:80` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/channels.rs:83` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/cluster.rs:91` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/cluster.rs:113` | cast_possible_truncation |  | casting `i32` to `u8` may truncate the value |
| `crates/ingress/src/cluster.rs:113` | cast_sign_loss |  | casting `i32` to `u8` may lose the sign of the value |
| `crates/ingress/src/json_rpc.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/json_rpc.rs:61` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/json_rpc.rs:84` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/json_rpc.rs:146` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/ingress/src/json_rpc.rs:175` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/ingress/src/json_rpc.rs:334` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/json_rpc.rs:414` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/ingress/src/json_rpc.rs:417` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/ingress/src/metrics.rs:12` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/metrics.rs:16` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/pending/mod.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/pending/mod.rs:88` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/src/pending/mod.rs:145` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/pending/mod.rs:152` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/pending/mod.rs:192` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/pending/mod.rs:238` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/pending/mod.rs:336` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/ingress/src/pending/mod.rs:415` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/proxy/mod.rs:44` | needless_continue |  | this `continue` expression is redundant |
| `crates/ingress/src/proxy/mod.rs:54` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/ingress/src/proxy/mod.rs:55` | cast_lossless |  | casts from `u16` to `u64` can be expressed infallibly using `From` |
| `crates/ingress/src/proxy/mod.rs:60` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/ingress/src/proxy/mod.rs:95` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:100` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:109` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:111` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:114` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:203` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:227` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:229` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/mod.rs:264` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/proxy/submit.rs:30` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/proxy/submit.rs:97` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/submit.rs:103` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/proxy/submit.rs:125` | unused_self |  | unused `self` argument |
| `crates/ingress/src/proxy/submit.rs:211` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/proxy/watchers.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/rate_limit.rs:36` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/rate_limit.rs:46` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/rate_limit.rs:52` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/ingress/src/receipt_cache.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/receipt_cache.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/receipt_cache.rs:12` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/receipt_cache.rs:22` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/receipt_cache.rs:34` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/ingress/src/receipt_cache.rs:34` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/receipt_cache.rs:44` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/receipt_cache.rs:83` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/receipt_cache.rs:89` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/receipt_cache.rs:93` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/receipt_cache.rs:97` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/receipt_cache.rs:154` | cast_possible_truncation |  | casting `u64` to `i32` may truncate the value |
| `crates/ingress/src/routing.rs:10` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/ingress/src/routing.rs:10` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/ingress/src/routing.rs:14` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/ingress/src/routing.rs:14` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/ingress/src/seen_receipts.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/seen_receipts.rs:45` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/ingress/src/seen_receipts.rs:45` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/seen_receipts.rs:60` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/ingress/src/sig_verify.rs:37` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/sig_verify.rs:80` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/sig_verify.rs:81` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/ingress/src/sig_verify.rs:87` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/ingress/src/sig_verify.rs:87` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/src/sig_verify.rs:210` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/ingress/src/sig_verify.rs:264` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/src/sig_verify.rs:271` | similar_names |  | binding's name is too similar to existing binding |
| `crates/ingress/src/tx_error_dedup.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/tx_error_dedup.rs:18` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/tx_error_dedup.rs:82` | doc_markdown |  | item in documentation is missing backticks |
| `crates/ingress/src/tx_error_dedup.rs:96` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/ingress/src/tx_error_dedup.rs:96` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/ingress/tests/common/mod.rs:24` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/tests/common/mod.rs:31` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/ingress/tests/common/mod.rs:52` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/tests/common/mod.rs:75` | default_trait_access | yes | calling `AccessList::default()` is more clear than this expression |
| `crates/ingress/tests/common/mod.rs:78` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/tests/end_to_end_test.rs:188` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/ingress/tests/end_to_end_test.rs:208` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/ingress/tests/end_to_end_test.rs:282` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/ingress/tests/replicated_cluster_test.rs:4` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/ingress/tests/replicated_cluster_test.rs:43` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/ingress/tests/replicated_cluster_test.rs:53` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/ingress/tests/replicated_cluster_test.rs:75` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/ingress/tests/replicated_cluster_test.rs:140` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/ingress/tests/replicated_cluster_test.rs:140` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/ingress/tests/sig_verify_batch.rs:28` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/tests/sig_verify_batch.rs:101` | borrow_as_ptr | yes | implicit borrow as raw pointer |
| `crates/ingress/tests/sig_verify_batch.rs:102` | cast_sign_loss | yes | casting `i64` to `u64` may lose the sign of the value |
| `crates/ingress/tests/sig_verify_batch.rs:155` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/sig_verify_batch.rs:156` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/sig_verify_batch.rs:163` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/sig_verify_batch.rs:164` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/sig_verify_batch.rs:184` | map_unwrap_or | yes | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/ingress/tests/sig_verify_batch.rs:208` | cast_precision_loss | yes | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/stage_costs.rs:18` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/ingress/tests/stage_costs.rs:41` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/stage_costs.rs:47` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/stage_costs.rs:50` | explicit_iter_loop | yes | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/ingress/tests/stage_costs.rs:53` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/stage_costs.rs:85` | explicit_iter_loop | yes | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/ingress/tests/stage_costs.rs:88` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/stage_costs.rs:117` | cast_precision_loss | yes | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/stage_costs.rs:118` | cast_precision_loss | yes | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/ingress/tests/stage_costs.rs:119` | cast_precision_loss | yes | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/interop-feed/src/lib.rs:82` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/interop-feed/src/lib.rs:196` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/interop-feed/src/lib.rs:262` | must_use_candidate |  | this method could have a `#[must_use]` attribute |

## Mechanical rows for this group

| 94 | `main` | `crates/ingress/src/bin/kardamom-ingress/main.rs:166` |
| 52 | `receipt_to_rpc` | `crates/ingress/src/json_rpc.rs:268` |
| 90 | `ingress_stage_costs` | `crates/ingress/tests/stage_costs.rs:32` |
| 68 | `one_hundred_txs_route_and_receive_receipts` | `crates/ingress/tests/end_to_end_test.rs:26` |
| 67 | `bench_throughput` | `crates/ingress/benches/throughput.rs:47` |
| 62 | `mds_duplicate_receipts_dedup_resolves_submit_once` | `crates/ingress/tests/end_to_end_test.rs:191` |
| 60 | `bench_e2e_latency` | `crates/ingress/benches/latency.rs:44` |
| 59 | `each_tx_lands_on_keccak_partition` | `crates/ingress/tests/routing_test.rs:20` |
| 57 | `racing_replica_rejection_is_overridden_by_twin_success` | `crates/ingress/tests/end_to_end_test.rs:285` |
| 54 | `proxy_parks_until_watermark_advances` | `crates/ingress/tests/end_to_end_test.rs:119` |
| 51 | `subscription_streams_deduped_and_filtered_receipts` | `crates/ingress/tests/receipt_subscription_test.rs:136` |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:26` | `pub type RecorderReady = oneshot::Receiver<(u8, Result<i64, String>)>;` |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:38` | `pub fn spawn_tx_data_recorders(` |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:85` | `pub async fn wait_for_recorders(ready: Vec<RecorderReady>) -> Result<()> {` |
| `crates/ingress/src/json_rpc.rs:385` | `pub struct PeerAddrLayer;` |
| `crates/ingress/src/json_rpc.rs:395` | `pub struct PeerAddrService<S> {` |
| `crates/ingress/tests/common/mod.rs:15` | `pub fn sign_legacy_tx(signer: &PrivateKeySigner, nonce: u64) -> (TxEnvelope, Bytes, Addres` |
| `crates/ingress/tests/common/mod.rs:39` | `pub fn sign_legacy(signer: &PrivateKeySigner, nonce: u64) -> Bytes {` |
| `crates/ingress/tests/common/mod.rs:44` | `pub fn sign_legacy_with_gas(signer: &PrivateKeySigner, nonce: u64, gas_limit: u64) -> Byte` |
| `crates/ingress/tests/common/mod.rs:66` | `pub fn sign_eip4844(signer: &PrivateKeySigner, nonce: u64) -> Bytes {` |
| `crates/ingress/tests/sig_verify_batch.rs:18` | `pub fn signed(nonce: u64) -> (AlloyEnvelope, Bytes, Address) {` |
