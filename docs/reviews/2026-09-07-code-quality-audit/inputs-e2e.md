# Implementation inputs: e2e

Directories: crates/e2e

## Clippy pedantic sites (232)

| file:line | lint | test | message |
|---|---|---|---|
| `crates/e2e/benches/e2e_throughput.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/e2e/benches/e2e_throughput.rs:9` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/e2e/benches/e2e_throughput.rs:75` | match_same_arms | yes | these match arms have identical bodies |
| `crates/e2e/benches/e2e_throughput.rs:92` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/e2e/benches/e2e_throughput.rs:93` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/e2e/benches/e2e_throughput.rs:111` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/e2e/src/bin/kardamom-semantics.rs:6` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/bin/kardamom-semantics.rs:224` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/e2e/src/harness/aeron.rs:20` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/aeron.rs:51` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/inject.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/harness/inject.rs:28` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/inject.rs:90` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:33` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/harness/l1/mod.rs:43` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/e2e/src/harness/l1/mod.rs:65` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:176` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/l1/mod.rs:181` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/l1/mod.rs:186` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:197` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:210` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:239` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:266` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:301` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:322` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:341` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:395` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:400` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:406` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:422` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1/mod.rs:442` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1_verified.rs:68` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/e2e/src/harness/l1_verified.rs:68` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l1_verified.rs:123` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/l1_verified.rs:128` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/e2e/src/harness/l1_verified.rs:132` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/l1_verified.rs:233` | match_same_arms |  | these match arms have identical bodies |
| `crates/e2e/src/harness/l1_verified.rs:250` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/e2e/src/harness/l2.rs:79` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l2.rs:152` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/e2e/src/harness/l2.rs:166` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l2.rs:183` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l2.rs:203` | similar_names |  | binding's name is too similar to existing binding |
| `crates/e2e/src/harness/l2.rs:219` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l2.rs:238` | similar_names |  | binding's name is too similar to existing binding |
| `crates/e2e/src/harness/l2.rs:254` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/l2.rs:275` | similar_names |  | binding's name is too similar to existing binding |
| `crates/e2e/src/harness/l2.rs:300` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/e2e/src/harness/metrics.rs:20` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/metrics.rs:46` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/metrics.rs:66` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/metrics.rs:75` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:44` | struct_excessive_bools |  | more than 3 bools in a struct |
| `crates/e2e/src/harness/mod.rs:80` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/harness/mod.rs:244` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:256` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:264` | too_many_lines |  | this function has too many lines (156/100) |
| `crates/e2e/src/harness/mod.rs:473` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:487` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:492` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:497` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:501` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:505` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:542` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:574` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:598` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:607` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:612` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:620` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:647` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:658` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/harness/mod.rs:668` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/mod.rs:715` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/harness/mod.rs:721` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/harness/mod.rs:753` | float_cmp |  | strict comparison of `f32` or `f64` |
| `crates/e2e/src/harness/mod.rs:768` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/mod.rs:809` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/e2e/src/harness/proc.rs:33` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/proc.rs:75` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/proc.rs:94` | cast_possible_wrap |  | casting `u32` to `i32` may wrap around the value |
| `crates/e2e/src/harness/proc.rs:113` | cast_possible_wrap |  | casting `u32` to `i32` may wrap around the value |
| `crates/e2e/src/harness/proc.rs:121` | cast_possible_wrap |  | casting `u32` to `i32` may wrap around the value |
| `crates/e2e/src/harness/proc.rs:145` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/proc.rs:167` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/proc.rs:194` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/proc.rs:223` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/proc.rs:229` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/sealer.rs:21` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/sealer.rs:49` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/harness/sealer.rs:53` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/sealer.rs:53` | too_many_lines |  | this function has too many lines (101/100) |
| `crates/e2e/src/harness/sealer.rs:133` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/e2e/src/harness/sealer.rs:143` | map_unwrap_or |  | called `map(<f>).unwrap_or(false)` on a `Result` value |
| `crates/e2e/src/harness/services.rs:46` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/services.rs:60` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:71` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:86` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/e2e/src/harness/services.rs:141` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:170` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:211` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:252` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:261` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:344` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/harness/services.rs:429` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/lib.rs:13` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/lib.rs:19` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/pipeline.rs:18` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/e2e/src/scenarios/bridge.rs:52` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/bridge.rs:53` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/bridge.rs:135` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/bridge.rs:142` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/bridge.rs:224` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/e2e/src/scenarios/bridge.rs:224` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/bridge.rs:224` | too_many_lines |  | this function has too many lines (172/100) |
| `crates/e2e/src/scenarios/bridge.rs:257` | default_trait_access |  | calling `Arc::default()` is more clear than this expression |
| `crates/e2e/src/scenarios/bridge.rs:264` | default_trait_access |  | calling `Arc::default()` is more clear than this expression |
| `crates/e2e/src/scenarios/consistency.rs:72` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/consistency.rs:72` | too_many_lines |  | this function has too many lines (128/100) |
| `crates/e2e/src/scenarios/consistency.rs:73` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/consistency.rs:123` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/e2e/src/scenarios/consistency.rs:174` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/e2e/src/scenarios/consistency.rs:228` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/crash_recovery.rs:64` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/crash_recovery.rs:65` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/crash_recovery.rs:81` | cast_possible_truncation |  | casting `f64` to `u64` may truncate the value |
| `crates/e2e/src/scenarios/crash_recovery.rs:81` | cast_sign_loss |  | casting `f64` to `u64` may lose the sign of the value |
| `crates/e2e/src/scenarios/crash_recovery.rs:90` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/crash_recovery.rs:91` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/crash_recovery.rs:106` | cast_possible_truncation |  | casting `f64` to `u64` may truncate the value |
| `crates/e2e/src/scenarios/crash_recovery.rs:106` | cast_sign_loss |  | casting `f64` to `u64` may lose the sign of the value |
| `crates/e2e/src/scenarios/crash_recovery.rs:123` | cast_possible_truncation |  | casting `f64` to `u64` may truncate the value |
| `crates/e2e/src/scenarios/crash_recovery.rs:123` | cast_sign_loss |  | casting `f64` to `u64` may lose the sign of the value |
| `crates/e2e/src/scenarios/da_parity.rs:75` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/da_parity.rs:76` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/da_parity.rs:159` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/da_parity.rs:181` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/e2e/src/scenarios/da_parity.rs:194` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/da_parity.rs:257` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:46` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:72` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:91` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:156` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:158` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/e2e/src/scenarios/derivation.rs:187` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:258` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:322` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/derivation.rs:371` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/e2e/src/scenarios/derivation.rs:384` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/divergence.rs:10` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/scenarios/divergence.rs:30` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/divergence.rs:68` | cast_possible_truncation |  | casting `f64` to `u64` may truncate the value |
| `crates/e2e/src/scenarios/divergence.rs:68` | cast_sign_loss |  | casting `f64` to `u64` may lose the sign of the value |
| `crates/e2e/src/scenarios/divergence.rs:121` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/divergence.rs:206` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/divergence.rs:206` | too_many_lines |  | this function has too many lines (110/100) |
| `crates/e2e/src/scenarios/divergence.rs:328` | uninlined_format_args |  | variables can be used directly in the `format!` string |
| `crates/e2e/src/scenarios/l1_batch.rs:30` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/l1_batch.rs:99` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/e2e/src/scenarios/l1_batch.rs:104` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/e2e/src/scenarios/mod.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/scenarios/mod.rs:75` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:81` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:90` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:98` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:114` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:146` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:167` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:178` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/e2e/src/scenarios/mod.rs:184` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:194` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:224` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:252` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/e2e/src/scenarios/mod.rs:262` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:294` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:301` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/mod.rs:304` | float_cmp |  | strict comparison of `f32` or `f64` |
| `crates/e2e/src/scenarios/mod.rs:310` | float_cmp |  | strict comparison of `f32` or `f64` |
| `crates/e2e/src/scenarios/nonce_gap.rs:50` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/nonce_gap.rs:50` | too_many_lines |  | this function has too many lines (129/100) |
| `crates/e2e/src/scenarios/nonce_gap.rs:52` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/nonce_gap.rs:130` | uninlined_format_args |  | variables can be used directly in the `format!` string |
| `crates/e2e/src/scenarios/nonce_gap.rs:131` | float_cmp |  | strict comparison of `f32` or `f64` |
| `crates/e2e/src/scenarios/nonce_unordered.rs:40` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/nonce_unordered.rs:41` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/nonce_unordered.rs:97` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/e2e/src/scenarios/nonce_unordered.rs:100` | uninlined_format_args |  | variables can be used directly in the `format!` string |
| `crates/e2e/src/scenarios/nonce_unordered.rs:101` | float_cmp |  | strict comparison of `f32` or `f64` |
| `crates/e2e/src/scenarios/nonce_unordered.rs:101` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/e2e/src/scenarios/rpc_liveness.rs:91` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/rpc_liveness.rs:92` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/rpc_liveness.rs:204` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/rpc_liveness.rs:211` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/rpc_liveness.rs:270` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/rpc_liveness.rs:271` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/rpc_vectors.rs:74` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/rpc_vectors.rs:107` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/rpc_vectors.rs:161` | default_trait_access |  | calling `AccessList::default()` is more clear than this expression |
| `crates/e2e/src/scenarios/upgrade.rs:196` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/upgrade.rs:234` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/upgrade.rs:344` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/upgrade.rs:368` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/src/scenarios/xchain.rs:56` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/scenarios/xchain.rs:133` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/e2e/src/scenarios/xchain.rs:166` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain.rs:166` | too_many_lines |  | this function has too many lines (223/100) |
| `crates/e2e/src/scenarios/xchain.rs:401` | float_cmp |  | strict comparison of `f32` or `f64` |
| `crates/e2e/src/scenarios/xchain.rs:431` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:70` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:70` | too_many_lines |  | this function has too many lines (124/100) |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:154` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:226` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:237` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:251` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:317` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/e2e/src/scenarios/xchain_da_parity.rs:320` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/e2e/src/scenarios/xchain_two_stacks.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/e2e/src/scenarios/xchain_two_stacks.rs:59` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/e2e/src/scenarios/xchain_two_stacks.rs:92` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain_two_stacks.rs:198` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/src/scenarios/xchain_two_stacks.rs:198` | too_many_lines |  | this function has too many lines (128/100) |
| `crates/e2e/src/scenarios/xchain_two_stacks.rs:357` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/e2e/tests/chain_semantics/bridge_da.rs:6` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/e2e/tests/chain_semantics/bridge_da.rs:115` | cast_possible_truncation | yes | casting `f64` to `u64` may truncate the value |
| `crates/e2e/tests/chain_semantics/bridge_da.rs:115` | cast_sign_loss | yes | casting `f64` to `u64` may lose the sign of the value |
| `crates/e2e/tests/chain_semantics/bridge_da.rs:119` | map_unwrap_or | yes | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/e2e/tests/chain_semantics/consistency.rs:40` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/e2e/tests/chain_semantics/consistency.rs:142` | redundant_closure_for_method_calls | yes | redundant closure |
| `crates/e2e/tests/chain_semantics/pipeline.rs:64` | cast_possible_truncation | yes | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/e2e/tests/chain_semantics/xchain.rs:66` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/e2e/tests/chain_semantics/xchain.rs:152` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/e2e/tests/chain_semantics/xchain.rs:258` | semicolon_if_nothing_returned | yes | consider adding a `;` to the last statement for consistent formatting |

## Mechanical rows for this group

| 156 | `launch_with_l1` | `crates/e2e/src/harness/mod.rs:264` |
| 101 | `launch` | `crates/e2e/src/harness/sealer.rs:53` |
| 96 | `launch` | `crates/e2e/src/harness/l1/mod.rs:65` |
| 74 | `run_case` | `crates/e2e/src/bin/kardamom-semantics.rs:152` |
| 56 | `launch` | `crates/e2e/src/harness/aeron.rs:51` |
| 51 | `spawn_executor_at` | `crates/e2e/src/harness/services.rs:261` |
| 223 | `delivery` | `crates/e2e/src/scenarios/xchain.rs:166` |
| 172 | `finalize_withdrawal` | `crates/e2e/src/scenarios/bridge.rs:224` |
| 129 | `run` | `crates/e2e/src/scenarios/nonce_gap.rs:50` |
| 128 | `forward_leg` | `crates/e2e/src/scenarios/xchain_two_stacks.rs:198` |
| 128 | `run` | `crates/e2e/src/scenarios/consistency.rs:72` |
| 124 | `collect_canonical_blocks` | `crates/e2e/src/scenarios/xchain_da_parity.rs:70` |
| 110 | `verified_l1_endpoint` | `crates/e2e/src/scenarios/divergence.rs:206` |
| 98 | `run_e2e_throughput` | `crates/e2e/benches/e2e_throughput.rs:25` |
| 85 | `s13_xchain_da_parity` | `crates/e2e/tests/chain_semantics/xchain.rs:161` |
| 80 | `run` | `crates/e2e/src/scenarios/rpc_liveness.rs:91` |
| 74 | `l1_batch` | `crates/e2e/src/scenarios/l1_batch.rs:30` |
| 70 | `run` | `crates/e2e/src/scenarios/nonce_unordered.rs:40` |
| 69 | `callback_leg` | `crates/e2e/src/scenarios/xchain_two_stacks.rs:357` |
| 68 | `run_workload` | `crates/e2e/src/scenarios/da_parity.rs:75` |
| 67 | `activates_at_timestamp` | `crates/e2e/src/scenarios/upgrade.rs:234` |
| 65 | `assert_reconstructed_interop_state` | `crates/e2e/src/scenarios/xchain_da_parity.rs:251` |
| 64 | `build_substitutions` | `crates/e2e/src/scenarios/rpc_vectors.rs:106` |
| 63 | `gap_halts_pair_not_chain` | `crates/e2e/src/scenarios/xchain.rs:431` |
| 60 | `corrupt_bal_halts_validator` | `crates/e2e/src/scenarios/divergence.rs:30` |
| 57 | `initiate_withdrawal` | `crates/e2e/src/scenarios/bridge.rs:135` |
| 52 | `s14_xchain_two_stacks` | `crates/e2e/tests/chain_semantics/xchain.rs:80` |
| 52 | `forged_epoch_halts_validator` | `crates/e2e/src/scenarios/divergence.rs:121` |
| 569 | 845 | `crates/e2e/src/harness/mod.rs` |
| crates/e2e/src/harness/mod.rs | 25 |
