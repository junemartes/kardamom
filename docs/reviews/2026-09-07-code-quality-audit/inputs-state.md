# Implementation inputs: state

Directories: crates/state, crates/types

## Clippy pedantic sites (237)

| file:line | lint | test | message |
|---|---|---|---|
| `crates/state/benches/write_throughput.rs:34` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/state/src/bin/kardamom-statecheck.rs:51` | map_unwrap_or |  | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/state/src/bin/kardamom-statecheck.rs:139` | bool_to_int_with_if |  | boolean to int conversion using if |
| `crates/state/src/checkpoint/manifest.rs:46` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/checkpoint/manifest.rs:53` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/manifest.rs:91` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/checkpoint/manifest.rs:127` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/manifest.rs:194` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/mod.rs:76` | case_sensitive_file_extension_comparisons |  | case-sensitive file extension comparison |
| `crates/state/src/checkpoint/mod.rs:106` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/mod.rs:156` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/mod.rs:179` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/mod.rs:200` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/mod.rs:244` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/mod.rs:261` | map_unwrap_or |  | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/state/src/checkpoint/mod.rs:331` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/mod.rs:358` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint/tests.rs:219` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/state/src/checkpoint_transfer.rs:87` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/checkpoint_transfer.rs:217` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/compaction.rs:30` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/env.rs:30` | doc_markdown |  | item in documentation is missing backticks |
| `crates/state/src/env.rs:63` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/env.rs:63` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/state/src/env.rs:77` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/env.rs:77` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/state/src/env.rs:93` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/env.rs:93` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/state/src/env.rs:98` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/env.rs:162` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/env.rs:166` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/genesis.rs:28` | doc_markdown |  | item in documentation is missing backticks |
| `crates/state/src/genesis.rs:56` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/genesis.rs:81` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/genesis.rs:95` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/state/src/genesis.rs:145` | match_same_arms |  | these match arms have identical bodies |
| `crates/state/src/integrity/compare.rs:24` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/integrity/compare.rs:24` | too_many_lines |  | this function has too many lines (102/100) |
| `crates/state/src/integrity/compare.rs:78` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/state/src/integrity/mod.rs:61` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/integrity/mod.rs:72` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/integrity/tests.rs:28` | unreadable_literal |  | long literal lacking separators |
| `crates/state/src/meta.rs:68` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/meta.rs:79` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/meta.rs:90` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/meta.rs:101` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/meta.rs:112` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/meta.rs:116` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/meta.rs:129` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/meta.rs:133` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/meta.rs:146` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/meta.rs:153` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/meta.rs:171` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/meta.rs:175` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/recovery/mod.rs:49` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/recovery/mod.rs:95` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/recovery/mod.rs:125` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/recovery/mod.rs:208` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/state/src/recovery/mod.rs:208` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/schema.rs:10` | doc_markdown |  | item in documentation is missing backticks |
| `crates/state/src/schema.rs:15` | doc_markdown |  | item in documentation is missing backticks |
| `crates/state/src/schema.rs:71` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:75` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:81` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/schema.rs:88` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:95` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:99` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/schema.rs:114` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:141` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:145` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:154` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/state/src/schema.rs:154` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/schema.rs:186` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/state/src/schema.rs:186` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:194` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/schema.rs:210` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:214` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/schema.rs:218` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/snapshot.rs:61` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/snapshot.rs:95` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/snapshot.rs:100` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/snapshot.rs:110` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/snapshot.rs:135` | used_underscore_binding |  | used underscore-prefixed binding |
| `crates/state/src/swap.rs:46` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/swap.rs:83` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/swap.rs:91` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/swap.rs:100` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/trie/incremental_tests.rs:106` | map_unwrap_or | yes | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/state/src/trie/incremental_tests.rs:211` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/state/src/trie/incremental_tests.rs:233` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/state/src/trie/incremental_tests.rs:244` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/state/src/trie/incremental_tests.rs:347` | many_single_char_names | yes | 5 bindings with single-character names in scope |
| `crates/state/src/trie/mod.rs:62` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/mod.rs:75` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/mod.rs:104` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/mod.rs:125` | match_same_arms |  | these match arms have identical bodies |
| `crates/state/src/trie/mod.rs:142` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/mod.rs:172` | match_same_arms |  | these match arms have identical bodies |
| `crates/state/src/trie/mod.rs:187` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/mod.rs:248` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/state/src/trie/mod.rs:293` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/mod.rs:377` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/trie/node.rs:21` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/state/src/trie/node.rs:33` | cast_possible_truncation |  | casting `usize` to `u16` may truncate the value |
| `crates/state/src/trie/node.rs:40` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/prefix_set.rs:37` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/trie/prefix_set.rs:45` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/trie/proofs.rs:36` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/proofs.rs:56` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/trie/walker.rs:303` | unnecessary_wraps |  | this function's return value is unnecessarily wrapped by `Result` |
| `crates/state/src/trie/walker.rs:305` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/state/src/writer/mod.rs:60` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/writer/mod.rs:66` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/state/src/writer/mod.rs:94` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/state/src/writer/mod.rs:94` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/writer/mod.rs:106` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/state/src/writer/mod.rs:142` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/writer/mod.rs:149` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/state/src/writer/mod.rs:153` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/state/src/writer/mod.rs:203` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/state/src/writer/mod.rs:203` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/state/src/writer/mod.rs:238` | too_many_lines |  | this function has too many lines (142/100) |
| `crates/state/src/writer/mod.rs:309` | match_same_arms |  | these match arms have identical bodies |
| `crates/state/src/writer/tests.rs:42` | map_unwrap_or |  | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/state/tests/common/mod.rs:29` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/state/tests/common/mod.rs:34` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/state/tests/compaction_smoke.rs:29` | map_unwrap_or | yes | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/state/tests/compaction_smoke.rs:33` | map_unwrap_or | yes | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/state/tests/docker_e2e.rs:6` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/state/tests/docker_e2e.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/state/tests/docker_e2e.rs:20` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/state/tests/docker_e2e.rs:23` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/state/tests/docker_e2e.rs:28` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/types/src/ack_policy.rs:37` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/ack_policy.rs:43` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/boundary.rs:18` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/boundary.rs:30` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/delta.rs:81` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/deposit.rs:90` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/epoch.rs:62` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/epoch.rs:68` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/epoch.rs:103` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/epoch.rs:266` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/epoch.rs:305` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/epoch.rs:343` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/types/src/genesis.rs:44` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/genesis.rs:50` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/types/src/genesis.rs:72` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/types/src/position.rs:40` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/position.rs:41` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/position.rs:44` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/position.rs:50` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/position.rs:58` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/position.rs:59` | cast_sign_loss |  | casting `i32` to `u32` may lose the sign of the value |
| `crates/types/src/position.rs:63` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/position.rs:68` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/position.rs:81` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/position.rs:100` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/types/src/position.rs:101` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/types/src/prover.rs:81` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/prover.rs:91` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/prover.rs:122` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/prover.rs:131` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/types/src/prover.rs:135` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/prover.rs:175` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/prover.rs:185` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/receipt.rs:24` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/receipt.rs:88` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/receipt.rs:90` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/receipt.rs:107` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/receipt.rs:112` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/receipt.rs:164` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/receipt.rs:192` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/receipt.rs:198` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/receipt.rs:204` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/state.rs:22` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/types/src/state.rs:23` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/types/src/state.rs:24` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/types/src/state.rs:27` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/types/src/state.rs:29` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/state.rs:31` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/types/src/tx_error.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:21` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:37` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:38` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:43` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/tx_ordering.rs:68` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:73` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:78` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:83` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:88` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:93` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:104` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:112` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:123` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/tx_ordering.rs:131` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/txref.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:6` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:13` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:21` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:33` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:35` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:47` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:50` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:52` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/txref.rs:63` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/upgrades.rs:62` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/withdrawals.rs:37` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/withdrawals.rs:55` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/withdrawals.rs:76` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/withdrawals.rs:117` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/withdrawals.rs:131` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/withdrawals.rs:151` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/withdrawals.rs:166` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/witness.rs:67` | doc_markdown |  | item in documentation is missing backticks |
| `crates/types/src/witness.rs:122` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/witness.rs:125` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/types/src/witness.rs:134` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/types/src/witness.rs:141` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/types/src/xchain.rs:68` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:75` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:90` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:118` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:148` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:158` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:253` | map_unwrap_or |  | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/types/src/xchain.rs:268` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:301` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:388` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:396` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/types/src/xchain.rs:414` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/types/src/xchain.rs:414` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |

## Mechanical rows for this group

| 142 | `apply` | `crates/state/src/writer/mod.rs:238` |
| 102 | `deep_compare` | `crates/state/src/integrity/compare.rs:24` |
| 93 | `fetch_latest_checkpoint` | `crates/state/src/checkpoint_transfer.rs:217` |
| 78 | `seed_genesis` | `crates/state/src/genesis.rs:81` |
| 74 | `main` | `crates/state/src/bin/kardamom-statecheck.rs:60` |
| 72 | `check_receipts_index` | `crates/state/src/integrity/checks.rs:153` |
| 68 | `bootstrap_trie_from_state` | `crates/state/src/recovery/mod.rs:125` |
| 56 | `derive_remote_epoch` | `crates/types/src/xchain.rs:414` |
| 52 | `check_headers` | `crates/state/src/integrity/checks.rs:93` |
| 51 | `read_response_head` | `crates/state/src/checkpoint_transfer.rs:376` |
| 97 | `incremental_equals_full_rebuild_over_random_blocks` | `crates/state/src/trie/incremental_tests.rs:181` |
| 84 | `update_for_block` | `crates/state/src/trie/mod.rs:187` |
| 81 | `extension_collapse_regrow_no_stale_orphans` | `crates/state/src/trie/incremental_tests.rs:321` |
| 71 | `incremental_block_on_bootstrapped_trie_matches_oracle` | `crates/state/src/recovery/tests.rs:178` |
| 64 | `bootstrap_builds_trie_matching_oracle_on_trie_off_state` | `crates/state/src/recovery/tests.rs:15` |
| 58 | `trie_writer_root_matches_model_and_persists` | `crates/state/src/writer/tests.rs:59` |
| 58 | `bootstrap_corrects_genesis_stale_mirror_under_newer_state` | `crates/state/src/recovery/tests.rs:101` |
| 52 | `four_readers_with_distinct_snapshots` | `crates/state/tests/concurrent_readers.rs:17` |
| 541 | 805 | `crates/types/src/xchain.rs` |
| `crates/state/tests/common/mod.rs:16` | `pub fn open_tmp_writer() -> (tempfile::TempDir, WriterHandle) {` |
| `crates/state/tests/common/mod.rs:26` | `pub fn bpos(block: u64) -> BPosition {` |
| `crates/state/tests/common/mod.rs:39` | `pub fn simple_delta(` |
| `crates/state/tests/common/mod.rs:90` | `pub fn slot_key(idx: u64) -> B256 {` |
| crates/types/src/xchain.rs:268 | 9 | msg_leaf |
| crates/state/src/trie/walker.rs:92 | 8 | walk_account |
| crates/state/src/trie/walker.rs:186 | 8 | walk_storage |
| crates/state/src/schema.rs | 19 |
