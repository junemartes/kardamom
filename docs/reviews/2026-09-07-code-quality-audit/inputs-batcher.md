# Implementation inputs: batcher

Directories: crates/batcher, crates/da_watcher, crates/deployer

## Clippy pedantic sites (282)

| file:line | lint | test | message |
|---|---|---|---|
| `crates/batcher/benches/pack_throughput.rs:19` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/batcher/build.rs:40` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/batcher/src/archive_reader.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:10` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:12` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:28` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:50` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:61` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:62` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:77` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/archive_reader.rs:90` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/archive_reader.rs:161` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:162` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:165` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:181` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:182` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/archive_reader.rs:183` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/batcher/src/archive_reader.rs:194` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/batcher/src/batch.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/batch.rs:45` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/batch.rs:65` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/batcher/src/batch.rs:78` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/batcher.rs:69` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/batcher.rs:115` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/batcher.rs:135` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/batcher.rs:156` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/batcher/src/batcher.rs:157` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/batcher/src/bin/kardamom-batch-claimer.rs:51` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/batcher/src/bin/kardamom-batch-claimer.rs:57` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/batcher/src/bin/kardamom-batch-watcher.rs:61` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/batcher/src/bin/kardamom-batcher.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:12` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:13` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:41` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:47` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:49` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:53` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:101` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:109` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:114` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:118` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:119` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/bin/kardamom-batcher.rs:230` | similar_names |  | binding's name is too similar to existing binding |
| `crates/batcher/src/blob.rs:27` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/blob.rs:34` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/blob.rs:64` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/blob.rs:90` | large_stack_arrays |  | allocating a local array larger than 16384 bytes |
| `crates/batcher/src/compress.rs:9` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/compress.rs:13` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/da_store.rs:31` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/da_store.rs:43` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/da_store.rs:56` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/da_store.rs:62` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/da_store.rs:63` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/batcher/src/da_store.rs:72` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/da_store.rs:89` | large_stack_arrays |  | allocating a local array larger than 16384 bytes |
| `crates/batcher/src/frame.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/frame.rs:106` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/frame.rs:247` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/l1.rs:52` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/l1.rs:70` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/l1.rs:123` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/l1.rs:156` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/l1.rs:187` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/lib.rs:13` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/lib.rs:16` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/lib.rs:22` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/live.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/live.rs:7` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/live.rs:90` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/live.rs:98` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:110` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/batcher/src/live.rs:110` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:132` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:143` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:161` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:193` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:254` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:334` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/batcher/src/live.rs:335` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/batcher/src/live.rs:365` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:389` | match_same_arms |  | these match arms have identical bodies |
| `crates/batcher/src/live.rs:410` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/batcher/src/live.rs:472` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/live.rs:473` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/live.rs:477` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/live.rs:477` | too_many_lines |  | this function has too many lines (103/100) |
| `crates/batcher/src/multi_archive_reader.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:6` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:16` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:43` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:44` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:48` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:51` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:72` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:74` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:90` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/multi_archive_reader.rs:114` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:120` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:132` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/multi_archive_reader.rs:145` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:146` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/multi_archive_reader.rs:151` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/multi_archive_reader.rs:152` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/batcher/src/multi_archive_reader.rs:152` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/batcher/src/multi_archive_reader.rs:207` | needless_continue |  | this `continue` expression is redundant |
| `crates/batcher/src/multi_archive_reader.rs:231` | doc_markdown |  | item in documentation is missing backticks |
| `crates/batcher/src/multi_archive_reader.rs:233` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/optimistic.rs:47` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/batcher/src/optimistic.rs:59` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/optimistic.rs:138` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/optimistic.rs:138` | too_many_lines |  | this function has too many lines (111/100) |
| `crates/batcher/src/optimistic.rs:177` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/batcher/src/optimistic.rs:234` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/batcher/src/prover_submit.rs:48` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/prover_submit.rs:77` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/batcher/src/recon.rs:21` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/rereplicate.rs:64` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/rereplicate.rs:78` | nonminimal_bool |  | this boolean expression can be simplified |
| `crates/batcher/src/rereplicate.rs:141` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/rereplicate.rs:182` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/batcher/src/rereplicate.rs:187` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/rereplicate.rs:193` | nonminimal_bool |  | this boolean expression can be simplified |
| `crates/batcher/src/rereplicate.rs:205` | nonminimal_bool |  | this boolean expression can be simplified |
| `crates/batcher/src/rereplicate.rs:256` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/src/rereplicate.rs:269` | case_sensitive_file_extension_comparisons |  | case-sensitive file extension comparison |
| `crates/batcher/src/settlement.rs:30` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/batcher/src/settlement.rs:53` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/batcher/tests/anvil_e2e.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/anvil_e2e.rs:63` | manual_let_else | yes | this could be rewritten as `let...else` |
| `crates/batcher/tests/anvil_e2e.rs:63` | single_match_else | yes | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/batcher/tests/anvil_e2e.rs:127` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/batcher/tests/anvil_e2e.rs:157` | too_many_lines | yes | this function has too many lines (129/100) |
| `crates/batcher/tests/anvil_e2e.rs:165` | manual_let_else | yes | this could be rewritten as `let...else` |
| `crates/batcher/tests/anvil_e2e.rs:165` | single_match_else | yes | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/batcher/tests/archive_reader.rs:105` | unreadable_literal | yes | long literal lacking separators |
| `crates/batcher/tests/blob_packing.rs:54` | cast_possible_truncation | yes | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/batcher/tests/compress_roundtrip.rs:7` | cast_possible_truncation | yes | casting `u32` to `u8` may truncate the value |
| `crates/batcher/tests/docker_e2e.rs:16` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/docker_e2e.rs:18` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/docker_e2e.rs:26` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/docker_e2e.rs:61` | too_many_lines | yes | this function has too many lines (104/100) |
| `crates/batcher/tests/metrics_endpoint.rs:16` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/batcher/tests/multi_archive_reader.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/multi_archive_reader.rs:12` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/multi_archive_reader.rs:142` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/batcher/tests/multi_archive_reader.rs:142` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/batcher/tests/multi_archive_reader.rs:144` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/batcher/tests/optimistic_e2e.rs:43` | unreadable_literal | yes | long literal lacking separators |
| `crates/batcher/tests/optimistic_e2e.rs:51` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/optimistic_e2e.rs:53` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/optimistic_e2e.rs:59` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/optimistic_e2e.rs:78` | too_many_lines | yes | this function has too many lines (236/100) |
| `crates/batcher/tests/optimistic_proof_e2e.rs:45` | unreadable_literal | yes | long literal lacking separators |
| `crates/batcher/tests/optimistic_proof_e2e.rs:54` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/optimistic_proof_e2e.rs:55` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/optimistic_proof_e2e.rs:92` | too_many_lines | yes | this function has too many lines (190/100) |
| `crates/batcher/tests/proof_submission_e2e.rs:42` | unreadable_literal | yes | long literal lacking separators |
| `crates/batcher/tests/proof_submission_e2e.rs:50` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/proof_submission_e2e.rs:52` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/proof_submission_e2e.rs:57` | too_many_lines | yes | this function has too many lines (158/100) |
| `crates/batcher/tests/recon_proptest.rs:23` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/batcher/tests/recon_proptest.rs:26` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/recon_proptest.rs:27` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/recon_proptest.rs:28` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/recon_roundtrip.rs:23` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/batcher/tests/recon_roundtrip.rs:23` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/batcher/tests/recon_roundtrip.rs:27` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/batcher/tests/recon_roundtrip.rs:28` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/batcher/tests/recon_roundtrip.rs:35` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/batcher/tests/recon_roundtrip.rs:35` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/batcher/tests/recon_roundtrip.rs:84` | cast_sign_loss | yes | casting `i32` to `u64` may lose the sign of the value |
| `crates/batcher/tests/recon_roundtrip.rs:101` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/recon_roundtrip.rs:161` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/section6_conformance.rs:5` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/section6_conformance.rs:6` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/batcher/tests/section6_conformance.rs:67` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/section6_conformance.rs:75` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/batcher/tests/section6_conformance.rs:197` | too_many_lines | yes | this function has too many lines (115/100) |
| `crates/batcher/tests/section6_conformance.rs:294` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/batcher/tests/section6_conformance.rs:322` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/da_watcher/src/bin/kardamom-da-watcher.rs:107` | doc_markdown |  | item in documentation is missing backticks |
| `crates/da_watcher/src/bin/kardamom-da-watcher.rs:227` | too_many_lines |  | this function has too many lines (158/100) |
| `crates/da_watcher/src/bin/kardamom-da-watcher.rs:401` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/da_watcher/src/interop/cursor.rs:68` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/cursor.rs:75` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/da_watcher/src/interop/cursor.rs:99` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/da_watcher/src/interop/mock.rs:86` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/mock.rs:113` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/mock.rs:118` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/mock.rs:125` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/mock.rs:143` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/mock.rs:156` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/mock.rs:171` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/mock.rs:171` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/mock.rs:176` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/mock.rs:176` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/publisher.rs:35` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/da_watcher/src/interop/publisher.rs:45` | wildcard_imports |  | usage of wildcard import |
| `crates/da_watcher/src/interop/publisher.rs:66` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/publisher.rs:66` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/publisher.rs:71` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/interop/publisher.rs:71` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/publisher.rs:90` | cast_possible_truncation |  | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/da_watcher/src/interop/publisher.rs:90` | cast_possible_wrap |  | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/da_watcher/src/interop/publisher.rs:97` | cast_possible_truncation |  | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/da_watcher/src/interop/publisher.rs:97` | cast_possible_wrap |  | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/da_watcher/src/interop/source.rs:141` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/da_watcher/src/interop/source.rs:320` | wildcard_imports |  | usage of wildcard import |
| `crates/da_watcher/src/interop/source.rs:337` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/interop/watcher.rs:122` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/da_watcher/src/interop/watcher.rs:313` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/da_watcher/src/interop/watcher.rs:337` | unused_async |  | unused `async` for function with no await statements |
| `crates/da_watcher/src/interop/watcher.rs:562` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/da_watcher/src/interop/watcher.rs:565` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/da_watcher/src/lib.rs:32` | doc_markdown |  | item in documentation is missing backticks |
| `crates/da_watcher/src/publisher.rs:36` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/da_watcher/src/publisher.rs:43` | wildcard_imports |  | usage of wildcard import |
| `crates/da_watcher/src/publisher.rs:63` | cast_possible_truncation |  | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/da_watcher/src/publisher.rs:63` | cast_possible_wrap |  | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/da_watcher/src/publisher.rs:78` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/da_watcher/src/source.rs:69` | doc_markdown |  | item in documentation is missing backticks |
| `crates/da_watcher/src/source.rs:89` | wildcard_imports |  | usage of wildcard import |
| `crates/da_watcher/src/source.rs:111` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/source.rs:112` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/da_watcher/src/source.rs:117` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/da_watcher/src/source.rs:126` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/source.rs:130` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/da_watcher/src/watcher.rs:112` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/da_watcher/src/watcher.rs:135` | similar_names |  | binding's name is too similar to existing binding |
| `crates/da_watcher/src/watcher.rs:160` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/da_watcher/src/watcher.rs:242` | unnested_or_patterns |  | unnested or-patterns |
| `crates/deployer/build.rs:134` | format_push_string |  | `format!(..)` appended to existing `String` |
| `crates/deployer/build.rs:135` | unnecessary_debug_formatting |  | unnecessary `Debug` formatting in `format!` args |
| `crates/deployer/build.rs:198` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/deployer/src/addresses.rs:20` | doc_markdown |  | you should put bare URLs between `<`/`>` or make a proper Markdown link |
| `crates/deployer/src/addresses.rs:29` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/addresses.rs:34` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/addresses.rs:39` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/deployer.rs:175` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/deployer/src/deployer.rs:224` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/deployer/src/deployer.rs:257` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/deployer/src/deployer.rs:274` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/deployer/src/deployer.rs:308` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/deployer/src/embedded.rs:17` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/embedded.rs:21` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/embedded.rs:23` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/embedded.rs:28` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/embedded.rs:33` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/embedded.rs:38` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/embedded.rs:44` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/ids.rs:42` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/deployer/src/ids.rs:52` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/deployer/src/ids.rs:70` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/deployer/src/ids.rs:85` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/deployer/src/ids.rs:92` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/deployer/src/ids.rs:98` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/deployer/src/ids.rs:106` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/deployer/src/main.rs:42` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/main.rs:51` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/main.rs:55` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/main.rs:56` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/main.rs:65` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/main.rs:66` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/main.rs:93` | doc_markdown |  | item in documentation is missing backticks |
| `crates/deployer/src/main.rs:149` | match_same_arms |  | these match arms have identical bodies |
| `crates/deployer/src/main.rs:341` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/deployer/src/spec.rs:51` | match_same_arms |  | these match arms have identical bodies |
| `crates/deployer/src/spec.rs:58` | match_same_arms |  | these match arms have identical bodies |
| `crates/deployer/src/spec.rs:117` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/spec.rs:124` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/spec.rs:129` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/src/spec.rs:138` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/deployer/tests/deploy_e2e.rs:24` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/deployer/tests/deploy_e2e.rs:59` | manual_let_else | yes | this could be rewritten as `let...else` |
| `crates/deployer/tests/deploy_e2e.rs:59` | single_match_else | yes | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/deployer/tests/deploy_e2e.rs:66` | manual_let_else | yes | this could be rewritten as `let...else` |
| `crates/deployer/tests/deploy_e2e.rs:66` | single_match_else | yes | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/deployer/tests/deploy_e2e.rs:98` | manual_let_else | yes | this could be rewritten as `let...else` |
| `crates/deployer/tests/deploy_e2e.rs:98` | single_match_else | yes | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/deployer/tests/factory_address_sync.rs:4` | doc_markdown | yes | item in documentation is missing backticks |

## Mechanical rows for this group

| 158 | `main` | `crates/da_watcher/src/bin/kardamom-da-watcher.rs:227` |
| 111 | `watch_and_challenge` | `crates/batcher/src/optimistic.rs:138` |
| 103 | `run` | `crates/batcher/src/live.rs:477` |
| 92 | `spawn` | `crates/da_watcher/src/interop/watcher.rs:178` |
| 84 | `post_confirmed` | `crates/batcher/src/live.rs:254` |
| 82 | `main` | `crates/deployer/src/main.rs:122` |
| 82 | `main` | `crates/batcher/src/bin/kardamom-batcher.rs:177` |
| 74 | `main` | `crates/batcher/src/bin/kardamom-archive-rereplicate.rs:66` |
| 70 | `process_once` | `crates/da_watcher/src/watcher.rs:95` |
| 70 | `?` | `crates/da_watcher/src/interop/source.rs:228` |
| 70 | `resolve_paths` | `crates/da_watcher/src/bin/kardamom-da-watcher.rs:144` |
| 55 | `submit_next_proof` | `crates/batcher/src/prover_submit.rs:48` |
| 54 | `process_once` | `crates/da_watcher/src/interop/watcher.rs:89` |
| 53 | `claim_next_batch` | `crates/batcher/src/optimistic.rs:59` |
| 236 | `optimistic_claim_finalize_and_challenge_paths` | `crates/batcher/tests/optimistic_e2e.rs:78` |
| 190 | `real_groth16_proof_accepted_on_chain_challenge_and_grief_rejected` | `crates/batcher/tests/optimistic_proof_e2e.rs:92` |
| 158 | `posted_batch_proof_advances_the_oracle_root_chain` | `crates/batcher/tests/proof_submission_e2e.rs:57` |
| 129 | `live_sender_confirms_and_rejects_foreign_writer` | `crates/batcher/tests/anvil_e2e.rs:157` |
| 115 | `section6_conformance_m_plus_one_to_l1_and_back` | `crates/batcher/tests/section6_conformance.rs:197` |
| 104 | `write_synthetic_archives` | `crates/batcher/tests/docker_e2e.rs:61` |
| 94 | `multi_l2_deploy_and_atomic_upgrade` | `crates/deployer/tests/deploy_e2e.rs:97` |
| 84 | `write_archives` | `crates/batcher/tests/section6_conformance.rs:60` |
| 71 | `deploy_settlement_and_post_batch_emits_event` | `crates/batcher/tests/anvil_e2e.rs:62` |
| 67 | `happy_path_in_order_resolution` | `crates/batcher/tests/multi_archive_reader.rs:69` |
| 56 | `out_of_order_b_refs_a_positions_still_resolve` | `crates/batcher/tests/multi_archive_reader.rs:155` |
| 53 | `aeron_cluster_starts_and_batcher_round_trips_the_m_plus_one_topology` | `crates/batcher/tests/docker_e2e.rs:179` |
| 518 | 724 | `crates/da_watcher/src/interop/watcher.rs` |
| crates/deployer/src/main.rs:251 | 10 | run_deploy |
| crates/batcher/src/prover_submit.rs:24 | 8 | ? |
| crates/batcher/src/multi_archive_reader.rs | 22 |
| crates/batcher/src/live.rs | 20 |
