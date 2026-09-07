# Clippy pedantic: every warning site

Generated from `cargo clippy --workspace --all-targets --all-features -- -W clippy::pedantic` at the audited revision. One row per unique (lint, file, line). Test files are marked.

Total: 2526 sites.


## batcher

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

## bench

| file:line | lint | test | message |
|---|---|---|---|
| `crates/bench/examples/custom_workflow.rs:44` | unused_async_trait_impl |  | unused `async` for async trait impl function with no `.await` statements |
| `crates/bench/src/bin/load.rs:25` | struct_excessive_bools |  | more than 3 bools in a struct |
| `crates/bench/src/bin/load.rs:30` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/load.rs:85` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/load.rs:86` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/load.rs:91` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/load.rs:95` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/load.rs:103` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/load.rs:128` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/perf.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/perf.rs:20` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/perf.rs:109` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/perf.rs:111` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/perf.rs:248` | format_collect |  | use of `format!` to build up a string from an iterator |
| `crates/bench/src/bin/stm-contention.rs:57` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/bench/src/bin/stm-contention.rs:58` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/bench/src/bin/stm-contention.rs:60` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/bench/src/bin/stm-contention.rs:90` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/bench/src/bin/stm-contention.rs:107` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-contention.rs:108` | cast_precision_loss |  | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:48` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/stm-p0.rs:115` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:116` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:117` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:119` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:121` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:126` | too_many_lines |  | this function has too many lines (196/100) |
| `crates/bench/src/bin/stm-p0.rs:128` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/bench/src/bin/stm-p0.rs:225` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/bench/src/bin/stm-p0.rs:269` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:271` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:306` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p0.rs:307` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:40` | struct_excessive_bools |  | more than 3 bools in a struct |
| `crates/bench/src/bin/stm-p2.rs:118` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/stm-p2.rs:180` | unreadable_literal |  | long literal lacking separators |
| `crates/bench/src/bin/stm-p2.rs:362` | too_many_lines |  | this function has too many lines (373/100) |
| `crates/bench/src/bin/stm-p2.rs:362` | fn_params_excessive_bools |  | more than 3 bools in function parameters |
| `crates/bench/src/bin/stm-p2.rs:374` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/bench/src/bin/stm-p2.rs:492` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/bench/src/bin/stm-p2.rs:528` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/bench/src/bin/stm-p2.rs:559` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/bench/src/bin/stm-p2.rs:570` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:571` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:624` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/src/bin/stm-p2.rs:681` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/bench/src/bin/stm-p2.rs:686` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/bench/src/bin/stm-p2.rs:710` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/bench/src/bin/stm-p2.rs:741` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/bench/src/bin/stm-p2.rs:825` | too_many_lines |  | this function has too many lines (407/100) |
| `crates/bench/src/bin/stm-p2.rs:825` | fn_params_excessive_bools |  | more than 3 bools in function parameters |
| `crates/bench/src/bin/stm-p2.rs:839` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/bench/src/bin/stm-p2.rs:1015` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/bench/src/bin/stm-p2.rs:1102` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1103` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1104` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1138` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/bench/src/bin/stm-p2.rs:1147` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/bench/src/bin/stm-p2.rs:1193` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1204` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1205` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1206` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1207` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1208` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1217` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1218` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1219` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1220` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1221` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1225` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1226` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1227` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1228` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1238` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1239` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1240` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1241` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1242` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1246` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1247` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1250` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1257` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1258` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1259` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1260` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1264` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1265` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1266` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1267` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1269` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/src/bin/stm-p2.rs:1277` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1278` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1279` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1280` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1283` | uninlined_format_args |  | variables can be used directly in the `format!` string |
| `crates/bench/src/bin/stm-p2.rs:1291` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1292` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1297` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1298` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1299` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1308` | too_many_lines |  | this function has too many lines (618/100) |
| `crates/bench/src/bin/stm-p2.rs:1327` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/bench/src/bin/stm-p2.rs:1434` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/bench/src/bin/stm-p2.rs:1475` | cast_possible_truncation |  | casting `usize` to `u16` may truncate the value |
| `crates/bench/src/bin/stm-p2.rs:1504` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/bench/src/bin/stm-p2.rs:1511` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/bench/src/bin/stm-p2.rs:1589` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/bench/src/bin/stm-p2.rs:1697` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/src/bin/stm-p2.rs:1706` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/bin/stm-p2.rs:1735` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/bench/src/bin/stm-p2.rs:1754` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/bench/src/bin/stm-p2.rs:1770` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/src/bin/stm-p2.rs:1873` | cast_lossless |  | casts from `bool` to `usize` can be expressed infallibly using `From` |
| `crates/bench/src/bin/stm-p2.rs:1884` | cast_lossless |  | casts from `u32` to `f64` can be expressed infallibly using `From` |
| `crates/bench/src/bin/stm-p2.rs:1884` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1897` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1898` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1921` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1923` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1948` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1955` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1967` | uninlined_format_args |  | variables can be used directly in the `format!` string |
| `crates/bench/src/bin/stm-p2.rs:1977` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1984` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1985` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1986` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/bin/stm-p2.rs:1987` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/harness/inprocess.rs:29` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/harness/inprocess.rs:51` | cast_possible_truncation |  | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/bench/src/harness/inprocess.rs:51` | cast_possible_wrap |  | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/bench/src/load/accounting.rs:136` | too_many_lines |  | this function has too many lines (113/100) |
| `crates/bench/src/load/config.rs:30` | struct_excessive_bools |  | more than 3 bools in a struct |
| `crates/bench/src/load/config.rs:52` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/load/config.rs:53` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/load/defi.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/load/defi.rs:56` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/bench/src/load/defi.rs:172` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/load/defi.rs:228` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/src/load/defi.rs:229` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/src/load/defi.rs:230` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/src/load/defi.rs:282` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/load/defi.rs:285` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/load/defi.rs:333` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/load/mod.rs:101` | too_many_lines |  | this function has too many lines (216/100) |
| `crates/bench/src/load/mod.rs:429` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/load/mod.rs:445` | inconsistent_struct_constructor |  | struct constructor field order is inconsistent with struct definition field order |
| `crates/bench/src/load/scrape.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/load/scrape.rs:229` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/bench/src/load/scrape.rs:234` | unnested_or_patterns |  | unnested or-patterns |
| `crates/bench/src/mnemonic.rs:30` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/perf/cluster.rs:38` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/cluster.rs:54` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/cluster.rs:64` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/cluster.rs:108` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/cluster.rs:187` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/cluster.rs:203` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/mod.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/perf/mod.rs:9` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/perf/mod.rs:28` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/mod.rs:37` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/bench/src/perf/profile.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/perf/profile.rs:7` | doc_markdown |  | item in documentation is missing backticks |
| `crates/bench/src/perf/profile.rs:47` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/profile.rs:72` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/profile.rs:130` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/profile.rs:133` | map_unwrap_or |  | called `map(<f>).unwrap_or(false)` on a `Result` value |
| `crates/bench/src/perf/report.rs:48` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/bench/src/perf/report.rs:74` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/src/perf/report.rs:106` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/perf/report.rs:207` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/stm/capture.rs:21` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/bench/src/stm/capture.rs:73` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/bench/src/stm/capture.rs:90` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/bench/src/stm/uniswap.rs:115` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/bench/src/stm/uniswap.rs:115` | too_many_lines |  | this function has too many lines (188/100) |
| `crates/bench/src/stm/uniswap.rs:230` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/bench/src/stm/uniswap.rs:245` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/bench/src/stm/uniswap.rs:271` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/bench/src/stm/uniswap.rs:277` | cast_lossless |  | casts from `u64` to `u128` can be expressed infallibly using `From` |
| `crates/bench/src/stm/uniswap.rs:311` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/bench/src/stm/uniswap.rs:313` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/bench/src/stm/uniswap.rs:314` | cast_lossless |  | casts from `u64` to `u128` can be expressed infallibly using `From` |
| `crates/bench/tests/alloc_profile.rs:4` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile.rs:10` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile.rs:33` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile.rs:34` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile.rs:43` | too_many_lines | yes | this function has too many lines (173/100) |
| `crates/bench/tests/alloc_profile.rs:44` | cast_possible_truncation | yes | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/bench/tests/alloc_profile.rs:106` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/tests/alloc_profile.rs:168` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/bench/tests/alloc_profile.rs:216` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile.rs:217` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile.rs:218` | cast_precision_loss | yes | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile.rs:221` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile.rs:226` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile_ingress.rs:4` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile_ingress.rs:10` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile_ingress.rs:11` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile_ingress.rs:31` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile_ingress.rs:62` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile_ingress.rs:63` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/alloc_profile_ingress.rs:70` | needless_pass_by_value | yes | this argument is passed by value, but not consumed in the function body |
| `crates/bench/tests/alloc_profile_ingress.rs:71` | needless_pass_by_value | yes | this argument is passed by value, but not consumed in the function body |
| `crates/bench/tests/alloc_profile_ingress.rs:161` | cast_lossless | yes | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/bench/tests/alloc_profile_ingress.rs:209` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile_ingress.rs:210` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile_ingress.rs:211` | cast_precision_loss | yes | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile_ingress.rs:214` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/alloc_profile_ingress.rs:218` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/bench/tests/defi_on_engine.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/parallel_defi_repro.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/bench/tests/parallel_defi_repro.rs:70` | too_many_lines | yes | this function has too many lines (133/100) |
| `crates/bench/tests/parallel_defi_repro.rs:139` | unreadable_literal | yes | long literal lacking separators |
| `crates/bench/tests/parallel_defi_repro.rs:146` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |

## cluster-adapter

| file:line | lint | test | message |
|---|---|---|---|
| `crates/cluster-adapter/src/config.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-adapter/src/config.rs:38` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/config.rs:38` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/cluster-adapter/src/config.rs:51` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/gateway.rs:24` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-adapter/src/gateway.rs:49` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/gateway.rs:55` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/cluster-adapter/src/gateway.rs:58` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/cluster-adapter/src/gateway.rs:58` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/gateway.rs:83` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/gateway.rs:89` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/cluster-adapter/src/gateway.rs:93` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/cluster-adapter/src/live/endpoints.rs:78` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/cluster-adapter/src/live/endpoints.rs:80` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/cluster-adapter/src/live/mod.rs:167` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/live/mod.rs:179` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/live/mod.rs:188` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/live/mod.rs:204` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/live/mod.rs:317` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/cluster-adapter/src/live/session_loop.rs:118` | struct_excessive_bools |  | more than 3 bools in a struct |
| `crates/cluster-adapter/src/live/session_loop.rs:384` | cast_possible_wrap |  | casting `u64` to `i64` may wrap around the value |
| `crates/cluster-adapter/src/live/session_loop.rs:419` | cast_possible_wrap |  | casting `u64` to `i64` may wrap around the value |
| `crates/cluster-adapter/src/live/session_loop.rs:484` | cast_possible_wrap |  | casting `u64` to `i64` may wrap around the value |
| `crates/cluster-adapter/src/watermark.rs:19` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/watermark.rs:37` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/egress.rs:44` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/wire/egress.rs:204` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/egress.rs:208` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/cluster-adapter/src/wire/egress.rs:214` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/egress.rs:230` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/egress.rs:235` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/egress.rs:240` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/ingress.rs:24` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/ingress.rs:43` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/ingress.rs:62` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/wire/ingress.rs:87` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/wire/ingress.rs:110` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/ingress.rs:115` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/ingress.rs:119` | cast_possible_truncation |  | casting `usize` to `u16` may truncate the value |
| `crates/cluster-adapter/src/wire/ingress.rs:121` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/cluster-adapter/src/wire/ingress.rs:129` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/cluster-adapter/src/wire/ingress.rs:129` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/wire/ingress.rs:161` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/ingress.rs:167` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/wire/ingress.rs:181` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/cluster-adapter/src/wire/ingress.rs:181` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/wire/ingress.rs:204` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-adapter/src/wire/mod.rs:80` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-adapter/src/wire/mod.rs:82` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-adapter/src/wire/mod.rs:177` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/mod.rs:186` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-adapter/src/wire/tests.rs:33` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-adapter/src/wire/tests.rs:131` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/cluster-adapter/src/wire/tests.rs:206` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/cluster-adapter/tests/end_to_end.rs:4` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/cluster-adapter/tests/end_to_end.rs:84` | cast_lossless | yes | casts from `u8` to `i32` can be expressed infallibly using `From` |

## cluster-client

| file:line | lint | test | message |
|---|---|---|---|
| `crates/cluster-client/src/bytes.rs:20` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/bytes.rs:25` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/bytes.rs:30` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/bytes.rs:35` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/bytes.rs:40` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/protocol.rs:10` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:11` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:12` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:13` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:14` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:15` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:85` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-client/src/protocol.rs:147` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/cluster-client/src/protocol.rs:155` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/protocol.rs:189` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-client/src/protocol.rs:211` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:221` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/protocol.rs:230` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/protocol.rs:239` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/protocol.rs:241` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-client/src/protocol.rs:251` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/protocol.rs:296` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/protocol.rs:308` | trivially_copy_pass_by_ref |  | this argument (8 byte) is passed by reference, but would be more efficient if passed by value (limit: 8 byte) |
| `crates/cluster-client/src/protocol.rs:338` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/cluster-client/src/protocol.rs:348` | trivially_copy_pass_by_ref |  | this argument (8 byte) is passed by reference, but would be more efficient if passed by value (limit: 8 byte) |
| `crates/cluster-client/src/protocol.rs:374` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/cluster-client/src/protocol.rs:405` | trivially_copy_pass_by_ref |  | this argument (8 byte) is passed by reference, but would be more efficient if passed by value (limit: 8 byte) |
| `crates/cluster-client/src/session/mod.rs:27` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/session/mod.rs:36` | doc_markdown |  | item in documentation is missing backticks |
| `crates/cluster-client/src/session/mod.rs:143` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-client/src/session/mod.rs:154` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-client/src/session/mod.rs:158` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/cluster-client/src/session/mod.rs:385` | must_use_candidate |  | this method could have a `#[must_use]` attribute |

## da_watcher

| file:line | lint | test | message |
|---|---|---|---|
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

## deployer

| file:line | lint | test | message |
|---|---|---|---|
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

## e2e

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

## engine

| file:line | lint | test | message |
|---|---|---|---|
| `crates/engine/src/actor.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:9` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:12` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:19` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:22` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:92` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:93` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:99` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor.rs:112` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/engine/src/actor.rs:112` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/actor.rs:113` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/engine/src/actor/commit_tests.rs:61` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/engine/src/actor/commit_tests.rs:162` | cast_possible_truncation | yes | casting `i32` to `u8` may truncate the value |
| `crates/engine/src/actor/commit_tests.rs:162` | cast_sign_loss | yes | casting `i32` to `u8` may lose the sign of the value |
| `crates/engine/src/actor/commit_tests.rs:233` | cast_possible_truncation | yes | casting `i32` to `u8` may truncate the value |
| `crates/engine/src/actor/commit_tests.rs:233` | cast_sign_loss | yes | casting `i32` to `u8` may lose the sign of the value |
| `crates/engine/src/actor/commit_thread.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_pipeline_tests.rs:24` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_pipeline_tests.rs:31` | many_single_char_names | yes | 5 bindings with single-character names in scope |
| `crates/engine/src/actor/exec_settle.rs:46` | unnecessary_wraps |  | this function's return value is unnecessarily wrapped by `Result` |
| `crates/engine/src/actor/exec_settle.rs:58` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/engine/src/actor/exec_settle.rs:99` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/engine/src/actor/exec_settle.rs:137` | unnecessary_wraps |  | this function's return value is unnecessary |
| `crates/engine/src/actor/exec_tests.rs:114` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_tests.rs:141` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/engine/src/actor/exec_tests.rs:343` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/engine/src/actor/exec_tests.rs:618` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/engine/src/actor/exec_tests.rs:626` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_tests.rs:646` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/engine/src/actor/exec_tests.rs:646` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/engine/src/actor/exec_tests.rs:837` | match_wildcard_for_single_variants | yes | wildcard matches only a single variant and will also match any future added variants |
| `crates/engine/src/actor/exec_thread.rs:86` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:134` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:135` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:136` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:149` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:152` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:173` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:193` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/exec_thread.rs:414` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/engine/src/actor/exec_thread.rs:475` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/engine/src/actor/exec_thread.rs:517` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/engine/src/actor/exec_thread.rs:602` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/engine/src/actor/exec_thread.rs:674` | too_many_lines |  | this function has too many lines (123/100) |
| `crates/engine/src/actor/exec_thread.rs:674` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/engine/src/actor/exec_thread.rs:826` | match_same_arms |  | these match arms have identical bodies |
| `crates/engine/src/actor/exec_thread.rs:852` | match_same_arms |  | these match arms have identical bodies |
| `crates/engine/src/actor/ports.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/ports.rs:15` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/ports.rs:17` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/actor/ports.rs:47` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/actor/ports.rs:54` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/actor/ports.rs:60` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/actor/test_support.rs:94` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/engine/src/actor/types.rs:27` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/types.rs:28` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/types.rs:58` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/actor/types.rs:132` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/types.rs:138` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/wiring.rs:67` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/wiring.rs:70` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/wiring.rs:72` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/wiring.rs:91` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/wiring.rs:92` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/wiring.rs:107` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/actor/wiring.rs:143` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/bin_support.rs:7` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:48` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:49` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/bin_support.rs:59` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/bin_support.rs:67` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/engine/src/bin_support.rs:100` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/engine/src/bin_support.rs:144` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/engine/src/bin_support.rs:156` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:178` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:186` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:188` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:193` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/bin_support.rs:236` | cast_lossless |  | casts from `u8` to `i32` can be expressed infallibly using `From` |
| `crates/engine/src/bin_support.rs:287` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/engine/src/bin_support.rs:311` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/engine/src/bin_support.rs:330` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/bin_support.rs:349` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:354` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/engine/src/bin_support.rs:358` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:366` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:367` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/bin_support.rs:370` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/bin_support.rs:396` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/bin_support.rs:419` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/engine/src/bin_support.rs:421` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/engine/src/lib.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/lib.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/persist.rs:35` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/persist.rs:65` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/persist.rs:89` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/persist.rs:96` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/engine/src/persist.rs:111` | needless_continue |  | this `continue` expression is redundant |
| `crates/engine/src/persist.rs:131` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:7` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:27` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:32` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:42` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:43` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:64` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:79` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader.rs:82` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:91` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader.rs:116` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:117` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:137` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/reader.rs:155` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/reader.rs:167` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:168` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/reader.rs:172` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/reader.rs:179` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:193` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:200` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:204` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader.rs:212` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:215` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader.rs:230` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:231` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:247` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:268` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:271` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:321` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:325` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/engine/src/reader.rs:362` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader.rs:402` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader.rs:445` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:451` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader.rs:466` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:480` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/engine/src/reader.rs:480` | too_many_lines |  | this function has too many lines (170/100) |
| `crates/engine/src/reader.rs:527` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/engine/src/reader.rs:527` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/engine/src/reader.rs:535` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/engine/src/reader.rs:541` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/engine/src/reader.rs:806` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:874` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:1011` | cast_lossless |  | casts from `u8` to `u128` can be expressed infallibly using `From` |
| `crates/engine/src/reader.rs:1284` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:1396` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader.rs:1452` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/engine/src/reader.rs:1458` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/engine/src/reader/cluster/mod.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/reader/cluster/mod.rs:37` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/reader/cluster/mod.rs:46` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/engine/src/reader/cluster/mod.rs:95` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/engine/src/reader/cluster/mod.rs:115` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/engine/src/reader/cluster/mod.rs:273` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/reader/cluster/tests.rs:11` | cast_possible_truncation |  | casting `i32` to `u8` may truncate the value |
| `crates/engine/src/reader/cluster/tests.rs:11` | cast_sign_loss |  | casting `i32` to `u8` may lose the sign of the value |
| `crates/engine/src/reader/cluster/tests.rs:36` | cast_possible_truncation |  | casting `u64` to `i32` may truncate the value |
| `crates/engine/src/reader/cluster/tests.rs:106` | cast_possible_truncation |  | casting `u64` to `i32` may truncate the value |
| `crates/engine/src/replay.rs:97` | doc_markdown |  | item in documentation is missing backticks |
| `crates/engine/src/replay.rs:117` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/engine/src/replay.rs:179` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/engine/src/replay.rs:234` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/engine/src/shadow.rs:75` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/engine/src/shadow.rs:78` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/engine/src/shadow.rs:88` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/engine/src/shadow.rs:101` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/engine/src/shadow.rs:169` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/engine/src/shadow.rs:170` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/engine/src/shadow.rs:171` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/engine/src/state.rs:29` | must_use_candidate |  | this method could have a `#[must_use]` attribute |

## exec-core

| file:line | lint | test | message |
|---|---|---|---|
| `crates/exec-core/src/anchor/mod.rs:104` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/anchor/mod.rs:175` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/anchor/mod.rs:247` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/exec-core/src/anchor/mod.rs:306` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/anchor/mod.rs:329` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/exec-core/src/anchor/sparse.rs:63` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/exec-core/src/anchor/sparse.rs:104` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/anchor/sparse.rs:118` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/anchor/sparse.rs:165` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/anchor/sparse.rs:265` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/anchor/sparse.rs:341` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/exec-core/src/anchor/sparse.rs:368` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/anchor/sparse.rs:418` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/exec-core/src/anchor/sparse.rs:430` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/exec-core/src/bal_ladder.rs:99` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/bal_ladder.rs:100` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/bal_ladder.rs:101` | default_trait_access |  | calling `BTreeMap::default()` is more clear than this expression |
| `crates/exec-core/src/bal_ladder.rs:104` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/bal_ladder.rs:106` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/exec-core/src/block_env.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/block_env.rs:46` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/block_env.rs:57` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/block_env.rs:65` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/block_env.rs:110` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/delta.rs:38` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/delta.rs:40` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/delta.rs:42` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/delta.rs:48` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/delta.rs:85` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/exec-core/src/delta.rs:156` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/exec-core/src/delta.rs:168` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/exec-core/src/delta.rs:290` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/delta.rs:332` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/error.rs:74` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/error.rs:75` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/error.rs:76` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/error.rs:88` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/exec_types.rs:14` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/exec_types.rs:15` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/exec_types.rs:24` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/exec_types.rs:24` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/exec-core/src/exec_types.rs:29` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/exec_types.rs:47` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/executor/deposit.rs:43` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/deposit.rs:123` | match_same_arms |  | these match arms have identical bodies |
| `crates/exec-core/src/executor/deposit.rs:290` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/executor/deposit.rs:291` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/executor/deposit.rs:397` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/exec-core/src/executor/scope.rs:76` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:92` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:116` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:136` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:186` | match_same_arms |  | these match arms have identical bodies |
| `crates/exec-core/src/executor/scope.rs:263` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:302` | match_same_arms |  | these match arms have identical bodies |
| `crates/exec-core/src/executor/scope.rs:348` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:419` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:498` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/scope.rs:503` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/scope.rs:507` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/scope.rs:585` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/scope.rs:622` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/exec-core/src/executor/scope.rs:1041` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/executor/tx_env.rs:33` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/tx_env.rs:169` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/exec-core/src/executor/tx_env.rs:196` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/exec-core/src/executor/tx_env.rs:227` | default_trait_access |  | calling `AccessList::default()` is more clear than this expression |
| `crates/exec-core/src/executor/tx_env.rs:229` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/exec-core/src/executor/write_set.rs:41` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/exec-core/src/executor/write_set.rs:47` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/executor/write_set.rs:71` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/executor/write_set.rs:147` | default_trait_access |  | calling `HashMap::default()` is more clear than this expression |
| `crates/exec-core/src/executor/write_set.rs:161` | default_trait_access |  | calling `HashMap::default()` is more clear than this expression |
| `crates/exec-core/src/executor/write_set.rs:190` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/write_set.rs:254` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/write_set.rs:276` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/write_set.rs:306` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/write_set.rs:335` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/exec-core/src/executor/xchain.rs:53` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/executor/xchain.rs:126` | match_same_arms |  | these match arms have identical bodies |
| `crates/exec-core/src/features.rs:48` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/exec-core/src/features.rs:67` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/exec-core/src/features.rs:77` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/exec-core/src/features.rs:82` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/exec-core/src/features.rs:117` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/features.rs:157` | unnecessary_wraps |  | this function's return value is unnecessarily wrapped by `Result` |
| `crates/exec-core/src/state.rs:56` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/state.rs:61` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/state.rs:66` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/state.rs:72` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/state.rs:80` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/exec-core/src/state.rs:119` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/state.rs:119` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/exec-core/src/state.rs:124` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/state.rs:124` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/exec-core/src/state.rs:129` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/exec-core/src/state.rs:134` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/state.rs:134` | return_self_not_must_use |  | missing `#[must_use]` attribute on a method returning `Self` |
| `crates/exec-core/src/state.rs:141` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/stateless.rs:60` | doc_markdown |  | item in documentation is missing backticks |
| `crates/exec-core/src/stateless.rs:93` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/stateless.rs:99` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/exec-core/src/stateless.rs:107` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/stateless.rs:114` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/exec-core/src/stateless.rs:123` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/stateless.rs:130` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/exec-core/src/stateless.rs:176` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/stateless.rs:222` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/stateless.rs:258` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/stateless.rs:311` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/exec-core/src/stateless.rs:311` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/exec-core/src/stateless.rs:339` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/exec-core/src/stateless.rs:382` | unreadable_literal |  | long literal lacking separators |
| `crates/exec-core/src/stateless.rs:388` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/exec-core/src/witness.rs:58` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/exec-core/src/witness.rs:146` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/exec-core/tests/anchor_state.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/exec-core/tests/anchor_state.rs:33` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/exec-core/tests/anchor_state.rs:73` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/exec-core/tests/code_hash_scope_invariance.rs:5` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/exec-core/tests/code_hash_scope_invariance.rs:10` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/exec-core/tests/code_hash_scope_invariance.rs:11` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/exec-core/tests/code_hash_scope_invariance.rs:30` | unreadable_literal | yes | long literal lacking separators |
| `crates/exec-core/tests/code_hash_scope_invariance.rs:49` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/exec-core/tests/decode_cost.rs:17` | unreadable_literal | yes | long literal lacking separators |
| `crates/exec-core/tests/decode_cost.rs:37` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/exec-core/tests/decode_cost.rs:43` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/exec-core/tests/eest_state.rs:86` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/exec-core/tests/eest_state.rs:168` | too_many_lines | yes | this function has too many lines (126/100) |
| `crates/exec-core/tests/eest_state.rs:187` | map_unwrap_or | yes | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/exec-core/tests/eest_state.rs:323` | too_many_lines | yes | this function has too many lines (116/100) |
| `crates/exec-core/tests/hash_cost.rs:15` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/exec-core/tests/hash_cost.rs:29` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/exec-core/tests/hash_cost.rs:36` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/exec-core/tests/hash_cost.rs:44` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/exec-core/tests/hash_cost.rs:47` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/exec-core/tests/touch_set.rs:18` | unreadable_literal | yes | long literal lacking separators |
| `crates/exec-core/tests/touch_set.rs:52` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/exec-core/tests/write_set_encoding.rs:40` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/exec-core/tests/write_set_hash.rs:67` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/exec-core/tests/write_set_hash.rs:79` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/exec-core/tests/write_set_hash.rs:134` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/exec-core/tests/write_set_hash.rs:139` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/exec-core/tests/write_set_hash.rs:165` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |

## executor

| file:line | lint | test | message |
|---|---|---|---|
| `crates/executor/benches/sequential_throughput.rs:50` | needless_pass_by_value | yes | this argument is passed by value, but not consumed in the function body |
| `crates/executor/benches/sequential_throughput.rs:115` | semicolon_if_nothing_returned | yes | consider adding a `;` to the last statement for consistent formatting |
| `crates/executor/benches/sequential_throughput.rs:156` | semicolon_if_nothing_returned | yes | consider adding a `;` to the last statement for consistent formatting |
| `crates/executor/benches/sequential_throughput.rs:251` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/executor/benches/sequential_throughput.rs:252` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/executor/benches/sequential_throughput.rs:253` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/executor/benches/sequential_throughput.rs:256` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/executor/benches/sequential_throughput.rs:261` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/executor/benches/sequential_throughput.rs:267` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/executor/src/bal.rs:112` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/executor/src/bal.rs:120` | default_trait_access |  | calling `VecDeque::default()` is more clear than this expression |
| `crates/executor/src/bal.rs:165` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/executor/src/bal.rs:217` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/executor/src/bin/kardamom-executor/args.rs:29` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/args.rs:38` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/args.rs:90` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/args.rs:91` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/args.rs:98` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/main.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/main.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/main.rs:47` | too_many_lines |  | this function has too many lines (138/100) |
| `crates/executor/src/bin/kardamom-executor/main.rs:268` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/executor/src/bin/kardamom-executor/state.rs:55` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/executor/src/bin/kardamom-executor/state.rs:123` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/executor/src/bin/kardamom-executor/wiring.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/wiring.rs:17` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/bin/kardamom-executor/wiring.rs:56` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/executor/src/bin/kardamom-executor/wiring.rs:103` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/config.rs:27` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/lib.rs:14` | doc_markdown |  | item in documentation is missing backticks |
| `crates/executor/src/parallel.rs:79` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/executor/src/parallel.rs:79` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/executor/src/parallel.rs:116` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/executor/src/parallel.rs:117` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/executor/src/parallel.rs:176` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/executor/src/parallel.rs:300` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/executor/tests/determinism.rs:2` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/determinism.rs:6` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/determinism.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/determinism.rs:9` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/determinism.rs:146` | needless_pass_by_value | yes | this argument is passed by value, but not consumed in the function body |
| `crates/executor/tests/diff_reference.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/diff_reference.rs:8` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/diff_reference.rs:102` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/diff_reference.rs:103` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/diff_reference.rs:134` | needless_pass_by_value | yes | this argument is passed by value, but not consumed in the function body |
| `crates/executor/tests/diff_reference.rs:167` | default_trait_access | yes | calling `FixedBytes::default()` is more clear than this expression |
| `crates/executor/tests/diff_reference.rs:184` | too_many_lines | yes | this function has too many lines (115/100) |
| `crates/executor/tests/diff_reference.rs:249` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/executor/tests/diff_reference.rs:249` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/executor/tests/diff_reference.rs:253` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/executor/tests/diff_reference.rs:253` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/executor/tests/diff_reference.rs:259` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/executor/tests/diff_reference.rs:259` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/executor/tests/diff_reference.rs:265` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/executor/tests/diff_reference.rs:265` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/executor/tests/docker_aeron_e2e.rs:4` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/docker_aeron_e2e.rs:11` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/docker_aeron_e2e.rs:17` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/docker_aeron_e2e.rs:21` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/hash_invariance.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/hash_invariance.rs:46` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/executor/tests/hash_invariance.rs:52` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/executor/tests/hash_invariance.rs:61` | unreadable_literal | yes | long literal lacking separators |
| `crates/executor/tests/hash_invariance.rs:64` | unreadable_literal | yes | long literal lacking separators |
| `crates/executor/tests/m_plus_one_join.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/m_plus_one_join.rs:12` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/m_plus_one_join.rs:17` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/m_plus_one_join.rs:173` | too_many_lines | yes | this function has too many lines (151/100) |
| `crates/executor/tests/m_plus_one_join.rs:204` | cast_lossless | yes | casts from `u8` to `i32` can be expressed infallibly using `From` |
| `crates/executor/tests/m_plus_one_join.rs:229` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/executor/tests/m_plus_one_join.rs:291` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/executor/tests/m_plus_one_join.rs:376` | too_many_lines | yes | this function has too many lines (109/100) |
| `crates/executor/tests/m_plus_one_join.rs:411` | used_underscore_binding | yes | used underscore-prefixed binding |
| `crates/executor/tests/replay_integration.rs:2` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/replay_integration.rs:5` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/replay_integration.rs:9` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/replay_integration.rs:33` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/replay_integration.rs:92` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/replay_integration.rs:93` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/executor/tests/stm_block_exec_ab.rs:56` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/executor/tests/stm_block_exec_ab.rs:65` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/executor/tests/stm_block_exec_ab.rs:72` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/executor/tests/stm_block_exec_ab.rs:76` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |

## footprint

| file:line | lint | test | message |
|---|---|---|---|
| `crates/footprint/src/classifier.rs:82` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/classifier.rs:119` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/classifier.rs:155` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/classifier.rs:190` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/classifier.rs:244` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/classifier.rs:265` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/classifier.rs:284` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/classifier.rs:314` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/grade.rs:59` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/grade.rs:63` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/grade.rs:66` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/grade.rs:70` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/grade.rs:73` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/grade.rs:77` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/grade.rs:90` | implicit_hasher |  | parameter of type `HashSet` should be generalized over different hashers |
| `crates/footprint/src/grade.rs:178` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/footprint/src/grade.rs:225` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/grade.rs:262` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/grade.rs:265` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/grade.rs:296` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/grade.rs:302` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/grade.rs:320` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/grade.rs:336` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/footprint/src/lib.rs:62` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/footprint/src/lib.rs:73` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/footprint/src/oracle.rs:64` | similar_names |  | binding's name is too similar to existing binding |
| `crates/footprint/src/oracle.rs:131` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/footprint/src/oracle.rs:138` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:142` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:152` | implicit_hasher |  | parameter of type `HashSet` should be generalized over different hashers |
| `crates/footprint/src/oracle.rs:176` | cast_possible_truncation |  | casting `f64` to `u64` may truncate the value |
| `crates/footprint/src/oracle.rs:176` | cast_sign_loss |  | casting `f64` to `u64` may lose the sign of the value |
| `crates/footprint/src/oracle.rs:176` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:243` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/footprint/src/oracle.rs:254` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:257` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/footprint/src/oracle.rs:262` | cast_possible_truncation |  | casting `f64` to `usize` may truncate the value |
| `crates/footprint/src/oracle.rs:262` | cast_sign_loss |  | casting `f64` to `usize` may lose the sign of the value |
| `crates/footprint/src/oracle.rs:262` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:269` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:275` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:295` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:300` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:305` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/footprint/src/oracle.rs:307` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |

## ingress

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

## interop-feed

| file:line | lint | test | message |
|---|---|---|---|
| `crates/interop-feed/src/lib.rs:82` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/interop-feed/src/lib.rs:196` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/interop-feed/src/lib.rs:262` | must_use_candidate |  | this method could have a `#[must_use]` attribute |

## log

| file:line | lint | test | message |
|---|---|---|---|
| `crates/log/src/aeron_live/handles/simple.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:46` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/simple.rs:65` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/simple.rs:87` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:89` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/simple.rs:93` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/aeron_live/handles/simple.rs:97` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:103` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:105` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/simple.rs:109` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/aeron_live/handles/simple.rs:113` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:119` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:121` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/simple.rs:125` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/aeron_live/handles/simple.rs:129` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/simple.rs:137` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/simple.rs:152` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_data.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/tx_data.rs:10` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/tx_data.rs:17` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_data.rs:30` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_data.rs:34` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/aeron_live/handles/tx_data.rs:39` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/tx_data.rs:49` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:97` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:123` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:138` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:176` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:180` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:184` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:196` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:203` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:240` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:259` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:280` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:301` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:306` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:335` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:340` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:351` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:363` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:385` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:404` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:408` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/mod.rs:101` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/pending.rs:45` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/aeron_live/pending.rs:114` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/log/src/aeron_live/pending.rs:141` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/log/src/aeron_live/pending.rs:362` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/log/src/aeron_live/runtime.rs:123` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:132` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:142` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:146` | unnecessary_debug_formatting |  | unnecessary `Debug` formatting in `format!` args |
| `crates/log/src/aeron_live/runtime.rs:163` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/runtime.rs:165` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:211` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:232` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:254` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:264` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:275` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:297` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/runtime.rs:302` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:331` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:348` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/runtime.rs:355` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:424` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/log/src/aeron_live/runtime.rs:455` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/runtime.rs:473` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/aeron_live/thread.rs:32` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/aeron_live/thread.rs:65` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/log/src/aeron_live/thread.rs:66` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/log/src/aeron_live/thread.rs:94` | match_same_arms |  | these match arms have identical bodies |
| `crates/log/src/aeron_live/thread.rs:113` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/log/src/aeron_live/thread.rs:123` | match_same_arms |  | these match arms have identical bodies |
| `crates/log/src/aeron_live/thread.rs:151` | match_same_arms |  | these match arms have identical bodies |
| `crates/log/src/aeron_live/thread.rs:163` | unnecessary_wraps |  | this function's return value is unnecessary |
| `crates/log/src/aeron_live/thread.rs:198` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/log/src/aeron_live/thread.rs:219` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/log/src/codec.rs:21` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/codec.rs:31` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/codec.rs:42` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/codec.rs:58` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:39` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/config/mod.rs:55` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/config/mod.rs:83` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:95` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:107` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:115` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:124` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:132` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:133` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:135` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:139` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:146` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:154` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:201` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:207` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:214` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:225` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:235` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:236` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:240` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:248` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:254` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:255` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:263` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:276` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/config/mod.rs:302` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:306` | cast_sign_loss |  | casting `i32` to `u32` may lose the sign of the value |
| `crates/log/src/config/mod.rs:329` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:333` | cast_sign_loss |  | casting `i32` to `u32` may lose the sign of the value |
| `crates/log/src/config/mod.rs:336` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:337` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:338` | cast_lossless |  | casts from `u8` to `i32` can be expressed infallibly using `From` |
| `crates/log/src/config/mod.rs:341` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:342` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:347` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:348` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:353` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/config/mod.rs:354` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/config/mod.rs:355` | cast_lossless |  | casts from `u8` to `i32` can be expressed infallibly using `From` |
| `crates/log/src/config/tests.rs:55` | needless_raw_string_hashes |  | unnecessary hashes around raw string literal |
| `crates/log/src/lib.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/lib.rs:6` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/offer_retry.rs:91` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/log/src/offer_retry.rs:173` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/log/src/publisher.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:7` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:13` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:79` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:82` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:87` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:94` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:111` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/publisher.rs:115` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:116` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:118` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:119` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:128` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:131` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:138` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:148` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:150` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:154` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:155` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:165` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:176` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:186` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/publisher.rs:193` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/publisher.rs:236` | cast_possible_wrap |  | casting `usize` to `i64` may wrap around the value on targets with 64-bit wide pointers |
| `crates/log/src/recorder.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:6` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:53` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/recorder.rs:59` | unnecessary_debug_formatting |  | unnecessary `Debug` formatting in `format!` args |
| `crates/log/src/recorder.rs:91` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/recorder.rs:92` | used_underscore_binding |  | used underscore-prefixed binding |
| `crates/log/src/recorder.rs:101` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/recorder.rs:113` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/recorder.rs:123` | unnecessary_debug_formatting |  | unnecessary `Debug` formatting in `format!` args |
| `crates/log/src/recorder.rs:154` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/log/src/recorder.rs:177` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:179` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:181` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:188` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/recorder.rs:199` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:200` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:204` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:227` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/recorder.rs:277` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:289` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/recorder.rs:319` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/recorder.rs:367` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/log/src/recorder.rs:396` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:398` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:399` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:405` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:411` | unnecessary_wraps |  | this function's return value is unnecessarily wrapped by `Result` |
| `crates/log/src/recorder.rs:435` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/log/src/recorder.rs:462` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/recorder.rs:525` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/recorder.rs:528` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:7` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:69` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:71` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:85` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:116` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/refetch.rs:128` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:133` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/refetch.rs:133` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/refetch.rs:224` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/refetch.rs:228` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/refetch.rs:228` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/refetch.rs:394` | cast_lossless |  | casts from `i32` to `i64` can be expressed infallibly using `From` |
| `crates/log/src/refetch.rs:402` | cast_lossless |  | casts from `i32` to `i64` can be expressed infallibly using `From` |
| `crates/log/src/refetch.rs:409` | cast_lossless |  | casts from `i32` to `i64` can be expressed infallibly using `From` |
| `crates/log/src/refetch.rs:442` | similar_names |  | binding's name is too similar to existing binding |
| `crates/log/src/refetch.rs:492` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/replay.rs:92` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/replay.rs:134` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/replay.rs:222` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/replay.rs:223` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/replay.rs:241` | too_many_lines |  | this function has too many lines (117/100) |
| `crates/log/src/replay.rs:259` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/log/src/replay.rs:365` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/log/src/replay.rs:365` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/log/src/replay.rs:549` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/replay.rs:555` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/replay.rs:579` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/replay.rs:581` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:46` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:81` | cast_sign_loss |  | casting `i32` to `usize` may lose the sign of the value |
| `crates/log/src/subscriber.rs:99` | cast_sign_loss |  | casting `i32` to `usize` may lose the sign of the value |
| `crates/log/src/subscriber.rs:115` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/subscriber.rs:118` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/subscriber.rs:121` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/subscriber.rs:140` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/subscriber.rs:142` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:150` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:158` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:166` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:174` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:182` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:190` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/subscriber.rs:193` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/subscriber.rs:201` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/supervisor.rs:27` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/supervisor.rs:37` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/supervisor.rs:68` | unused_async |  | unused `async` for function with no await statements |
| `crates/log/src/supervisor.rs:69` | similar_names |  | binding's name is too similar to existing binding |
| `crates/log/src/supervisor.rs:88` | unused_async |  | unused `async` for function with no await statements |
| `crates/log/src/supervisor.rs:89` | similar_names |  | binding's name is too similar to existing binding |
| `crates/log/src/supervisor.rs:119` | unnecessary_debug_formatting |  | unnecessary `Debug` formatting in `format!` args |
| `crates/log/src/supervisor.rs:131` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/log/src/testing.rs:41` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:42` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:51` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:71` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:76` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:77` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/testing.rs:77` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:81` | cast_possible_wrap |  | casting `usize` to `i64` may wrap around the value on targets with 64-bit wide pointers |
| `crates/log/src/testing.rs:103` | cast_possible_wrap |  | casting `usize` to `i64` may wrap around the value on targets with 64-bit wide pointers |
| `crates/log/src/testing.rs:121` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:124` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:127` | cast_possible_truncation |  | casting `i64` to `i32` may truncate the value |
| `crates/log/src/testing.rs:132` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:135` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:138` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:152` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/testing.rs:173` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:181` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/testing.rs:201` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:251` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:266` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:267` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:271` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:280` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:296` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:302` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:307` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:309` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/testing.rs:314` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:316` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:317` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:318` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:324` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:362` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:363` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:371` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:379` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:381` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/testing.rs:385` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:387` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/testing.rs:399` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/src/testing.rs:407` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:446` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:450` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/testing.rs:455` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/testing.rs:455` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:544` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/testing.rs:549` | map_unwrap_or |  | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/log/src/testing.rs:566` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/testing.rs:579` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/testing.rs:593` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/log/src/testing.rs:610` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:615` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:619` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:623` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/log/src/testing.rs:630` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/log/src/testing.rs:656` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/log/src/watermark.rs:6` | doc_markdown |  | item in documentation is missing backticks |
| `crates/log/tests/aeron_live_e2e.rs:30` | map_unwrap_or | yes | called `map(<f>).unwrap_or(false)` on a `Result` value |
| `crates/log/tests/aeron_live_e2e.rs:88` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/log/tests/aeron_live_e2e.rs:89` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/log/tests/offer_connect_race.rs:14` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/log/tests/offer_connect_race.rs:34` | map_unwrap_or | yes | called `map(<f>).unwrap_or(false)` on a `Result` value |
| `crates/log/tests/offer_starvation.rs:53` | map_unwrap_or | yes | called `map(<f>).unwrap_or(false)` on a `Result` value |
| `crates/log/tests/offer_starvation.rs:102` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/log/tests/testing_fakes.rs:161` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/log/tests/testing_fakes.rs:162` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/log/tests/testing_fakes.rs:201` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/log/tests/testing_fakes.rs:202` | similar_names | yes | binding's name is too similar to existing binding |

## obs

| file:line | lint | test | message |
|---|---|---|---|
| `crates/obs/src/lib.rs:64` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/obs/tests/init.rs:3` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/obs/tests/init.rs:23` | uninlined_format_args | yes | variables can be used directly in the `format!` string |
| `crates/obs/tests/init_without_runtime.rs:12` | doc_markdown | yes | item in documentation is missing backticks |

## reconstruct

| file:line | lint | test | message |
|---|---|---|---|
| `crates/reconstruct/src/lib.rs:43` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/reconstruct/src/lib.rs:65` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/reconstruct/src/lib.rs:97` | unreadable_literal |  | long literal lacking separators |
| `crates/reconstruct/src/lib.rs:221` | too_many_lines |  | this function has too many lines (130/100) |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:34` | unreadable_literal | yes | long literal lacking separators |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:69` | too_many_lines | yes | this function has too many lines (133/100) |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:70` | manual_let_else | yes | this could be rewritten as `let...else` |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:70` | single_match_else | yes | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |

## sequencer

| file:line | lint | test | message |
|---|---|---|---|
| `crates/sequencer/benches/throughput.rs:47` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/sequencer/benches/throughput.rs:57` | default_trait_access | yes | calling `FixedBytes::default()` is more clear than this expression |
| `crates/sequencer/benches/throughput.rs:66` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/benches/throughput.rs:69` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:56` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:67` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:68` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:69` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:84` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:194` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:266` | similar_names |  | binding's name is too similar to existing binding |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:298` | similar_names |  | binding's name is too similar to existing binding |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:4` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:61` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:65` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:66` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:68` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:71` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:80` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:82` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:104` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:147` | cast_possible_truncation |  | casting `u32` to `u8` may truncate the value |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:172` | too_many_lines |  | this function has too many lines (122/100) |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:327` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/sequencer/src/config.rs:13` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/config.rs:15` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/config.rs:37` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/config.rs:39` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/config.rs:82` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/config.rs:103` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/config.rs:114` | cast_possible_truncation |  | casting `u32` to `u8` may truncate the value |
| `crates/sequencer/src/epoch.rs:9` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/epoch.rs:37` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/epoch.rs:48` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/epoch.rs:65` | wildcard_imports |  | usage of wildcard import |
| `crates/sequencer/src/epoch.rs:77` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/sequencer/src/epoch.rs:81` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/sequencer/src/epoch.rs:110` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/sequencer/src/epoch.rs:113` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/sequencer/src/inbound.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/inbound.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/inbound.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/inbound.rs:16` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/inbound.rs:19` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/inbound.rs:33` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/inbound.rs:44` | wildcard_imports |  | usage of wildcard import |
| `crates/sequencer/src/inbound.rs:46` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/lib.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/metrics.rs:124` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/sequencer/src/metrics.rs:140` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/src/metrics.rs:148` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/src/metrics.rs:152` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/src/nonce_decode.rs:39` | many_single_char_names |  | 5 bindings with single-character names in scope |
| `crates/sequencer/src/nonce_decode.rs:159` | default_trait_access |  | calling `AccessList::default()` is more clear than this expression |
| `crates/sequencer/src/nonce_decode.rs:176` | default_trait_access |  | calling `AccessList::default()` is more clear than this expression |
| `crates/sequencer/src/nonce_decode.rs:177` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/sequencer/src/outbound.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound.rs:36` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound.rs:40` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound.rs:48` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/outbound.rs:81` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/outbound.rs:90` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/outbound.rs:93` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound.rs:112` | wildcard_imports |  | usage of wildcard import |
| `crates/sequencer/src/outbound.rs:114` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound.rs:224` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/sequencer/src/outbound/cluster.rs:110` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/outbound/cluster.rs:125` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/outbound/cluster.rs:192` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound/cluster.rs:222` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/outbound/cluster.rs:224` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/partition.rs:15` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/sequencer/src/partition.rs:15` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/sequencer/src/partition.rs:19` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/partition.rs:19` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/sequencer/src/partition.rs:28` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/partition.rs:48` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/sequencer/src/pending.rs:49` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/pending.rs:56` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/pending.rs:60` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/pending.rs:64` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/pending.rs:69` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/pending.rs:73` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/sequencer/src/pending.rs:230` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/pending.rs:235` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/remote_epoch.rs:26` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/remote_epoch.rs:42` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/remote_epoch.rs:52` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/remote_epoch.rs:70` | wildcard_imports |  | usage of wildcard import |
| `crates/sequencer/src/remote_epoch.rs:82` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/sequencer/src/remote_epoch.rs:86` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/sequencer/src/remote_epoch.rs:117` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/sequencer/src/remote_epoch.rs:121` | cast_possible_truncation |  | casting `usize` to `u8` may truncate the value |
| `crates/sequencer/src/resync/mod.rs:12` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/resync/mod.rs:41` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/resync/mod.rs:86` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/mod.rs:92` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/mod.rs:102` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/mod.rs:155` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/mod.rs:174` | struct_excessive_bools |  | more than 3 bools in a struct |
| `crates/sequencer/src/resync/mod.rs:216` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/mod.rs:244` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/mod.rs:248` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/mod.rs:402` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/sequencer/src/resync/mod.rs:404` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/sequencer/src/resync/mod.rs:449` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/sequencer/src/resync/mod.rs:477` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/sequencer/src/resync/tests.rs:53` | duration_suboptimal_units |  | constructing a `Duration` using a smaller unit when a larger unit would be more readable |
| `crates/sequencer/src/sender.rs:44` | default_trait_access |  | calling `FixedBytes::default()` is more clear than this expression |
| `crates/sequencer/src/sequencer.rs:3` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:5` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:8` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:26` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:31` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:44` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:45` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:78` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:85` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:88` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:115` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/sequencer/src/sequencer.rs:115` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/sequencer.rs:143` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/sequencer.rs:370` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:376` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:377` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/src/sequencer.rs:378` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/sequencer.rs:478` | match_same_arms |  | these match arms have identical bodies |
| `crates/sequencer/src/sequencer.rs:546` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/sequencer/src/sequencer.rs:551` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/sequencer/src/shutdown.rs:18` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/shutdown.rs:25` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/shutdown.rs:33` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/shutdown.rs:38` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/shutdown.rs:44` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/sequencer/src/state/mod.rs:56` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/state/mod.rs:64` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/state/mod.rs:72` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/sequencer/src/state/mod.rs:184` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/sequencer/src/state/mod.rs:219` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/sequencer/src/state/tests.rs:103` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/state/tests.rs:108` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/state/tests.rs:119` | match_wildcard_for_single_variants |  | wildcard matches only a single variant and will also match any future added variants |
| `crates/sequencer/src/state/tests.rs:139` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/state/tests.rs:148` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/state/tests.rs:180` | cast_possible_truncation |  | casting `u64` to `u32` may truncate the value |
| `crates/sequencer/src/unconfirmed.rs:28` | doc_markdown |  | item in documentation is missing backticks |
| `crates/sequencer/tests/alloc_profile.rs:4` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/alloc_profile.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/alloc_profile.rs:9` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/alloc_profile.rs:13` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/alloc_profile.rs:23` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/alloc_profile.rs:62` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/alloc_profile.rs:133` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/alloc_profile.rs:160` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/tests/alloc_profile.rs:161` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/tests/alloc_profile.rs:162` | cast_precision_loss | yes | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/tests/alloc_profile.rs:165` | cast_precision_loss | yes | casting `u128` to `f64` may cause a loss of precision (`u128` is 128 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/tests/alloc_profile.rs:169` | cast_precision_loss | yes | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/sequencer/tests/e2e_docker.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/e2e_docker.rs:40` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/e2e_docker.rs:64` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:2` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:5` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:55` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:65` | default_trait_access | yes | calling `FixedBytes::default()` is more clear than this expression |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:83` | too_many_lines | yes | this function has too many lines (135/100) |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:100` | cast_possible_truncation | yes | casting `u32` to `u8` may truncate the value |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:101` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:109` | unreadable_literal | yes | long literal lacking separators |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:132` | cast_possible_truncation | yes | casting `usize` to `i32` may truncate the value on targets with 64-bit wide pointers |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:132` | cast_possible_wrap | yes | casting `usize` to `i32` may wrap around the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:136` | cast_possible_truncation | yes | casting `u32` to `u8` may truncate the value |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:146` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:147` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:177` | cast_possible_truncation | yes | casting `u32` to `u8` may truncate the value |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:181` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:200` | cast_lossless | yes | casts from `u8` to `u32` can be expressed infallibly using `From` |
| `crates/sequencer/tests/multi_sequencer_dual_write.rs:216` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/partition_routing.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:2` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:6` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:56` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:71` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/sequencer/tests/replicated_shard_racing.rs:95` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:145` | cloned_instead_of_copied | yes | used `cloned` where `copied` could be used instead |
| `crates/sequencer/tests/replicated_shard_racing.rs:207` | default_trait_access | yes | calling `HashMap::default()` is more clear than this expression |
| `crates/sequencer/tests/replicated_shard_racing.rs:226` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:233` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:261` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:264` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/replicated_shard_racing.rs:278` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/sequencer/tests/replicated_shard_racing.rs:280` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/sequencer/tests/replicated_shard_racing.rs:290` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/sequencer/tests/replicated_shard_racing.rs:297` | items_after_statements | yes | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/sequencer/tests/replicated_shard_racing.rs:307` | cast_possible_truncation | yes | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/sequencer/tests/resync_filter.rs:37` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/sequencer/tests/resync_filter.rs:48` | default_trait_access | yes | calling `FixedBytes::default()` is more clear than this expression |
| `crates/sequencer/tests/resync_filter.rs:141` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/sequencer/tests/sequencer_integration.rs:1` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/sequencer_integration.rs:2` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/sequencer_integration.rs:4` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/sequencer_integration.rs:7` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/sequencer_integration.rs:45` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/sequencer/tests/sequencer_integration.rs:55` | default_trait_access | yes | calling `FixedBytes::default()` is more clear than this expression |
| `crates/sequencer/tests/sequencer_integration.rs:60` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/sequencer/tests/sequencer_integration.rs:64` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/sequencer/tests/sequencer_integration.rs:111` | needless_continue | yes | this `continue` expression is redundant |
| `crates/sequencer/tests/sequencer_step.rs:32` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/sequencer/tests/sequencer_step.rs:43` | default_trait_access | yes | calling `FixedBytes::default()` is more clear than this expression |
| `crates/sequencer/tests/state_proptest.rs:1` | doc_markdown | yes | item in documentation is missing backticks |

## state

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

## stm

| file:line | lint | test | message |
|---|---|---|---|
| `crates/stm/src/execute.rs:59` | elidable_lifetime_names |  | the following explicit lifetimes could be elided: 'a |
| `crates/stm/src/execute.rs:66` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:81` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/stm/src/execute.rs:104` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:111` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/stm/src/execute.rs:132` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:137` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/stm/src/execute.rs:274` | elidable_lifetime_names |  | the following explicit lifetimes could be elided: 'a |
| `crates/stm/src/execute.rs:304` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:324` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/stm/src/execute.rs:410` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:420` | redundant_closure_for_method_calls |  | redundant closure |
| `crates/stm/src/execute.rs:450` | elidable_lifetime_names |  | the following explicit lifetimes could be elided: 'a |
| `crates/stm/src/execute.rs:463` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:470` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:481` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:613` | cast_lossless |  | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/stm/src/execute.rs:616` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/stm/src/execute.rs:631` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:633` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:634` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:655` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/stm/src/execute.rs:852` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:927` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:938` | struct_excessive_bools |  | more than 3 bools in a struct |
| `crates/stm/src/execute.rs:1115` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:1126` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/stm/src/execute.rs:1248` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:1425` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:1503` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/src/execute.rs:1537` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/src/execute.rs:1541` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:1597` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:1722` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:1732` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/execute.rs:1732` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:1743` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/stm/src/execute.rs:1761` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/stm/src/execute.rs:1814` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:1877` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/execute.rs:1877` | too_many_lines |  | this function has too many lines (252/100) |
| `crates/stm/src/execute.rs:1943` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:1950` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:1963` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:1987` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/stm/src/execute.rs:2126` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/stm/src/execute.rs:2208` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/execute.rs:2208` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2229` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2242` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2262` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2281` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2296` | too_many_lines |  | this function has too many lines (141/100) |
| `crates/stm/src/execute.rs:2349` | map_unwrap_or |  | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/stm/src/execute.rs:2468` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:2469` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2490` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:2494` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/execute.rs:2524` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/execute.rs:2541` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/execute.rs:2550` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:2579` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:2593` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:2624` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:2633` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/stm/src/execute.rs:2650` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:2670` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2689` | elidable_lifetime_names |  | the following explicit lifetimes could be elided: 'p, 'a |
| `crates/stm/src/execute.rs:2708` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:2712` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:2713` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/stm/src/execute.rs:2746` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:2781` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2793` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:2803` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/execute.rs:2803` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:2803` | too_many_lines |  | this function has too many lines (242/100) |
| `crates/stm/src/execute.rs:2818` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/src/execute.rs:2855` | similar_names |  | binding's name is too similar to existing binding |
| `crates/stm/src/execute.rs:2915` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:2981` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3017` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3056` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:3118` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3125` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3134` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:3142` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:3189` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:3232` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:3283` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:3288` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:3295` | too_many_lines |  | this function has too many lines (458/100) |
| `crates/stm/src/execute.rs:3308` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/stm/src/execute.rs:3418` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/stm/src/execute.rs:3468` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:3475` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:3478` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:3513` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/src/execute.rs:3516` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3553` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/stm/src/execute.rs:3557` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/stm/src/execute.rs:3558` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/stm/src/execute.rs:3588` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/src/execute.rs:3593` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3596` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3609` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/stm/src/execute.rs:3636` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3679` | manual_let_else |  | this could be rewritten as `let...else` |
| `crates/stm/src/execute.rs:3692` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3900` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:3916` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/stm/src/execute.rs:3980` | too_many_lines |  | this function has too many lines (205/100) |
| `crates/stm/src/execute.rs:4158` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4182` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4188` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4208` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4217` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:4259` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/stm/src/execute.rs:4269` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:4305` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:4331` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4341` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/stm/src/execute.rs:4348` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/execute.rs:4370` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4380` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/stm/src/execute.rs:4400` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/execute.rs:4421` | too_many_lines |  | this function has too many lines (146/100) |
| `crates/stm/src/execute.rs:4447` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/stm/src/execute.rs:4469` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/stm/src/execute.rs:4513` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4537` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/stm/src/execute.rs:4538` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/execute.rs:4568` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/stm/src/execute.rs:4572` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/stm/src/execute.rs:4607` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/stm/src/execute.rs:4613` | inconsistent_struct_constructor |  | struct constructor field order is inconsistent with struct definition field order |
| `crates/stm/src/lib.rs:38` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/mv.rs:27` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/mv.rs:64` | cast_lossless |  | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/stm/src/mv.rs:67` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/stm/src/mv.rs:91` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/stm/src/mv.rs:118` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/mv.rs:121` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/mv.rs:124` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/stm/src/mv.rs:143` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/mv.rs:154` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/mv.rs:165` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/mv.rs:174` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/mv.rs:184` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/mv.rs:199` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/mv.rs:233` | doc_markdown |  | item in documentation is missing backticks |
| `crates/stm/src/mv.rs:237` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/mv.rs:261` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/pool.rs:110` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/pool.rs:110` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/stm/src/pool.rs:110` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/stm/src/pool.rs:164` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/stm/src/pool.rs:198` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/stm/src/pool.rs:210` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/stm/src/pool.rs:210` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/stm/src/pool.rs:217` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/stm/src/pool.rs:219` | ptr_as_ptr |  | `as` casting between raw pointers without changing their constness |
| `crates/stm/src/pool.rs:223` | ptr_as_ptr |  | `as` casting between raw pointers without changing their constness |
| `crates/stm/src/pool.rs:223` | ref_as_ptr |  | reference as raw pointer |
| `crates/stm/src/pool.rs:350` | manual_assert |  | only a `panic!` in `if`-then statement |
| `crates/stm/src/schedule.rs:78` | cast_lossless |  | casts from `u32` to `u64` can be expressed infallibly using `From` |
| `crates/stm/src/schedule.rs:124` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/stm/src/schedule.rs:171` | implicit_hasher |  | parameter of type `HashSet` should be generalized over different hashers |
| `crates/stm/src/schedule.rs:181` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/src/schedule.rs:182` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/src/schedule.rs:184` | cast_possible_truncation |  | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/tests/equivalence.rs:22` | unreadable_literal | yes | long literal lacking separators |
| `crates/stm/tests/equivalence.rs:35` | cast_possible_truncation | yes | casting `usize` to `u8` may truncate the value |
| `crates/stm/tests/equivalence.rs:117` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/stm/tests/equivalence.rs:233` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/stm/tests/equivalence.rs:383` | cast_possible_truncation | yes | casting `usize` to `u32` may truncate the value on targets with 64-bit wide pointers |
| `crates/stm/tests/equivalence.rs:650` | too_many_lines | yes | this function has too many lines (102/100) |
| `crates/stm/tests/equivalence.rs:656` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/stm/tests/equivalence.rs:688` | similar_names | yes | binding's name is too similar to existing binding |
| `crates/stm/tests/equivalence.rs:772` | too_many_lines | yes | this function has too many lines (116/100) |
| `crates/stm/tests/equivalence.rs:778` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/stm/tests/equivalence.rs:953` | too_many_lines | yes | this function has too many lines (122/100) |
| `crates/stm/tests/equivalence.rs:959` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/stm/tests/equivalence.rs:1136` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/stm/tests/equivalence.rs:1228` | too_many_lines | yes | this function has too many lines (126/100) |
| `crates/stm/tests/equivalence.rs:1336` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |

## types

| file:line | lint | test | message |
|---|---|---|---|
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

## validator

| file:line | lint | test | message |
|---|---|---|---|
| `crates/validator/src/attester.rs:106` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/validator/src/attester.rs:131` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/attester.rs:147` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/attester.rs:158` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/attester.rs:218` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/attester.rs:227` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/attester.rs:252` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/attester.rs:305` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/attester.rs:314` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/attester.rs:331` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/attester.rs:368` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/attester.rs:396` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/validator/src/attester.rs:414` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/attester.rs:442` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/attester.rs:494` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/attester.rs:495` | needless_pass_by_value |  | this argument is passed by value, but not consumed in the function body |
| `crates/validator/src/attester.rs:572` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/attester.rs:826` | match_wildcard_for_single_variants |  | wildcard matches only a single variant and will also match any future added variants |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:25` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:92` | cast_possible_truncation |  | casting `u128` to `u64` may truncate the value |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:106` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:15` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:36` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:39` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:40` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:80` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:81` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:84` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:94` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/args.rs:95` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/main.rs:69` | too_many_lines |  | this function has too many lines (347/100) |
| `crates/validator/src/bin/kardamom-validator/main.rs:180` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/validator/src/bin/kardamom-validator/main.rs:379` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on a `Result` value |
| `crates/validator/src/bin/kardamom-validator/main.rs:421` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/validator/src/bin/kardamom-validator/main.rs:493` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:1` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:16` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:25` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:27` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:54` | similar_names |  | binding's name is too similar to existing binding |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:62` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:130` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:138` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:159` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:189` | ignored_unit_patterns |  | matching over `()` is more explicit |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:215` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/validator/src/buffers.rs:2` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/buffers.rs:184` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/buffers.rs:243` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/buffers.rs:275` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/epoch_verify.rs:171` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/epoch_verify.rs:224` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/epoch_verify.rs:416` | items_after_statements |  | adding items after statements is confusing, since items exist from the start of the scope |
| `crates/validator/src/epoch_verify.rs:811` | single_char_pattern |  | single-character string constant used as pattern |
| `crates/validator/src/flight.rs:53` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/flight.rs:76` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/flight.rs:86` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/interop/extract.rs:61` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/validator/src/interop/extract.rs:79` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/validator/src/interop/extract.rs:93` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/interop/extract.rs:149` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/interop/extract.rs:238` | map_unwrap_or |  | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/validator/src/interop/extract.rs:321` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/interop/extract.rs:333` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/validator/src/interop/extract.rs:465` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/src/interop/serve.rs:150` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/interop/serve.rs:188` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/validator/src/interop/serve.rs:195` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/src/interop/store.rs:46` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/interop/store.rs:62` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/interop/store.rs:86` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/interop/store.rs:116` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/interop/store.rs:125` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/interop/store.rs:138` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/interop/store.rs:140` | map_unwrap_or |  | called `map(<f>).unwrap_or(<a>)` on an `Option` value |
| `crates/validator/src/interop/store.rs:163` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/validator/src/interop/store.rs:170` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/src/interop/store.rs:233` | cast_possible_truncation |  | casting `u64` to `u8` may truncate the value |
| `crates/validator/src/interop/verify.rs:120` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/interop/verify.rs:231` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/src/lib.rs:65` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/lib.rs:71` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/lib.rs:84` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/metrics.rs:29` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/metrics.rs:71` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/metrics.rs:113` | cast_precision_loss |  | casting `usize` to `f64` may cause a loss of precision (`usize` can be up to 64 bits wide depending on the target architecture, but `f64`'s mantissa is only 52 bits wide) |
| `crates/validator/src/metrics.rs:144` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/validator/src/metrics.rs:152` | cast_precision_loss |  | casting `u64` to `f64` may cause a loss of precision (`u64` is 64 bits wide, but `f64`'s mantissa is only 52 bits wide) |
| `crates/validator/src/parallel/claims.rs:49` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/parallel/claims.rs:51` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/parallel/claims.rs:53` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/parallel/claims.rs:57` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/parallel/claims.rs:67` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/claims.rs:69` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/validator/src/parallel/claims.rs:127` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/claims.rs:132` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/claims.rs:137` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/claims.rs:142` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/claims.rs:149` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/claims.rs:214` | must_use_candidate |  | this method could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/claims.rs:266` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/validator/src/parallel/dump.rs:128` | semicolon_if_nothing_returned |  | consider adding a `;` to the last statement for consistent formatting |
| `crates/validator/src/parallel/engine.rs:32` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/parallel/engine.rs:142` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/parallel/engine.rs:253` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/parallel/engine.rs:253` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/parallel/engine.rs:307` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/validator/src/parallel/engine.rs:335` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/validator/src/parallel/engine.rs:390` | cast_possible_truncation |  | casting `u64` to `usize` may truncate the value on targets with 32-bit wide pointers |
| `crates/validator/src/parallel/engine.rs:419` | explicit_iter_loop |  | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/validator/src/parallel/engine.rs:449` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/parallel/engine_tests.rs:29` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/src/parallel/engine_tests.rs:40` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/validator/src/parallel/engine_tests.rs:46` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/validator/src/parallel/engine_tests.rs:62` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/validator/src/parallel/engine_tests.rs:65` | cast_possible_truncation | yes | casting `u64` to `u8` may truncate the value |
| `crates/validator/src/parallel/engine_tests.rs:84` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/validator/src/parallel/engine_tests.rs:93` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/src/parallel/engine_tests.rs:328` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/validator/src/parallel/engine_tests.rs:364` | semicolon_if_nothing_returned | yes | consider adding a `;` to the last statement for consistent formatting |
| `crates/validator/src/prover.rs:99` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/prover.rs:159` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/prover.rs:190` | missing_panics_doc |  | docs for function which may panic missing `# Panics` section |
| `crates/validator/src/seams.rs:25` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/validator/src/seams.rs:65` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/validator/src/seams.rs:145` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/validator/src/seams.rs:231` | single_match_else |  | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |
| `crates/validator/src/seams.rs:500` | default_trait_access |  | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/src/witness.rs:15` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/witness.rs:29` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/witness.rs:63` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/src/witness.rs:166` | doc_markdown |  | item in documentation is missing backticks |
| `crates/validator/src/witness.rs:170` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/validator/tests/prover_spool.rs:8` | doc_markdown | yes | item in documentation is missing backticks |
| `crates/validator/tests/prover_spool.rs:29` | unreadable_literal | yes | long literal lacking separators |
| `crates/validator/tests/prover_spool.rs:44` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/tests/prover_spool.rs:55` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/validator/tests/prover_spool.rs:76` | too_many_lines | yes | this function has too many lines (210/100) |
| `crates/validator/tests/prover_spool.rs:161` | map_unwrap_or | yes | called `map(<f>).unwrap_or_else(<g>)` on an `Option` value |
| `crates/validator/tests/prover_spool.rs:306` | unnested_or_patterns | yes | unnested or-patterns |
| `crates/validator/tests/stateless_reexec.rs:26` | unreadable_literal | yes | long literal lacking separators |
| `crates/validator/tests/stateless_reexec.rs:54` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/tests/stateless_reexec.rs:271` | explicit_iter_loop | yes | it is more concise to loop over references to containers instead of using explicit iteration methods |
| `crates/validator/tests/withdrawal_e2e.rs:241` | too_many_lines | yes | this function has too many lines (112/100) |
| `crates/validator/tests/witness_anchoring.rs:35` | unreadable_literal | yes | long literal lacking separators |
| `crates/validator/tests/witness_anchoring.rs:57` | default_trait_access | yes | calling `Bytes::default()` is more clear than this expression |
| `crates/validator/tests/witness_anchoring.rs:68` | cast_possible_truncation | yes | casting `u64` to `i32` may truncate the value |
| `crates/validator/tests/witness_anchoring.rs:92` | too_many_lines | yes | this function has too many lines (236/100) |
| `crates/validator/tests/witness_anchoring.rs:144` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |
| `crates/validator/tests/witness_anchoring.rs:178` | cast_lossless | yes | casts from `u8` to `u64` can be expressed infallibly using `From` |
