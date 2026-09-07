# Implementation inputs: validator

Directories: crates/validator

## Clippy pedantic sites (151)

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

## Mechanical rows for this group

| 347 | `main` | `crates/validator/src/bin/kardamom-validator/main.rs:69` |
| 83 | `anchor_block_witness` | `crates/validator/src/witness.rs:63` |
| 68 | `decode_message_sent` | `crates/validator/src/interop/extract.rs:189` |
| 68 | `spawn_bal_pump` | `crates/validator/src/bin/kardamom-validator/pumps.rs:32` |
| 61 | `execute_block_parallel_scoped` | `crates/validator/src/parallel/engine.rs:363` |
| 59 | `spawn` | `crates/validator/src/epoch_verify.rs:249` |
| 58 | `execute_block_parallel` | `crates/validator/src/parallel/engine.rs:253` |
| 58 | `spawn_attester` | `crates/validator/src/attester.rs:494` |
| 57 | `spawn_prover_spool` | `crates/validator/src/prover.rs:190` |
| 54 | `from_alloy` | `crates/validator/src/parallel/claims.rs:67` |
| 53 | `execute_batch` | `crates/validator/src/parallel/engine.rs:142` |
| 52 | `records_json` | `crates/validator/src/parallel/dump.rs:15` |
| 236 | `capture_anchor_guest_and_live_trie_agree` | `crates/validator/tests/witness_anchoring.rs:92` |
| 210 | `spooled_frame_reverifies_and_matches_the_live_writer_root` | `crates/validator/tests/prover_spool.rs:76` |
| 112 | `full_withdrawal_finalize_and_challenge` | `crates/validator/tests/withdrawal_e2e.rs:241` |
| 71 | `setup` | `crates/validator/tests/withdrawal_e2e.rs:88` |
| 69 | `stateless_replay_reproduces_recorded_execution` | `crates/validator/tests/stateless_reexec.rs:168` |
| 57 | `run_pipeline` | `crates/validator/tests/forged_envelope_chaos.rs:138` |
| 57 | `pooled_dispatch_matches_scoped_reference` | `crates/validator/src/parallel/engine_tests.rs:702` |
| 56 | `exec_record` | `crates/validator/src/parallel/engine_tests.rs:116` |
| 52 | `receipt_mismatch_dumps_flight_ring` | `crates/validator/src/seams.rs:424` |
| 657 | 814 | `crates/validator/src/parallel/engine_tests.rs` |
| 605 | 813 | `crates/validator/src/epoch_verify.rs` |
| 555 | 848 | `crates/validator/src/attester.rs` |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:113` | `pub fn resync_after_engine_error(` |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:30` | `pub fn adopt_checkpoint_if_fresh(` |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:83` | `pub fn bootstrap_trie_if_adopted(state_dir: &Path, env: &StateEnv) -> Result<()> {` |
| `crates/validator/src/bin/kardamom-validator/args.rs:14` | `pub struct ValidatorFileConfig {` |
| `crates/validator/src/bin/kardamom-validator/args.rs:180` | `pub fn resolve_attester_key(key: &str) -> Result<String> {` |
| `crates/validator/src/bin/kardamom-validator/args.rs:26` | `pub struct Args {` |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:144` | `pub fn spawn_receipts_pump(` |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:174` | `pub fn spawn_commit_poller(` |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:32` | `pub fn spawn_bal_pump(` |
| crates/validator/src/parallel/engine.rs:253 | 8 | execute_block_parallel |
| crates/validator/src/attester.rs | 18 |
