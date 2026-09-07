# Implementation inputs: engine

Directories: crates/engine

## Clippy pedantic sites (173)

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

## Mechanical rows for this group

| 170 | `spawn_tx_ordering_reader` | `crates/engine/src/reader.rs:480` |
| 123 | `on_boundary` | `crates/engine/src/actor/exec_thread.rs:674` |
| 81 | `drive_block` | `crates/engine/src/replay.rs:174` |
| 73 | `process_block` | `crates/engine/src/shadow.rs:112` |
| 70 | `describe` | `crates/engine/src/metrics.rs:71` |
| 66 | `on_tx` | `crates/engine/src/actor/exec_thread.rs:386` |
| 65 | `join_envelope` | `crates/engine/src/reader.rs:733` |
| 64 | `ingest` | `crates/engine/src/reader/cluster/mod.rs:140` |
| 62 | `replay_unavailable_fallback` | `crates/engine/src/bin_support.rs:396` |
| 52 | `spawn_commit` | `crates/engine/src/actor/commit_thread.rs:59` |
| 95 | `whole_block_strategy_receives_buffered_xchain_records` | `crates/engine/src/actor/exec_tests.rs:741` |
| 82 | `exec_pipelines_commit_and_next_block_reads_parent_layer` | `crates/engine/src/actor/exec_pipeline_tests.rs:28` |
| 71 | `exec_runs_two_txs_and_emits_slim_boundary` | `crates/engine/src/actor/exec_tests.rs:21` |
| 68 | `channel_b_reader_emits_tx_and_boundary_in_canonical_order` | `crates/engine/src/reader.rs:930` |
| 68 | `deposit_credit_is_visible_to_later_txs_in_the_block` | `crates/engine/src/actor/exec_tests.rs:119` |
| 66 | `exec_hands_off_shadow_captures_at_boundary` | `crates/engine/src/actor/exec_tests.rs:324` |
| 65 | `channel_b_reader_expands_an_epoch_into_marker_plus_deposits` | `crates/engine/src/reader.rs:1007` |
| 63 | `channel_b_reader_expands_a_remote_epoch_into_marker_plus_messages` | `crates/engine/src/reader.rs:1136` |
| 58 | `remote_epoch_messages_execute_as_0x7d_receipts` | `crates/engine/src/actor/exec_tests.rs:667` |
| 58 | `resume_after_empty_block_backlog` | `crates/engine/src/actor/exec_resume_tests.rs:107` |
| 58 | `resume_executes_from_cursor_with_absolute_counts` | `crates/engine/src/actor/exec_resume_tests.rs:27` |
| 56 | `run` | `crates/engine/src/actor.rs:112` |
| 55 | `exec_settles_inflight_commits_while_idle` | `crates/engine/src/actor/exec_pipeline_tests.rs:204` |
| 53 | `reader_joins_two_sessions_at_same_position` | `crates/engine/src/reader.rs:1402` |
| 53 | `an_active_feature_beats_once_per_block` | `crates/engine/src/actor/exec_tests.rs:475` |
| 52 | `exec_handoff_carries_a_populated_bal` | `crates/engine/src/actor/exec_tests.rs:203` |
| 52 | `exec_pipelines_k_deep_and_blocks_only_at_capacity` | `crates/engine/src/actor/exec_pipeline_tests.rs:134` |
| 51 | `activation_is_judged_against_the_blocks_own_header_timestamp` | `crates/engine/src/actor/exec_tests.rs:541` |
| 1032 | 1466 | `crates/engine/src/reader.rs` |
| 750 | 893 | `crates/engine/src/actor/exec_tests.rs` |
| 629 | 937 | `crates/engine/src/actor/exec_thread.rs` |
| crates/engine/src/actor/exec_thread.rs:199 | 12 | new |
| crates/engine/src/actor/exec_thread.rs:897 | 12 | spawn_exec |
| crates/engine/src/reader.rs | 51 |
| crates/engine/src/bin_support.rs | 25 |
