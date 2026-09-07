# Implementation inputs: exec-core

Directories: crates/exec-core, crates/footprint, crates/reconstruct, guest

## Clippy pedantic sites (197)

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
| `crates/reconstruct/src/lib.rs:43` | must_use_candidate |  | this function could have a `#[must_use]` attribute |
| `crates/reconstruct/src/lib.rs:65` | missing_errors_doc |  | docs for function returning `Result` missing `# Errors` section |
| `crates/reconstruct/src/lib.rs:97` | unreadable_literal |  | long literal lacking separators |
| `crates/reconstruct/src/lib.rs:221` | too_many_lines |  | this function has too many lines (130/100) |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:34` | unreadable_literal | yes | long literal lacking separators |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:69` | too_many_lines | yes | this function has too many lines (133/100) |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:70` | manual_let_else | yes | this could be rewritten as `let...else` |
| `crates/reconstruct/tests/reconstruct_l1_e2e.rs:70` | single_match_else | yes | you seem to be trying to use `match` for destructuring a single pattern. Consider using `if let` |

## Mechanical rows for this group

| 100 | `execute_tx_decoded` | `crates/exec-core/src/executor/scope.rs:419` |
| 92 | `verify_witness_anchored` | `crates/exec-core/src/anchor/mod.rs:175` |
| 79 | `recompute_post_root` | `crates/exec-core/src/anchor/mod.rs:306` |
| 78 | `grade_block` | `crates/footprint/src/grade.rs:87` |
| 78 | `execute_deposit` | `crates/exec-core/src/executor/scope.rs:136` |
| 78 | `insert_in` | `crates/exec-core/src/anchor/sparse.rs:172` |
| 76 | `execute_xchain_tx` | `crates/exec-core/src/executor/xchain.rs:53` |
| 74 | `execute_deposit_tx` | `crates/exec-core/src/executor/deposit.rs:43` |
| 73 | `analyze` | `crates/footprint/src/oracle.rs:152` |
| 72 | `remove_in` | `crates/exec-core/src/anchor/sparse.rs:275` |
| 67 | `summary` | `crates/footprint/src/oracle.rs:243` |
| 61 | `record_writeset_into_bal_inner` | `crates/exec-core/src/executor/write_set.rs:115` |
| 55 | `execute_xchain` | `crates/exec-core/src/executor/scope.rs:263` |
| 54 | `main` | `crates/reconstruct/src/bin/kardamom-reconstruct.rs:64` |
| 133 | `rebuild_from_l1_reconstructs_canonical_state_root` | `crates/reconstruct/tests/reconstruct_l1_e2e.rs:69` |
| 130 | `blob_roundtrip_executes_remote_epochs` | `crates/reconstruct/src/lib.rs:221` |
| 126 | `run_case` | `crates/exec-core/tests/eest_state.rs:168` |
| 116 | `eest_state_tests_conform` | `crates/exec-core/tests/eest_state.rs:323` |
| 89 | `old_and_new_deposit_paths_agree` | `crates/exec-core/src/executor/deposit.rs:297` |
| 74 | `every_witness_lie_is_refuted` | `crates/exec-core/tests/anchor_state.rs:308` |
| 64 | `blob_roundtrip_reconstructs_identical_state_root` | `crates/reconstruct/src/lib.rs:136` |
| 58 | `nonce_too_low_skips_with_marker_receipt_and_chain_continues` | `crates/exec-core/src/executor/scope.rs:781` |
| 54 | `encoding_is_injective_across_compactions` | `crates/exec-core/tests/write_set_encoding.rs:61` |
| 53 | `honest_witness_verifies_and_recomputes_the_oracle_post_root` | `crates/exec-core/tests/anchor_state.rs:237` |
| 51 | `witness` | `crates/exec-core/tests/anchor_state.rs:157` |
| 809 | 1102 | `crates/exec-core/src/executor/scope.rs` |
| crates/exec-core/src/executor/xchain.rs:53 | 11 | execute_xchain_tx |
| crates/exec-core/src/executor/deposit.rs:43 | 10 | execute_deposit_tx |
| crates/exec-core/src/executor/scope.rs:585 | 10 | execute_once |
| crates/exec-core/src/executor/scope.rs:419 | 9 | execute_tx_decoded |
| crates/exec-core/src/executor/scope.rs:658 | 9 | skip_receipt |
| crates/exec-core/src/executor/scope.rs:711 | 9 | skip |
| crates/exec-core/src/executor/scope.rs:263 | 8 | execute_xchain |
| crates/exec-core/src/executor/scope.rs:348 | 8 | execute_tx |
| crates/footprint/src/oracle.rs | 20 |
