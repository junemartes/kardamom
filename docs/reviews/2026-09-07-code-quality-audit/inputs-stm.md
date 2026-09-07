# Implementation inputs: stm

Directories: crates/stm

## Clippy pedantic sites (186)

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

## Mechanical rows for this group

| 458 | `block_tail` | `crates/stm/src/execute.rs:3295` |
| 252 | `with_pool` | `crates/stm/src/execute.rs:1877` |
| 242 | `push_prepared` | `crates/stm/src/execute.rs:2803` |
| 205 | `run_worker_block` | `crates/stm/src/execute.rs:3980` |
| 146 | `execute_one` | `crates/stm/src/execute.rs:4421` |
| 141 | `begin_block_deferred_inner` | `crates/stm/src/execute.rs:2296` |
| 78 | `new` | `crates/stm/src/pool.rs:110` |
| 77 | `basic_inner` | `crates/stm/src/execute.rs:275` |
| 65 | `prune` | `crates/stm/src/execute.rs:1487` |
| 57 | `run` | `crates/stm/src/pool.rs:210` |
| 57 | `flush_admit_batch` | `crates/stm/src/execute.rs:2695` |
| 55 | `advance_base` | `crates/stm/src/execute.rs:2541` |
| 126 | `sharded_admission_byte_identical` | `crates/stm/tests/equivalence.rs:1228` |
| 122 | `mv_as_layer_pipeline_wound_aborts_and_recovers` | `crates/stm/tests/equivalence.rs:953` |
| 116 | `speculative_pipeline_wound_aborts_and_recovers` | `crates/stm/tests/equivalence.rs:772` |
| 102 | `streaming_release_and_wound_correction` | `crates/stm/tests/equivalence.rs:650` |
| 94 | `bag_scheduler_byte_identical` | `crates/stm/tests/equivalence.rs:1091` |
| 54 | `hot_chain_streams_through_the_fifo` | `crates/stm/tests/equivalence.rs:562` |
| 52 | `cheap_blocks_are_declined_and_still_match` | `crates/stm/tests/equivalence.rs:447` |
| 3153 | 4620 | `crates/stm/src/execute.rs` |
| 1154 | 1372 | `crates/stm/tests/equivalence.rs` |
| crates/stm/src/execute.rs:3295 | 15 | block_tail |
| crates/stm/src/execute.rs:4421 | 12 | execute_one |
| crates/stm/src/execute.rs | 136 |
