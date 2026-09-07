# Implementation inputs: bench

Directories: crates/bench, crates/executor

## Clippy pedantic sites (299)

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

## Mechanical rows for this group

| 618 | `main` | `crates/bench/src/bin/stm-p2.rs:1308` |
| 407 | `run_mdbx_ab` | `crates/bench/src/bin/stm-p2.rs:825` |
| 373 | `run_pipelined` | `crates/bench/src/bin/stm-p2.rs:362` |
| 216 | `run` | `crates/bench/src/load/mod.rs:101` |
| 196 | `main` | `crates/bench/src/bin/stm-p0.rs:126` |
| 188 | `generate` | `crates/bench/src/stm/uniswap.rs:115` |
| 138 | `main` | `crates/executor/src/bin/kardamom-executor/main.rs:47` |
| 113 | `evaluate` | `crates/bench/src/load/accounting.rs:136` |
| 90 | `run_one` | `crates/executor/src/parallel.rs:186` |
| 85 | `write_summary` | `crates/bench/src/perf/report.rs:106` |
| 84 | `run_bal_publisher` | `crates/executor/src/bal.rs:112` |
| 83 | `ramp_to_max` | `crates/bench/src/load/mod.rs:366` |
| 82 | `run_capture` | `crates/bench/src/stm/capture.rs:21` |
| 82 | `pregenerate_family` | `crates/bench/src/load/defi.rs:333` |
| 78 | `run` | `crates/bench/src/bin/perf.rs:185` |
| 75 | `snapshot` | `crates/bench/src/load/scrape.rs:117` |
| 73 | `up` | `crates/bench/src/perf/cluster.rs:108` |
| 73 | `print_report` | `crates/bench/src/load/accounting.rs:313` |
| 70 | `prepare` | `crates/bench/src/workflows/mixed.rs:87` |
| 60 | `deploy_and_confirm` | `crates/bench/src/load/defi.rs:210` |
| 59 | `receipt_feed_task` | `crates/bench/src/load/feed.rs:19` |
| 59 | `shadow_replay` | `crates/bench/src/bin/stm-p0.rs:64` |
| 59 | `dispatch` | `crates/bench/src/benchmark.rs:192` |
| 56 | `main` | `crates/bench/src/bin/load.rs:179` |
| 173 | `defi_execution_allocation_profile` | `crates/bench/tests/alloc_profile.rs:43` |
| 151 | `m4_canonical_b_order_drives_receipts` | `crates/executor/tests/m_plus_one_join.rs:173` |
| 133 | `k20_defi_parallel_matches_sequential_across_compositions` | `crates/bench/tests/parallel_defi_repro.rs:70` |
| 115 | `actor_receipts_match_naive_reference` | `crates/executor/tests/diff_reference.rs:184` |
| 109 | `tx_ref_arriving_before_envelope_still_joins` | `crates/executor/tests/m_plus_one_join.rs:376` |
| 99 | `replay_10_txs_across_3_blocks_yields_expected_c_stream` | `crates/executor/tests/replay_integration.rs:124` |
| 89 | `defi_workload_executes_on_the_engine` | `crates/bench/tests/defi_on_engine.rs:35` |
| 76 | `stm_strategy_matches_sequential_capture_byte_for_byte` | `crates/executor/tests/stm_block_exec_ab.rs:82` |
| 76 | `bench_actor_throughput` | `crates/executor/benches/sequential_throughput.rs:229` |
| 63 | `create_then_call_across_chunks_in_one_block` | `crates/bench/tests/parallel_defi_repro.rs:236` |
| 63 | `ingress_submission_allocation_profile` | `crates/bench/tests/alloc_profile_ingress.rs:142` |
| 1679 | 1993 | `crates/bench/src/bin/stm-p2.rs` |
| crates/bench/src/bin/stm-p2.rs:825 | 16 | run_mdbx_ab |
| crates/bench/src/bin/stm-p2.rs:362 | 15 | run_pipelined |
| crates/bench/src/load/engine.rs:201 | 11 | pacer |
| crates/bench/src/load/defi.rs:128 | 8 | sign |
| crates/bench/src/load/engine.rs:104 | 8 | submit_task |
| crates/bench/src/load/mod.rs:366 | 8 | ramp_to_max |
| crates/bench/src/stm/uniswap.rs:115 | 8 | generate |
| crates/bench/src/bin/stm-p2.rs | 94 |
