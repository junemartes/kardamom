# Implementation inputs: sequencer

Directories: crates/sequencer, crates/cluster-adapter, crates/cluster-client

## Clippy pedantic sites (305)

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

## Mechanical rows for this group

| 122 | `main` | `crates/sequencer/src/bin/kardamom-sequencer/main.rs:172` |
| 74 | `decode_relayed_payload` | `crates/cluster-adapter/src/wire/egress.rs:106` |
| 60 | `connect_inner` | `crates/cluster-adapter/src/live/mod.rs:260` |
| 59 | `resync_tick` | `crates/sequencer/src/sequencer.rs:236` |
| 56 | `run_egress_watermark_feed` | `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:63` |
| 55 | `on_session_event` | `crates/cluster-client/src/session/mod.rs:266` |
| 55 | `decode_egress` | `crates/cluster-adapter/src/wire/egress.rs:44` |
| 53 | `process` | `crates/sequencer/src/state/mod.rs:83` |
| 135 | `m_eq_4_sequencers_publish_canonical_refs` | `crates/sequencer/tests/multi_sequencer_dual_write.rs:83` |
| 69 | `integration_1000_txs_100_senders_with_chaos` | `crates/sequencer/tests/sequencer_integration.rs:69` |
| 67 | `peek_nonce_matches_full_decode` | `crates/sequencer/src/nonce_decode.rs:113` |
| 63 | `sequencer_core_loop_allocation_profile` | `crates/sequencer/tests/alloc_profile.rs:90` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:15` | `pub struct LiveTxDataSub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:20` | `pub fn new(handle: TxDataSubscriberHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:33` | `pub struct LiveEpochSub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:38` | `pub fn new(handle: TxDepositsSubscriberHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:49` | `pub struct LiveRemoteEpochSub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:54` | `pub fn new(handle: TxRemoteEpochsSubscriberHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:71` | `pub struct LiveTxErrorPub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:76` | `pub fn new(handle: TxErrorsPublisherHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:166` | `pub fn spawn_receipt_floor_feed(` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:244` | `pub type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258` | `pub fn spawn_publish_loops<P>(` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:47` | `pub fn spawn_egress_watermark_feed(` |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258 | 12 | spawn_publish_loops |
| crates/cluster-client/src/protocol.rs | 22 |
| crates/sequencer/src/sequencer.rs | 20 |
