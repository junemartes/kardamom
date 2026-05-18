//! Library half of `kardamom-bench`. The binary in `src/main.rs` is a thin
//! wrapper that parses CLI flags and calls `generator::run`. Splitting the
//! modules into a library keeps the smoke test (`tests/smoke.rs`) able to
//! drive the generator against an in-process node without exec'ing the bin.

pub mod config;
pub mod generator;
pub mod mnemonic;
pub mod report;
pub mod signers;
