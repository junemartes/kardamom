//! This is the library half of `kardamom-bench`.
//!
//! It exposes:
//! - the `BenchWorkflow` trait
//! - the generic `Benchmark<W>` dispatcher
//! - the in-process `Harness<W>`
//! - three built-in workflows: transfers, calls, and mixed
//! - primitives that workflows reuse, such as mnemonic derivation and transfer presigning

pub mod benchmark;
pub mod config;
pub mod harness;
pub mod load;
pub mod mnemonic;
pub mod perf;
pub mod report;
pub mod signers;
pub mod stm;
pub mod workflow;
pub mod workflows;

pub use benchmark::{Benchmark, Outputs, Prepared};
pub use harness::Harness;
pub use workflow::BenchWorkflow;
pub use workflows::{CallsWorkflow, MixedWorkflow, TransfersWorkflow};
