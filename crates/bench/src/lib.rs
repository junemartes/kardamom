//! Library half of `kardamom-bench`. Exposes the `BenchWorkflow` trait, the
//! generic `Benchmark<W>` dispatcher, the in-process `Harness<W>`, three
//! built-in workflows (transfers/calls/mixed), and a few primitives
//! (mnemonic derivation, transfer presigning) that workflows reuse.

pub mod benchmark;
pub mod config;
pub mod harness;
pub mod mnemonic;
pub mod report;
pub mod signers;
pub mod workflow;
pub mod workflows;

pub use benchmark::{Benchmark, Outputs, Prepared};
pub use harness::{Harness, run_harness};
pub use workflow::BenchWorkflow;
pub use workflows::{CallsWorkflow, MixedWorkflow, TransfersWorkflow};
