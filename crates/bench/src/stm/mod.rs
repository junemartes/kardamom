//! This module does offline Block-STM analysis: footprint statistics and
//! oracle dependency analysis.
//!
//! Everything here is offline analysis: no engine changes, no cluster.
//! The capture runner executes workload blocks in order, through the
//! real engine, with a fresh per-transaction `Bal`, so `storage_reads`
//! is attributed per transaction. The classifier recovers mapping
//! base-slots by keccak inversion. The oracle builds the true
//! dependency graph from actual read and write sets. Together these
//! yield the go/no-go numbers: critical-path ratio, prediction
//! hit rates, over-merge cost, and fee-sink identification.

pub mod capture;
pub mod uniswap;

// The classifier, oracle, and cell model moved to `kardamom-footprint`,
// so the live executor's shadow scheduler (`kardamom-engine::shadow`)
// grades on exactly the code the offline go verdict was measured with.
// This module re-exports them, so the `kardamom-stm-p0` binary's import
// surface stays unchanged.
pub use kardamom_footprint::{Cell, TxObs, classifier, oracle};
