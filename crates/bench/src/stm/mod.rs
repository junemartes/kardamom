//! Block-STM P0: footprint statistics + oracle dependency analysis
//! (spec: `docs/agents/block-stm-executor-spec.md`).
//!
//! Everything here is OFFLINE analysis — no engine changes, no cluster.
//! The capture runner executes workload blocks sequentially through the
//! REAL engine with a fresh per-tx `Bal` (so `storage_reads` attribute per
//! tx), the classifier recovers mapping base-slots by keccak inversion,
//! and the oracle builds the TRUE dependency graph from actual read/write
//! sets — yielding the go/no-go numbers of the spec: critical-path ratio,
//! prediction hit-rates, over-merge cost, fee-sink identification.

pub mod capture;
pub mod uniswap;

// The classifier / oracle / cell model moved to `kardamom-footprint` so the
// live executor's P1 shadow (`kardamom-engine::shadow`) grades on EXACTLY
// the code the P0 GO verdict was measured with. Re-exported here so the
// `kardamom-stm-p0` bin's import surface is unchanged.
pub use kardamom_footprint::{Cell, TxObs, classifier, oracle};
