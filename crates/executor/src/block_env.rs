//! Build a deterministic revm `BlockEnv` / `CfgEnv` for a single executed tx.
//!
//! Spec invariant I3: every field is a pure function of the canonical
//! tx_ordering input. No wall clocks, no entropy.

use alloy_primitives::U256;
use revm::context::{BlockEnv, CfgEnv};
use types::BlockBoundaryStart;

/// Per-block execution context derived from the sealer's BlockBoundaryStart.
/// Stable for every tx in the block; rebuilt at each boundary.
#[derive(Debug, Clone, Copy)]
pub struct ExecEnv {
    pub chain_id: u64,
    pub block_number: u64,
    pub l2_timestamp: u64,
}

impl ExecEnv {
    pub fn new(chain_id: u64, boundary: &BlockBoundaryStart) -> Self {
        Self {
            chain_id,
            block_number: boundary.block_number,
            l2_timestamp: boundary.l2_timestamp,
        }
    }

    pub fn block_env(&self) -> BlockEnv {
        BlockEnv {
            number: U256::from(self.block_number),
            timestamp: U256::from(self.l2_timestamp),
            gas_limit: 30_000_000,
            basefee: 0,
            // V0 choice: prevrandao = zero. Deterministic and trivially
            // documented; a per-block hash chain is a v1 follow-up.
            prevrandao: Some(Default::default()),
            ..Default::default()
        }
    }

    /// CfgEnv is `#[non_exhaustive]`; field-by-field assignment is required.
    #[allow(clippy::field_reassign_with_default)]
    pub fn cfg_env(&self) -> CfgEnv {
        let mut c = CfgEnv::default();
        c.chain_id = self.chain_id;
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::BPosition;

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

    #[test]
    fn exec_env_carries_boundary_fields() {
        let b = BlockBoundaryStart {
            block_number: 7,
            end_tx_idx: pos(42),
            l2_timestamp: 1_700_000_000,
        };
        let e = ExecEnv::new(412346, &b);
        assert_eq!(e.chain_id, 412346);
        assert_eq!(e.block_number, 7);
        assert_eq!(e.l2_timestamp, 1_700_000_000);
    }

    #[test]
    fn block_env_uses_boundary_timestamp() {
        let b = BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(0),
            l2_timestamp: 12345,
        };
        let env = ExecEnv::new(1, &b).block_env();
        assert_eq!(env.timestamp, U256::from(12345u64));
        assert_eq!(env.number, U256::from(1u64));
    }

    #[test]
    fn cfg_env_carries_chain_id() {
        let b = BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(0),
            l2_timestamp: 0,
        };
        let cfg = ExecEnv::new(412346, &b).cfg_env();
        assert_eq!(cfg.chain_id, 412346);
    }
}
