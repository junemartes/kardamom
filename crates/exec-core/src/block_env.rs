//! Build a deterministic revm `BlockEnv` / `CfgEnv` for a single executed tx.
//!
//! Spec invariant I3: every field is a pure function of the canonical
//! tx_ordering input. No wall clocks, no entropy.
//!
//! Every parameter here is deliberate (W1b of
//! `docs/agents/l1-client-suite-port-spec.md`): kardamom supports exactly one
//! hardfork — the latest — pinned as [`SPEC_ID`], and nothing rides a silent
//! revm default. `BlockEnv` is built as a full struct literal so a revm
//! upgrade that adds a field is a compile error (a forced decision, not a
//! silently-adopted default). `CfgEnv` is `#[non_exhaustive]`, so its
//! effective values are pinned by the `cfg_pinning` tests below instead.

use alloy_primitives::{Address, B256, U256};
use kardamom_types::BlockBoundaryStart;
use revm::context::{BlockEnv, CfgEnv};
use revm::context_interface::block::BlobExcessGasAndPrice;
use revm::primitives::eip4844::BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE;
use revm::primitives::eip7825;
use revm::primitives::hardfork::SpecId;

/// The one hardfork kardamom executes. There is no fork schedule: the chain
/// is born at genesis on the latest mainnet fork and stays there. A fork
/// upgrade is a single PR that bumps this constant plus the EEST fixture tag
/// (and, per the fork-bump checklist in the spec, revisits `BlockEnv.slot_num`
/// once EIP-7843/Amsterdam is the pin).
///
/// This must be assigned explicitly: `CfgEnv::default()` inherits revm's
/// `SpecId::default()`, which tracks whatever fork upstream considers current
/// — semantics would shift under a routine `cargo update` with no diff here.
pub const SPEC_ID: SpecId = SpecId::OSAKA;

/// Fixed per-block gas limit (v0: no dynamic adjustment).
pub const BLOCK_GAS_LIMIT: u64 = 30_000_000;

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
        // Full struct literal on purpose — see the module doc. Do NOT add
        // `..Default::default()` back: silent defaults are how we ended up
        // with fees crediting `address(0)` and a Prague blob constant under
        // an Osaka spec without anyone deciding either.
        BlockEnv {
            number: U256::from(self.block_number),
            // V0 fee sink: with `basefee = 0`, the full `gas_price × gas_used`
            // of every tx is a priority fee paid to the beneficiary. Zero
            // address = documented burn. Revisit if fees become real
            // (fee-vault predeploy), not by flipping this silently.
            beneficiary: Address::ZERO,
            timestamp: U256::from(self.l2_timestamp),
            gas_limit: BLOCK_GAS_LIMIT,
            basefee: 0,
            // Unused post-merge: DIFFICULTY (0x44) resolves to `prevrandao`.
            difficulty: U256::ZERO,
            // V0 choice: prevrandao = zero. Deterministic and trivially
            // documented; a per-block hash chain is a v1 follow-up.
            prevrandao: Some(B256::ZERO),
            // Kardamom carries no blob transactions (type-3 is rejected at
            // ingress and `max_blobs_per_tx = 0` in `cfg_env`), so this only
            // feeds the BLOBBASEFEE opcode: excess 0 ⇒ blob gasprice 1 for
            // ANY update fraction, making the Prague constant inert — but it
            // is written out so a future blob decision starts from an
            // explicit value, not a revm default.
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice::new(
                0,
                BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE,
            )),
            // EIP-7843 (Amsterdam) slot number — inert at the Osaka pin.
            // Fork-bump checklist item: derive deliberately when Amsterdam
            // becomes `SPEC_ID`.
            slot_num: 0,
        }
    }

    /// CfgEnv is `#[non_exhaustive]`; field-by-field assignment is required.
    /// The effective values — including the spec-derived ones we deliberately
    /// leave as `None` (code-size limits) — are pinned by `cfg_pinning`.
    pub fn cfg_env(&self) -> CfgEnv {
        // `new_with_spec`, NOT `default()` + `c.spec = …`: the per-opcode gas
        // table is built from the spec at construction and is NOT rebuilt on
        // a later `spec` assignment. With assign-after-default, a revm bump
        // that moves the default spec would leave us executing OSAKA rules
        // over the *new* default's gas table. `cfg_pinning` asserts the
        // table matches `GasParams::new_spec(SPEC_ID)`.
        let mut c = CfgEnv::new_with_spec(SPEC_ID);
        c.chain_id = self.chain_id;
        // EIP-7825: revm has enforced this cap since the Osaka default
        // (`None` ⇒ spec default) — adopt it explicitly. The ingress rejects
        // over-cap submissions early with a clear error; a tx that reaches
        // the canonical stream anyway becomes a deterministic invalid-skip.
        c.tx_gas_limit_cap = Some(eip7825::TX_GAS_LIMIT_CAP);
        // No blob transactions on kardamom: with `None` revm SKIPS the
        // max-blob check entirely; `Some(0)` makes any type-3 tx that slips
        // past ingress deterministically invalid.
        c.max_blobs_per_tx = Some(0);
        // Reachable only through BLOBBASEFEE given the two settings above —
        // see the `blob_excess_gas_and_price` comment in `block_env`.
        c.blob_base_fee_update_fraction = Some(BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE);
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_types::BPosition;

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

    fn env() -> ExecEnv {
        let b = BlockBoundaryStart {
            block_number: 7,
            end_tx_idx: pos(42),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        };
        ExecEnv::new(412_346, &b)
    }

    #[test]
    fn exec_env_carries_boundary_fields() {
        let e = env();
        assert_eq!(e.chain_id, 412_346);
        assert_eq!(e.block_number, 7);
        assert_eq!(e.l2_timestamp, 1_700_000_000);
    }

    #[test]
    fn block_env_uses_boundary_timestamp() {
        let env = env().block_env();
        assert_eq!(env.timestamp, U256::from(1_700_000_000u64));
        assert_eq!(env.number, U256::from(7u64));
    }

    #[test]
    fn cfg_env_carries_chain_id() {
        assert_eq!(env().cfg_env().chain_id, 412_346);
    }
}
