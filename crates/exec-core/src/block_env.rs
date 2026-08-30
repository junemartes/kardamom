//! Build a deterministic revm `BlockEnv` and `CfgEnv` for one executed tx.
//!
//! Spec invariant I3: every field is a pure function of the canonical
//! tx_ordering input. There is no wall clock and no entropy.
//!
//! Every parameter here is a deliberate choice (see W1b in
//! `docs/agents/l1-client-suite-port-spec.md`). Kardamom supports exactly one
//! hardfork, the latest, pinned as [`SPEC_ID`]. No field uses a silent revm
//! default. `BlockEnv` uses a full struct literal, so a revm upgrade that
//! adds a field causes a compile error, not a silent default. `CfgEnv` is
//! `#[non_exhaustive]`, so the `cfg_pinning` tests pin its effective values
//! instead.

use alloy_primitives::{Address, B256, U256};
use kardamom_types::BlockBoundaryStart;
use revm::context::{BlockEnv, CfgEnv};
use revm::context_interface::block::BlobExcessGasAndPrice;
use revm::primitives::eip4844::BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE;
use revm::primitives::eip7825;
use revm::primitives::hardfork::SpecId;

/// The one hardfork kardamom executes. There is no fork schedule. The chain
/// is born at genesis on the latest mainnet fork and stays there. A fork
/// upgrade is a single PR that bumps this constant and the EEST fixture tag
/// (and, per the fork-bump checklist in the spec, revisits `BlockEnv.slot_num`
/// once EIP-7843/Amsterdam becomes the pin).
///
/// Assign this explicitly. `CfgEnv::default()` inherits revm's
/// `SpecId::default()`, which tracks whatever fork upstream treats as
/// current. Without this constant, semantics could shift on a routine
/// `cargo update` with no diff here.
///
/// Zk-guest note: the 0x0A KZG point-evaluation precompile is registered
/// unconditionally, with a backend cascade of `c-kzg` (live builds), then
/// `blst`, then a pure-Rust arkworks fallback. The `no_std` guest build uses
/// the arkworks backend. Backend equivalence is part of revm's tested
/// contract (shared c-kzg-4844 vectors), and the host side is EEST-attested.
/// In-guest pairing cost is a performance question, not a soundness one.
/// Revm's own doc comment on `Precompiles::cancun` still claims c-kzg
/// gating; that comment is stale. The registration code is unconditional.
pub const SPEC_ID: SpecId = SpecId::OSAKA;

/// Fixed per-block gas limit. Version 0 has no dynamic adjustment.
pub const BLOCK_GAS_LIMIT: u64 = 30_000_000;

/// Per-block execution context, built from the sealer's BlockBoundaryStart.
/// It stays the same for every tx in the block, and is rebuilt at each
/// boundary.
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
        // This is a full struct literal on purpose. See the module doc.
        // Do not add `..Default::default()` back. Silent defaults caused
        // fees to credit `address(0)` and a Prague blob constant under an
        // Osaka spec, with no one deciding either value.
        BlockEnv {
            number: U256::from(self.block_number),
            // Version 0 fee sink. With `basefee = 0`, the full
            // `gas_price * gas_used` of every tx is a priority fee paid to
            // the beneficiary. The zero address means a documented burn.
            // Revisit this with a fee-vault predeploy if fees become real.
            // Do not change it silently.
            beneficiary: Address::ZERO,
            timestamp: U256::from(self.l2_timestamp),
            gas_limit: BLOCK_GAS_LIMIT,
            basefee: 0,
            // Unused after the merge. DIFFICULTY (0x44) resolves to
            // `prevrandao`.
            difficulty: U256::ZERO,
            // Version 0 choice: `prevrandao` is zero. This is deterministic
            // and simple to document. A per-block hash chain is a
            // version-1 follow-up.
            prevrandao: Some(B256::ZERO),
            // Kardamom carries no blob transactions. Ingress rejects type-3
            // txs, and `cfg_env` sets `max_blobs_per_tx = 0`. So this field
            // only feeds the BLOBBASEFEE opcode: an excess of 0 gives a
            // blob gas price of 1 for any update fraction, so the Prague
            // constant has no effect. It is written out anyway, so a
            // future blob decision starts from an explicit value, not a
            // revm default.
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice::new(
                0,
                BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE,
            )),
            // EIP-7843 (Amsterdam) slot number. It has no effect at the
            // Osaka pin. Fork-bump checklist: derive this value on purpose
            // when Amsterdam becomes `SPEC_ID`.
            slot_num: 0,
        }
    }

    /// `CfgEnv` is `#[non_exhaustive]`, so this assigns fields one by one.
    /// The `cfg_pinning` tests pin the effective values, including the
    /// spec-derived ones this code deliberately leaves as `None` (code-size
    /// limits).
    pub fn cfg_env(&self) -> CfgEnv {
        // Use `new_with_spec`, not `default()` plus `c.spec = ...`. Revm
        // builds the per-opcode gas table from the spec at construction, and
        // does not rebuild it on a later `spec` assignment. Assign-after-
        // default would let a revm bump move the default spec, and this
        // code would then run OSAKA rules over the new default's gas
        // table. `cfg_pinning` checks that the table matches
        // `GasParams::new_spec(SPEC_ID)`.
        let mut c = CfgEnv::new_with_spec(SPEC_ID);
        c.chain_id = self.chain_id;
        // EIP-7825: revm enforces this cap by default since Osaka (`None`
        // means the spec default). Set it explicitly here. Ingress rejects
        // over-cap submissions early, with a clear error. A tx that still
        // reaches the canonical stream becomes a deterministic invalid-skip.
        c.tx_gas_limit_cap = Some(eip7825::TX_GAS_LIMIT_CAP);
        // Kardamom has no blob transactions. With `None`, revm skips the
        // max-blob check entirely. `Some(0)` makes any type-3 tx that slips
        // past ingress deterministically invalid.
        c.max_blobs_per_tx = Some(0);
        // Reachable only through BLOBBASEFEE, given the two settings above.
        // See the `blob_excess_gas_and_price` comment in `block_env`.
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
