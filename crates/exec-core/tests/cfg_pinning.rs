//! Pin every effective execution parameter (see W1b in
//! `docs/agents/l1-client-suite-port-spec.md`).
//!
//! `CfgEnv` is `#[non_exhaustive]`, so unlike `BlockEnv` this cannot force
//! a compile error when revm grows a field. Instead, these tests assert
//! the effective value of every parameter, including the spec-derived
//! ones this code deliberately leaves unset. A failure here after a revm
//! bump is the alarm working: some execution parameter changed
//! underneath this code and needs a recorded decision, not a silent
//! adoption.

use alloy_primitives::{Address, B256, U256, keccak256};
use kardamom_exec_core::block_env::{BLOCK_GAS_LIMIT, ExecEnv, SPEC_ID};
use kardamom_types::{BPosition, BlockBoundaryStart};
use revm::context::BlockEnv;
use revm::context_interface::block::BlobExcessGasAndPrice;
use revm::context_interface::cfg::GasParams;
use revm::primitives::eip4844::BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE;
use revm::primitives::hardfork::SpecId;
use revm::primitives::{eip170, eip3860, eip7825};

fn env() -> ExecEnv {
    let b = BlockBoundaryStart {
        block_number: 7,
        end_tx_idx: BPosition {
            term_id: 0,
            term_offset: 42,
        },
        l2_timestamp: 1_700_000_000,
        l1_origin: 0,
    };
    ExecEnv::new(412_346, &b)
}

#[test]
fn spec_is_pinned_to_osaka() {
    assert_eq!(SPEC_ID, SpecId::OSAKA);
    assert_eq!(env().cfg_env().spec, SPEC_ID);
}

/// A failure here means upstream revm moved its default spec past this
/// pin. Behavior is unchanged (that is what the pin is for), but a newer
/// fork now exists upstream. Follow the fork-bump procedure in the
/// spec's hardfork policy section, then update this assertion.
#[test]
fn upstream_default_spec_still_matches_pin() {
    assert_eq!(SpecId::default(), SPEC_ID);
}

#[test]
fn tx_gas_limit_cap_is_the_eip7825_cap() {
    let cfg = env().cfg_env();
    assert_eq!(cfg.tx_gas_limit_cap, Some(eip7825::TX_GAS_LIMIT_CAP));
    assert_eq!(cfg.tx_gas_limit_cap, Some(16_777_216));
    // The revm-free mirror the ingress validates against must stay equal.
    assert_eq!(
        kardamom_types::limits::TX_GAS_LIMIT_CAP,
        eip7825::TX_GAS_LIMIT_CAP
    );
}

#[test]
fn blob_parameters_are_explicit_and_blobs_are_impossible() {
    let cfg = env().cfg_env();
    // `None` would skip the max-blob check entirely. `Some(0)` makes any
    // type-3 tx that slips past ingress deterministically invalid.
    assert_eq!(cfg.max_blobs_per_tx, Some(0));
    assert_eq!(
        cfg.blob_base_fee_update_fraction,
        Some(BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE)
    );
}

#[test]
fn code_size_limits_are_spec_derived() {
    let cfg = env().cfg_env();
    // Deliberately unset. The effective limits come from the pinned spec.
    assert_eq!(cfg.limit_contract_code_size, None);
    assert_eq!(cfg.limit_contract_initcode_size, None);
    // Pin what "spec-derived" currently resolves to (EIP-170 and EIP-3860).
    assert_eq!(eip170::MAX_CODE_SIZE, 24_576);
    assert_eq!(eip3860::MAX_INITCODE_SIZE, 49_152);
}

#[test]
fn misc_cfg_values_pinned() {
    let cfg = env().cfg_env();
    assert_eq!(cfg.chain_id, 412_346);
    assert!(cfg.tx_chain_id_check);
    assert!(!cfg.disable_nonce_check);
}

/// The per-opcode gas cost table. This makes two assertions:
///
/// 1. The table in this `CfgEnv` is the one derived from `SPEC_ID`. This
///    guards the construction-order hazard: the table is built when the
///    `CfgEnv` is constructed, and is not rebuilt on a later `spec`
///    assignment.
/// 2. A golden keccak of the table itself, so an upstream opcode
///    repricing (an EIP-7904-class change, or a quiet revm patch)
///    surfaces as a reviewable diff.
#[test]
fn gas_table_matches_pinned_spec_and_golden_hash() {
    let cfg = env().cfg_env();
    let pinned = GasParams::new_spec(SPEC_ID);
    assert_eq!(cfg.gas_params.table(), pinned.table());

    let bytes: Vec<u8> = cfg
        .gas_params
        .table()
        .iter()
        .flat_map(|g| g.to_le_bytes())
        .collect();
    assert_eq!(
        keccak256(&bytes),
        "0xfc878ab5a7c84bb9731f3a2178e7722ab3247fd82502e233727c8048a2e9e90c"
            .parse::<B256>()
            .unwrap(),
        "per-opcode gas table changed — record the repricing decision, then \
         update this golden hash",
    );
}

/// `BlockEnv` is a plain struct, so full-literal equality pins every
/// field at once. Adding a field upstream breaks `block_env()` at
/// compile time first.
#[test]
fn block_env_fully_pinned() {
    let be = env().block_env();
    assert_eq!(
        be,
        BlockEnv {
            number: U256::from(7u64),
            beneficiary: Address::ZERO,
            timestamp: U256::from(1_700_000_000u64),
            gas_limit: BLOCK_GAS_LIMIT,
            basefee: 0,
            difficulty: U256::ZERO,
            prevrandao: Some(B256::ZERO),
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice::new(
                0,
                BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE,
            )),
            slot_num: 0,
        }
    );
    // An excess of 0 gives a blob gas price of 1 for any update
    // fraction. This is the documented reason the Prague constant has
    // no effect.
    assert_eq!(
        be.blob_excess_gas_and_price.unwrap().blob_gasprice,
        1,
        "BLOBBASEFEE would observe a different value"
    );
}
