//! Block generators shared by the `kardamom-stm-p0` and
//! `kardamom-stm-p2` offline benchmark binaries.

use std::num::NonZeroUsize;

use alloy_consensus::TxLegacy;
use alloy_primitives::{TxKind, U256};
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::TxEnvelope;

use crate::load::defi;
use crate::load::plan::TxPlanParams;
use crate::signers::{DerivedSigner, SignerSet};

/// A scenario's setup blocks, then its flow blocks.
pub struct ScenarioBlocks {
    pub setup: Vec<Vec<TxEnvelope>>,
    pub flows: Vec<Vec<TxEnvelope>>,
}

/// The funding amount every offline-benchmark and test signer starts
/// with: far more than any scenario in this crate spends.
const FUNDED_BALANCE_WEI: u128 = 10u128.pow(21);

/// Build a [`MockStateDatabase`] with every signer funded at
/// `FUNDED_BALANCE_WEI`, nonce 0, and an empty code hash.
///
/// This is the starting snapshot every offline `DeFi` scenario and
/// engine-execution test uses, so the funding amount and account shape
/// stay in one place.
#[must_use]
pub fn funded_snapshot(signers: &[DerivedSigner]) -> MockStateDatabase {
    let mut b = MockStateDatabase::builder();
    for s in signers {
        b = b.account(
            s.signer.address(),
            U256::from(FUNDED_BALANCE_WEI),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        );
    }
    b.build()
}

/// The `BenchDefi` single-instance scenario: one deploy block, then
/// `blocks` flow blocks that interleave each sender's pregenerated
/// queue in rotation.
///
/// # Errors
///
/// Returns an error if building the deploy transactions or
/// pregenerating a sender's transfer queue fails, for example a
/// contract-loading or signing failure.
pub fn defi_blocks(
    signers: &SignerSet,
    chain_id: u64,
    blocks: usize,
    block_size: usize,
    senders: NonZeroUsize,
) -> anyhow::Result<ScenarioBlocks> {
    let plan_params = TxPlanParams {
        chain_id,
        nonce_start: 0,
        gas_price: 1_000_000_000,
    };
    let dep = defi::deployment_txs(signers, plan_params)?;
    let per_sender = (blocks * block_size) / senders + 2;
    let queues = defi::pregenerate_defi(signers, &dep.contracts, per_sender, plan_params)?;
    let setup: Vec<TxEnvelope> = dep
        .txs
        .iter()
        .map(|d| d.to_envelope(signers[0].signer.address(), 0))
        .collect();
    let mut cursors = vec![0usize; queues.len()];
    let flows = (0..blocks)
        .map(|_| interleave_defi_block(&queues, &mut cursors, signers, block_size))
        .collect();
    Ok(ScenarioBlocks {
        setup: vec![setup],
        flows,
    })
}

/// One interleaved `DeFi` block: round-robin across `queues`, draining
/// each sender's cursor in turn, until the block is full or two full
/// rotations produce nothing new (every queue drained).
fn interleave_defi_block(
    queues: &[Vec<crate::load::plan::PlannedTx>],
    cursors: &mut [usize],
    signers: &SignerSet,
    block_size: usize,
) -> Vec<TxEnvelope> {
    let mut blk = Vec::with_capacity(block_size);
    let mut si = 0usize;
    while blk.len() < block_size {
        let q = &queues[si % queues.len()];
        let c = &mut cursors[si % queues.len()];
        if *c < q.len() {
            blk.push(q[*c].to_envelope(signers[si % queues.len()].signer.address(), 0));
            *c += 1;
        }
        si += 1;
        if si > block_size * queues.len() * 2 {
            break;
        }
    }
    blk
}

/// Plain transfers, round-robin over senders and recipients: block
/// `bidx`, slot `i` sends from signer `(bidx * 7 + i) % len` to the
/// next signer in the ring.
///
/// # Errors
///
/// Returns an error if signing a transfer fails.
pub fn transfers_blocks(
    signers: &[DerivedSigner],
    chain_id: u64,
    blocks: usize,
    block_size: usize,
) -> anyhow::Result<ScenarioBlocks> {
    let mut nonces = vec![0u64; signers.len()];
    let flows = (0..blocks)
        .map(|bidx| transfers_block(signers, chain_id, block_size, &mut nonces, bidx))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ScenarioBlocks {
        setup: Vec::new(),
        flows,
    })
}

/// One `transfers` block: `block_size` plain transfers, round-robin over
/// senders and recipients (see [`transfers_blocks`]). Mutates `nonces`
/// as it signs.
fn transfers_block(
    signers: &[DerivedSigner],
    chain_id: u64,
    block_size: usize,
    nonces: &mut [u64],
    bidx: usize,
) -> anyhow::Result<Vec<TxEnvelope>> {
    (0..block_size)
        .map(|i| {
            let si = (bidx * 7 + i) % signers.len();
            let to = signers[(si + 1 + i % (signers.len() - 1)) % signers.len()]
                .signer
                .address();
            let tx = TxLegacy {
                chain_id: Some(chain_id),
                nonce: nonces[si],
                gas_price: 1_000_000_000,
                gas_limit: 21_000,
                to: TxKind::Call(to),
                value: U256::from(1000u64),
                input: alloy_primitives::Bytes::default(),
            };
            nonces[si] += 1;
            signers[si].sign_envelope(tx)
        })
        .collect()
}
