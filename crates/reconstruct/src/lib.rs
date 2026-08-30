//! Rebuild-from-L1: re-execute DA-recovered blocks into a fresh state DB.
//!
//! [`kardamom_batcher::recon::reconstruct`] turns the blobs the batcher
//! posted back into `Vec<BlockFrame>`. This crate drives those blocks
//! through the shared execution engine ([`kardamom_engine::replay_blocks`])
//! into a trie-aware libMDBX state DB, and returns the reconstructed head
//! and canonical state root. Comparing that root against the chain's
//! canonical root (the validator's) proves the L2 state is fully
//! recoverable from L1 data alone: the bottom-of-stack data-availability
//! backstop.
//!
//! This is the only consumer of the state DB outside the executor and the
//! validator. It lives in its own crate, not the batcher, so the batcher
//! itself stays state-free: it only produces and decodes DA blobs.
//!
//! Deposits are out of scope here, for the same reason they are absent
//! from the DA payload: the batcher's `MultiArchiveReader` skips
//! `DepositRef`s. See [`kardamom_engine::replay`].

use std::path::Path;

use kardamom_batcher::BlockFrame;
use kardamom_engine::{ReplayBlock, ReplayOutcome, replay_blocks};
use kardamom_state::{Durability, StateEnvBuilder};
use kardamom_types::{AccountChange, CodeEntry, TxEnvelope};

/// Error from the rebuild-from-L1 reconstruction path: opening the state
/// env, or replaying blocks through the engine.
#[derive(Debug, thiserror::Error)]
#[error("reconstruct: {0}")]
pub struct ReconstructError(pub String);

/// Convert a DA-recovered [`BlockFrame`] into an engine [`ReplayBlock`].
///
/// Each `TxFrame` becomes a [`TxEnvelope`]. Its proxy-stamped `sender` and
/// `tx_hash` carry over verbatim across the blob round-trip. The execution
/// engine trusts them exactly as it does on the hot path.
pub fn block_frame_to_replay(frame: &BlockFrame) -> ReplayBlock {
    ReplayBlock {
        block_number: frame.block_number,
        l2_timestamp: frame.l2_timestamp,
        txs: frame
            .txs
            .iter()
            .map(|t| TxEnvelope {
                correlation_id: t.correlation_id,
                raw_tx: t.raw_tx.clone(),
                sender: t.sender,
                tx_hash: t.tx_hash,
            })
            .collect(),
    }
}

/// Re-execute DA-recovered `blocks` (in order) into a fresh durable state DB
/// at `state_dir`, seeding genesis first. Returns the reconstructed head and
/// state root. `blocks` must be the chain's blocks in canonical block order,
/// as recovered from consecutive posted batches.
pub fn reconstruct_state(
    state_dir: &Path,
    chain_id: u64,
    genesis_accounts: &[AccountChange],
    genesis_code: &[CodeEntry],
    blocks: &[BlockFrame],
) -> Result<ReplayOutcome, ReconstructError> {
    let env = StateEnvBuilder::new(state_dir)
        .durability(Durability::Durable)
        .open()
        .map_err(|e| ReconstructError(format!("open state env: {e}")))?;

    let replay = blocks.iter().map(block_frame_to_replay).collect::<Vec<_>>();
    replay_blocks(env, chain_id, genesis_accounts, genesis_code, replay)
        .map_err(|e| ReconstructError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{Address, Bytes as AlloyBytes, TxKind, U256, address, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
    use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
    use kardamom_batcher::recon::reconstruct;
    use kardamom_engine::replay_blocks as engine_replay;
    use kardamom_types::{BPosition, TxEnvelope};

    const CHAIN_ID: u64 = 412346;

    fn transfer(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> TxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(CHAIN_ID),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value: U256::from(value),
            input: AlloyBytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        TxEnvelope {
            correlation_id: nonce,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }

    fn genesis(from: Address) -> Vec<kardamom_types::AccountChange> {
        // keccak256("") is the canonical empty-code hash for an EOA.
        vec![kardamom_types::AccountChange {
            address: from,
            nonce: 0,
            balance: U256::from(1_000_000_000_000_000_000u128),
            code_hash: keccak256(b""),
        }]
    }

    /// The core DA guarantee: packing blocks into blobs, recovering them, and
    /// re-executing gives the same state root as executing the originals. If
    /// the KAR1 codec or blob packing changed a single tx byte, the roots
    /// would diverge.
    #[test]
    fn blob_roundtrip_reconstructs_identical_state_root() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to1 = address!("00000000000000000000000000000000000C0001");
        let to2 = address!("00000000000000000000000000000000000C0002");

        // Two closed blocks of transfers.
        let block1 = ClosedBlock {
            block_number: 1,
            l2_timestamp: 1_700_000_000,
            end_tx_idx: BPosition::from_index(2),
            txs: vec![
                RecordedTx {
                    position: BPosition::from_index(0),
                    envelope: transfer(&signer, to1, 0, 100),
                },
                RecordedTx {
                    position: BPosition::from_index(1),
                    envelope: transfer(&signer, to2, 1, 50),
                },
            ],
        };
        let block2 = ClosedBlock {
            block_number: 2,
            l2_timestamp: 1_700_000_001,
            end_tx_idx: BPosition::from_index(3),
            txs: vec![RecordedTx {
                position: BPosition::from_index(2),
                envelope: transfer(&signer, to1, 2, 25),
            }],
        };

        // Pack into blobs, reconstruct, then re-execute.
        let cfg = BatcherConfig::default();
        let batch = pack_blocks(&cfg, &[block1.clone(), block2.clone()]).unwrap();
        let frames = reconstruct(&batch.blobs).unwrap();
        assert_eq!(frames.len(), 2);

        let recon_dir = tempfile::tempdir().unwrap();
        let recovered =
            reconstruct_state(recon_dir.path(), CHAIN_ID, &genesis(from), &[], &frames).unwrap();

        // Directly replay the original envelopes (no DA round trip) as the
        // oracle.
        let oracle_dir = tempfile::tempdir().unwrap();
        let oracle_env = StateEnvBuilder::new(oracle_dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        let oracle_blocks = vec![
            ReplayBlock {
                block_number: 1,
                l2_timestamp: 1_700_000_000,
                txs: block1.txs.iter().map(|t| t.envelope.clone()).collect(),
            },
            ReplayBlock {
                block_number: 2,
                l2_timestamp: 1_700_000_001,
                txs: block2.txs.iter().map(|t| t.envelope.clone()).collect(),
            },
        ];
        let oracle =
            engine_replay(oracle_env, CHAIN_ID, &genesis(from), &[], oracle_blocks).unwrap();

        assert_eq!(recovered.head_block, 2);
        assert_eq!(recovered.txs_applied, 3);
        assert_eq!(
            recovered.state_root, oracle.state_root,
            "DA-reconstructed root must equal the directly-executed root"
        );
    }
}
