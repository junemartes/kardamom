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
//!
//! Cross-chain (interop) deliveries are in scope. Remote-epoch records
//! travel in the DA payload by value (KAR1 v2, spec §16 Q8): they are not
//! derivable again from this chain's L1. The replay runs their messages
//! again as 0x7D transactions, at the head of the block each record leads.

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
#[must_use]
pub fn block_frame_to_replay(frame: &BlockFrame) -> ReplayBlock {
    ReplayBlock {
        block_number: frame.block_number,
        l2_timestamp: frame.l2_timestamp,
        remote_epochs: frame.remote_epochs.clone(),
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
///
/// # Errors
///
/// Returns [`ReconstructError`] when the state env fails to open, or when
/// replaying the blocks through the engine fails.
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

/// Shared test fixtures: signed transfers, a funded-EOA genesis, closed-
/// block builders, and an oracle-replay helper. This crate's own unit
/// tests (below) and the external `tests/reconstruct_l1_e2e.rs` binary
/// both build the same two-block transfer scenario and replay it as the
/// no-DA-round-trip oracle, so they share one copy here instead of each
/// keeping its own.
///
/// Gated on `feature = "test-support"` (in addition to `cfg(test)`):
/// `cfg(test)` alone only reaches this crate's own test binary, not an
/// external integration-test crate. `tests/reconstruct_l1_e2e.rs` pulls
/// this module in through the crate's own `[dev-dependencies]` entry on
/// itself, with that feature enabled.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{Address, TxKind, U256, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
    use kardamom_engine::{ReplayBlock, ReplayOutcome, replay_blocks};
    use kardamom_state::{Durability, StateEnvBuilder};
    use kardamom_types::{AccountChange, BPosition, CodeEntry, TxEnvelope};

    /// The dev/test L2 chain id every reconstruction fixture below uses.
    pub const CHAIN_ID: u64 = 412_346;

    /// A signed legacy transfer from `signer` to `to`, on [`CHAIN_ID`].
    ///
    /// # Panics
    ///
    /// Panics if signing fails. `signer` is always a freshly generated
    /// in-process key in every caller, so this never happens in
    /// practice.
    #[must_use]
    pub fn transfer(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> TxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(CHAIN_ID),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value: U256::from(value),
            input: alloy_primitives::Bytes::new(),
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

    /// A funded EOA genesis: one account with a large starting balance
    /// and the canonical empty-code hash (`keccak256("")`).
    #[must_use]
    pub fn genesis(from: Address) -> Vec<AccountChange> {
        vec![AccountChange {
            address: from,
            nonce: 0,
            balance: U256::from(1_000_000_000_000_000_000u128),
            code_hash: keccak256(b""),
        }]
    }

    /// Two closed blocks of ordinary transfers from `user`: block 1 pays
    /// `to1` then `to2`, block 2 pays `to1` again.
    ///
    /// # Panics
    ///
    /// Panics if signing a transfer fails; see [`transfer`].
    #[must_use]
    pub fn two_transfer_blocks(
        user: &PrivateKeySigner,
        to1: Address,
        to2: Address,
    ) -> (ClosedBlock, ClosedBlock) {
        let block1 = ClosedBlock {
            block_number: 1,
            l2_timestamp: 1_700_000_000,
            end_tx_idx: BPosition::from_index(2),
            remote_epochs: vec![],
            txs: vec![
                RecordedTx {
                    position: BPosition::from_index(0),
                    envelope: transfer(user, to1, 0, 100),
                },
                RecordedTx {
                    position: BPosition::from_index(1),
                    envelope: transfer(user, to2, 1, 50),
                },
            ],
        };
        let block2 = ClosedBlock {
            block_number: 2,
            l2_timestamp: 1_700_000_001,
            end_tx_idx: BPosition::from_index(3),
            remote_epochs: vec![],
            txs: vec![RecordedTx {
                position: BPosition::from_index(2),
                envelope: transfer(user, to1, 2, 25),
            }],
        };
        (block1, block2)
    }

    /// Replay `blocks` directly (no DA round trip) into a fresh,
    /// throwaway state DB, on [`CHAIN_ID`] — the oracle every
    /// reconstruction gate compares its recovered root against.
    ///
    /// # Panics
    ///
    /// Panics if the throwaway state env fails to open, or if replay
    /// fails — both a test-fixture setup failure, not an assertion this
    /// helper's callers grade.
    pub fn oracle_replay(
        genesis_accounts: &[AccountChange],
        genesis_code: &[CodeEntry],
        blocks: Vec<ReplayBlock>,
    ) -> ReplayOutcome {
        let oracle_dir = tempfile::tempdir().unwrap();
        let oracle_env = StateEnvBuilder::new(oracle_dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        replay_blocks(oracle_env, CHAIN_ID, genesis_accounts, genesis_code, blocks).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{CHAIN_ID, genesis, oracle_replay, transfer, two_transfer_blocks};
    use alloy_primitives::{Address, B256, U256, address};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
    use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
    use kardamom_batcher::recon::reconstruct;
    use kardamom_types::BPosition;

    /// The interop scenario's remote origin chain id.
    const REMOTE_ORIGIN: u64 = 412_399;

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
        let (block1, block2) = two_transfer_blocks(&signer, to1, to2);

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
        let oracle_blocks = vec![
            ReplayBlock {
                block_number: 1,
                l2_timestamp: 1_700_000_000,
                remote_epochs: vec![],
                txs: block1.txs.iter().map(|t| t.envelope.clone()).collect(),
            },
            ReplayBlock {
                block_number: 2,
                l2_timestamp: 1_700_000_001,
                remote_epochs: vec![],
                txs: block2.txs.iter().map(|t| t.envelope.clone()).collect(),
            },
        ];
        let oracle = oracle_replay(&genesis(from), &[], oracle_blocks);

        assert_eq!(recovered.head_block, 2);
        assert_eq!(recovered.txs_applied, 3);
        assert_eq!(
            recovered.state_root, oracle.state_root,
            "DA-reconstructed root must equal the directly-executed root"
        );
    }

    /// One interop scenario: real interop genesis (Outbox/Inbox predeploy
    /// bytecode), a funded EOA, and one closed block that carries a
    /// remote-epoch record (two 0x7D deliveries, one with a callback) plus
    /// one ordinary transfer.
    struct InteropScenario {
        accounts: Vec<AccountChange>,
        code: Vec<CodeEntry>,
        block: ClosedBlock,
        record: kardamom_types::xchain::RemoteEpochRecord,
    }

    impl InteropScenario {
        fn build() -> Self {
            use kardamom_types::xchain::{Callback, OutboxMessage, derive_remote_epoch};

            // The real interop genesis: Outbox/Inbox runtime bytecode at
            // their canonical predeploys — the same file
            // `kardamom-reconstruct --chain` seeds from.
            let genesis_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../chains/dev-interop.toml");
            let raw = std::fs::read_to_string(&genesis_path).expect("read dev-interop genesis");
            let chain: kardamom_types::Genesis = toml::from_str(&raw).expect("parse genesis");
            chain.validate().expect("valid genesis");
            assert_eq!(chain.chain_id, CHAIN_ID, "test pins the dev chain id");
            let (mut accounts, code) = chain.to_alloc();

            // Fund a random user so the block also carries an ordinary tx.
            let signer = PrivateKeySigner::random();
            let from = signer.address();
            accounts.extend(genesis(from));

            // One record, two messages (block-100 batch): an EOA call, and
            // one with a callback so the response is enqueued through the
            // destination's OWN Outbox — real predeploy writes on both
            // sides.
            let payee = address!("00000000000000000000000000000000000E0001");
            let cb = Callback {
                target: Address::repeat_byte(0xCB),
                gas_limit: 90_000,
                context: B256::repeat_byte(0x42),
            };
            let msg = |seq: u64, callback: Option<Callback>| OutboxMessage {
                origin_block_number: 100,
                origin_block_hash: B256::repeat_byte(0x64),
                dest_chain_id: CHAIN_ID,
                seq,
                sender: Address::repeat_byte(0xA1),
                target: payee,
                value: 0,
                gas_limit: 150_000,
                data: alloy_primitives::Bytes::copy_from_slice(&[0xDE, 0xAD]),
                callback,
            };
            let record = derive_remote_epoch(
                CHAIN_ID,
                REMOTE_ORIGIN,
                0,
                &[msg(0, None), msg(1, Some(cb))],
            )
            .expect("derive record");

            let block = ClosedBlock {
                block_number: 1,
                l2_timestamp: 1_700_000_000,
                end_tx_idx: BPosition::from_index(4),
                remote_epochs: vec![record.clone()],
                txs: vec![RecordedTx {
                    position: BPosition::from_index(3),
                    envelope: transfer(&signer, payee, 0, 100),
                }],
            };

            Self {
                accounts,
                code,
                block,
                record,
            }
        }
    }

    /// The rebuilt DB, opened read-only after a reconstruction, checked
    /// against one origin chain's expected interop lane state and
    /// receipts.
    struct RebuiltDb<'a> {
        snap: &'a kardamom_state::StateSnapshot,
        origin: u64,
    }

    impl RebuiltDb<'_> {
        /// Check the Inbox/Outbox lane state the two deliveries wrote:
        /// `Inbox.delivered` marks both `(origin, seq)` pairs success,
        /// `Inbox.nextSeq` advances past the batch, and the callback
        /// response is enqueued through the destination's own Outbox.
        fn assert_lane_state(&self) {
            use kardamom_types::StateDatabase;
            use kardamom_types::xchain::{INBOX, OUTBOX};

            let origin = self.origin;
            let pad = |v: u64| {
                let mut w = [0u8; 32];
                w[24..].copy_from_slice(&v.to_be_bytes());
                B256::from(w)
            };
            let map_slot = |key: B256, base: u64| {
                alloy_primitives::keccak256([key.as_slice(), pad(base).as_slice()].concat())
            };
            let delivered_slot = |seq: u64| {
                let inner = map_slot(pad(origin), 0);
                alloy_primitives::keccak256([pad(seq).as_slice(), inner.as_slice()].concat())
            };
            for seq in 0..2u64 {
                assert_eq!(
                    self.snap.storage(INBOX, delivered_slot(seq)).unwrap(),
                    U256::from(1),
                    "Inbox.delivered[{origin}][{seq}] must be success in the rebuilt DB"
                );
            }
            assert_eq!(
                self.snap.storage(INBOX, map_slot(pad(origin), 1)).unwrap(),
                U256::from(2),
                "Inbox.nextSeq[{origin}] must cover the batch"
            );
            assert_eq!(
                self.snap.storage(OUTBOX, map_slot(pad(origin), 0)).unwrap(),
                U256::from(1),
                "the callback response must be enqueued through the local Outbox"
            );
        }

        /// Check the two 0x7D receipts are reproduced in the rebuilt DB,
        /// keyed by `remote_source_hash`.
        fn assert_receipts(&self) {
            use kardamom_types::xchain::{INBOX, remote_source_hash, xchain_tx_sender};
            use kardamom_types::{StateDatabase, TX_TYPE_XCHAIN};

            let origin = self.origin;
            for seq in 0..2u64 {
                let source_hash = remote_source_hash(origin, seq);
                let pos = self
                    .snap
                    .get_tx_position(source_hash)
                    .unwrap()
                    .unwrap_or_else(|| panic!("rebuilt DB must index the seq-{seq} 0x7D receipt"));
                let receipt = self
                    .snap
                    .get_receipt(pos)
                    .unwrap()
                    .expect("receipt at position");
                assert_eq!(receipt.tx_hash, source_hash);
                assert_eq!(receipt.tx_type, TX_TYPE_XCHAIN);
                assert!(receipt.status, "delivery seq {seq} must succeed");
                assert_eq!(receipt.to, Some(INBOX));
                assert_eq!(receipt.from, xchain_tx_sender(origin));
                assert_eq!(receipt.effective_gas_price, 0, "delivery is fee-free");
            }
        }
    }

    /// The interop DA guarantee (spec §16 Q8), at the reexec level. No
    /// deposits-in-DA analogue exists (deposits are absent from DA by
    /// design), so this is the first record-in-DA replay test: a block led
    /// by a remote-epoch record — carried in the blob by VALUE — must
    /// reconstruct to the same root as direct execution, with the 0x7D
    /// deliveries executed against the REAL Inbox predeploy (the
    /// `chains/dev-interop.toml` genesis), the Inbox/Outbox lane state
    /// present in the rebuilt DB, and the 0x7D receipts reproduced.
    ///
    /// The scenario setup lives in [`InteropScenario::build`], and the two
    /// side-effect checks in [`RebuiltDb::assert_lane_state`] and
    /// [`RebuiltDb::assert_receipts`]; this sequences them around the
    /// pack/reconstruct/replay round trip and the root comparison.
    #[test]
    fn blob_roundtrip_executes_remote_epochs() {
        let scenario = InteropScenario::build();

        // Pack → blobs → reconstruct → re-execute.
        let batch = pack_blocks(
            &BatcherConfig::default(),
            std::slice::from_ref(&scenario.block),
        )
        .unwrap();
        let frames = reconstruct(&batch.blobs).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].remote_epochs, vec![scenario.record.clone()]);

        let recon_dir = tempfile::tempdir().unwrap();
        let recovered = reconstruct_state(
            recon_dir.path(),
            CHAIN_ID,
            &scenario.accounts,
            &scenario.code,
            &frames,
        )
        .unwrap();
        assert_eq!(recovered.head_block, 1);
        assert_eq!(
            recovered.txs_applied, 3,
            "two 0x7D deliveries + one user tx"
        );

        // Oracle: direct replay of the same block, no DA round-trip.
        let oracle = oracle_replay(
            &scenario.accounts,
            &scenario.code,
            vec![ReplayBlock {
                block_number: 1,
                l2_timestamp: 1_700_000_000,
                remote_epochs: vec![scenario.record],
                txs: scenario
                    .block
                    .txs
                    .iter()
                    .map(|t| t.envelope.clone())
                    .collect(),
            }],
        );
        assert_eq!(
            recovered.state_root, oracle.state_root,
            "DA-reconstructed root must equal the directly-executed root — \
             including the 0x7D deliveries"
        );

        let env = StateEnvBuilder::new(recon_dir.path())
            .read_only(true)
            .open()
            .unwrap();
        let snap = kardamom_state::StateSnapshot::open(&env).unwrap();
        let rebuilt = RebuiltDb {
            snap: &snap,
            origin: REMOTE_ORIGIN,
        };
        rebuilt.assert_lane_state();
        rebuilt.assert_receipts();
    }
}
