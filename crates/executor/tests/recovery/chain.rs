//! The canonical chain of the recovery tests: funded signers, the
//! genesis file, and the production egress frames of every block.

use std::fmt::Write;
use std::path::Path;

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_cluster_adapter::wire::{CanonicalTx, encode_egress_boundary, encode_egress_record};
use kardamom_engine::actor::fixtures::{LegacyTx, byte_signer};
use kardamom_types::{BPosition, TxEnvelope};

/// The transactions in each block.
pub(crate) const TXS_PER_BLOCK: u64 = 3;
/// The funded senders. Each block rotates over them.
const SENDERS: u8 = 4;
/// The timestamp of block 0.
const T0: u64 = 1_700_000_000;

/// One entry of the sealer's retained log: a canonical record at its
/// index, or the boundary of a block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Entry {
    Record {
        index: u64,
        frame: Vec<u8>,
    },
    Boundary {
        block: u64,
        end: u64,
        frame: Vec<u8>,
    },
}

/// The deterministic chain: block `b` holds [`TXS_PER_BLOCK`] transfers.
pub(crate) struct Chain {
    signers: Vec<PrivateKeySigner>,
}

impl Chain {
    pub(crate) fn new() -> Self {
        Self {
            signers: (0..SENDERS).map(|i| byte_signer(0xA1 + i)).collect(),
        }
    }

    /// Write the genesis that funds every sender.
    pub(crate) fn write_genesis(&self, path: &Path) {
        let alloc = self.signers.iter().fold(String::new(), |mut text, s| {
            let _ = write!(
                text,
                "[[alloc]]\naddress = \"{}\"\nbalance = \"1000000000000000000000\"\n\n",
                s.address()
            );
            text
        });
        std::fs::write(path, format!("chain_id = 1\n\n{alloc}")).unwrap();
    }

    /// Every sender with its committed nonce after `block`: the number of
    /// its records below the block's canonical end.
    pub(crate) fn nonces_after(&self, block: u64) -> Vec<(Address, u64)> {
        let end = block * TXS_PER_BLOCK;
        let senders = u64::from(SENDERS);
        self.signers
            .iter()
            .zip(0u64..)
            .map(|(signer, s)| (signer.address(), (end + senders - 1 - s) / senders))
            .collect()
    }

    /// The canonical index of the first record of `block`.
    pub(crate) fn first_index(block: u64) -> u64 {
        (block - 1) * TXS_PER_BLOCK
    }

    /// The entries of `block`: its records, then its boundary.
    pub(crate) fn block(&self, block: u64) -> Vec<Entry> {
        let first = Self::first_index(block);
        let end = first + TXS_PER_BLOCK;
        (first..end)
            .map(|index| self.record(index))
            .chain(std::iter::once(Entry::Boundary {
                block,
                end,
                frame: encode_egress_boundary(block, end, T0 + block, 0),
            }))
            .collect()
    }

    /// The transaction at canonical `index`: a transfer from sender
    /// `index % SENDERS`. Every sender sends every `SENDERS`th record, so
    /// its nonce is `index / SENDERS`.
    pub(crate) fn envelope(&self, index: u64) -> TxEnvelope {
        let sender = usize::try_from(index % u64::from(SENDERS)).unwrap();
        LegacyTx {
            to: Self::recipient(index),
            nonce: index / u64::from(SENDERS),
            value: index + 1,
            ..Default::default()
        }
        .sign(&self.signers[sender])
    }

    /// The recipient of record `index`: an ordinary account, never a
    /// precompile, so that every transfer succeeds.
    fn recipient(index: u64) -> Address {
        let mut bytes = [0xEE; 20];
        bytes[19] = u8::try_from(index % 200).unwrap();
        Address::from(bytes)
    }

    fn record(&self, index: u64) -> Entry {
        let envelope = self.envelope(index);
        let tx = CanonicalTx {
            canonical_id: envelope.tx_hash,
            lane: 0,
            envelope,
        };
        Entry::Record {
            index,
            frame: encode_egress_record(index, &tx.payload().unwrap()).unwrap(),
        }
    }

    /// Blocks `1..=head` as the batcher closes them for the DA store.
    pub(crate) fn closed_blocks(&self, head: u64) -> Vec<ClosedBlock> {
        (1..=head)
            .map(|block| {
                let first = Self::first_index(block);
                ClosedBlock {
                    block_number: block,
                    l2_timestamp: T0 + block,
                    end_tx_idx: BPosition::from_index(first + TXS_PER_BLOCK),
                    l1_origin: 0,
                    remote_epochs: Vec::new(),
                    txs: (first..first + TXS_PER_BLOCK)
                        .map(|index| RecordedTx {
                            position: BPosition::from_index(index),
                            envelope: self.envelope(index),
                        })
                        .collect(),
                }
            })
            .collect()
    }
}
