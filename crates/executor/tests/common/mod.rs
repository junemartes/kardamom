//! Shared corpus builder for [`determinism`](../determinism.rs) and
//! [`replay_integration`](../replay_integration.rs): both feed a stream
//! of signed transfers and block boundaries into
//! [`kardamom_engine::actor::fixtures::ChannelHarness`].

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;

use kardamom_engine::actor::fixtures::LegacyTx;
use kardamom_engine::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage, TxRef};

pub(crate) fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// A `tx_data` input stream: [`kardamom_engine::actor::fixtures::HarnessInput::tx_data`]'s type.
pub(crate) type TxDataVec = Vec<(BPosition, TxEnvelope)>;
/// A `tx_ordering` input stream: [`kardamom_engine::actor::fixtures::HarnessInput::tx_ordering`]'s type.
pub(crate) type TxOrderingVec = Vec<(BPosition, TxOrderingMessage)>;

/// The mutable state [`Corpus::send_block`] carries from one block to
/// the next, plus the two input streams it appends to. `expected_hashes`
/// collects every sent tx's hash, in send order; callers that do not
/// need it (determinism, which compares receipts directly) may leave it
/// unread.
pub(crate) struct Corpus<'a> {
    pub(crate) signer: &'a PrivateKeySigner,
    pub(crate) to: Address,
    pub(crate) tx_data: TxDataVec,
    pub(crate) tx_ordering: TxOrderingVec,
    pub(crate) nonce: u64,
    pub(crate) bpos_off: i32,
    pub(crate) a_pos: i32,
    pub(crate) expected_hashes: Vec<B256>,
}

impl<'a> Corpus<'a> {
    pub(crate) fn new(signer: &'a PrivateKeySigner, to: Address) -> Self {
        Self {
            signer,
            to,
            tx_data: Vec::new(),
            tx_ordering: Vec::new(),
            nonce: 0,
            bpos_off: 0,
            a_pos: 0,
            expected_hashes: Vec::new(),
        }
    }

    /// Sign one transfer at the current `nonce`, publish it to
    /// `tx_data[0]` at `a_pos`, then publish its `TxRef` onto
    /// `tx_ordering` at canonical position `bpos_off` (the executor uses
    /// the B position as the tx's canonical id, `Receipt.tx_idx`).
    /// Advances `nonce`, `bpos_off`, and `a_pos`, and records the tx's
    /// hash in `expected_hashes`.
    fn send_one_tx(&mut self) {
        let env = LegacyTx {
            chain_id: 1,
            to: self.to,
            nonce: self.nonce,
            value: 1,
            gas_limit: 21_000,
            gas_price: 0,
            ..Default::default()
        }
        .sign(self.signer);
        let tx_hash = env.tx_hash;
        self.expected_hashes.push(tx_hash);
        let tx_data_position = bpos(self.a_pos);
        self.tx_data.push((tx_data_position, env));
        self.tx_ordering.push((
            bpos(self.bpos_off),
            TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, tx_data_position, 0)),
        ));
        self.nonce += 1;
        self.bpos_off += 1;
        self.a_pos += 200;
    }

    /// Append one block's `n_txs` transfers, then the block's boundary.
    /// `end_tx_idx` equals the cumulative count of canonical records
    /// through this block; `bpos_off` has advanced once per `TxRef`, so
    /// it equals that count.
    pub(crate) fn send_block(&mut self, n_txs: u64, blk: u64) {
        for _ in 0..n_txs {
            self.send_one_tx();
        }
        self.tx_ordering.push((
            bpos(self.bpos_off),
            TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                block_number: blk,
                end_tx_idx: bpos(self.bpos_off),
                l2_timestamp: 1_700_000_000 + blk,
                l1_origin: 0,
            }),
        ));
    }
}
