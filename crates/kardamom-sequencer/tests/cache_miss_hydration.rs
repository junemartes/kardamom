//! Cache-miss hydration: first tx for a sender pulls its committed
//! nonce from the state DB and seeds `PartitionState` before the
//! ingest-side nonce check.

use std::sync::Arc;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::{BPosition, TxEnvelope};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::fakes::ScriptedTxData;
use kardamom_sequencer::outbound::fakes::{
    InMemoryReceiptCachePublisher, InMemoryTxOrderingRefPublisher,
};
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testing::FakeStateDatabase;

fn signed_envelope(signer: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).expect("sign");
    let signed = alloy_consensus::TxEnvelope::Legacy(tx.into_signed(sig));
    let mut buf = Vec::new();
    signed.encode(&mut buf);
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: signer.address(),
        tx_hash: Default::default(),
    }
}

fn one_partition_cfg() -> SequencerConfig {
    SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    }
}

fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

#[test]
fn hydrates_committed_nonce_for_established_sender() {
    // Sender already has on-chain activity: state DB reports their next
    // nonce is 5. The sequencer must hydrate at 5, not 0, so the first
    // tx_data tx (nonce 5) lands on the matched path instead of being
    // buffered as a future nonce.
    let signer = PrivateKeySigner::random();
    let db = Arc::new(FakeStateDatabase::new());
    db.set_nonce(signer.address(), 5);

    let mut seq = Sequencer::new(one_partition_cfg(), db);
    let mut channel_a = ScriptedTxData {
        queue: [(pos(0), signed_envelope(&signer, 5, 100))].into(),
        disconnected: false,
    };
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();

    while seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap() {}

    assert_eq!(
        b.refs.lock().unwrap().len(),
        1,
        "tx must be published, not buffered"
    );
}

#[test]
fn hydrates_zero_for_brand_new_sender() {
    let signer = PrivateKeySigner::random();
    let db = Arc::new(FakeStateDatabase::new());
    // db.set_nonce not called → basic() returns Ok(None) for this sender.

    let mut seq = Sequencer::new(one_partition_cfg(), db);
    let mut channel_a = ScriptedTxData {
        queue: [(pos(0), signed_envelope(&signer, 0, 100))].into(),
        disconnected: false,
    };
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();

    while seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap() {}

    assert_eq!(b.refs.lock().unwrap().len(), 1);
}

#[test]
fn hydration_runs_once_per_sender() {
    // Second tx from the same sender must NOT re-query the state DB —
    // the in-memory map is the authority after the first hydration.
    let signer = PrivateKeySigner::random();
    let db = Arc::new(FakeStateDatabase::new());

    let mut seq = Sequencer::new(one_partition_cfg(), db);
    let mut channel_a = ScriptedTxData {
        queue: [
            (pos(0), signed_envelope(&signer, 0, 100)),
            (pos(64), signed_envelope(&signer, 1, 101)),
        ]
        .into(),
        disconnected: false,
    };
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();

    while seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap() {}

    assert_eq!(
        b.refs.lock().unwrap().len(),
        2,
        "both nonces must be published"
    );
}
