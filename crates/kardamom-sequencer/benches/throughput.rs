//! Per-sequencer throughput on one core.
//!
//! Sender supplied by the proxy (no secp256k1 on this hot path per D-Sh3).
//! Target per the spec is >100k tx/s per core for simple sigs; this bench
//! measures `run_once` loop throughput on a single thread.

use std::collections::VecDeque;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use kardamom_types::{BPosition, TxEnvelope};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::fakes::{
    InMemoryReceiptCachePublisher, InMemoryTxOrderingRefPublisher,
};
use kardamom_sequencer::sequencer::Sequencer;

struct DequeChannelA(VecDeque<(BPosition, TxEnvelope)>);
impl TxDataSubscriber for DequeChannelA {
    fn poll(&mut self) -> Result<Option<(BPosition, TxEnvelope)>, SequencerError> {
        Ok(self.0.pop_front())
    }
}

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_envelope(s: &PrivateKeySigner, n: u64, correlation_id: u64) -> TxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce: n,
        gas_price: 1,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = s.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: ConsensusEnvelope = tx.into_signed(sig).into();
    let mut buf = Vec::with_capacity(256);
    alloy_env.encode(&mut buf);
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: s.address(),
        tx_hash: Default::default(),
    }
}

fn bench_in_order(c: &mut Criterion) {
    let signers: Vec<_> = (1..=64u64).map(signer).collect();
    let mut batch: Vec<(BPosition, TxEnvelope)> = Vec::with_capacity(64 * 16);
    for (i, s) in signers.iter().enumerate() {
        for n in 0u64..16 {
            let correlation = (i * 16 + n as usize) as u64;
            let position = BPosition {
                term_id: 0,
                term_offset: (correlation as i32) * 64,
            };
            batch.push((position, signed_envelope(s, n, correlation)));
        }
    }
    c.bench_function("sequencer_run_once_1024_proxy_sender", |b| {
        b.iter_batched(
            || {
                (
                    Sequencer::new(
                        SequencerConfig {
                            partition_count: 1,
                            partition_index: 0,
                            sequencer_id: 0,
                            max_pending_per_sender: 16,
                            ..Default::default()
                        },
                        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
                    ),
                    DequeChannelA(batch.clone().into_iter().collect()),
                    InMemoryTxOrderingRefPublisher::default(),
                    InMemoryReceiptCachePublisher::default(),
                )
            },
            |(mut seq, mut ch_a, mut bp, mut rc)| {
                while seq.run_once(&mut ch_a, &mut bp, &mut rc).unwrap() {}
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_in_order);
criterion_main!(benches);
