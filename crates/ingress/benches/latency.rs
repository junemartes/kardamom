//! End-to-end latency: client → proxy → mock executor → receipt.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, Signature, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, criterion_group, criterion_main};
use k256::ecdsa::{RecoveryId, signature::hazmat::PrehashSigner};

use ingress::config::IngressConfig;
use ingress::{InMemoryStateDb, IngressProxy, MockChannels};
use types::{BPosition, QuorumWatermark, Receipt};

fn sign(s: &PrivateKeySigner, nonce: u64) -> Bytes {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let (sig, rid): (k256::ecdsa::Signature, RecoveryId) = s
        .credential()
        .sign_prehash(tx.signature_hash().as_slice())
        .unwrap();
    let alloy_sig = Signature::from_signature_and_parity(sig, rid.is_y_odd());
    let env: TxEnvelope = tx.into_signed(alloy_sig).into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    Bytes::from(buf)
}

fn nonce_of(raw: &bytes::Bytes) -> u64 {
    use alloy_consensus::transaction::Transaction;
    use alloy_rlp::Decodable;
    TxEnvelope::decode(&mut raw.as_ref()).unwrap().nonce()
}

fn bench_e2e_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let proxy = rt.block_on(async {
        let cfg = IngressConfig {
            partition_count_m: 8,
            pending_receipt_timeout: Duration::from_secs(2),
            ..IngressConfig::default()
        };
        let (mock, mut rx_vec) = MockChannels::new(8);
        let state_db = Arc::new(InMemoryStateDb::new());
        let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone(), state_db));
        for (i, mut rx) in rx_vec.drain(..).enumerate() {
            let receipt_bus = mock.receipt_bus.clone();
            let watermark_bus = mock.watermark_bus.clone();
            tokio::spawn(async move {
                let mut local: i32 = 0;
                while let Some(envelope) = rx.recv().await {
                    local += 1;
                    let pos = BPosition {
                        term_id: i as i32,
                        term_offset: local,
                    };
                    let nonce = nonce_of(&envelope.raw_tx);
                    let receipt = Receipt {
                        tx_idx: pos,
                        tx_hash: envelope.tx_hash,
                        status: true,
                        gas_used: 21_000,
                        logs: Vec::new(),
                        write_set_hash: B256::ZERO,
                        from: envelope.sender,
                        nonce,
                        ..Default::default()
                    };
                    let _ = receipt_bus.send(receipt);
                    let _ = watermark_bus.send(QuorumWatermark { position: pos });
                }
            });
        }
        proxy
    });

    // Pre-sign 1000 unique-sender txs so signing isn't on the hot path.
    let pre: Vec<Bytes> = (0..1000)
        .map(|_| sign(&PrivateKeySigner::random(), 0))
        .collect();
    let mut idx = 0usize;

    c.bench_function("ingress/e2e_latency_simple_transfer", |b| {
        b.to_async(&rt).iter(|| {
            let raw = pre[idx % pre.len()].clone();
            idx = idx.wrapping_add(1);
            let proxy = proxy.clone();
            async move {
                let _ = proxy
                    .submit_raw("127.0.0.1".parse().unwrap(), raw)
                    .await
                    .unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_e2e_latency);
criterion_main!(benches);
