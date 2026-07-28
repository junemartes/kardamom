//! Sustained throughput per proxy: txs/sec a single proxy can ingest
//! with everything past the sequencer mocked.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, Signature, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use k256::ecdsa::{RecoveryId, signature::hazmat::PrehashSigner};

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::{BPosition, QuorumWatermark, Receipt};

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

const BATCH: usize = 1024;

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let proxy = rt.block_on(async {
        let cfg = IngressConfig {
            partition_count_m: 8,
            pending_receipt_timeout: Duration::from_secs(5),
            ..IngressConfig::default()
        };
        let (mock, mut rx_vec) = MockChannels::new(8);
        let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));
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
    let pre: Vec<Bytes> = (0..BATCH)
        .map(|_| sign(&PrivateKeySigner::random(), 0))
        .collect();

    let mut group = c.benchmark_group("ingress/throughput");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("submit_raw_batch_1024", |b| {
        b.to_async(&rt).iter(|| {
            let proxy = proxy.clone();
            let pre = pre.clone();
            async move {
                let mut futs = Vec::with_capacity(BATCH);
                for raw in pre {
                    let p = proxy.clone();
                    futs.push(async move {
                        p.submit_raw("127.0.0.1".parse().unwrap(), raw)
                            .await
                            .unwrap()
                    });
                }
                let _ = futures::future::join_all(futs).await;
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
