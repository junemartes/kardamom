//! The batch verifier's contract: every submitted transaction gets its own
//! correct answer regardless of how the ring is chunked, and the recovery
//! work does not run on the async runtime's threads.

use std::time::Duration;

use kardamom_ingress::sig_verify::BatchVerifier;

mod fixtures {
    use alloy_consensus::{SignableTransaction, TxEnvelope as AlloyEnvelope, TxLegacy};
    use alloy_primitives::{Address, Bytes, Signature, TxKind, U256};
    use alloy_rlp::Encodable;
    use alloy_signer_local::PrivateKeySigner;
    use k256::ecdsa::{RecoveryId, signature::hazmat::PrehashSigner};

    /// A signed legacy tx (decoded, as the verifier takes it), its RLP
    /// bytes, and the address that signed it.
    pub fn signed(nonce: u64) -> (AlloyEnvelope, Bytes, Address) {
        let signer = PrivateKeySigner::random();
        let addr = signer.address();
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Default::default(),
        };
        let (sig, rid): (k256::ecdsa::Signature, RecoveryId) = signer
            .credential()
            .sign_prehash(tx.signature_hash().as_slice())
            .unwrap();
        let alloy_sig = Signature::from_signature_and_parity(sig, rid.is_y_odd());
        let env: AlloyEnvelope = tx.into_signed(alloy_sig).into();
        let mut raw = Vec::new();
        env.encode(&mut raw);
        (env, Bytes::from(raw), addr)
    }
}

/// Every caller gets the sender that signed ITS transaction — chunking must
/// never cross responses. Run at several batch sizes so both the
/// single-chunk path and the fanned-out path are covered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_caller_gets_its_own_sender() {
    for n in [1usize, 3, 8, 64, 100, 200] {
        let v = std::sync::Arc::new(BatchVerifier::with_parallelism(
            64,
            Duration::from_micros(50),
            4,
        ));
        let cases: Vec<_> = (0..n as u64).map(fixtures::signed).collect();
        let mut handles = Vec::new();
        for (env, raw, addr) in cases {
            let v = v.clone();
            handles.push(tokio::spawn(async move {
                let (sender, hash) = v.recover(env, raw.clone()).await.unwrap();
                assert_eq!(sender, addr, "recovered the wrong signer at n={n}");
                assert_eq!(hash, alloy_primitives::keccak256(raw.as_ref()));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}

/// Two transactions sharing a batch must each get their own signer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_callers_never_cross_answers() {
    let v = std::sync::Arc::new(BatchVerifier::with_parallelism(
        8,
        Duration::from_micros(50),
        4,
    ));
    let (good_env, good_raw, good_addr) = fixtures::signed(1);
    let (bad_env, bad_raw, bad_addr) = fixtures::signed(2);

    let vg = v.clone();
    let g = tokio::spawn(async move { vg.recover(good_env, good_raw).await });
    let vb = v.clone();
    let b = tokio::spawn(async move { vb.recover(bad_env, bad_raw).await });
    let (gs, _) = g.await.unwrap().expect("valid tx must recover");
    assert_eq!(gs, good_addr);
    // The other transaction in the same batch must resolve to ITS OWN
    // signer — chunking must never cross responses.
    let (bs, _) = b.await.unwrap().expect("second tx must recover");
    assert_ne!(bs, good_addr, "batch crossed two callers' answers");
    assert_eq!(bs, bad_addr);
}

/// The reactor must stay responsive while recovery runs.
///
/// 256 recoveries are ~10.7ms of CPU. On a single-threaded runtime that
/// is 10.7ms of stall if the work runs on the runtime's own thread —
/// which is exactly what the sequential in-task loop did. With recovery
/// on the blocking pool a 1ms timer still fires on time. The threshold
/// sits between those two worlds: this test FAILS on the old code.
#[tokio::test(flavor = "current_thread")]
async fn recovery_does_not_block_the_reactor() {
    let v = std::sync::Arc::new(BatchVerifier::with_parallelism(
        64,
        Duration::from_micros(50),
        4,
    ));
    let cases: Vec<_> = (0..256u64).map(fixtures::signed).collect();
    let mut handles = Vec::new();
    for (env, raw, _) in cases {
        let v = v.clone();
        handles.push(tokio::spawn(async move { v.recover(env, raw).await }));
    }
    tokio::task::yield_now().await;
    let t = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(1)).await;
    let slept = t.elapsed();
    for h in handles {
        h.await.unwrap().unwrap();
    }
    assert!(
        slept < Duration::from_millis(6),
        "reactor stalled {slept:?} during batch recovery — CPU work is running on a runtime thread"
    );
}

/// What the fan-out actually buys. ECDSA recovery is ~42µs of pure CPU per
/// transaction and cannot be batched mathematically (every signature
/// recovers a DISTINCT public key — there is no shared-scalar trick as with
/// Ed25519 batch verification), so the only win available is running the
/// recoveries at the same time. This measures a full ring through the
/// verifier at parallelism 1 vs the machine's width.
///
/// Reports numbers rather than asserting a speedup: on a loaded or
/// single-core CI runner there is nothing to win, and a timing threshold
/// would be a flake. The correctness assertion — every caller gets its own
/// sender — holds either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanning_out_a_full_ring_scales_with_cores() {
    const RING: usize = 256;

    let txs: Vec<_> = (0..RING).map(|_| fixtures::signed(0)).collect();
    let width = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(1);

    let mut timings = Vec::new();
    for parallelism in [1usize, width] {
        let v = std::sync::Arc::new(BatchVerifier::with_parallelism(
            RING,
            Duration::from_micros(50),
            parallelism,
        ));
        let start = std::time::Instant::now();
        let mut handles = Vec::with_capacity(RING);
        for (env, raw, addr) in txs.iter().cloned() {
            let v = v.clone();
            handles.push(tokio::spawn(async move {
                let (sender, _hash) = v.recover(env, raw).await.expect("recover");
                assert_eq!(sender, addr, "caller got another transaction's sender");
            }));
        }
        for h in handles {
            h.await.expect("verify task");
        }
        let elapsed = start.elapsed();
        let rate = RING as f64 / elapsed.as_secs_f64();
        eprintln!("sig-verify ring={RING} parallelism={parallelism}: {elapsed:?} ({rate:.0} tx/s)");
        timings.push(rate);
    }

    if width > 1 {
        eprintln!(
            "sig-verify fan-out speedup at width {width}: {:.2}x",
            timings[1] / timings[0]
        );
    }
}
