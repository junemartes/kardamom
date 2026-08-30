//! Per-tx cost of the ingress admission stage, for the end-to-end model.
//! ECDSA recovery is the largest cost. This is why ingress capacity scales
//! with core count, and why batching there spreads out wakeups, not math.
use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, Bytes, Signature, TxKind, U256};
use alloy_rlp::{Decodable, Encodable};
use alloy_signer_local::PrivateKeySigner;
use k256::ecdsa::{RecoveryId, signature::hazmat::PrehashSigner};

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

#[test]
fn ingress_stage_costs() {
    let signer = PrivateKeySigner::random();
    let raws: Vec<Bytes> = (0..2000u64).map(|n| sign(&signer, n)).collect();

    let t = std::time::Instant::now();
    let envs: Vec<TxEnvelope> = raws
        .iter()
        .map(|r| TxEnvelope::decode(&mut r.as_ref()).unwrap())
        .collect();
    let decode = t.elapsed().as_nanos() as f64 / raws.len() as f64;

    let t = std::time::Instant::now();
    for (e, r) in envs.iter().zip(raws.iter()) {
        std::hint::black_box(kardamom_ingress::sig_verify::recover_single(e, r).unwrap());
    }
    let recover = t.elapsed().as_nanos() as f64 / raws.len() as f64;

    let t = std::time::Instant::now();
    for r in raws.iter() {
        std::hint::black_box(alloy_primitives::keccak256(r.as_ref()));
    }
    let hash = t.elapsed().as_nanos() as f64 / raws.len() as f64;

    // This runs the same operation through libsecp256k1 (C, with
    // endomorphism and asm). It is the measurement behind the model's
    // ingress recommendation.
    let secp = secp256k1::SECP256K1;
    let sighashes: Vec<[u8; 32]> = envs
        .iter()
        .map(|e| match e {
            TxEnvelope::Legacy(t) => t.signature_hash().0,
            _ => unreachable!("legacy fixtures"),
        })
        .collect();
    let sigs: Vec<(secp256k1::ecdsa::RecoverableSignature, secp256k1::Message)> = envs
        .iter()
        .zip(sighashes.iter())
        .map(|(e, sh)| {
            let s = match e {
                TxEnvelope::Legacy(t) => *t.signature(),
                _ => unreachable!("legacy fixtures"),
            };
            let mut compact = [0u8; 64];
            compact[..32].copy_from_slice(&s.r().to_be_bytes::<32>());
            compact[32..].copy_from_slice(&s.s().to_be_bytes::<32>());
            let rid = secp256k1::ecdsa::RecoveryId::try_from(i32::from(s.v())).unwrap();
            (
                secp256k1::ecdsa::RecoverableSignature::from_compact(&compact, rid).unwrap(),
                secp256k1::Message::from_digest(*sh),
            )
        })
        .collect();
    let t = std::time::Instant::now();
    for (sig, msg) in sigs.iter() {
        std::hint::black_box(secp.recover_ecdsa(*msg, sig).unwrap());
    }
    let libsecp = t.elapsed().as_nanos() as f64 / sigs.len() as f64;
    // This checks whether recovery scales across threads. Today, the
    // batch verifier's process_batch runs them in a plain sequential loop.
    for threads in [1usize, 2, 4] {
        let chunk = envs.len() / threads;
        let t = std::time::Instant::now();
        std::thread::scope(|sc| {
            for w in 0..threads {
                let envs = &envs;
                let raws = &raws;
                sc.spawn(move || {
                    let lo = w * chunk;
                    let hi = if w == threads - 1 {
                        envs.len()
                    } else {
                        lo + chunk
                    };
                    for i in lo..hi {
                        std::hint::black_box(
                            kardamom_ingress::sig_verify::recover_single(&envs[i], &raws[i])
                                .unwrap(),
                        );
                    }
                });
            }
        });
        let el = t.elapsed().as_secs_f64();
        eprintln!(
            "INGRESS recovery threads={threads}: {:.0} tx/s ({:.1}x vs 1 thread, {:.1} µs/tx effective)",
            envs.len() as f64 / el,
            (envs.len() as f64 / el) / (1e9 / recover),
            el * 1e6 / envs.len() as f64
        );
    }

    eprintln!(
        "INGRESS recovery backends: k256 {:.0} ns/tx | libsecp256k1 {libsecp:.0} ns/tx => {:.1}x, {:.0} tx/s per core",
        recover - hash,
        (recover - hash) / libsecp,
        1e9 / (libsecp + hash + decode)
    );

    eprintln!(
        "INGRESS per tx: decode {decode:.0} ns | recover+hash {recover:.0} ns (hash alone {hash:.0} ns) => {:.0} tx/s per core at the sig boundary",
        1e9 / (decode + recover)
    );
}
