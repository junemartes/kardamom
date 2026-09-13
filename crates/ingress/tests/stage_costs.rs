//! Per-tx cost of the ingress admission stage, for the end-to-end model.
//! ECDSA recovery is the largest cost. This is why ingress capacity scales
//! with core count, and why batching there spreads out wakeups, not math.
#![allow(
    clippy::cast_precision_loss,
    reason = "every cast to f64 here converts a small counter or a nanosecond \
              duration for a printed rate; the values never approach the \
              mantissa's precision limit"
)]

use alloy_consensus::{Signed, TxEnvelope, TxLegacy};
use alloy_primitives::Bytes;
use alloy_rlp::Decodable;
use alloy_signer_local::PrivateKeySigner;
use kardamom_ingress::test_support::sign_legacy;

/// One worker's share of `[0, len)`, splitting as evenly as possible
/// across `threads` workers. The last worker absorbs the remainder.
fn worker_range(worker: usize, threads: usize, len: usize) -> std::ops::Range<usize> {
    let chunk = len / threads;
    let lo = worker * chunk;
    let hi = if worker == threads - 1 {
        len
    } else {
        lo + chunk
    };
    lo..hi
}

/// Recovers every `(env, raw)` pair in `range`, discarding the result
/// through `black_box`. One thread's share of the scaling loop below.
fn recover_range(envs: &[TxEnvelope], raws: &[Bytes], range: std::ops::Range<usize>) {
    for i in range {
        std::hint::black_box(
            kardamom_ingress::sig_verify::recover_single(&envs[i], &raws[i]).unwrap(),
        );
    }
}

/// The `Signed<TxLegacy>` inside `e`. Every fixture in this file is a
/// legacy tx.
///
/// # Panics
///
/// Panics if `e` is not a legacy envelope.
fn legacy(e: &TxEnvelope) -> &Signed<TxLegacy> {
    match e {
        TxEnvelope::Legacy(t) => t,
        _ => unreachable!("legacy fixtures"),
    }
}

#[test]
fn ingress_stage_costs() {
    let signer = PrivateKeySigner::random();
    let raws: Vec<Bytes> = (0..2000u64).map(|n| sign_legacy(&signer, n)).collect();

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
    for r in &raws {
        std::hint::black_box(alloy_primitives::keccak256(r.as_ref()));
    }
    let hash = t.elapsed().as_nanos() as f64 / raws.len() as f64;

    // This runs the same operation through libsecp256k1 (C, with
    // endomorphism and asm). It is the measurement behind the model's
    // ingress recommendation.
    let secp = secp256k1::SECP256K1;
    let sighashes: Vec<[u8; 32]> = envs.iter().map(|e| legacy(e).signature_hash().0).collect();
    let sigs: Vec<(secp256k1::ecdsa::RecoverableSignature, secp256k1::Message)> = envs
        .iter()
        .zip(sighashes.iter())
        .map(|(e, sh)| {
            let s = *legacy(e).signature();
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
    for (sig, msg) in &sigs {
        std::hint::black_box(secp.recover_ecdsa(*msg, sig).unwrap());
    }
    let libsecp = t.elapsed().as_nanos() as f64 / sigs.len() as f64;
    // This checks whether recovery scales across threads. Today, the
    // batch verifier's process_batch runs them in a plain sequential loop.
    for threads in [1usize, 2, 4] {
        let t = std::time::Instant::now();
        std::thread::scope(|sc| {
            (0..threads).for_each(|w| {
                let envs = &envs;
                let raws = &raws;
                let range = worker_range(w, threads, envs.len());
                sc.spawn(move || recover_range(envs, raws, range));
            });
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
