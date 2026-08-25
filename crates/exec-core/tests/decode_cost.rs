//! RLP decode cost per tx — the number behind the "prepay decode for both
//! engines" fairness question in the A/B harness.
use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_rlp::{Decodable, Encodable};

#[test]
fn rlp_decode_cost() {
    // Bare transfer, and a call with 200 bytes of calldata (defi-shaped).
    for (label, input) in [
        ("transfer (0B calldata)", Bytes::new()),
        ("call (200B calldata)", Bytes::from(vec![7u8; 200])),
    ] {
        let raws: Vec<Bytes> = (0..2000u64)
            .map(|nonce| {
                let t = TxLegacy {
                    chain_id: Some(412346),
                    nonce,
                    gas_price: 1_000_000_000,
                    gas_limit: 200_000,
                    to: TxKind::Call(Address::with_last_byte(9)),
                    value: U256::from(1u64),
                    input: input.clone(),
                };
                let sig =
                    alloy_primitives::Signature::new(U256::from(1u64), U256::from(2u64), false);
                let env: TxEnvelope = t.into_signed(sig).into();
                let mut buf = Vec::new();
                env.encode(&mut buf);
                Bytes::from(buf)
            })
            .collect();
        for r in &raws {
            std::hint::black_box(TxEnvelope::decode(&mut r.as_ref()).ok());
        }
        let t = std::time::Instant::now();
        const REPS: usize = 20;
        for _ in 0..REPS {
            for r in &raws {
                std::hint::black_box(TxEnvelope::decode(&mut r.as_ref()).ok());
            }
        }
        let ns = t.elapsed().as_nanos() as f64 / (REPS * raws.len()) as f64;
        eprintln!(
            "RLP decode {label}: {ns:.0} ns/tx ({:.2} ms per 4000-tx block)",
            ns * 4000.0 / 1e6
        );
    }
}
