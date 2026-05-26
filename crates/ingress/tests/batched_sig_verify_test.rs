//! Equivalence: for any signed legacy tx, `BatchVerifier::recover` and
//! `recover_single` agree on `(sender, tx_hash)` — the canonical pair the
//! proxy produces at the system boundary.

mod common;

use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use ingress::sig_verify::{BatchVerifier, recover_single};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_matches_single_on_1000_txs() {
    let v = BatchVerifier::new(64, Duration::from_micros(50));
    let mut single_results: Vec<(Address, B256)> = Vec::new();
    let mut batched_futs = Vec::new();
    let mut expected = Vec::new();

    for _ in 0..1000 {
        let signer = PrivateKeySigner::random();
        let (env, raw, addr) = common::sign_legacy_tx(&signer, 0);
        expected.push(addr);
        single_results.push(recover_single(&env, &raw).unwrap());
        batched_futs.push(v.recover(env, raw));
    }

    let batched_results: Vec<(Address, B256)> = futures::future::join_all(batched_futs)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(batched_results, single_results);
    for (i, (addr, _)) in batched_results.iter().enumerate() {
        assert_eq!(*addr, expected[i]);
    }
}
