//! Equivalence test. For any signed legacy tx, `BatchVerifier::recover` and
//! `recover_single` return the same `(sender, tx_hash)` pair. The proxy
//! produces this pair at the system boundary.

use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_ingress::sig_verify::{BatchVerifier, recover_single};
use kardamom_ingress::test_support::{SignedTx, sign_legacy_tx};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_matches_single_on_1000_txs() {
    let v = BatchVerifier::new(NonZeroUsize::new(64).unwrap(), Duration::from_micros(50));

    // One pass does the signing and single-path recovery; the loop below
    // only distributes the already-computed values into three vectors.
    let items: Vec<_> = (0..1000)
        .map(|_| {
            let signer = PrivateKeySigner::random();
            let SignedTx {
                env,
                raw,
                sender: addr,
            } = sign_legacy_tx(&signer, 0);
            let single = recover_single(&env, &raw).unwrap();
            (addr, single, v.recover(env, raw))
        })
        .collect();
    let mut expected: Vec<Address> = Vec::with_capacity(items.len());
    let mut single_results: Vec<(Address, B256)> = Vec::with_capacity(items.len());
    let mut batched_futs = Vec::with_capacity(items.len());
    for (addr, single, fut) in items {
        expected.push(addr);
        single_results.push(single);
        batched_futs.push(fut);
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
