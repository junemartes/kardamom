//! The flush window of the batch verifier, timed alone: this file runs in
//! its own test process, so no other test competes for the CPU while it
//! measures.

use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;
use kardamom_ingress::sig_verify::{BatchVerifier, recover_single};
use kardamom_ingress::test_support::{SignedTx, sign_legacy_tx};

/// A transaction of a new signer.
fn signed(nonce: u64) -> SignedTx {
    sign_legacy_tx(&PrivateKeySigner::random(), nonce)
}

/// A lone transaction waits for the 50 µs window, not for a tick of the
/// tokio timer (1 ms). One request at a time never fills the ring, so
/// each one pays the whole window. The bound compares with a plain
/// recovery of the same transactions on this machine: the batch path may
/// add at most 0.5 ms each, and a timer-held window adds about 1 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lone_transaction_waits_for_the_window_not_a_timer_tick() {
    const LONE: u32 = 20;
    let v = BatchVerifier::with_parallelism(
        NonZeroUsize::new(64).unwrap(),
        Duration::from_micros(50),
        NonZeroUsize::new(1).unwrap(),
    );
    let cases: Vec<_> = (0..u64::from(LONE)).map(signed).collect();
    let started = std::time::Instant::now();
    for SignedTx { env, raw, .. } in &cases {
        recover_single(env, raw).unwrap();
    }
    let plain = started.elapsed();
    let started = std::time::Instant::now();
    for SignedTx { env, raw, sender } in cases {
        let (recovered, _) = v.recover(env, raw).await.unwrap();
        assert_eq!(recovered, sender);
    }
    let batched = started.elapsed();
    assert!(
        batched < plain + Duration::from_micros(500) * LONE,
        "{LONE} lone recoveries took {batched:?}, against {plain:?} for plain ones: the window waits on the timer"
    );
}
