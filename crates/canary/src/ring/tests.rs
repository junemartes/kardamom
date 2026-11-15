use super::*;
use crate::test_support::FakeIngress;

/// One ether, in wei.
const ETHER: u64 = 1_000_000_000_000_000_000;

impl FakeIngress {
    /// A ring of one account that the canary takes as funded.
    async fn funded_ring(&self) -> Ring {
        let ring = self.ring().await;
        ring.set_funded(0, true);
        ring
    }

    /// Lease the account and send a transfer. The lease ends without a
    /// settle, so the journal keeps the transaction.
    async fn send_unsettled(&self, ring: &Ring) -> Sent {
        let mut lease = ring.lease(&self.rpc, Self::wait()).await.unwrap();
        lease.send(&self.rpc, Self::call()).await.unwrap()
    }

    /// Lease the account, send a transfer, and settle it on its receipt.
    async fn transfer(&self, ring: &Ring) -> (Sent, Settled) {
        let mut lease = ring.lease(&self.rpc, Self::wait()).await.unwrap();
        let sent = lease.send(&self.rpc, Self::call()).await.unwrap();
        let receipt = self.rpc.receipt(sent.hash).await.unwrap().unwrap();
        let settled = lease.settle(&self.rpc, &receipt, Self::wait()).await;
        (sent, settled)
    }
}

#[tokio::test]
async fn a_skipped_send_frees_its_nonce() {
    let ingress = FakeIngress::start().await;
    let ring = ingress.funded_ring().await;
    let (sent, settled) = ingress.transfer(&ring).await;
    assert_eq!((sent.nonce, settled), (0, Settled::Skipped));
    assert_eq!(ingress.chain.lock().unwrap().nonce, 0);
    ingress.fund(ETHER);
    let (sent, settled) = ingress.transfer(&ring).await;
    assert_eq!((sent.nonce, settled), (0, Settled::Applied));
    let (sent, _) = ingress.transfer(&ring).await;
    assert_eq!(sent.nonce, 1);
}

#[tokio::test]
async fn a_revert_uses_its_nonce() {
    let ingress = FakeIngress::start().await;
    ingress.fund(ETHER);
    ingress.chain.lock().unwrap().revert = true;
    let ring = ingress.funded_ring().await;
    let (sent, settled) = ingress.transfer(&ring).await;
    let receipt = ingress.rpc.receipt(sent.hash).await.unwrap().unwrap();
    assert!(!receipt.succeeded());
    assert_eq!((sent.nonce, settled), (0, Settled::Applied));
    let (sent, _) = ingress.transfer(&ring).await;
    assert_eq!(sent.nonce, 1);
}

#[tokio::test]
async fn a_skipped_in_flight_send_frees_its_nonce_on_the_next_lease() {
    let ingress = FakeIngress::start().await;
    let ring = ingress.funded_ring().await;
    assert_eq!(ingress.send_unsettled(&ring).await.nonce, 0);
    ingress.fund(ETHER);
    let (sent, settled) = ingress.transfer(&ring).await;
    assert_eq!((sent.nonce, settled), (0, Settled::Applied));
    // The lease settled the skip from its receipt and sent no copy of it.
    let chain = ingress.chain.lock().unwrap();
    assert_eq!((chain.nonce, chain.sends), (1, 2));
}

#[tokio::test]
async fn an_account_serves_no_lease_before_its_first_balance_read() {
    let ingress = FakeIngress::start().await;
    ingress.fund(ETHER);
    let ring = ingress.ring().await;
    let leased = ring.lease(&ingress.rpc, FakeIngress::wait()).await;
    assert_eq!(leased.err(), Some(Outcome::Unfunded));
    assert_eq!(ingress.chain.lock().unwrap().sends, 0);
}

/// The balance task marks the accounts funded one at a time. A probe that
/// meets one funded account in use, and the others not read yet, waits
/// for the account. Without a funded account there is nothing to wait
/// for, and a blocked account is a stall.
#[test]
fn a_lease_waits_for_a_busy_account_while_the_others_have_no_funds_yet() {
    let misses = |all: &[Miss]| Misses(all.to_vec());
    assert!(misses(&[Miss::Busy]).wait_for_busy());
    assert!(misses(&[Miss::Busy, Miss::Unfunded, Miss::Unfunded]).wait_for_busy());
    assert!(!misses(&[Miss::Unfunded, Miss::Unfunded]).wait_for_busy());
    assert!(!misses(&[Miss::Busy, Miss::Blocked]).wait_for_busy());
    assert_eq!(
        misses(&[Miss::Unfunded, Miss::Unfunded]).outcome(),
        Outcome::Unfunded
    );
    assert_eq!(
        misses(&[Miss::Busy, Miss::Blocked]).outcome(),
        Outcome::AccountStalled
    );
}
