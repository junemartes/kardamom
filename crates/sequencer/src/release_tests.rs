use alloy_primitives::{Address, B256};
use kardamom_cluster_adapter::wire::NonceRelease;

use super::{QUEUE, RECENT, ReleaseSender};
use crate::error::SequencerError;
use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;

fn release(n: u64) -> NonceRelease {
    NonceRelease {
        sender: Address::repeat_byte(0x11),
        nonce: n,
        tx_hash: B256::left_padding_from(&n.to_be_bytes()),
    }
}

fn sent(publisher: &InMemoryTxOrderingRefPublisher) -> Vec<NonceRelease> {
    publisher.releases.lock().unwrap().clone()
}

fn block(publisher: &InMemoryTxOrderingRefPublisher, blocked: bool) {
    *publisher.fail_with_backpressure.lock().unwrap() = blocked;
}

#[test]
fn a_skipped_entry_gets_one_release() {
    let mut sender = ReleaseSender::new(0);
    let mut publisher = InMemoryTxOrderingRefPublisher::default();
    sender.push(release(1));
    sender.push(release(1));
    sender.flush(&mut publisher);
    sender.push(release(1));
    sender.flush(&mut publisher);
    assert_eq!(sent(&publisher), vec![release(1)]);
}

#[test]
fn the_releases_leave_in_order_after_a_blocked_transport() {
    let mut sender = ReleaseSender::new(0);
    let mut publisher = InMemoryTxOrderingRefPublisher::default();
    block(&publisher, true);
    (1..=3).for_each(|n| sender.push(release(n)));
    sender.flush(&mut publisher);
    assert_eq!(sender.queued(), 3);
    block(&publisher, false);
    sender.flush(&mut publisher);
    assert_eq!(sent(&publisher), vec![release(1), release(2), release(3)]);
    assert_eq!(sender.queued(), 0);
}

#[test]
fn a_full_queue_drops_its_oldest_release() {
    let mut sender = ReleaseSender::new(0);
    let mut publisher = InMemoryTxOrderingRefPublisher::default();
    block(&publisher, true);
    let count = u64::try_from(QUEUE).unwrap() + 1;
    (0..count).for_each(|n| sender.push(release(n)));
    assert_eq!(sender.queued(), QUEUE);
    block(&publisher, false);
    sender.flush(&mut publisher);
    assert_eq!(sent(&publisher).first(), Some(&release(1)));
}

#[test]
fn the_sender_forgets_the_oldest_entry_past_its_bound() {
    let mut sender = ReleaseSender::new(0);
    let mut publisher = InMemoryTxOrderingRefPublisher::default();
    let count = u64::try_from(RECENT).unwrap() + 1;
    (0..count).for_each(|n| {
        sender.push(release(n));
        sender.flush(&mut publisher);
    });
    sender.push(release(0));
    sender.flush(&mut publisher);
    assert_eq!(sent(&publisher).len(), RECENT + 2, "entry 0 left the bound");
}

/// A transport that refuses the frame for good, not for a moment.
struct Refusing;

impl crate::outbound::TxOrderingRefPublisher for Refusing {
    fn try_publish_ref(&mut self, _: &crate::outbound::RefOffer) -> Result<(), SequencerError> {
        unreachable!()
    }

    fn try_publish_epoch(&mut self, _: &kardamom_types::EpochRecord) -> Result<(), SequencerError> {
        unreachable!()
    }

    fn try_publish_remote_epoch(
        &mut self,
        _: &kardamom_types::xchain::RemoteEpochRecord,
    ) -> Result<(), SequencerError> {
        unreachable!()
    }

    fn try_publish_release(&mut self, _: &NonceRelease) -> Result<(), SequencerError> {
        Err(SequencerError::EncodeFailed("refused".into()))
    }
}

#[test]
fn a_refused_release_drops_and_the_next_one_goes() {
    let mut sender = ReleaseSender::new(0);
    sender.push(release(1));
    sender.push(release(2));
    sender.flush(&mut Refusing);
    assert_eq!(sender.queued(), 0);
}
