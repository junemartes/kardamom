use alloy_primitives::B256;
use kardamom_cluster_adapter::wire::{self, CanonicalTx, GuardHeader, RelayedRef};

use super::{SealerRules, SealerVersion};
use crate::outbound::fakes::InMemorySidePublisher;
use crate::testkit::{EnvelopeSpec, envelope_with, signer};
use crate::v2::AccountEvent;

struct Feed {
    rules: SealerRules<InMemorySidePublisher>,
    events: crossbeam_channel::Receiver<AccountEvent>,
}

impl Feed {
    fn new(version: u8) -> Self {
        let (tx, events) = crossbeam_channel::bounded(8);
        Self {
            rules: SealerRules::new(
                0,
                SealerVersion::starting_at(version),
                InMemorySidePublisher::default(),
            )
            .telling(tx),
            events,
        }
    }

    fn reference(record_type: u8) -> RelayedRef {
        RelayedRef {
            index: 123,
            canonical_id: B256::repeat_byte(17),
            record_type,
        }
    }

    fn assert_reference(&self, kind: u8) {
        let reference = Self::reference(kind);
        self.assert_confirmation(&reference.encode(), reference.canonical_id);
    }

    fn canonical() -> CanonicalTx {
        let envelope = envelope_with(
            &signer(11),
            0,
            1,
            EnvelopeSpec {
                real_hash: true,
                ..EnvelopeSpec::default()
            },
        );
        CanonicalTx {
            canonical_id: envelope.tx_hash,
            lane: 0,
            envelope,
        }
    }

    fn assert_confirmation(&self, frame: &[u8], hash: B256) {
        assert!(self.rules.on_canonical_frame(frame));
        assert!(
            matches!(self.events.try_recv().unwrap(), AccountEvent::Canonical(actual) if actual == hash)
        );
        assert!(self.events.is_empty());
    }
}

#[test]
fn retained_transaction_references_confirm_the_exact_hash() {
    let feed = Feed::new(5);
    for kind in [wire::RT_TX, wire::RT_TXREF, wire::RT_TXREF_OFFER] {
        feed.assert_reference(kind);
    }
}

#[test]
fn full_and_carried_transactions_confirm_the_exact_hash() {
    let feed = Feed::new(5);
    let tx = Feed::canonical();
    for payload in [tx.payload().unwrap(), tx.full_ref_payload().unwrap()] {
        feed.assert_confirmation(
            &wire::encode_egress_record(123, &payload).unwrap(),
            tx.canonical_id,
        );
    }
}

#[test]
fn a_legacy_relayed_reference_confirms_the_exact_hash() {
    let feed = Feed::new(4);
    let reference = wire::txref(17);
    let ingress = wire::encode_ingress_txref(&reference, GuardHeader::EXEMPT);
    let (_, payload) = wire::split_ingress(&ingress).unwrap();
    feed.assert_confirmation(
        &wire::encode_egress_record(123, payload).unwrap(),
        reference.tx_hash,
    );
}

#[test]
fn other_record_types_and_truncated_frames_never_confirm_a_transaction() {
    let feed = Feed::new(5);
    let reference = Feed::reference(wire::RT_TX).encode();
    (0..reference.len()).for_each(|end| {
        feed.rules.on_canonical_frame(&reference[..end]);
        assert!(feed.events.is_empty());
    });
    (0..=u8::MAX)
        .filter(|kind| !matches!(*kind, wire::RT_TX | wire::RT_TXREF | wire::RT_TXREF_OFFER))
        .for_each(|kind| {
            assert!(
                feed.rules
                    .on_canonical_frame(&Feed::reference(kind).encode())
            );
            assert!(feed.events.is_empty());
        });
    assert!(!feed.rules.on_canonical_frame(&[wire::EGRESS_KIND_BOUNDARY]));
}

#[test]
fn a_mismatched_carried_hash_never_confirms_a_transaction() {
    let feed = Feed::new(5);
    let mut payload = Feed::canonical().payload().unwrap();
    payload[0] ^= 1;
    assert!(
        feed.rules
            .on_canonical_frame(&wire::encode_egress_record(123, &payload).unwrap())
    );
    assert!(feed.events.is_empty());
}

#[test]
fn legacy_admission_does_not_receive_canonical_events() {
    let feed = Feed::new(3);
    assert!(
        feed.rules
            .on_canonical_frame(&Feed::reference(wire::RT_TX).encode())
    );
    assert!(feed.events.is_empty());
}
