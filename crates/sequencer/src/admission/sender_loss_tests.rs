use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, U256};

use super::super::{AccountState, Admission, AdmissionConfig, Candidate, ImagePos};

struct Fixture {
    admission: Admission<()>,
    unit: u64,
}

impl Fixture {
    fn recovered_confirmation(&mut self, sender: Address, nonce: u64) {
        let block = nonce.checked_mul(3).unwrap().checked_add(2).unwrap();
        self.state(sender, nonce, block);
        self.offer(sender, nonce);
        self.admission.on_rows(block + 1, [], &mut Vec::new());
        self.state(sender, nonce + 1, block + 2);
        self.admission.resume_loss_window();
    }

    fn new() -> Self {
        Self::with_sender_limit(NonZeroUsize::new(100).unwrap())
    }

    fn with_sender_limit(limit: NonZeroUsize) -> Self {
        Self {
            admission: Admission::new(
                AdmissionConfig {
                    max_parked: NonZeroUsize::new(16).unwrap(),
                    max_forwarded: NonZeroUsize::new(64).unwrap(),
                    loss_blocks: NonZeroU64::new(16).unwrap(),
                    park_width: NonZeroU64::new(1 << 17).unwrap(),
                    park_ttl: Duration::from_secs(60),
                    relook_after: Duration::from_millis(500),
                    max_senders: limit,
                },
                Instant::now(),
            ),
            unit: 0,
        }
    }

    fn state(&mut self, sender: Address, nonce: u64, block: u64) {
        self.admission.on_rows(
            block,
            [(
                sender,
                AccountState {
                    nonce,
                    balance: U256::from(100),
                    delegated: false,
                    tx_idx: block,
                },
            )],
            &mut Vec::new(),
        );
    }

    fn offer(&mut self, sender: Address, nonce: u64) {
        self.unit = self.unit.checked_add(1).unwrap();
        self.admission.offer(
            Candidate {
                sender,
                nonce,
                tx_hash: B256::ZERO,
                cost: U256::ZERO,
                image: ImagePos {
                    lane: 0,
                    session: 1,
                    unit: self.unit,
                },
                ingress: 0,
                answered: false,
                payload: (),
            },
            &mut Vec::new(),
        );
    }

    fn block(&mut self, block: u64) {
        self.admission.on_rows(block, [], &mut Vec::new());
        self.admission.sweep(Instant::now(), &mut Vec::new());
    }
}

#[test]
fn repeated_resumes_keep_only_markers_that_can_still_expire_a_live_run() {
    let mut f = Fixture::new();
    let sender = Address::repeat_byte(1);
    let waiting = Address::repeat_byte(2);
    f.state(waiting, 0, 1);
    f.offer(waiting, 0);
    (0..100).for_each(|nonce| f.recovered_confirmation(sender, nonce));
    assert!(f.admission.unconfirmed_at(waiting, 0).is_some());
    assert_eq!(
        f.admission.forwards.len(),
        1,
        "confirmed runs cannot grow metadata through repeated recovery"
    );
}

#[test]
fn an_evicted_senders_old_marks_cannot_expire_a_recreated_sender() {
    let mut f = Fixture::with_sender_limit(NonZeroUsize::new(1).unwrap());
    let sender = Address::repeat_byte(1);
    let other = Address::repeat_byte(2);
    f.state(sender, 0, 1);
    f.offer(sender, 0);
    f.block(2);
    f.state(sender, 1, 3);
    f.state(other, 0, 3);
    f.offer(other, 0);
    f.block(3);
    assert!(!f.admission.senders.contains_key(&sender));
    f.admission.on_rollback();
    f.state(sender, 0, 4);
    f.offer(sender, 0);
    f.block(5);
    f.block(18);
    assert!(
        f.admission.unconfirmed_at(sender, 0).is_some(),
        "an old tag from the evicted state cannot name the new state"
    );
    assert!(f.admission.unconfirmed_at(other, 0).is_some());
    f.block(19);
    assert!(f.admission.unconfirmed_at(other, 0).is_none());
    assert!(f.admission.unconfirmed_at(sender, 0).is_some());
    f.block(21);
    assert!(f.admission.unconfirmed_at(sender, 0).is_none());
}

#[test]
fn rollover_forgets_only_old_tags_of_its_sender_before_reusing_zero() {
    let mut f = Fixture::new();
    let sender = Address::repeat_byte(1);
    let other = Address::repeat_byte(2);
    f.state(sender, 0, 1);
    f.state(other, 0, 1);
    f.offer(sender, 0);
    f.offer(other, 0);
    f.block(2);
    f.state(sender, 1, 3);
    f.admission.senders.get_mut(&sender).unwrap().generation = u32::MAX;
    f.admission.on_rollback();
    f.state(sender, 0, 4);
    f.offer(sender, 0);
    f.block(5);
    f.state(sender, 1, 6);
    f.admission.on_rollback();
    f.state(sender, 0, 7);
    f.offer(sender, 0);
    f.block(8);
    f.block(18);
    assert!(
        f.admission.unconfirmed_at(sender, 0).is_some(),
        "the oldest tag 1 cannot match the new tag 1 across rollover"
    );
    assert!(
        f.admission.unconfirmed_at(other, 0).is_none(),
        "another sender keeps its original finite expiry"
    );
    f.block(23);
    assert!(f.admission.unconfirmed_at(sender, 0).is_some());
    f.block(24);
    assert!(f.admission.unconfirmed_at(sender, 0).is_none());
}

#[test]
fn later_forwards_of_tag_zero_keep_the_first_mark_of_the_wrapped_run() {
    let mut f = Fixture::new();
    let sender = Address::repeat_byte(1);
    f.state(sender, 0, 1);
    f.offer(sender, 0);
    f.block(2);
    f.state(sender, 1, 3);
    f.admission.senders.get_mut(&sender).unwrap().generation = u32::MAX;
    f.admission.on_rollback();
    f.state(sender, 0, 4);
    f.offer(sender, 0);
    f.block(5);
    f.offer(sender, 1);
    f.block(6);
    f.block(21);
    assert!(f.admission.unconfirmed_at(sender, 0).is_none());
    assert!(f.admission.unconfirmed_at(sender, 1).is_none());
}
