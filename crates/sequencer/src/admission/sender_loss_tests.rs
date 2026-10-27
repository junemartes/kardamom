use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, U256};

use super::super::{AccountState, Admission, AdmissionConfig, Candidate, ImagePos};

struct Fixture {
    admission: Admission<()>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            admission: Admission::new(
                AdmissionConfig {
                    max_parked: NonZeroUsize::new(16).unwrap(),
                    max_forwarded: NonZeroUsize::new(64).unwrap(),
                    loss_blocks: NonZeroU64::new(16).unwrap(),
                    park_width: NonZeroU64::new(1 << 17).unwrap(),
                    park_ttl: Duration::from_secs(60),
                    relook_after: Duration::from_millis(500),
                    max_senders: NonZeroUsize::new(100).unwrap(),
                },
                Instant::now(),
            ),
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
        self.admission.offer(
            Candidate {
                sender,
                nonce,
                tx_hash: B256::ZERO,
                cost: U256::ZERO,
                image: ImagePos {
                    lane: 0,
                    session: 1,
                    unit: nonce + 1,
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
    f.state(sender,0,4);
    f.offer(sender,0);
    f.block(5);
    f.state(sender,1,6);
    f.admission.on_rollback();
    f.state(sender,0,7);
    f.offer(sender,0);
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
