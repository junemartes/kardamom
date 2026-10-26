//! The offers that the v2 loop sends again, and the format of each offer.
//! These tests reproduce the stranded sender of a version switch and of a
//! leader change: the sealer must never get a later nonce of a sender in
//! place of a lost one.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, U256};
use kardamom_cluster_adapter::wire::POSITION_TAG;
use kardamom_types::TxDataLoc;

use super::{AccountEvent, AdmissionLoop, V2Config};
use crate::admission::{AccountState, AdmissionConfig};
use crate::outbound::fakes::InMemorySidePublisher;
use crate::sealer_rules::{SealerRules, SealerVersion};
use crate::testkit::{Rig, one_partition_cfg, pos, signed_envelope, signer};

#[path = "pause_tests.rs"]
mod pause_tests;

/// A loop that keeps the version of `shared`, with room in its outbox for
/// every offer of a test, and a sender with the chain nonce 0.
struct Bench {
    l: AdmissionLoop,
    accounts: crossbeam_channel::Sender<AccountEvent>,
    rig: Rig,
    signer: alloy_signer_local::PrivateKeySigner,
}

impl Bench {
    fn new(shared: SealerVersion) -> Self {
        let cfg = V2Config {
            partition: 0,
            admission: AdmissionConfig {
                max_parked: NonZeroUsize::new(16).unwrap(),
                max_forwarded: NonZeroUsize::new(64).unwrap(),
                loss_blocks: NonZeroU64::new(16).unwrap(),
                park_width: NonZeroU64::new(1 << 17).unwrap(),
                park_ttl: Duration::from_secs(4),
                relook_after: Duration::from_millis(500),
                max_senders: NonZeroUsize::new(1 << 16).unwrap(),
            },
            sweep_every: Duration::from_millis(50),
            outbox_cap: NonZeroUsize::new(64).unwrap(),
        };
        let (accounts, accounts_rx) = crossbeam_channel::unbounded();
        let (lookups_tx, _) = tokio::sync::mpsc::channel(16);
        let vslots = one_partition_cfg().vslot_set().unwrap();
        let mut l = AdmissionLoop::new(cfg, vslots, accounts_rx, lookups_tx);
        l.enable_positioned_refs(shared);
        let signer = signer(11);
        let state = AccountState {
            nonce: 0,
            balance: U256::from(10u64).pow(U256::from(24u64)),
            delegated: false,
            tx_idx: 1,
        };
        accounts
            .send(AccountEvent::Rows {
                block: 1,
                rows: vec![(signer.address(), state)],
            })
            .unwrap();
        Self {
            l,
            accounts,
            rig: Rig::default(),
            signer,
        }
    }

    fn sender(&self) -> Address {
        self.signer.address()
    }

    /// Send the transaction at `nonce`, at its own place in the image, and
    /// run the loop until it is idle.
    fn send(&mut self, nonce: u64) {
        self.queue(nonce);
        self.run();
    }

    fn queue(&mut self, nonce: u64) {
        let at = pos(64 * (i32::try_from(nonce).unwrap() + 1));
        self.rig.push(
            TxDataLoc::new(7, at),
            signed_envelope(&self.signer, nonce, 1),
        );
    }

    fn run(&mut self) {
        while self.l.run_once(&mut self.rig.ports()).unwrap() {}
    }

    /// The nonce of each offer so far, and whether it carries the bytes.
    fn offers(&self) -> Vec<(u64, bool)> {
        self.rig
            .offers()
            .iter()
            .map(|o| (o.guard.nonce, o.envelope.is_some()))
            .collect()
    }
}

/// The sealer switches from decision version 5 to 4 while nonce 1 waits
/// for nonce 0. Both go out in the format of version 4: no bytes, and the
/// tagged position. A full reference at nonce 1 after a reference at
/// nonce 0 meets the contiguity guard of the sealer, which never saw nonce
/// 0, and the sealer rejects it.
#[test]
fn a_parked_transaction_goes_out_in_the_format_of_its_release() {
    let shared = SealerVersion::default();
    let mut rules = SealerRules::new(0, shared.clone(), InMemorySidePublisher::default());
    rules.on_session_start(1);
    rules.on_sealer_info(1, 5);
    let mut b = Bench::new(shared);
    b.send(1);
    assert_eq!(b.offers(), [], "nonce 1 waits for nonce 0");

    rules.on_sealer_info(1, 4);
    b.send(0);
    assert_eq!(b.offers(), [(0, false), (1, false)]);
    let deadlines: Vec<u64> = b
        .rig
        .offers()
        .iter()
        .map(|o| o.guard.max_inclusion_block & POSITION_TAG)
        .collect();
    assert_eq!(deadlines, [POSITION_TAG, POSITION_TAG]);
}

/// The leader of the sealer dies after it took the offers of nonces 0 and
/// 1, and before it ordered them. The publisher reports the new session
/// at the next offer. The loop offers 0 and 1 again before nonce 2, so
/// the sealer never orders 2 without them. The sealer absorbs a copy of
/// an ordered offer.
#[test]
fn a_new_session_offers_the_unconfirmed_run_before_a_new_offer() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.send(0);
    b.send(1);
    assert_eq!(b.offers(), [(0, true), (1, true)]);

    *b.rig.refs.new_session.lock().unwrap() = true;
    b.send(2);
    assert_eq!(
        b.offers(),
        [(0, true), (1, true), (0, true), (1, true), (2, true)],
        "the run goes out again, before nonce 2"
    );
}

/// A row confirms nonce 0. The new session then gets only the unconfirmed
/// part of the run again.
#[test]
fn a_new_session_does_not_offer_a_confirmed_transaction_again() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.send(0);
    b.send(1);
    let confirmed = AccountState {
        nonce: 1,
        balance: U256::from(10u64).pow(U256::from(24u64)),
        delegated: false,
        tx_idx: 2,
    };
    b.accounts
        .send(AccountEvent::Rows {
            block: 2,
            rows: vec![(b.sender(), confirmed)],
        })
        .unwrap();
    *b.rig.refs.new_session.lock().unwrap() = true;
    b.send(2);
    let nonces: Vec<u64> = b.offers().iter().map(|(n, _)| *n).collect();
    assert_eq!(nonces, [0, 1, 1, 2]);
}

/// The contiguity guard of the sealer expects nonce 1 and rejects nonce 2.
/// The loop offers 1 and 2 again, in that order.
#[test]
fn a_contiguity_reject_offers_the_expected_and_the_rejected_nonce_again() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    (0..3).for_each(|n| b.send(n));
    b.accounts
        .send(AccountEvent::Rejected {
            sender: b.sender(),
            nonce: 2,
            expected: 1,
        })
        .unwrap();
    b.run();
    let nonces: Vec<u64> = b.offers().iter().map(|(n, _)| *n).collect();
    assert_eq!(nonces, [0, 1, 2, 1, 2]);
}

/// A guard that expects a nonce that the filter does not hold gets no
/// offer: an offer that it rejects again would repeat without end.
#[test]
fn a_contiguity_reject_for_a_nonce_out_of_the_run_offers_nothing() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    (0..2).for_each(|n| b.send(n));
    b.accounts
        .send(AccountEvent::Rejected {
            sender: b.sender(),
            nonce: 1,
            expected: 7,
        })
        .unwrap();
    b.run();
    assert_eq!(b.offers().len(), 2);
}
