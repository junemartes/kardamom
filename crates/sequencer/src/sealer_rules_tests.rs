use alloy_primitives::{Address, B256};
use kardamom_cluster_adapter::wire::RefRefused;
use kardamom_types::{DropReason, TxDrop};

use super::{SealerRules, SealerVersion};
use crate::outbound::fakes::InMemorySidePublisher;
use crate::v2::AccountEvent;

struct Rig {
    rules: SealerRules<InMemorySidePublisher>,
    version: SealerVersion,
    drops: InMemorySidePublisher,
}

impl Rig {
    fn new() -> Self {
        Self::from_floor(0)
    }

    fn from_floor(floor: u8) -> Self {
        let version = SealerVersion::starting_at(floor);
        let drops = InMemorySidePublisher::default();
        let rules = SealerRules::new(0, version.clone(), drops.clone());
        Self {
            rules,
            version,
            drops,
        }
    }

    /// Open session `id`, and let the sealer report `version` for it.
    fn session(&mut self, id: i64, version: u8) {
        self.rules.on_session_start(id);
        self.rules.on_sealer_info(id, version);
    }

    fn drops(&self) -> Vec<TxDrop> {
        self.drops.drops.lock().unwrap().clone()
    }
}

fn refused(decision_version: u8) -> RefRefused {
    RefRefused {
        canonical_id: B256::repeat_byte(0xAB),
        sender: Address::repeat_byte(0x13),
        nonce: 7,
        decision_version,
    }
}

#[test]
fn a_report_after_its_session_start_counts() {
    let mut r = Rig::new();
    r.session(7, 3);
    assert_eq!(r.version.get(), 3);
}

#[test]
fn a_report_before_its_session_start_counts() {
    let mut r = Rig::new();
    r.rules.on_sealer_info(7, 3);
    r.rules.on_session_start(7);
    assert_eq!(r.version.get(), 3);
}

#[test]
fn a_new_session_keeps_the_version() {
    let mut r = Rig::new();
    r.session(7, 5);
    r.rules.on_session_start(8);
    assert_eq!(r.version.get(), 5);
    assert!(r.version.carries_bytes(), "a new session keeps the carry");
}

#[test]
fn a_lower_report_of_the_current_session_lowers_the_version() {
    let mut r = Rig::new();
    r.session(7, 5);
    r.session(8, 4);
    assert_eq!(r.version.get(), 4);
    assert!(r.version.orders_by_position());
    assert!(!r.version.carries_bytes());
}

#[test]
fn a_lower_report_of_an_old_session_does_not_lower_the_version() {
    let mut r = Rig::new();
    r.session(7, 4);
    r.session(8, 5);
    r.rules.on_sealer_info(7, 4);
    assert_eq!(r.version.get(), 5);
}

#[test]
fn a_higher_report_of_another_session_raises_the_version() {
    let mut r = Rig::new();
    r.session(7, 4);
    r.rules.on_sealer_info(6, 5);
    assert_eq!(r.version.get(), 5);
}

#[test]
fn an_early_lower_report_lowers_the_version_when_its_session_starts() {
    let mut r = Rig::new();
    r.session(7, 5);
    r.rules.on_sealer_info(8, 4);
    assert_eq!(r.version.get(), 5, "session 8 has not started yet");
    r.rules.on_session_start(8);
    assert_eq!(r.version.get(), 4);
}

#[test]
fn an_early_report_of_another_session_does_not_set_the_version() {
    let mut r = Rig::from_floor(5);
    r.rules.on_sealer_info(6, 3);
    r.rules.on_session_start(7);
    assert_eq!(r.version.get(), 5);
}

#[test]
fn a_new_process_starts_from_the_floor() {
    let mut r = Rig::from_floor(5);
    assert!(r.version.carries_bytes(), "before any session");
    r.rules.on_session_start(7);
    assert!(r.version.carries_bytes(), "before the first report");
    r.rules.on_sealer_info(7, 4);
    assert_eq!(r.version.get(), 4, "the first report lowers a wrong floor");
}

#[test]
fn the_default_floor_is_zero() {
    let r = Rig::new();
    assert_eq!(r.version.get(), 0);
    assert_eq!(SealerVersion::default().get(), 0);
}

#[test]
fn a_refused_reference_raises_the_version_and_drops_the_transaction() {
    let mut r = Rig::new();
    r.session(7, 4);
    r.rules.on_ref_refused(&refused(5));
    assert!(r.version.carries_bytes());
    assert_eq!(
        r.drops(),
        [TxDrop {
            tx_hash: B256::repeat_byte(0xAB),
            sender: Address::repeat_byte(0x13),
            nonce: 7,
            reason: DropReason::SealerRefusedReference,
        }]
    );
}

#[test]
fn a_refused_reference_never_lowers_the_version() {
    let mut r = Rig::new();
    r.session(7, 5);
    r.rules.on_ref_refused(&refused(4));
    assert_eq!(r.version.get(), 5);
    assert_eq!(r.drops().len(), 1);
}

#[test]
fn a_refused_reference_tells_the_loop_once() {
    let (accounts_tx, accounts_rx) = crossbeam_channel::unbounded();
    let mut rules = SealerRules::new(
        0,
        SealerVersion::default(),
        InMemorySidePublisher::default(),
    )
    .telling(accounts_tx);
    rules.on_ref_refused(&refused(5));
    let events: Vec<_> = accounts_rx.try_iter().collect();
    assert!(matches!(
        events[..],
        [AccountEvent::Refused { sender, nonce: 7 }] if sender == Address::repeat_byte(0x13)
    ));
}

/// A contiguity reject goes to the v2 loop with the expected nonce.
#[test]
fn a_contiguity_reject_tells_the_loop() {
    let (accounts_tx, accounts_rx) = crossbeam_channel::unbounded();
    let rules = SealerRules::new(
        0,
        SealerVersion::default(),
        InMemorySidePublisher::default(),
    )
    .telling(accounts_tx);
    rules.on_contiguity_reject(Address::repeat_byte(0x13), 9, 8);
    let events: Vec<_> = accounts_rx.try_iter().collect();
    assert!(matches!(
        events[..],
        [AccountEvent::Rejected { sender, nonce: 9, expected: 8 }]
            if sender == Address::repeat_byte(0x13)
    ));
}
