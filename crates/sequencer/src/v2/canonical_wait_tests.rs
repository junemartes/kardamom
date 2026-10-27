use super::*;

impl Bench {
    fn advance_without_sweep(&mut self, block: u64) {
        self.accounts
            .send(AccountEvent::Rows {
                block,
                rows: Vec::new(),
            })
            .unwrap();
        self.run();
    }
}

#[test]
fn unchanged_blocked_queue_does_not_rescan_or_publish() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.l.cfg.sweep_every = Duration::from_secs(3600);
    (0..32).for_each(|nonce| b.send(nonce));
    let scans = b.l.canonical.scans;
    (0..100).for_each(|_| {
        assert!(!b.l.run_once(&mut b.rig.ports()).unwrap());
        assert_eq!(b.l.canonical.scans, scans);
    });
    assert_eq!(b.offers(), [(0, true)]);
    assert_eq!(b.l.outbox.len(), 31);
    (0..32).for_each(|nonce| b.canonical(nonce));
    assert!(b.l.outbox.is_empty());
    assert_eq!(b.offers().len(), 32);
    assert!(b.rig.errors().is_empty());
    assert!(b.rig.drops().is_empty());
}

#[test]
fn accepted_lookup_releases_an_idle_sender_without_new_intake() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.l.cfg.sweep_every = Duration::from_secs(3600);
    b.send(0);
    b.send(1);
    b.accounts
        .send(AccountEvent::Answer {
            address: b.sender(),
            state: AccountState {
                nonce: 1,
                balance: U256::MAX,
                delegated: false,
                tx_idx: 2,
            },
        })
        .unwrap();
    b.run();
    assert_eq!(b.offers(), [(0, true), (1, true)]);
    assert!(b.l.outbox.is_empty());
}

#[test]
fn sweep_removes_lost_suffixes_from_an_idle_outbox() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.l.cfg.sweep_every = Duration::from_secs(3600);
    b.send(0);
    b.send(1);
    for block in [2, 100] {
        b.advance_without_sweep(block);
    }
    assert_eq!(b.l.outbox.len(), 1);
    b.sweep();
    assert!(b.l.outbox.is_empty());
    assert!(b.l.admission.unconfirmed_at(b.sender(), 0).is_none());
    assert!(b.l.admission.unconfirmed_at(b.sender(), 1).is_none());
    assert_eq!(b.offers(), [(0, true)]);
}
