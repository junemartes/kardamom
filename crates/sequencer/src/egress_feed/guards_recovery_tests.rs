use crate::admission::AccountState;
use crate::testkit::{EnvelopeSpec, Rig, envelope_with, one_partition_cfg, pos, signer};
use crate::v2::{AccountEvent, AdmissionLoop, AdmissionSettings};
use alloy_primitives::{Address, U256};
use kardamom_types::TxDataLoc;

use super::*;

struct Twin {
    fixture: Fixture,
    core: AdmissionLoop,
    accounts: crossbeam_channel::Sender<AccountEvent>,
    rig: Rig,
    sender: Address,
}

impl Twin {
    fn hold_for_pause(&mut self) {
        self.status(true);
        self.fixture.boundary();
        self.turn();
        assert_eq!(self.nonces(), [9, 10]);
    }

    fn recover_after_empty_block(&mut self) {
        self.rows(100, None);
        self.fixture.boundary();
        self.turn();
        assert_eq!(self.nonces(), [9, 10]);
        self.status(false);
        self.turn();
        assert_eq!(self.nonces(), [9, 10, 10]);
        self.canonical(10);
        assert_eq!(self.nonces(), [9, 10, 10, 11]);
        self.canonical(11);
        assert!(self.rig.errors().is_empty());
    }

    fn new() -> Self {
        Self::at_version(5)
    }

    fn at_version(version: u8) -> Self {
        let mut fixture = Fixture::new();
        let (accounts, rx) = crossbeam_channel::unbounded();
        let shared = SealerVersion::starting_at(version);
        fixture.feed.rules = SealerRules::new(0, shared.clone(), InMemorySidePublisher::default())
            .telling(accounts.clone());
        let (lookups, _) = tokio::sync::mpsc::channel(16);
        let cfg = AdmissionSettings::default().loop_config(Duration::from_secs(4), 0);
        let mut core =
            AdmissionLoop::new(cfg, one_partition_cfg().vslot_set().unwrap(), rx, lookups);
        core.enable_pause(fixture.life.subscribe());
        core.enable_positioned_refs(shared);
        let mut twin = Self {
            fixture,
            core,
            accounts,
            rig: Rig::default(),
            sender: signer(11).address(),
        };
        twin.rows(1, Some(9));
        twin.send(9);
        twin.rows(2, Some(10));
        twin.send(10);
        twin.send(11);
        twin
    }

    fn turn(&mut self) {
        self.core.run_once(&mut self.rig.ports()).unwrap();
    }

    fn rows(&mut self, block: u64, nonce: Option<u64>) {
        self.accounts
            .send(AccountEvent::Rows {
                block,
                rows: nonce
                    .map(|nonce| {
                        (
                            self.sender,
                            AccountState {
                                nonce,
                                balance: U256::from(10u64).pow(U256::from(24u64)),
                                delegated: false,
                                tx_idx: block,
                            },
                        )
                    })
                    .into_iter()
                    .collect(),
            })
            .unwrap();
        self.turn();
    }

    fn send(&mut self, nonce: u64) {
        self.copy(nonce, 1000, i32::try_from((nonce + 1) * 64).unwrap());
    }

    fn copy(&mut self, nonce: u64, deadline: u64, offset: i32) {
        let mut envelope = envelope_with(
            &signer(11),
            nonce,
            1,
            EnvelopeSpec {
                real_hash: true,
                ..EnvelopeSpec::default()
            },
        );
        envelope.max_inclusion_block = deadline;
        self.rig.push(TxDataLoc::new(7, pos(offset)), envelope);
        self.turn();
    }

    fn canonical(&mut self, nonce: u64) {
        use kardamom_cluster_adapter::wire::{RT_TX, RelayedRef};
        let hash = self
            .rig
            .offers()
            .iter()
            .rev()
            .find(|offer| offer.guard.nonce == nonce)
            .unwrap()
            .tx_ref
            .tx_hash;
        let frame = RelayedRef {
            index: nonce,
            canonical_id: hash,
            record_type: RT_TX,
        }
        .encode();
        self.fixture.feed.on_frame(&frame, &self.fixture.life);
        self.turn();
    }

    fn nonces(&self) -> Vec<u64> {
        self.rig
            .offers()
            .iter()
            .map(|offer| offer.guard.nonce)
            .collect()
    }

    fn status(&mut self, halted: bool) {
        self.fixture.status(&ClusterStatus {
            halted,
            ..ClusterStatus::default()
        });
    }
}

impl Twin {
    fn assert_both_guard_retries(record_lag: bool) {
        let mut twins = [Twin::new(), Twin::new()];
        for twin in &mut twins {
            twin.assert_guard_retry(record_lag);
        }
    }

    fn assert_guard_retry(&mut self, record_lag: bool) {
        use kardamom_cluster_adapter::wire::{encode_da_lag_reject, encode_record_lag_reject};
        let frame = if record_lag {
            encode_record_lag_reject(
                self.sender,
                10,
                100,
                &RecordLagStatus {
                    halted: true,
                    best_recorded: Some(30),
                    budget: 16,
                },
            )
        } else {
            encode_da_lag_reject(
                self.sender,
                10,
                &ClusterStatus {
                    halted: true,
                    sealed_head: 30,
                    posted_head: 10,
                    budget_blocks: 8,
                    ..ClusterStatus::default()
                },
            )
        };
        self.fixture.feed.on_frame(&frame, &self.fixture.life);
        self.fixture.boundary();
        self.turn();
        assert_eq!(self.nonces(), [9, 10]);
        assert!(self.fixture.root().is_some());
        self.status(false);
        self.turn();
        assert_eq!(self.nonces(), [9, 10, 10]);
        self.canonical(10);
        assert_eq!(self.nonces(), [9, 10, 10, 11]);
        self.canonical(11);
        assert!(self.rig.errors().is_empty());
        assert!(self.rig.drops().is_empty());
    }
}

#[test]
fn a_guard_refusal_before_its_status_pauses_and_retries_both_twins_in_order() {
    [false, true]
        .into_iter()
        .for_each(Twin::assert_both_guard_retries);
}

#[test]
fn a_delayed_refusal_after_clear_keeps_rows_and_the_confirmed_prefix() {
    use kardamom_cluster_adapter::wire::encode_da_lag_reject;
    let mut twin = Twin::new();
    twin.status(true);
    twin.turn();
    twin.status(false);
    twin.turn();
    twin.rows(3, Some(11));
    let frame = encode_da_lag_reject(
        twin.sender,
        10,
        &ClusterStatus {
            halted: true,
            sealed_head: 30,
            posted_head: 10,
            budget_blocks: 8,
            ..ClusterStatus::default()
        },
    );
    twin.fixture.feed.on_frame(&frame, &twin.fixture.life);
    twin.turn();
    twin.status(false);
    twin.turn();
    assert_eq!(twin.nonces(), [9, 10, 10, 11, 11]);
    twin.canonical(11);
    assert!(twin.rig.errors().is_empty());
    assert!(twin.rig.drops().is_empty());
}

#[test]
fn a_real_past_deadline_refusal_keeps_the_suffix_and_ignores_the_old_copy() {
    use kardamom_cluster_adapter::wire::encode_past_deadline;
    let mut twin = Twin::at_version(3);
    twin.send(12);
    let frame = encode_past_deadline(twin.sender, 10, 1000, 1001);
    twin.fixture.feed.on_frame(&frame, &twin.fixture.life);
    twin.turn();
    assert_eq!(twin.nonces(), [9, 10, 11, 12]);
    assert_eq!(twin.rig.errors().len(), 1);
    assert_eq!(twin.rig.errors()[0].nonce, 10);
    assert_eq!(twin.rig.drops().len(), 1);
    assert_eq!(twin.fixture.root(), None);
    twin.copy(10, 2000, 1024);
    assert_eq!(twin.nonces(), [9, 10, 11, 12, 10, 11, 12]);
    twin.fixture.feed.on_frame(&frame, &twin.fixture.life);
    twin.turn();
    assert_eq!(twin.rig.errors().len(), 1);
    assert_eq!(twin.rig.drops().len(), 1);
    twin.send(13);
    assert_eq!(twin.nonces(), [9, 10, 11, 12, 10, 11, 12, 13]);
}

#[test]
fn a_real_window_full_answer_retries_without_freeing_a_nonce() {
    use kardamom_cluster_adapter::wire::encode_window_full;
    let mut twin = Twin::at_version(3);
    let frame = encode_window_full(twin.sender, 10);
    twin.fixture.feed.on_frame(&frame, &twin.fixture.life);
    twin.turn();
    assert_eq!(twin.nonces(), [9, 10, 11, 10, 11]);
    assert!(twin.rig.errors().is_empty());
    assert!(twin.rig.drops().is_empty());
}

#[test]
fn real_status_and_boundary_frames_hold_both_twins_and_retry_without_new_input() {
    let mut twins = [Twin::new(), Twin::new()];
    twins.iter_mut().for_each(Twin::hold_for_pause);
    std::thread::sleep(Duration::from_millis(60));
    twins.iter_mut().for_each(Twin::recover_after_empty_block);
}

#[test]
fn a_status_pulse_between_turns_replays_after_rows_confirm_its_prefix() {
    let mut t = Twin::new();
    t.status(true);
    t.fixture.boundary();
    t.fixture.status(&ClusterStatus::default());
    t.rows(3, Some(11));
    assert_eq!(t.nonces(), [9, 10, 11]);
    t.turn();
    assert_eq!(t.nonces(), [9, 10, 11]);
}
