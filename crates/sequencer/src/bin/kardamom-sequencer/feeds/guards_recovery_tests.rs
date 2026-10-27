use alloy_primitives::{Address, U256};
use kardamom_sequencer::admission::AccountState;
use kardamom_sequencer::testkit::{Rig, one_partition_cfg, pos, signed_envelope, signer};
use kardamom_sequencer::v2::{AccountEvent, AdmissionLoop, AdmissionSettings};
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
    fn new() -> Self {
        let fixture = Fixture::new();
        let (accounts, rx) = crossbeam_channel::unbounded();
        let (lookups, _) = tokio::sync::mpsc::channel(16);
        let cfg = AdmissionSettings::default().loop_config(Duration::from_secs(4), 0);
        let mut core =
            AdmissionLoop::new(cfg, one_partition_cfg().vslot_set().unwrap(), rx, lookups);
        core.enable_pause(fixture.life.subscribe());
        core.enable_positioned_refs(SealerVersion::starting_at(5));
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
        self.rig.push(
            TxDataLoc::new(7, pos(i32::try_from((nonce + 1) * 64).unwrap())),
            signed_envelope(&signer(11), nonce, 1),
        );
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

#[test]
fn real_status_and_boundary_frames_hold_both_twins_and_retry_without_new_input() {
    let mut twins = [Twin::new(), Twin::new()];
    twins.iter_mut().for_each(|twin| {
        twin.status(true);
        twin.fixture.boundary();
        twin.core.run_once(&mut twin.rig.ports()).unwrap();
        assert_eq!(twin.nonces(), [9, 10, 11]);
    });
    std::thread::sleep(Duration::from_millis(60));
    twins.iter_mut().for_each(|twin| {
        twin.rows(100, None);
        twin.fixture.boundary();
        twin.turn();
        assert_eq!(twin.nonces(), [9, 10, 11]);
        twin.status(false);
        twin.turn();
        assert_eq!(twin.nonces(), [9, 10, 11, 10, 11]);
        assert!(twin.rig.errors().is_empty());
    });
}

#[test]
fn a_status_pulse_between_turns_replays_after_rows_confirm_its_prefix() {
    let mut t = Twin::new();
    t.status(true);
    t.fixture.boundary();
    t.fixture.status(&ClusterStatus::default());
    t.rows(3, Some(11));
    assert_eq!(t.nonces(), [9, 10, 11, 11]);
    t.turn();
    assert_eq!(t.nonces(), [9, 10, 11, 11]);
}
