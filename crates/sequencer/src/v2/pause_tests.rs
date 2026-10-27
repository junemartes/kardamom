use kardamom_obs::halt::{HaltCause, HaltRef};
use kardamom_obs::lifecycle::Lifecycle;

use super::*;

impl Bench {
    fn with_lifecycle() -> (Self, Lifecycle) {
        let life = Lifecycle::new(Some("sequencer"));
        let mut bench = Self::new(SealerVersion::starting_at(5));
        bench.l.enable_pause(life.subscribe());
        bench.run();
        (bench, life)
    }
}

#[test]
fn a_paused_v2_loop_leaves_the_input_in_its_archive_until_clear() {
    let (mut bench, life) = Bench::with_lifecycle();
    life.follow(Some(HaltRef::sealer(HaltCause::SealerNoQuorum)));
    bench.queue(0);
    assert!(!bench.l.run_once(&mut bench.rig.ports()).unwrap());
    assert_eq!(bench.rig.tx_data.queue.len(), 1);
    assert!(bench.offers().is_empty());

    life.follow(None);
    bench.run();
    assert_eq!(bench.offers(), [(0, true)]);
}

#[test]
fn an_offer_waiting_on_backpressure_cannot_escape_a_pause() {
    let (mut bench, life) = Bench::with_lifecycle();
    *bench.rig.refs.fail_with_backpressure.lock().unwrap() = true;
    bench.queue(0);
    assert!(matches!(
        bench.l.run_once(&mut bench.rig.ports()),
        Err(crate::error::SequencerError::Backpressure)
    ));
    life.follow(Some(HaltRef::sealer(HaltCause::DaLag)));
    *bench.rig.refs.fail_with_backpressure.lock().unwrap() = false;
    bench.queue(1);
    bench.l.run_once(&mut bench.rig.ports()).unwrap();
    assert!(bench.offers().is_empty());
    assert_eq!(bench.rig.tx_data.queue.len(), 1);

    life.follow(None);
    bench.run();
    assert_eq!(bench.offers(), [(0, true), (1, true)]);
}

#[test]
fn account_rows_keep_draining_while_ready_offers_wait_for_clear() {
    let (mut bench, life) = Bench::with_lifecycle();
    bench.send(1);
    assert!(bench.offers().is_empty());
    life.follow(Some(HaltRef::sealer(HaltCause::DaLag)));
    bench
        .accounts
        .send(AccountEvent::Rows {
            block: 2,
            rows: vec![(
                bench.sender(),
                AccountState {
                    nonce: 1,
                    balance: U256::from(10u64).pow(U256::from(24u64)),
                    delegated: false,
                    tx_idx: 2,
                },
            )],
        })
        .unwrap();
    bench.l.run_once(&mut bench.rig.ports()).unwrap();
    assert!(
        bench.l.accounts.is_empty(),
        "account feeds must not fill behind a pause"
    );
    assert!(bench.offers().is_empty());

    life.follow(None);
    bench.run();
    assert_eq!(bench.offers(), [(1, true)]);
}

#[test]
fn a_session_retry_waits_for_resume_and_excludes_confirmed_offers() {
    let (mut bench, life) = Bench::with_lifecycle();
    let mut rules = SealerRules::new(
        0,
        bench.l.sealer_version.clone(),
        InMemorySidePublisher::default(),
    );
    rules.on_session_start(1);
    bench.sweep();
    bench.send(0);
    bench.send(1);
    life.follow(Some(HaltRef::sealer(HaltCause::SealerNoQuorum)));
    bench.confirm_first();
    rules.on_session_start(2);
    *bench.rig.refs.new_session.lock().unwrap() = true;
    bench.sweep();
    assert_eq!(bench.offers(), [(0, true), (1, true)]);
    assert!(bench.l.accounts.is_empty());
    life.follow(None);
    bench.run();
    bench.run();
    assert_eq!(bench.offers(), [(0, true), (1, true), (1, true)]);
}
