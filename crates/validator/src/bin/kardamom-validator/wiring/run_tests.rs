use alloy_primitives::B256;
use clap::Parser;
use kardamom_engine::EpochObserver;
use kardamom_types::EpochRecord;
use kardamom_types::epoch::derive_epoch;

use super::*;

fn epoch(l1_number: u64) -> EpochRecord {
    derive_epoch(l1_number, B256::repeat_byte(0x5A), &[]).unwrap()
}

/// A plain test with no runtime: the content check reads the current
/// runtime, so a wired content check would panic here.
#[test]
fn without_l1_flags_the_origin_sequence_rules_still_run() {
    let args = Args::try_parse_from(["kardamom-validator", "--config", "validator.toml"]).unwrap();
    let divergence = Divergence::new();
    let mut observer = args.epoch_observer(divergence.clone());

    observer.observe(&epoch(100)).unwrap();
    assert!(observer.observe(&epoch(102)).is_err());
    assert!(
        divergence
            .reason()
            .is_some_and(|r| r.contains("l1_origin skipped 1 block(s): 100 -> 102")),
        "{:?}",
        divergence.reason()
    );
}
