use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256};
use kardamom_types::{DropReason, TxDrop};

use super::{Dropped, Drops};

fn drop_of(hash: u8, reason: DropReason) -> TxDrop {
    TxDrop {
        tx_hash: B256::with_last_byte(hash),
        sender: Address::repeat_byte(0xA1),
        nonce: u64::from(hash),
        reason,
    }
}

fn drops(capacity: usize) -> Drops {
    Drops::new(NonZeroUsize::new(capacity).unwrap())
}

#[test]
fn the_report_of_the_other_twin_keeps_the_first_status() {
    let mut d = drops(4);
    d.take(&drop_of(1, DropReason::NonceTooLow));
    d.take(&drop_of(1, DropReason::TxpoolFull));
    assert_eq!(
        d.dropped(&B256::with_last_byte(1)),
        Some(Dropped {
            sender: Address::repeat_byte(0xA1),
            nonce: 1,
            reason: DropReason::NonceTooLow,
        })
    );
    assert_eq!(d.order.len(), 1, "one status for the two reports");
}

#[test]
fn the_map_evicts_the_oldest_drop() {
    let mut d = drops(2);
    (1..=3).for_each(|hash| d.take(&drop_of(hash, DropReason::Expired)));
    let kept: Vec<bool> = (1..=3)
        .map(|hash| d.dropped(&B256::with_last_byte(hash)).is_some())
        .collect();
    assert_eq!(kept, [false, true, true]);
}

#[test]
fn a_forgotten_hash_dropped_again_outlives_its_old_entry() {
    let mut d = drops(2);
    d.take(&drop_of(1, DropReason::Expired));
    d.forget(&B256::with_last_byte(1));
    d.take(&drop_of(1, DropReason::NonceTooLow));
    d.take(&drop_of(2, DropReason::Expired));
    assert_eq!(
        d.dropped(&B256::with_last_byte(1)).map(|x| x.reason),
        Some(DropReason::NonceTooLow),
        "the stale entry of hash 1 goes, and its new drop stays"
    );
}
