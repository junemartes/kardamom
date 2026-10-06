use alloy_primitives::{Address, B256, Bytes};
use kardamom_da_watcher::source::fakes::MockL1Source;
use kardamom_engine::CanonicalEnd;
use kardamom_types::epoch::{DepositLog, LockboxLog};

use super::*;

const LOCKBOX: Address = Address::repeat_byte(0x10);

/// A block with no item, and the L1 origin `origin` when its payload
/// carries one.
fn block(block_number: u64, origin: Option<u64>) -> ReplayBlock {
    ReplayBlock {
        block_number,
        l2_timestamp: 1_700_000_000 + block_number,
        canonical_end: origin.map(|l1_origin| CanonicalEnd {
            end_tx_idx: 0,
            l1_origin,
        }),
        l1_epochs: Vec::new(),
        remote_epochs: Vec::new(),
        txs: Vec::new(),
    }
}

/// One deposit log of L1 block `number`, under the block hash `hash`.
fn deposit_log(number: u64, hash: B256) -> LockboxLog {
    LockboxLog::Deposit(DepositLog {
        block_number: number,
        block_hash: hash,
        log_index: 0,
        from: Address::repeat_byte(0xD0),
        to: Address::repeat_byte(0xD1),
        mint: 1_000,
        gas_limit: 100_000,
        data: Bytes::new(),
    })
}

/// The L1 numbers of the epochs each block leads with.
fn epoch_numbers(blocks: &[ReplayBlock]) -> Vec<Vec<u64>> {
    blocks
        .iter()
        .map(|b| b.l1_epochs.iter().map(|e| e.l1_number).collect())
        .collect()
}

/// The first step from origin 0 takes its own epoch only: the chain holds
/// no epoch before the producer's first one. Every later step takes each
/// epoch it moved over, in order. A block whose origin did not move, or
/// whose payload carries none, leads with no epoch.
#[tokio::test]
async fn each_origin_step_leads_its_block_with_the_epochs_it_moved_over() {
    let l1 = MockL1Source::new();
    l1.push_logs(Ok(vec![deposit_log(40, MockL1Source::filler_hash(40))]));
    l1.push_logs(Ok(Vec::new()));
    l1.push_logs(Ok(Vec::new()));
    let blocks = vec![
        block(1, Some(0)),
        block(2, Some(40)),
        block(3, Some(40)),
        block(4, Some(42)),
        block(5, None),
    ];

    let blocks = L1Epochs::new(l1, LOCKBOX).attach(blocks).await.unwrap();

    assert_eq!(
        epoch_numbers(&blocks),
        [vec![], vec![40], vec![], vec![41, 42], vec![]]
    );
    let deposits: Vec<usize> = blocks[1..4]
        .iter()
        .flat_map(|b| b.l1_epochs.iter().map(|e| e.deposits.len()))
        .collect();
    assert_eq!(deposits, [1, 0, 0]);
}

#[tokio::test]
async fn an_origin_below_the_one_before_is_refused() {
    let l1 = MockL1Source::new();
    let blocks = vec![block(1, Some(41)), block(2, Some(40))];
    let err = L1Epochs::new(l1, LOCKBOX)
        .attach(blocks)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("block 2: L1 origin 40 is below the origin 41"),
        "{err}"
    );
}

/// A log under another block hash than the L1 block's own does not
/// belong to the epoch, and the rebuild stops.
#[tokio::test]
async fn a_log_of_another_l1_block_is_refused() {
    let l1 = MockL1Source::new();
    l1.push_logs(Ok(vec![deposit_log(40, B256::repeat_byte(0xEE))]));
    let err = L1Epochs::new(l1, LOCKBOX)
        .attach(vec![block(1, Some(40))])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("derive the epoch of L1 block 40"), "{err}");
}
