//! Four reader threads each hold a snapshot at a different block. Each
//! continuously reads its frozen view, while the writer commits more
//! blocks concurrently. This test asserts that each reader sees only
//! its own view, and that no panics or page-reuse-during-read occur.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use alloy_primitives::{Address, U256, address};
use kardamom_state::StateSnapshot;
use kardamom_types::StateDatabase;

/// One reader thread's frozen view: its snapshot, the address it reads,
/// its index (for the drift assertion message), and the values it
/// expects never to drift from.
struct Reader<'a> {
    snap: &'a StateSnapshot,
    addr: Address,
    index: usize,
    expected_balance: U256,
    expected_slot: U256,
}

impl Reader<'_> {
    /// Read this reader's frozen view in a tight loop until `stop` is
    /// set, asserting it never drifts from `expected_balance`/
    /// `expected_slot`.
    fn run(&self, stop: &AtomicBool) {
        while !stop.load(Ordering::Relaxed) {
            let (_, bal, _) = self.snap.basic(self.addr).unwrap().unwrap();
            assert_eq!(
                bal, self.expected_balance,
                "reader {} saw drift",
                self.index
            );
            let slot = self.snap.storage(self.addr, common::slot_key(7)).unwrap();
            assert_eq!(slot, self.expected_slot, "reader {} saw drift", self.index);
        }
    }
}

#[test]
fn four_readers_with_distinct_snapshots() {
    let (_dir, mut writer) = common::open_tmp_writer();
    let addr = address!("0x00000000000000000000000000000000000000aa");

    // Drop the genesis snapshot.
    let _ = writer.snapshot_rx.recv();

    // Preload 4 blocks, and capture a snapshot after each one.
    let snapshots: Vec<_> = (1..=4u64)
        .map(|block| common::commit_block(&writer, block, addr, 1000 + block, 7, block * 100))
        .collect();

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for (i, snap) in snapshots.into_iter().enumerate() {
        let expected_balance = U256::from(1001 + i as u64);
        let expected_slot = U256::from(((i + 1) as u64) * 100);
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            Reader {
                snap: &snap,
                addr,
                index: i,
                expected_balance,
                expected_slot,
            }
            .run(&stop);
        }));
    }

    // Concurrently apply blocks 5..=12.
    common::commit_range(&writer, 5..=12, addr);

    // Let readers race for a bit longer.
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.shutdown().unwrap();
}
