//! Four reader threads each hold a snapshot at a different block; each
//! continuously reads its frozen view; the writer commits more blocks
//! concurrently. Assert each reader sees only its own view and that no
//! panics or page-reuse-during-read occurs.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use alloy_primitives::{U256, address};
use types::StateDatabase;

#[test]
fn four_readers_with_distinct_snapshots() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("0x00000000000000000000000000000000000000aa");

    // Drop the genesis snapshot.
    let _ = writer.snapshot_rx.recv();

    // Pre-load 4 blocks; capture a snapshot after each.
    let mut snapshots = Vec::new();
    for block in 1..=4u64 {
        writer
            .delta_tx
            .send(common::simple_delta(
                block,
                addr,
                1000 + block,
                7,
                block * 100,
            ))
            .unwrap();
        snapshots.push(writer.snapshot_rx.recv().unwrap());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for (i, snap) in snapshots.into_iter().enumerate() {
        let expected_balance = U256::from(1001 + i as u64);
        let expected_slot = U256::from(((i + 1) as u64) * 100);
        let stop = stop.clone();
        let handle = thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (_, bal, _) = snap.basic(addr).unwrap().unwrap();
                assert_eq!(bal, expected_balance, "reader {i} saw drift");
                let slot = snap.storage(addr, common::slot_key(7)).unwrap();
                assert_eq!(slot, expected_slot, "reader {i} saw drift");
            }
        });
        handles.push(handle);
    }

    // Concurrently apply blocks 5..=12.
    for block in 5..=12u64 {
        writer
            .delta_tx
            .send(common::simple_delta(
                block,
                addr,
                1000 + block,
                7,
                block * 100,
            ))
            .unwrap();
        writer.snapshot_rx.recv().unwrap();
    }

    // Let readers race for a bit longer.
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.shutdown().unwrap();
}
