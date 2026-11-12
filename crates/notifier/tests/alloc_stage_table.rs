//! The allocations of the events path (`docs/specs/2026-10-25-client-events.md`,
//! decision 6): an event that the stage table does not send, a copy or a
//! late stage, allocates nothing. The test runs without the test harness,
//! so no other thread allocates in the measured window.
//!
//!   cargo test -p kardamom-notifier --test `alloc_stage_table`

use std::time::Instant;

use alloy_primitives::B256;
use kardamom_notifier::events::EventsPath;
use kardamom_types::tx_events::{TxEventKind, TxEventsFrame};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// The hashes of the measured window.
const HASHES: usize = 1024;
/// The copies of the window: each frame again, as the twin sequencer sends
/// it.
const ROUNDS: usize = 16;

fn main() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let start = Instant::now();
    let mut path = EventsPath::new(4 * HASHES, start);
    let hashes: Vec<B256> = (0..HASHES)
        .map(|i| {
            let mut bytes = [0_u8; 32];
            bytes[..8]
                .copy_from_slice(&(i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            B256::from(bytes)
        })
        .collect();
    // The twins of lane 3: two origins, one frame each, with the seq of
    // each frame in order, so no gap is counted.
    let mut twins = [
        TxEventsFrame::new(TxEventKind::Sent, 0x0003),
        TxEventsFrame::new(TxEventKind::Sent, 0x0103),
    ];
    for twin in &mut twins {
        for hash in &hashes {
            twin.push_sent(hash, 3);
        }
    }
    let mut accepted = TxEventsFrame::new(TxEventKind::Accepted, 1);
    for hash in &hashes {
        accepted.push_accepted(hash);
    }
    // Warm-up: the first `sent` of each hash is sent.
    let mut sent = 0_usize;
    path.on_frame(twins[0].bytes(), start, |_| sent += 1);
    assert_eq!(sent, HASHES);
    twins[0].advance();

    let before = dhat::HeapStats::get();
    let clock = Instant::now();
    let mut late = 0_usize;
    for _ in 0..ROUNDS {
        for twin in &mut twins {
            path.on_frame(twin.bytes(), start, |_| late += 1);
            twin.advance();
        }
        // `accepted` after `sent` is late.
        path.on_frame(accepted.bytes(), start, |_| late += 1);
        accepted.advance();
    }
    let elapsed = clock.elapsed();
    let after = dhat::HeapStats::get();

    assert_eq!(late, 0, "no copy and no late event is sent");
    let measured = ROUNDS * 3 * HASHES;
    let blocks = after.total_blocks - before.total_blocks;
    println!(
        "events path: {measured} events not sent, {blocks} heap blocks, {} us in all",
        elapsed.as_micros()
    );
    dhat::assert_eq!(blocks, 0);
}
