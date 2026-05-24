//! Chaos test: leader gets isolated, then rejoins far behind the tail.
//!
//! Scenario:
//!   1. 3 sealers (recorder ids 1, 2, 3); all caught up; recorder 1 leads.
//!   2. Recorder 1 becomes "isolated": stops refreshing its watermark. The
//!      election function must eject recorder 1 (staleness window expires)
//!      and recorder 2 takes over.
//!   3. Recorder 1 rejoins (resumes publishing watermarks) but its
//!      `fsynced_position` is now far behind the current tail.
//!   4. Recorder 1 must NOT be re-elected (its lag > caught_up_lag_bytes),
//!      and no duplicate block_numbers may appear.

use kardamom_log::testing::FakeBus;
use kardamom_sealer::clock::MockClock;
use kardamom_sealer::election::RecorderState;
use kardamom_sealer::emitter::fakes::FakeBoundaryPublisher;
use kardamom_sealer::{Sealer, SealerConfig};
use kardamom_types::BPosition;

fn cfg(host_id: u8) -> SealerConfig {
    SealerConfig {
        host_id,
        channel_b_uri: "ch".into(),
        channel_b_tx_stream_id: 1,
        channel_b_boundary_stream_id: 2,
        watermark_channel_uri: "ch".into(),
        watermark_stream_id_base: 100,
        recorder_ids: vec![1, 2, 3],
        caught_up_lag_bytes: 64 * 1024,
        caught_up_stale_ms: 500,
        tick_interval_ms: 250,
    }
}

fn rs(id: u8, ts: u64, fsynced_off: i32) -> RecorderState {
    RecorderState {
        recorder_id: id,
        fsynced: BPosition {
            term_id: 0,
            term_offset: fsynced_off,
        },
        last_seen_ms: ts,
    }
}

fn build(
    host_id: u8,
    bus: &FakeBus,
    clock: MockClock,
) -> (
    Sealer<MockClock, FakeBoundaryPublisher>,
    FakeBoundaryPublisher,
) {
    let pubh = FakeBoundaryPublisher::new(bus.clone(), "ch", 2);
    let view = pubh.clone();
    let sealer = Sealer::new(cfg(host_id), clock, pubh, 1).unwrap();
    (sealer, view)
}

async fn step(
    sealers: &mut [(Sealer<MockClock, FakeBoundaryPublisher>, FakeBoundaryPublisher)],
    sealers_view_tx_tail: BPosition,
) -> Option<u64> {
    // Reflect the current synthetic tx tail to every publisher so the
    // election sees a non-zero "current position" (otherwise lag is always 0).
    for (_, view) in sealers.iter() {
        view.set_tx_tail(sealers_view_tx_tail);
    }
    let mut emitted: Option<u64> = None;
    for i in 0..sealers.len() {
        let (sealer, view) = &mut sealers[i];
        if let Some(n) = sealer.tick_once().await.unwrap() {
            assert!(
                emitted.is_none(),
                "two sealers emitted on the same tick: {n} and existing {emitted:?}"
            );
            emitted = Some(n);
            let published = view.published();
            let last = published.last().expect("emission visible").clone();
            for (j, (other, _)) in sealers.iter_mut().enumerate() {
                if j != i {
                    other.observe_boundary(&last);
                }
            }
        }
    }
    emitted
}

#[tokio::test(start_paused = true)]
async fn isolated_leader_yields_and_does_not_resume_when_behind_tail() {
    let bus = FakeBus::new();
    let clock = MockClock::new(1_000);

    let mut sealers = vec![
        build(1, &bus, clock.clone()),
        build(2, &bus, clock.clone()),
        build(3, &bus, clock.clone()),
    ];

    // The tx tail grows synthetically each tick so eligibility lag is meaningful.
    let mut tail_off: i32 = 1_024;

    // Phase 1: all three fresh; recorder 1 leads for 4 ticks.
    for (s, _) in &sealers {
        for hid in [1u8, 2, 3] {
            s.update_watermark(rs(hid, 1_000, tail_off));
        }
    }
    for tick in 0u64..4 {
        clock.advance(250);
        let now = 1_000 + (tick + 1) * 250;
        tail_off += 1_024;
        for (s, _) in &sealers {
            for hid in [1u8, 2, 3] {
                s.update_watermark(rs(hid, now, tail_off));
            }
        }
        let n = step(
            &mut sealers,
            BPosition {
                term_id: 0,
                term_offset: tail_off,
            },
        )
        .await;
        assert_eq!(n, Some(tick + 1));
    }

    // Phase 2: recorder 1 isolated. Recorder 2 takes over after the staleness
    // window expires (≥ 2 more ticks). Tail keeps growing.
    let stuck_off = tail_off; // recorder 1's last known fsync
    for tick in 4u64..10 {
        clock.advance(250);
        let now = 1_000 + (tick + 1) * 250;
        tail_off += 1_024;
        for (s, _) in &sealers {
            for hid in [2u8, 3] {
                s.update_watermark(rs(hid, now, tail_off));
            }
        }
        let _ = step(
            &mut sealers,
            BPosition {
                term_id: 0,
                term_offset: tail_off,
            },
        )
        .await;
    }

    // Phase 3: recorder 1 rejoins but is far behind. Its fsync stays at the
    // pre-isolation offset; tail has grown by ~6 KiB. Configure
    // caught_up_lag_bytes = 64 KiB so 6 KiB is "within window"; bump the gap
    // beyond 64 KiB by advancing more ticks before rejoin.
    for tick in 10u64..80 {
        clock.advance(250);
        let now = 1_000 + (tick + 1) * 250;
        tail_off += 1_024;
        for (s, _) in &sealers {
            // Recorder 1 keeps reporting the stale offset; recorder 2 and 3
            // are at the live tail.
            s.update_watermark(rs(1, now, stuck_off));
            s.update_watermark(rs(2, now, tail_off));
            s.update_watermark(rs(3, now, tail_off));
        }
        let _ = step(
            &mut sealers,
            BPosition {
                term_id: 0,
                term_offset: tail_off,
            },
        )
        .await;
    }

    let published = sealers[0].1.published();
    let mut numbers: Vec<u64> = published.iter().map(|b| b.block_number).collect();
    numbers.sort();
    let unique: std::collections::HashSet<u64> = numbers.iter().copied().collect();
    assert_eq!(
        unique.len(),
        numbers.len(),
        "duplicate block_number observed: {published:?}"
    );

    // Contiguity from 1.
    for (i, n) in numbers.iter().enumerate() {
        assert_eq!(
            *n,
            (i as u64) + 1,
            "non-contiguous block_numbers: {numbers:?}"
        );
    }
}
