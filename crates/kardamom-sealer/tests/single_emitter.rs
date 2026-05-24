//! Single-emitter invariant: with 3 sealers running in lockstep, exactly one
//! emits per tick — the deterministic lowest-id winner.

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

fn rs(id: u8, ts: u64) -> RecorderState {
    RecorderState {
        recorder_id: id,
        fsynced: BPosition::ZERO,
        last_seen_ms: ts,
    }
}

#[tokio::test(start_paused = true)]
async fn exactly_one_sealer_publishes_per_tick() {
    let bus = FakeBus::new();
    let clock = MockClock::new(1_000);

    let mut all: Vec<(
        Sealer<MockClock, FakeBoundaryPublisher>,
        FakeBoundaryPublisher,
    )> = Vec::new();
    for hid in [1u8, 2, 3] {
        let pubh = FakeBoundaryPublisher::new(bus.clone(), "ch", 2);
        let view = pubh.clone();
        let sealer = Sealer::new(cfg(hid), clock.clone(), pubh, 1).unwrap();
        all.push((sealer, view));
    }

    // Initial watermarks.
    for (s, _) in &all {
        for hid in [1u8, 2, 3] {
            s.update_watermark(rs(hid, 1_000));
        }
    }

    for tick in 0u64..5 {
        clock.advance(250);
        let now = 1_000 + (tick + 1) * 250;
        for (s, _) in &all {
            for hid in [1u8, 2, 3] {
                s.update_watermark(rs(hid, now));
            }
        }
        let mut emitted = 0u32;
        for (s, _) in &mut all {
            if s.tick_once().await.unwrap().is_some() {
                emitted += 1;
            }
        }
        assert_eq!(emitted, 1, "tick {tick}: expected exactly one emission");
    }

    let published = all[0].1.published();
    assert_eq!(published.len(), 5);
    for (i, b) in published.iter().enumerate() {
        assert_eq!(b.block_number, (i as u64) + 1);
    }
}
