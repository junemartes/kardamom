//! A manual release comparison of the steady tee, including its queue consumer.

use std::hint::black_box;
use std::time::Instant;

use super::{record, record_item, tee};
use crate::reader::tee::Forward;
use kardamom_types::BPosition;

#[test]
#[ignore = "manual release A/B profile; timings depend on the host"]
fn steady_tee_profile() {
    let (mut tee, frames) = tee();
    let frame = record(0);
    (0..2_000).for_each(|_| sample(&mut tee, &frames, &frame));
    let started = Instant::now();
    let samples: Vec<_> = (0..20_000)
        .map(|_| {
            let at = Instant::now();
            sample(&mut tee, &frames, &frame);
            u64::try_from(at.elapsed().as_nanos()).unwrap()
        })
        .collect();
    let elapsed = started.elapsed();
    let mut ordered = samples.clone();
    ordered.sort_unstable();
    println!(
        "TEE_PROFILE {{\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"frames_per_second\":{},\"samples_ns\":{samples:?}}}",
        ordered[10_000],
        ordered[19_000],
        ordered[19_800],
        20_000.0 / elapsed.as_secs_f64(),
    );
}

fn sample(tee: &mut Forward, frames: &crossbeam_channel::Receiver<Vec<u8>>, frame: &[u8]) {
    tee.hold(black_box(frame));
    black_box(tee.release((BPosition::from_index(0), record_item())));
    black_box(frames.try_recv().unwrap());
}
