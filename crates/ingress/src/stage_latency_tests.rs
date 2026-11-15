use super::*;

#[test]
fn a_stamped_hash_gives_one_latency() {
    let (stamps, mut clocks) = StageClock::pair();
    let hash = B256::repeat_byte(1);
    stamps.stamp(hash, Instant::now());
    let later = Instant::now() + Duration::from_millis(40);
    let spans = clocks
        .sealed
        .spans_at(hash, later)
        .expect("the hash has a stamp");
    assert!(spans.from_publish >= Duration::from_millis(40));
    assert_eq!(
        clocks.sealed.spans_at(hash, later),
        None,
        "one sample per seal"
    );
}

#[test]
fn each_stage_gets_its_own_stamp() {
    let (stamps, mut clocks) = StageClock::pair();
    let hash = B256::repeat_byte(6);
    stamps.stamp(hash, Instant::now());
    let later = Instant::now() + Duration::from_millis(5);
    assert!(clocks.sealed.spans_at(hash, later).is_some());
    assert!(
        clocks.executed.spans_at(hash, later).is_some(),
        "the seal leaves the executed stamp"
    );
    assert_eq!(
        clocks.executed.spans_at(hash, later),
        None,
        "a second receipt copy gives no sample"
    );
}

#[test]
fn an_observation_after_a_minute_still_gives_a_sample() {
    let (stamps, mut clocks) = StageClock::pair();
    let hash = B256::repeat_byte(5);
    stamps.stamp(hash, Instant::now());
    let late = Instant::now() + Duration::from_secs(60);
    assert!(clocks.executed.spans_at(hash, late).is_some());
}

#[test]
fn a_hash_without_a_stamp_gives_none() {
    let (_stamps, mut clocks) = StageClock::pair();
    assert_eq!(
        clocks.sealed.spans_at(B256::repeat_byte(2), Instant::now()),
        None
    );
}

#[test]
fn the_sweep_removes_old_stamps() {
    let (stamps, mut clocks) = StageClock::pair();
    stamps.stamp(B256::repeat_byte(3), Instant::now());
    let late = Instant::now() + STAMP_TTL + SWEEP_EVERY;
    assert_eq!(clocks.sealed.spans_at(B256::repeat_byte(4), late), None);
    assert!(clocks.sealed.open.is_empty(), "the old stamp is gone");
}

/// The submit arrived 3 ms before its publish. The time from the arrival
/// is the time from the publish plus those 3 ms.
#[test]
fn the_time_from_the_arrival_holds_the_time_before_the_publish() {
    let (stamps, mut clocks) = StageClock::pair();
    let hash = B256::repeat_byte(7);
    let published = Instant::now();
    let arrived = published.checked_sub(Duration::from_millis(3)).unwrap();
    stamps.stamp(hash, arrived);
    let spans = clocks
        .executed
        .spans_at(hash, published + Duration::from_millis(5))
        .expect("the hash has a stamp");
    assert!(spans.from_publish <= Duration::from_millis(5));
    assert!(spans.from_arrival >= Duration::from_millis(8));
    assert!(
        spans
            .from_arrival
            .checked_sub(spans.from_publish)
            .is_some_and(|before_publish| before_publish >= Duration::from_millis(3))
    );
}

/// The samples of the histogram `name` in the rendered metrics `text`. A
/// histogram with no sample has no series.
fn samples(text: &str, name: &str) -> f64 {
    let series = format!("{name}_count ");
    text.lines()
        .find(|line| line.starts_with(&series))
        .and_then(|line| line.rsplit(' ').next())
        .map_or(0.0, |count| count.parse().unwrap())
}

/// A receipt gives one sample of the execute latency and one of the
/// end-to-end latency. A seal gives its own latency and no end-to-end
/// latency. A second copy of the receipt gives no sample.
#[test]
fn a_receipt_records_the_end_to_end_latency_and_a_seal_does_not() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, || {
        let (stamps, mut clocks) = StageClock::pair();
        let hash = B256::repeat_byte(8);
        stamps.stamp(hash, Instant::now());
        clocks.sealed.observe(hash);
        let text = recorder.handle().render();
        assert_eq!(samples(&text, SEAL_LATENCY_SECONDS), 1.0, "{text}");
        assert_eq!(samples(&text, END_TO_END_LATENCY_SECONDS), 0.0, "{text}");
        clocks.executed.observe(hash);
        clocks.executed.observe(hash);
    });
    let text = recorder.handle().render();
    assert_eq!(samples(&text, EXECUTE_LATENCY_SECONDS), 1.0, "{text}");
    assert_eq!(samples(&text, END_TO_END_LATENCY_SECONDS), 1.0, "{text}");
}
