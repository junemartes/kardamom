use super::*;

async fn reading(delay: u64, value: Option<i64>) -> Option<i64> {
    tokio::time::sleep(Duration::from_secs(delay)).await;
    value
}

#[tokio::test(start_paused = true)]
async fn killed_endpoints_cannot_discard_a_survivor_in_any_position() {
    for survivor in 0..3 {
        let started = Instant::now();
        let mut samples = Samples::default();
        (0..3).for_each(|index| {
            samples.start(reading(
                if index == survivor { 1 } else { 30 },
                (index == survivor).then_some(622),
            ));
        });
        assert_eq!(samples.maximum().await, Some(622));
        assert_eq!(started.elapsed(), Duration::from_secs(1) + SETTLE);
    }
}

#[tokio::test(start_paused = true)]
async fn every_completed_reading_contributes_to_the_maximum() {
    let mut samples = Samples::default();
    for (delay, value) in [(1, Some(622)), (2, Some(623)), (30, None)] {
        samples.start(reading(delay, value));
    }
    assert_eq!(samples.maximum().await, Some(623));
}

#[tokio::test(start_paused = true)]
async fn missing_exporters_never_become_a_zero_head() {
    let mut samples = Samples::default();
    for delay in [1, 2, 30] {
        samples.start(reading(delay, None));
    }
    assert_eq!(samples.maximum().await, None);
    assert_eq!(Samples::default().maximum().await, None);
}

#[tokio::test(start_paused = true)]
async fn an_observation_never_reuses_a_previous_survivor_value() {
    let mut before = Samples::default();
    before.start(reading(0, Some(622)));
    assert_eq!(before.maximum().await, Some(622));
    let mut after = Samples::default();
    after.start(reading(30, Some(622)));
    assert_eq!(after.maximum().await, None);
}

#[tokio::test(start_paused = true)]
async fn a_reading_after_the_settle_does_not_count() {
    let mut samples = Samples::default();
    for (delay, value) in [(1, Some(622)), (2, Some(623)), (8, Some(900))] {
        samples.start(reading(delay, value));
    }
    assert_eq!(samples.maximum().await, Some(623));
}
