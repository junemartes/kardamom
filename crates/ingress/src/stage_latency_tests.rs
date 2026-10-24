use super::*;

#[test]
fn a_stamped_hash_gives_one_latency() {
    let (stamps, mut clocks) = StageClock::pair();
    let hash = B256::repeat_byte(1);
    stamps.stamp(hash);
    let later = Instant::now() + Duration::from_millis(40);
    let latency = clocks
        .sealed
        .latency_at(hash, later)
        .expect("the hash has a stamp");
    assert!(latency >= Duration::from_millis(40));
    assert_eq!(
        clocks.sealed.latency_at(hash, later),
        None,
        "one sample per seal"
    );
}

#[test]
fn each_stage_gets_its_own_stamp() {
    let (stamps, mut clocks) = StageClock::pair();
    let hash = B256::repeat_byte(6);
    stamps.stamp(hash);
    let later = Instant::now() + Duration::from_millis(5);
    assert!(clocks.sealed.latency_at(hash, later).is_some());
    assert!(
        clocks.executed.latency_at(hash, later).is_some(),
        "the seal leaves the executed stamp"
    );
    assert_eq!(
        clocks.executed.latency_at(hash, later),
        None,
        "a second receipt copy gives no sample"
    );
}

#[test]
fn an_observation_after_a_minute_still_gives_a_sample() {
    let (stamps, mut clocks) = StageClock::pair();
    let hash = B256::repeat_byte(5);
    stamps.stamp(hash);
    let late = Instant::now() + Duration::from_secs(60);
    assert!(clocks.executed.latency_at(hash, late).is_some());
}

#[test]
fn a_hash_without_a_stamp_gives_none() {
    let (_stamps, mut clocks) = StageClock::pair();
    assert_eq!(
        clocks
            .sealed
            .latency_at(B256::repeat_byte(2), Instant::now()),
        None
    );
}

#[test]
fn the_sweep_removes_old_stamps() {
    let (stamps, mut clocks) = StageClock::pair();
    stamps.stamp(B256::repeat_byte(3));
    let late = Instant::now() + STAMP_TTL + SWEEP_EVERY;
    assert_eq!(clocks.sealed.latency_at(B256::repeat_byte(4), late), None);
    assert!(clocks.sealed.open.is_empty(), "the old stamp is gone");
}
