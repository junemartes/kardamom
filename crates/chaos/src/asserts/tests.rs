use super::{FleetSample, Standing};

fn nodes() -> Vec<String> {
    ["executor-0", "executor-1", "executor-2"]
        .map(String::from)
        .to_vec()
}

fn sample(blocks: [Option<i64>; 3]) -> FleetSample {
    FleetSample {
        blocks: blocks.to_vec(),
    }
}

#[test]
fn a_replica_that_stopped_inside_the_lag_bound_is_stalled() {
    // The shape of issue #313: executor-2 stopped at block 300 while the
    // head went on, and one sample with lag <= 50 still called it close.
    let base = sample([Some(314), Some(314), Some(300)]);
    assert!(base.within(50));
    let next = sample([Some(320), Some(320), Some(300)]);
    assert_eq!(
        next.standings(&nodes(), 50, Some(&base)),
        vec![
            Standing::Converged,
            Standing::Converged,
            Standing::Stalled("executor-2".to_string(), 300),
        ]
    );
}

#[test]
fn every_replica_past_its_floor_is_converged() {
    let base = sample([Some(314), Some(313), Some(300)]);
    let next = sample([Some(320), Some(319), Some(306)]);
    assert!(
        next.standings(&nodes(), 50, Some(&base))
            .iter()
            .all(|s| *s == Standing::Converged)
    );
}

#[test]
fn a_sample_with_a_dark_or_lagging_replica_is_no_floor() {
    assert!(!sample([Some(314), None, Some(300)]).within(50));
    assert!(!sample([Some(400), Some(400), Some(300)]).within(50));
    assert!(!sample([None, None, None]).within(50));
}

#[test]
fn lag_and_a_failed_scrape_outrank_the_floor() {
    let base = sample([Some(314), Some(314), Some(300)]);
    let next = sample([Some(400), None, Some(301)]);
    assert_eq!(
        next.standings(&nodes(), 50, Some(&base)),
        vec![
            Standing::Converged,
            Standing::Unreachable("executor-1".to_string()),
            Standing::Lagging("executor-2".to_string(), 99),
        ]
    );
}

#[test]
fn without_a_floor_the_lag_bound_alone_decides() {
    let first = sample([Some(314), Some(314), Some(300)]);
    assert!(
        first
            .standings(&nodes(), 50, None)
            .iter()
            .all(|s| *s == Standing::Converged)
    );
}
