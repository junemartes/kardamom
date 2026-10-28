use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::time::Duration;

use super::CoverageWait;
use crate::poll::Budget;

enum Observation {
    Error,
    Missing(&'static str),
    Complete,
    Hang,
}

struct Source {
    observations: RefCell<VecDeque<Observation>>,
    calls: Cell<usize>,
}

impl Source {
    fn new(observations: impl IntoIterator<Item = Observation>) -> Self {
        Self {
            observations: RefCell::new(observations.into_iter().collect()),
            calls: Cell::new(0),
        }
    }

    async fn observe(&self) -> anyhow::Result<Vec<String>> {
        self.calls.set(self.calls.get().checked_add(1).unwrap());
        let observation = self
            .observations
            .borrow_mut()
            .pop_front()
            .unwrap_or(Observation::Hang);
        match observation {
            Observation::Error => {
                tokio::time::sleep(Duration::from_secs(20)).await;
                anyhow::bail!("HMGET timed out")
            }
            Observation::Missing(account) => Ok(vec![account.into()]),
            Observation::Complete => Ok(Vec::new()),
            Observation::Hang => std::future::pending().await,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_read_retries_a_complete_observation_within_the_original_budget() {
    let source = Source::new([
        Observation::Error,
        Observation::Missing("untouched funded account"),
        Observation::Complete,
    ]);
    let started = tokio::time::Instant::now();
    CoverageWait::run("test", Budget::secs(120, 3), || source.observe())
        .await
        .unwrap();
    assert_eq!(source.calls.get(), 3);
    assert_eq!(started.elapsed(), Duration::from_secs(26));
}

#[tokio::test(start_paused = true)]
async fn persistent_read_errors_fail_at_the_total_deadline_with_the_cause() {
    let source = Source::new((0..6).map(|_| Observation::Error));
    let started = tokio::time::Instant::now();
    let error = CoverageWait::run("test", Budget::secs(120, 3), || source.observe())
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(error.to_string().contains("HMGET timed out"));
}

#[tokio::test(start_paused = true)]
async fn partial_observations_cannot_combine_into_complete_coverage() {
    let source = Source::new([
        Observation::Missing("sender"),
        Observation::Missing("untouched funded account"),
        Observation::Hang,
    ]);
    let started = tokio::time::Instant::now();
    let error = CoverageWait::run("test", Budget::secs(120, 3), || source.observe())
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(error.to_string().contains("untouched funded account"));
    assert_eq!(source.calls.get(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_hanging_first_observation_cannot_extend_the_total_deadline() {
    let source = Source::new([Observation::Hang]);
    let started = tokio::time::Instant::now();
    let error = CoverageWait::run("test", Budget::secs(120, 3), || source.observe())
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(
        error
            .to_string()
            .contains("no completed coverage observation")
    );
    assert_eq!(source.calls.get(), 1);
}
