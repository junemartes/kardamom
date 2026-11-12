use std::cell::Cell;
use std::num::NonZeroU64;

use crate::harness::Harness;
use crate::metrics;
use crate::poll::{self, Budget};

const PUBLISHED: &str = "kardamom_sequencer_tx_published_to_b_total";
const STARTED: &str = "kardamom_sequencer_start_time_seconds";

#[derive(Clone, Copy)]
struct Publication {
    /// Exact clock bits distinguish process starts within one second.
    started: NonZeroU64,
    published: u64,
}

impl Publication {
    fn read(body: &str) -> Option<Self> {
        Some(Self {
            started: Self::start_stamp(body)?,
            published: u64::try_from(metrics::first(body, PUBLISHED)?).ok()?,
        })
    }

    fn start_stamp(body: &str) -> Option<NonZeroU64> {
        let line = body.lines().find(|line| {
            line.strip_prefix(STARTED)
                .is_some_and(|suffix| suffix.starts_with(' ') || suffix.starts_with('{'))
        })?;
        let seconds: f64 = line.split_whitespace().last()?.parse().ok()?;
        if !seconds.is_finite() || seconds <= 0.0 {
            return None;
        }
        NonZeroU64::new(seconds.to_bits())
    }
}

#[derive(Default)]
struct Recovery {
    previous: Cell<Option<Publication>>,
}

impl Recovery {
    fn observe(&self, body: Option<&str>) -> Option<Publication> {
        let current = Publication::read(body?)?;
        let previous = self.previous.replace(Some(current))?;
        (current.started == previous.started && current.published > previous.published)
            .then_some(current)
    }
}

impl Harness {
    /// Both publication observations belong to the affected replica after
    /// thaw. A restart establishes a new floor before progress can pass.
    pub(super) async fn assert_admission_lapse(&self) -> anyhow::Result<()> {
        let recovery = Recovery::default();
        let target = self.probes.sequencer_lane0_target(0);
        let outcome = poll::until(Budget::secs(240, 10), |_| async {
            let body = self.probes.scrape().fetch(&target).await;
            Ok(recovery.observe(body.as_deref()))
        })
        .await?;
        let (sample, elapsed) = outcome.or_fail(|elapsed| {
            crate::chaos_fail!(
                "sequencer-lapse: affected admission replica does not publish fresh work within {}s after thaw",
                elapsed.as_secs()
            )
        })?;
        crate::log(format!(
            "sequencer-lapse: affected admission replica publishes fresh work after {}s (process start {}, published {})",
            elapsed.as_secs(),
            f64::from_bits(sample.started.get()),
            sample.published
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Observation {
        started: f64,
        published: u64,
    }

    impl Observation {
        fn body(&self) -> String {
            format!(
                "{STARTED} {}\n{PUBLISHED} {}\nkardamom_sequencer_resync_mode 1\n",
                self.started, self.published
            )
        }
    }

    #[test]
    fn a_stale_resync_gauge_and_unchanged_counter_cannot_prove_admission_recovery() {
        let recovery = Recovery::default();
        let sample = Observation {
            started: 10.0,
            published: 100,
        }
        .body();
        assert!(recovery.observe(Some(&sample)).is_none());
        assert!(recovery.observe(Some(&sample)).is_none());
        let later = Observation {
            started: 10.0,
            published: 101,
        }
        .body();
        assert!(recovery.observe(Some(&later)).is_some());
    }

    #[test]
    fn a_new_process_needs_its_own_two_publication_observations() {
        let recovery = Recovery::default();
        let old = Observation {
            started: 10.0,
            published: 1_000,
        }
        .body();
        let first = Observation {
            started: 11.0,
            published: 2_000,
        }
        .body();
        let next = Observation {
            started: 11.0,
            published: 2_001,
        }
        .body();
        assert!(recovery.observe(Some(&old)).is_none());
        assert!(recovery.observe(Some(&first)).is_none());
        assert!(recovery.observe(Some(&next)).is_some());
    }

    #[test]
    fn missing_or_invalid_observations_cannot_supply_a_zero_floor() {
        let recovery = Recovery::default();
        assert!(recovery.observe(None).is_none());
        assert!(
            recovery
                .observe(Some("kardamom_sequencer_resync_mode 1\n"))
                .is_none()
        );
        assert!(
            recovery
                .observe(Some(&format!("{STARTED} 0\n{PUBLISHED} 0\n")))
                .is_none()
        );
        assert!(
            recovery
                .observe(Some(&format!("{STARTED} 10\n{PUBLISHED} -1\n")))
                .is_none()
        );
        let first = Observation {
            started: 10.0,
            published: 100,
        }
        .body();
        assert!(recovery.observe(Some(&first)).is_none());
    }
    #[test]
    fn a_restart_inside_one_second_cannot_reuse_the_old_publication_floor() {
        let recovery = Recovery::default();
        let old = Observation {
            started: 10.1,
            published: 100,
        }
        .body();
        let restarted = Observation {
            started: 10.9,
            published: 200,
        }
        .body();
        let advanced = Observation {
            started: 10.9,
            published: 201,
        }
        .body();
        assert!(recovery.observe(Some(&old)).is_none());
        assert!(recovery.observe(Some(&restarted)).is_none());
        assert!(recovery.observe(Some(&advanced)).is_some());
    }
}
