use std::time::{Duration, Instant};

use crate::outbound::fakes::InMemorySidePublisher;
use crate::resync::SharedWatermark;
use crate::sealer_rules::{SealerRules, SealerVersion};
use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_status};
use kardamom_obs::halt::HaltCause;
use kardamom_obs::lifecycle::Lifecycle;
use kardamom_types::ClusterStatus;
use kardamom_types::cluster_status::RecordLagStatus;

use super::EgressWatermarkFeed;

#[path = "guards_recovery_tests.rs"]
mod recovery;

struct Fixture {
    feed: EgressWatermarkFeed<InMemorySidePublisher>,
    life: Lifecycle,
}

impl Fixture {
    fn new() -> Self {
        let (answer_tx, _) = crossbeam_channel::unbounded();
        let (deadline_tx, _) = crossbeam_channel::unbounded();
        let (origins, _) = crossbeam_channel::unbounded();
        Self {
            feed: EgressWatermarkFeed::new(
                1000,
                0,
                SharedWatermark::new(),
                answer_tx,
                deadline_tx,
                origins,
                SealerRules::new(
                    0,
                    SealerVersion::starting_at(5),
                    InMemorySidePublisher::default(),
                ),
            ),
            life: Lifecycle::new(None),
        }
    }

    fn status(&mut self, status: &ClusterStatus) {
        self.feed.on_frame(&encode_status(status), &self.life);
    }

    fn boundary(&mut self) {
        self.feed
            .on_frame(&encode_egress_boundary(1, 0, 0, 0), &self.life);
    }

    fn root(&self) -> Option<HaltCause> {
        self.life
            .slots()
            .pause
            .and_then(|pause| pause.reason.root().map(|root| root.cause))
    }
}

#[test]
fn boundaries_do_not_clear_da_or_record_lag_guards() {
    let mut f = Fixture::new();
    f.status(&ClusterStatus {
        halted: true,
        ..ClusterStatus::default()
    });
    assert_eq!(f.root(), Some(HaltCause::DaLag));
    f.boundary();
    assert_eq!(f.root(), Some(HaltCause::DaLag));
    f.status(&ClusterStatus {
        record_lag: RecordLagStatus {
            halted: true,
            ..RecordLagStatus::default()
        },
        ..ClusterStatus::default()
    });
    assert_eq!(f.root(), Some(HaltCause::RecordLag));
    f.boundary();
    assert_eq!(f.root(), Some(HaltCause::RecordLag));
    f.status(&ClusterStatus::default());
    assert_eq!(f.root(), None);
}

#[test]
fn a_status_cannot_clear_boundary_silence_and_a_boundary_keeps_the_guard() {
    let mut f = Fixture::new();
    f.feed.pause_on_silence(
        Instant::now().checked_add(Duration::from_secs(20)).unwrap(),
        &f.life,
    );
    assert_eq!(f.root(), Some(HaltCause::SealerNoQuorum));
    f.status(&ClusterStatus::default());
    assert_eq!(f.root(), Some(HaltCause::SealerNoQuorum));
    f.status(&ClusterStatus {
        halted: true,
        ..ClusterStatus::default()
    });
    assert_eq!(f.root(), Some(HaltCause::SealerNoQuorum));
    f.boundary();
    assert_eq!(f.root(), Some(HaltCause::DaLag));
    f.status(&ClusterStatus::default());
    assert_eq!(f.root(), None);
}

#[test]
fn record_lag_has_priority_and_a_truncated_status_clears_nothing() {
    let mut f = Fixture::new();
    f.status(&ClusterStatus {
        halted: true,
        record_lag: RecordLagStatus {
            halted: true,
            ..RecordLagStatus::default()
        },
        ..ClusterStatus::default()
    });
    assert_eq!(f.root(), Some(HaltCause::RecordLag));
    let malformed = encode_status(&ClusterStatus::default());
    f.feed.on_frame(&malformed[..2], &f.life);
    assert_eq!(f.root(), Some(HaltCause::RecordLag));
    f.boundary();
    assert_eq!(f.root(), Some(HaltCause::RecordLag));
}

#[test]
fn repeated_healthy_frames_do_not_publish_watch_changes() {
    let mut f = Fixture::new();
    let watch = f.life.subscribe();
    f.status(&ClusterStatus::default());
    f.boundary();
    f.status(&ClusterStatus::default());
    assert!(
        !watch.has_changed().unwrap(),
        "healthy ticks must not force full-run retries"
    );
}

#[test]
fn a_da_refusal_cannot_clear_the_known_record_lag_guard() {
    let mut f = Fixture::new();
    f.feed.pauses.on_guard(HaltCause::RecordLag, &f.life);
    f.feed.pauses.on_guard(HaltCause::DaLag, &f.life);
    assert_eq!(f.root(), Some(HaltCause::RecordLag));
    f.status(&ClusterStatus {
        halted: true,
        ..ClusterStatus::default()
    });
    assert_eq!(
        f.root(),
        Some(HaltCause::DaLag),
        "a status can establish that record lag cleared"
    );
}
