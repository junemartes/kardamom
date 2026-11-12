use std::future::Future;
use std::time::Duration;

use alloy_primitives::U256;
use kardamom_cache::{ExecutorQueryConfig, LiveAccountsConfig, LiveAccountsWriter};
use kardamom_types::{AccountRow, BPosition};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use super::*;

const ADDRESS: Address = Address::repeat_byte(0xad);

struct Fixture {
    requests: tokio::sync::mpsc::Sender<Address>,
    answers: crossbeam_channel::Receiver<AccountEvent>,
    writer: LiveAccountsWriter,
    limit: Arc<Semaphore>,
    shutdown: Shutdown,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    /// A feed whose executor query always fails: no process listens on
    /// port 1, so each query fails at once.
    fn new() -> Self {
        let query = ExecutorQuery::new(&ExecutorQueryConfig {
            endpoints: vec!["http://127.0.0.1:1".to_owned()],
            ..ExecutorQueryConfig::default()
        });
        Self::with_query(query)
    }

    /// A feed with no source but the local layer: a miss there is a miss
    /// on every source.
    fn without_sources() -> Self {
        Self::with_query(None)
    }

    fn with_query(query: Option<ExecutorQuery>) -> Self {
        Self::with_retries(query, Retries::covering(Duration::from_secs(30)))
    }

    /// A feed whose executor query always fails, and whose lookups end
    /// after two retries.
    fn short_lived() -> Self {
        let query = ExecutorQuery::new(&ExecutorQueryConfig {
            endpoints: vec!["http://127.0.0.1:1".to_owned()],
            ..ExecutorQueryConfig::default()
        });
        Self::with_retries(query, Retries(2))
    }

    fn with_retries(query: Option<ExecutorQuery>, retries: Retries) -> Self {
        let (live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let (requests, rx) = tokio::sync::mpsc::channel(1);
        let (answers_tx, answers) = crossbeam_channel::unbounded();
        let shutdown = Shutdown::new();
        let sources = LookupSources {
            live,
            redis: None,
            query,
        };
        let feed = AccountLookupFeed::new(rx, answers_tx, sources, shutdown.clone(), 0, retries);
        let limit = feed.limit.clone();
        Self {
            requests,
            answers,
            writer,
            limit,
            shutdown,
            task: feed.spawn(),
        }
    }

    fn restore(&mut self) {
        self.writer.apply(
            BPosition::from_index(10),
            &[AccountRow {
                address: ADDRESS,
                nonce: 42,
                balance: U256::from(123),
            }],
        );
    }

    async fn answer(&self) {
        let event = tokio::time::timeout(Duration::from_secs(5), async {
            while self.answers.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            self.answers.try_recv().unwrap()
        })
        .await
        .expect("one request must recover without a second request");
        let AccountEvent::Answer { address, state } = event else {
            panic!("lookup emits an answer");
        };
        assert_eq!(address, ADDRESS);
        assert_eq!(state.nonce, 42);
        assert_eq!(state.balance, U256::from(123));
        assert_eq!(state.tx_idx, 10);
    }

    async fn stop(mut self) {
        self.halt().await;
        self.release_permits().await;
    }

    async fn halt(&mut self) {
        self.shutdown.signal();
        tokio::time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .expect("feed must stop while lookups wait")
            .unwrap();
    }

    async fn release_permits(&self) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.limit.available_permits() != IN_FLIGHT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown must release all lookup permits");
    }

    async fn profile_sample(&self) -> u128 {
        let offered = Instant::now();
        self.requests.send(ADDRESS).await.unwrap();
        while self.answers.is_empty() {
            tokio::task::yield_now().await;
        }
        let AccountEvent::Answer { state, .. } = self.answers.try_recv().unwrap() else {
            panic!("lookup emits an answer");
        };
        assert_eq!(state.nonce, 42);
        offered.elapsed().as_nanos()
    }
}

struct Recording(PrometheusHandle);

impl Recording {
    fn run<F: Future<Output = ()>>(test: impl FnOnce(Self) -> F) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let recording = Self(recorder.handle());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        ::metrics::with_local_recorder(&recorder, || runtime.block_on(test(recording)));
    }

    async fn failed(&self) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !self.has_failure() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first attempt must fail before sources return");
    }

    fn has_failure(&self) -> bool {
        self.0.render().lines().any(|line| {
            line.starts_with("kardamom_sequencer_nonce_lookups_total{")
                && line.contains("outcome=\"error\"")
                && line.ends_with(" 1")
        })
    }

    /// The attempts so far that ended with `outcome`.
    fn attempts(&self, outcome: &str) -> u64 {
        let label = format!("outcome=\"{outcome}\"");
        self.0
            .render()
            .lines()
            .filter(|line| line.starts_with("kardamom_sequencer_nonce_lookups_total{"))
            .filter(|line| line.contains(&label))
            .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
            .sum()
    }
}

/// The requests that a sender sends while every source is down: one for
/// each parked transaction that expires.
const EXPIRED: usize = 20;

/// Each expired transaction of a sender asks again while the first lookup
/// of the sender still retries. The feed runs one lookup for the address,
/// so the first round of attempts has one attempt.
#[test]
fn the_requests_of_a_sender_with_a_running_lookup_start_no_second_lookup() {
    Recording::run(|recording| async move {
        let fixture = Fixture::new();
        for _ in 0..EXPIRED {
            fixture.requests.send(ADDRESS).await.unwrap();
        }
        // The request channel holds one request, so each send returns once
        // the feed took the request before it.
        recording.failed().await;
        tokio::time::sleep(RETRY_AFTER / 4).await;
        assert_eq!(recording.attempts("error"), 1);
        fixture.stop().await;
    });
}

/// A lookup that ended leaves the set: the next request of the address
/// starts a new lookup.
#[test]
fn a_request_after_the_lookup_ended_starts_a_new_lookup() {
    Recording::run(|recording| async move {
        let fixture = Fixture::without_sources();
        fixture.requests.send(ADDRESS).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while recording.attempts("miss") < 2 {
                fixture.requests.send(ADDRESS).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a request after the end of a lookup must start a new one");
        fixture.stop().await;
    });
}

#[test]
fn a_failed_lookup_recovers_without_a_second_request() {
    Recording::run(|recording| async move {
        let mut fixture = Fixture::new();
        fixture.requests.send(ADDRESS).await.unwrap();
        recording.failed().await;
        fixture.restore();
        fixture.answer().await;
        fixture.stop().await;
    });
}

#[test]
fn shutdown_cancels_failed_lookups() {
    Recording::run(|recording| async move {
        let fixture = Fixture::new();
        fixture.requests.send(ADDRESS).await.unwrap();
        recording.failed().await;
        fixture.stop().await;
    });
}

#[test]
fn shutdown_cancels_a_feed_with_all_lookup_permits_held() {
    Recording::run(|_| async move {
        let mut fixture = Fixture::new();
        let held = fixture
            .limit
            .clone()
            .try_acquire_many_owned(u32::try_from(IN_FLIGHT).unwrap())
            .unwrap();
        // The second request goes in only after the feed took the first.
        fixture.requests.send(ADDRESS).await.unwrap();
        fixture.requests.send(ADDRESS).await.unwrap();
        fixture.halt().await;
        assert_eq!(held.num_permits(), IN_FLIGHT);
    });
}

#[test]
fn a_failed_lookup_releases_its_permit_while_it_waits_to_retry() {
    Recording::run(|recording| async move {
        let fixture = Fixture::new();
        fixture.requests.send(ADDRESS).await.unwrap();
        recording.failed().await;
        fixture.release_permits().await;
        fixture.stop().await;
    });
}

#[test]
fn unknown_accounts_do_not_hold_the_lookup_permits() {
    Recording::run(|_| async move {
        let mut fixture = Fixture::without_sources();
        for byte in 0..=u8::try_from(IN_FLIGHT).unwrap() {
            fixture
                .requests
                .send(Address::repeat_byte(byte))
                .await
                .unwrap();
        }
        fixture.release_permits().await;
        fixture.restore();
        fixture.requests.send(ADDRESS).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), fixture.answer())
            .await
            .expect("a known account must answer after many unknown ones");
        fixture.stop().await;
    });
}

#[test]
fn aborting_the_feed_cancels_its_retry_workers() {
    Recording::run(|recording| async move {
        let fixture = Fixture::new();
        fixture.requests.send(ADDRESS).await.unwrap();
        recording.failed().await;
        fixture.task.abort();
        tokio::task::yield_now().await;
        fixture.release_permits().await;
    });
}

#[test]
fn a_healthy_first_lookup_answers_without_a_retry() {
    Recording::run(|recording| async move {
        let mut fixture = Fixture::new();
        fixture.restore();
        fixture.requests.send(ADDRESS).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), fixture.answer())
            .await
            .expect("healthy lookups must not wait for a retry timer");
        assert!(!recording.has_failure());
        fixture.stop().await;
    });
}

#[test]
#[ignore = "manual release profile of successful first-attempt lookup tasks"]
fn healthy_lookup_profile() {
    Recording::run(|_| async move {
        let mut fixture = Fixture::new();
        fixture.restore();
        let mut samples = Vec::with_capacity(20_000);
        let started = Instant::now();
        for _ in 0..20_000 {
            samples.push(fixture.profile_sample().await);
        }
        let elapsed_ns = started.elapsed().as_nanos();
        samples.sort_unstable();
        println!(
            "LOOKUP_PROFILE {{\"samples\":{},\"elapsed_ns\":{elapsed_ns},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{}}}",
            samples.len(),
            samples[10_000],
            samples[19_000],
            samples[19_800]
        );
        fixture.stop().await;
    });
}

#[test]
fn the_retries_cover_the_configured_park_lifetime() {
    assert_eq!(Retries::covering(Duration::from_secs(30)), Retries(15));
    assert_eq!(Retries::covering(Duration::from_secs(120)), Retries(60));
    assert_eq!(Retries::covering(Duration::from_millis(2_001)), Retries(2));
    assert_eq!(Retries::covering(Duration::ZERO), Retries(0));
}

/// A request comes during the last retry of a lookup that the sources fail.
/// The lookup then ends with no answer. The feed starts a new lookup at
/// once, so the sources that return after the end answer the sender
/// before its next expiry.
#[test]
fn a_request_during_the_last_retry_starts_a_new_lookup_when_the_lookup_fails() {
    Recording::run(|recording| async move {
        let mut fixture = Fixture::short_lived();
        fixture.requests.send(ADDRESS).await.unwrap();
        // The first attempt and the first retry fail; the last retry waits.
        tokio::time::timeout(RETRY_AFTER * 2, async {
            while recording.attempts("error") < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first retry must fail");
        fixture.requests.send(ADDRESS).await.unwrap();
        // The last retry fails, and the lookup ends with no answer.
        tokio::time::timeout(RETRY_AFTER * 2, async {
            while recording.attempts("error") < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the last retry must fail");
        fixture.restore();
        tokio::time::timeout(RETRY_AFTER * 2, fixture.answer())
            .await
            .expect("the request of the last retry must start a new lookup");
        fixture.stop().await;
    });
}
