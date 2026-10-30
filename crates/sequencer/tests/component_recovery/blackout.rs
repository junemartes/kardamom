//! The lookup blackout: a restarted replica loses its receipt rows, the
//! executor query and Redis, while its cluster session stays live.
//!
//! The replica runs the production account lookup task against a real
//! HTTP executor query endpoint. During the blackout the endpoint takes
//! each connection and never answers, as a dropped TCP flow does, so each
//! query ends at the production two-second deadline. The production egress
//! feed reads the session egress of the replica and sets the
//! boundary-freshness metric that the chaos case reads.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use alloy_primitives::Address;
use kardamom_cache::{ExecutorQuery, ExecutorQueryConfig, LiveAccounts, LiveAccountsConfig};
use kardamom_cluster_adapter::LiveEgress;
use kardamom_sequencer::account_lookup::{AccountLookupFeed, LookupSources, Retries};
use kardamom_sequencer::admission::AccountState;
use kardamom_sequencer::egress_feed::EgressWatermarkFeed;
use kardamom_sequencer::epoch::EpochPump;
use kardamom_sequencer::metrics::LAST_BOUNDARY_UNIX_SECONDS;
use kardamom_sequencer::resync::ResyncChannel;
use kardamom_sequencer::sequencer::Shutdown;
use kardamom_sequencer::testkit::one_partition_cfg;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::lane::{Lane, Lookups, Replica};
use super::lane_cases::{CASE, FRESH};

/// The controlled executor query endpoint. The map and the switch cross
/// from the test thread to the server tasks.
#[derive(Clone)]
struct Executor {
    url: String,
    answers: Arc<AtomicBool>,
    state: Arc<Mutex<HashMap<Address, AccountState>>>,
}

impl Executor {
    async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let executor = Self {
            url: format!("http://{}", listener.local_addr().unwrap()),
            answers: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(HashMap::new())),
        };
        let server = executor.clone();
        tokio::spawn(async move {
            while let Ok((conn, _)) = listener.accept().await {
                tokio::spawn(server.clone().serve(conn));
            }
        });
        executor
    }

    /// One request. A blacked-out endpoint holds the connection open and
    /// never answers.
    async fn serve(self, mut conn: TcpStream) {
        let request = Self::read(&mut conn).await;
        if !self.answers.load(Ordering::Acquire) {
            std::future::pending::<()>().await;
        }
        let body = self.reply(&request);
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-state-tx-idx: {}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
            body.1,
            body.0.len()
        );
        let _ = conn.write_all(head.as_bytes()).await;
        let _ = conn.write_all(body.0.as_bytes()).await;
    }

    async fn read(conn: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        while !Self::complete(&buf) && Self::fill(conn, &mut buf).await {}
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// Read one chunk. False at the end of the stream.
    async fn fill(conn: &mut TcpStream, buf: &mut Vec<u8>) -> bool {
        let mut chunk = [0_u8; 1024];
        let n = conn.read(&mut chunk).await.unwrap_or(0);
        buf.extend_from_slice(&chunk[..n]);
        n > 0
    }

    fn complete(buf: &[u8]) -> bool {
        let text = String::from_utf8_lossy(buf);
        let Some((head, body)) = text.split_once("\r\n\r\n") else {
            return false;
        };
        let length = head
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
            })
            .unwrap_or(0);
        body.len() >= length
    }

    /// The JSON-RPC answer and the snapshot position of one query.
    fn reply(&self, request: &str) -> (String, u64) {
        let (_, params) = request.split_once(r#""params":[""#).unwrap();
        let address: Address = params[..42].parse().unwrap();
        let state = self
            .state
            .lock()
            .unwrap()
            .get(&address)
            .copied()
            .unwrap_or(AccountState {
                nonce: 0,
                balance: alloy_primitives::U256::ZERO,
                delegated: false,
                tx_idx: 0,
            });
        let value = if request.contains("eth_getTransactionCount") {
            format!("{:#x}", state.nonce)
        } else {
            format!("{:#x}", state.balance)
        };
        (
            format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{value}"}}"#),
            state.tx_idx,
        )
    }

    fn follow(&self, lane: &Lane) {
        let view = [CASE, FRESH].map(|seed| {
            let address = Lane::address(seed);
            (address, lane.state_of(address))
        });
        self.state.lock().unwrap().extend(view);
    }
}

impl Replica {
    /// The lookup counter of this replica process, by outcome.
    fn lookups(&self, outcome: &str) -> u64 {
        let (partition, outcome) = (
            format!("partition=\"{}\"", self.label),
            format!("outcome=\"{outcome}\""),
        );
        Self::recorder()
            .render()
            .lines()
            .filter(|l| {
                l.starts_with("kardamom_sequencer_nonce_lookups_total")
                    && l.contains(&partition)
                    && l.contains(&outcome)
            })
            .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
            .sum()
    }
}

impl Lane {
    /// Kill replica 0, and start it again with the production lookup task
    /// and egress feed, and no receipt rows. Redis is off and the live
    /// layer stays empty, as the blocked receipt stream leaves it.
    fn restart_blind(&mut self, runtime: &tokio::runtime::Runtime, executor: &Executor) -> Feeds {
        self.kill(0);
        self.restart(0, Lookups::Owned);
        self.set_blind(0, true);
        let replica = self.replica_mut(0);
        let (live, _writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let query = ExecutorQuery::new(&ExecutorQueryConfig {
            endpoints: vec![executor.url.clone()],
            ..ExecutorQueryConfig::default()
        });
        let shutdown = Shutdown::new();
        let lookups = AccountLookupFeed::new(
            replica.take_lookups(),
            replica.account_events(),
            LookupSources {
                live,
                redis: None,
                query,
            },
            shutdown.clone(),
            replica.label,
            Retries::covering(std::time::Duration::from_secs(30)),
        );
        let cfg = one_partition_cfg();
        let resync = ResyncChannel::for_config(&cfg).unwrap();
        let (origins, epochs) = EpochPump::new();
        let (frames, rx) = crossbeam_channel::unbounded();
        let egress = EgressWatermarkFeed::new(
            cfg.resync.boundary_silence_ms,
            replica.label,
            resync.watermark.clone(),
            resync.answer_tx.clone(),
            resync.deadline_tx.clone(),
            origins,
            replica.feed_egress(frames),
        );
        let _entered = runtime.enter();
        Feeds {
            lookup: lookups.spawn(),
            egress: egress.spawn(LiveEgress::from_frames(rx), shutdown.clone()),
            shutdown,
            _resync: resync,
            _epochs: epochs,
        }
    }
}

/// The running lookup task and egress feed of one replica process, and the
/// receivers that the egress feed writes to.
struct Feeds {
    shutdown: Shutdown,
    lookup: tokio::task::JoinHandle<()>,
    egress: tokio::task::JoinHandle<()>,
    _resync: ResyncChannel,
    _epochs: EpochPump,
}

impl Feeds {
    /// Both tasks end on shutdown, also with lookups in flight.
    fn stop(self, runtime: &tokio::runtime::Runtime) {
        self.shutdown.signal();
        runtime.block_on(async {
            let tasks = async { (self.lookup.await, self.egress.await) };
            let (lookup, egress) = tokio::time::timeout(super::lane::BOUND, tasks)
                .await
                .expect("the feeds stop");
            lookup.unwrap();
            egress.unwrap();
        });
    }
}

impl Feeds {
    /// The boundary-freshness gauge that the egress feed sets, in Unix
    /// seconds.
    fn last_boundary() -> f64 {
        Replica::recorder()
            .render()
            .lines()
            .find_map(|l| {
                l.strip_prefix(LAST_BOUNDARY_UNIX_SECONDS)?
                    .trim()
                    .parse()
                    .ok()
            })
            .unwrap_or(0.0)
    }

    /// The whole Unix second now, as the gauge counts it.
    fn now() -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            .floor()
    }
}

#[test]
fn a_lookup_blackout_fails_lookups_while_the_cluster_feed_lives_then_answers() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let executor = runtime.block_on(Executor::bind());
    let mut lane = Lane::start(0, 2);
    lane.establish();
    let started = Feeds::now();
    let blackout = lane.restart_blind(&runtime, &executor);
    lane.assert_progress();
    lane.until("lookups fail while boundaries stay fresh", |lane| {
        lane.replica(0).lookups("error") > 0 && Feeds::last_boundary() >= started
    });
    let victim = lane.replica(0);
    assert_eq!(
        victim.lookups("ok") + victim.lookups("live") + victim.lookups("redis"),
        0
    );
    assert_eq!(
        lane.replica(0).offers(),
        0,
        "a sender with no state is offered"
    );
    blackout.stop(&runtime);
    executor.answers.store(true, Ordering::Release);
    let answered = lane.restart_blind(&runtime, &executor);
    executor.follow(&lane);
    let restarted = Feeds::now();
    lane.assert_orders_alone(0, 1);
    assert!(
        Feeds::last_boundary() >= restarted,
        "no fresh boundary after the restart"
    );
    assert!(
        lane.replica(0).lookups("ok") > 0,
        "no executor answer reached the replica"
    );
    answered.stop(&runtime);
    lane.tail();
}
