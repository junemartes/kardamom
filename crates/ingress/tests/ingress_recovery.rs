//! The ingress chaos cases in isolation: `graceful-ingress`,
//! `hard-ingress` and `archive-driver-loss`. Each ingress replica is the
//! real `kardamom-ingress` binary on its own archiving media driver, as
//! on its own node. The other services are controlled peers that speak
//! the production codecs: one stand-in for the sequencer, the sealer and
//! the executor on the second driver, and an HTTP stub for the egress and
//! the executor query.
//!
//! Every test runs a client through ingress 0 for the whole case and
//! requires:
//! - every offered transfer lands, and the pipeline executes each one
//!   exactly once, in nonce order;
//! - both ingresses, the returned one too, serve every receipt;
//! - the client lands again within the restart bound.
//!
//! Needs Java and the aeron-all jar (`KARDAMOM_AERON_ALL_JAR`, or the
//! cache path of `just aeron-driver-up`).

#![cfg(target_os = "linux")]

#[path = "ingress_recovery/driver.rs"]
mod driver;
#[path = "ingress_recovery/load.rs"]
mod load;
#[path = "ingress_recovery/node.rs"]
mod node;
#[path = "ingress_recovery/peer.rs"]
mod peer;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use alloy_signer_local::PrivateKeySigner;
use kardamom_log::aeron_live::{AeronRuntime, PubHandle, TxDataSubscriberHandle};
use kardamom_log::config::ChannelsConfig;
use kardamom_obs::testkit::{PortBlock, PortSlot};

use driver::Driver;
use load::{Load, Report};
use node::{Replica, Stop};
use peer::{Pipeline, QueryStub};

const CHAIN_ID: u64 = 1;
/// The bound for a returned replica: from its start until it serves, and
/// until the client lands again. The chaos suite allows 60 s for a Nomad
/// restart.
const RESTART_BOUND: Duration = Duration::from_secs(30);

/// The fixed ports of this test process: its slot of
/// [`PortBlock::IngressRecovery`], below the ephemeral range. No port-0
/// socket can take a port of the slot, and a replica or a driver keeps
/// its ports across a restart.
///
/// | offset | port |
/// |---|---|
/// | `10 * l + r` | the `tx_data` control port of lane `l` of replica `r` |
/// | `2 + r` | the RPC port (TCP) of replica `r` |
/// | `4 + r` | the metrics port (TCP) of replica `r` |
/// | `6 + r` | the receipt endpoint of replica `r` |
/// | `12 + 3 * d` to `14 + 3 * d` | the archive control, response and replication ports of driver `d` |
/// | `22 + 2 * r`, `23 + 2 * r` | the refetch response and replay endpoints of the pipeline lane of replica `r` |
///
/// A control port ends in the digit of its replica, 0 or 1. So every
/// other port ends in a digit from 2 to 9.
#[derive(Clone, Copy, Debug)]
struct Ports(PortSlot);

impl Ports {
    fn of_process() -> Self {
        Self(PortBlock::IngressRecovery.of_process())
    }

    /// The archive control, response and replication ports of driver `d`.
    fn archive(self, d: u16) -> [u16; 3] {
        [12, 13, 14].map(|offset| self.0.port(offset + 3 * d))
    }
}

/// The ports and the UDP channels of one ingress replica: a dynamic MDC
/// publication of each lane, on a control port of its own, its receipt
/// endpoint, its RPC and metrics ports, and the refetch endpoints of the
/// pipeline lane that reads it.
#[derive(Clone, Debug)]
pub(crate) struct Channels {
    tx_data_template: String,
    receipts_port: u16,
    pub(crate) rpc_port: u16,
    pub(crate) metrics_port: u16,
    refetch_ports: [u16; 2],
}

impl Channels {
    /// The ports and channels of replica `id`. The lane id fills one
    /// digit of each control port. See [`Ports`].
    fn new(ports: Ports, id: u16) -> Self {
        let Ports(slot) = ports;
        let hundreds = slot.hundreds();
        Self {
            tx_data_template: format!(
                "aeron:udp?control=127.0.0.1:{hundreds}{{sid}}{id}|control-mode=dynamic"
            ),
            receipts_port: slot.port(6 + id),
            rpc_port: slot.port(2 + id),
            metrics_port: slot.port(4 + id),
            refetch_ports: [22, 23].map(|offset| slot.port(offset + 2 * id)),
        }
    }

    /// The refetch response and replay endpoints of the pipeline lane
    /// that reads this replica.
    fn refetch_endpoints(&self) -> [String; 2] {
        self.refetch_ports.map(|port| format!("127.0.0.1:{port}"))
    }

    fn defaults() -> ChannelsConfig {
        ChannelsConfig::default()
    }

    fn receipts_channel(&self) -> String {
        format!("aeron:udp?endpoint=127.0.0.1:{}", self.receipts_port)
    }

    /// The pipeline's subscription to lane 0 of this replica.
    fn subscriber(&self, rt: &AeronRuntime) -> TxDataSubscriberHandle {
        let channels = Self::defaults();
        TxDataSubscriberHandle::from_subscription(
            rt.open_tx_data_subscription_with_joins(
                &self.tx_data_template.replace("{sid}", "0"),
                channels.tx_data_stream_id(0),
            )
            .unwrap(),
        )
    }

    /// The pipeline's receipt publication to this replica.
    fn receipts_publisher(&self, rt: &AeronRuntime) -> PubHandle {
        rt.open_publication(
            &self.receipts_channel(),
            Self::defaults().tx_receipts_stream_id,
        )
        .unwrap()
    }
}

/// The scratch directory of one test; removed on drop. A failed test
/// keeps it and prints the tail of each process log, because a CI job
/// keeps only the output of the test.
struct Scratch(PathBuf);

/// The lines of each process log that a failed test prints.
const LOG_TAIL: usize = 80;

impl Scratch {
    /// Print the last [`LOG_TAIL`] lines of each log in the directory to
    /// stderr, then a closing line: a runner that shows only the end of a
    /// failed suite then still points to the failure message.
    fn print_log_tails(&self) {
        let mut logs: Vec<PathBuf> = std::fs::read_dir(&self.0)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "log"))
            .collect();
        logs.sort();
        for path in &logs {
            Self::print_tail(path);
        }
        let _ = writeln!(
            std::io::stderr(),
            "==> the failure message is above the log tails"
        );
    }

    /// Print the last [`LOG_TAIL`] lines of the log at `path`. A write
    /// error is ignored: a panic in this unwind would abort the process.
    fn print_tail(path: &Path) {
        let bytes = std::fs::read(path).unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        let lines: Vec<&str> = text.lines().collect();
        let tail = lines[lines.len().saturating_sub(LOG_TAIL)..].join("\n");
        let _ = writeln!(std::io::stderr(), "==> {}\n{tail}", path.display());
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.print_log_tails();
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// Two nodes: ingress 0 on driver A; ingress 1 and the pipeline on driver
/// B. Field order is drop order: the replicas stop before their drivers.
struct Fixture {
    replicas: Vec<Replica>,
    /// Held for its drop: the stub stops with the fixture.
    _stub: QueryStub,
    pipeline: Pipeline,
    drivers: Vec<Driver>,
    /// Held for its drop: the scratch directory goes last.
    _scratch: Scratch,
    started: Instant,
}

impl Fixture {
    async fn start(name: &str) -> Self {
        let started = Instant::now();
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("ingress-recovery-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let ports = Ports::of_process();
        let drivers = vec![
            Driver::start(&root, "node-a", ports.archive(0)),
            Driver::start(&root, "node-b", ports.archive(1)),
        ];
        let channels = [Channels::new(ports, 0), Channels::new(ports, 1)];
        let archives: Vec<String> = drivers.iter().map(Driver::archive_endpoint).collect();
        let pipeline = Pipeline::start(&drivers[1].aeron_dir(), &channels, &archives);
        let stub = QueryStub::start(pipeline.ledger.clone()).await;
        let mut replicas: Vec<Replica> = channels
            .into_iter()
            .zip(0u16..)
            .map(|(c, id)| Replica::new(id, &root, c, &stub.url))
            .collect();
        replicas[0].start(&drivers[0]);
        replicas[1].start(&drivers[1]);
        let fixture = Self {
            replicas,
            _stub: stub,
            pipeline,
            drivers,
            _scratch: Scratch(root),
            started,
        };
        fixture.await_live(0).await;
        fixture.await_live(1).await;
        fixture
    }

    fn urls(&self) -> Vec<String> {
        self.replicas.iter().map(Replica::url).collect()
    }

    fn log(&self, line: &str) {
        eprintln!("+{:.1}s {line}", self.started.elapsed().as_secs_f64());
    }

    /// Wait until replica `i` answers `eth_chainId`.
    async fn await_live(&self, i: usize) -> Duration {
        let client = load::client(&self.replicas[i].url());
        let started = Instant::now();
        tokio::time::timeout(RESTART_BOUND, async {
            while jsonrpsee::core::client::ClientT::request::<String, _>(
                &client,
                "eth_chainId",
                jsonrpsee::rpc_params![],
            )
            .await
            .is_err()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("ingress {i} does not answer within {RESTART_BOUND:?}"));
        started.elapsed()
    }

    /// Start a client through replica `i`, and wait until it lands.
    async fn load(&self, i: usize) -> Load {
        let load = Load::start(&self.replicas[i].url(), &self.urls());
        load.await_above(2, Duration::from_secs(20)).await;
        load
    }

    /// Restart replica `i` on its driver and wait until it answers.
    async fn restart(&mut self, i: usize) {
        let driver = usize::from(i != 0);
        self.replicas[i].start(&self.drivers[driver]);
        let took = self.await_live(i).await;
        self.log(&format!(
            "ingress {i} answers {:.1}s after its start",
            took.as_secs_f64()
        ));
    }

    /// The checks that end the case: no loss, each transfer executed once
    /// in nonce order, and every receipt on every ingress.
    async fn judge(&self, what: &str, sender: &PrivateKeySigner, report: &Report) {
        assert_eq!(report.lost, None, "{what}: a transfer never landed");
        let executed = self.pipeline.executed_by(sender.address());
        let nonces: Vec<u64> = executed.iter().map(|e| e.nonce).collect();
        let hashes: Vec<_> = executed.iter().map(|e| e.hash).collect();
        let expected: Vec<u64> = (0..u64::try_from(report.landed.len()).unwrap()).collect();
        assert_eq!(
            nonces, expected,
            "{what}: lost, doubled or reordered nonces"
        );
        assert_eq!(hashes, report.landed, "{what}: executed another transfer");
        for url in self.urls() {
            Self::assert_serves(what, &url, report).await;
        }
        self.log(&format!(
            "{what}: {} landed in {} submits, {} copies absorbed",
            report.landed.len(),
            report.submits,
            self.pipeline.ledger.lock().unwrap().copies
        ));
    }

    async fn assert_serves(what: &str, url: &str, report: &Report) {
        let reader = load::client(url);
        for hash in &report.landed {
            assert!(
                load::receipt(&reader, *hash).await,
                "{what}: {url} serves no receipt for {hash}"
            );
        }
    }
}

/// Stop ingress `victim` while the client sends through ingress 0, then
/// start it on its persisted node.
async fn stop_and_return(name: &str, victim: usize, stop: Stop) {
    let mut fx = Fixture::start(name).await;
    let load = fx.load(0).await;
    let status = fx.replicas[victim].stop(stop);
    fx.log(&format!("ingress {victim} stopped ({stop:?}): {status}"));
    if stop == Stop::Graceful {
        assert!(status.success(), "{name}: the drain ends with a clean exit");
    }
    fx.restart(victim).await;
    let landed = load.landed();
    let again = load.await_above(landed, RESTART_BOUND).await;
    fx.log(&format!(
        "the client lands again {:.1}s after the return",
        again.as_secs_f64()
    ));
    let sender = load.sender.clone();
    let report = load.finish().await;
    fx.judge(name, &sender, &report).await;
    println!(
        "INGRESS_RECOVERY {name} seconds={:.1}",
        fx.started.elapsed().as_secs_f64()
    );
}

/// How long the driver of the pipeline holds before a returned ingress
/// answers: the ingress publishes its first submits to its own archive
/// before the pipeline joins its new session.
const LATE_JOIN_HOLD: Duration = Duration::from_secs(4);

/// `late-join`: the archiving media driver of ingress 0 dies, and ingress
/// 0 returns on a new driver while the driver of the pipeline holds. The
/// new publication connects to its own archive first, so the pipeline
/// joins the new session after its first frames. A client that waits for
/// its first transfer sends nothing more, so no frame follows the join.
/// The pipeline must refetch the frames before its join from the archive
/// of ingress 0: every transfer of the client lands, once.
///
/// An ingress process that returns on the same driver keeps its session:
/// the driver shares the publication between its clients. So only a new
/// driver gives the race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real ingress processes and drivers; run the ingress-recovery component target"]
async fn a_late_join_to_a_returned_ingress_refetches_its_first_transfers() {
    let name = "late-join";
    let mut fx = Fixture::start(name).await;
    let case = fx.load(0).await;
    fx.drivers[0].kill();
    let exit = fx.replicas[0].await_exit(Duration::from_secs(30));
    fx.log(&format!("driver A killed; ingress 0 exit: {exit:?}"));
    let took = fx.drivers[0].restart(Duration::from_secs(60));
    fx.log(&format!(
        "driver A returns after {:.1}s",
        took.as_secs_f64()
    ));
    fx.drivers[1].freeze_for(LATE_JOIN_HOLD);
    fx.log(&format!("driver B holds for {LATE_JOIN_HOLD:?}"));
    fx.restart(0).await;
    let landed = case.landed();
    let again = case.await_above(landed, RESTART_BOUND).await;
    fx.log(&format!(
        "the client lands again {:.1}s after the return",
        again.as_secs_f64()
    ));
    let sender = case.sender.clone();
    let report = case.finish().await;
    fx.judge(name, &sender, &report).await;
    println!(
        "INGRESS_RECOVERY {name} seconds={:.1}",
        fx.started.elapsed().as_secs_f64()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real ingress processes and drivers; run the ingress-recovery component target"]
async fn a_graceful_stop_drains_and_the_returned_ingress_serves_every_receipt() {
    stop_and_return("graceful-ingress", 0, Stop::Graceful).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real ingress processes and drivers; run the ingress-recovery component target"]
async fn a_hard_killed_case_ingress_returns_with_no_lost_or_doubled_transfer() {
    stop_and_return("hard-ingress", 0, Stop::Hard).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real ingress processes and drivers; run the ingress-recovery component target"]
async fn a_hard_killed_twin_returns_and_serves_receipts_that_it_never_saw() {
    stop_and_return("hard-ingress-twin", 1, Stop::Hard).await;
}

/// `archive-driver-loss`: the archiving media driver of ingress 0 dies.
/// Ingress 0 exits on its own after its driver timeout, while the twin
/// on the other node keeps serving. The driver returns on its archive,
/// then ingress 0 returns and records again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real ingress processes and drivers; run the ingress-recovery component target"]
async fn a_lost_archiving_driver_ends_its_ingress_while_the_twin_serves() {
    let mut fx = Fixture::start("archive-driver-loss").await;
    let case = fx.load(0).await;
    let twin = fx.load(1).await;
    fx.drivers[0].kill();
    fx.log("driver A killed");
    let exit = fx.replicas[0].await_exit(Duration::from_secs(30));
    fx.log(&format!("ingress 0 exit after the driver loss: {exit:?}"));
    assert!(
        exit.is_some_and(|s| !s.success()),
        "ingress 0 must exit on its own after its driver timeout: {exit:?}"
    );
    let served = twin.landed();
    twin.await_above(served + 3, Duration::from_secs(10)).await;
    let took = fx.drivers[0].restart(Duration::from_secs(60));
    fx.log(&format!(
        "driver A returns after {:.1}s",
        took.as_secs_f64()
    ));
    fx.restart(0).await;
    let landed = case.landed();
    case.await_above(landed, RESTART_BOUND).await;
    let (case_sender, twin_sender) = (case.sender.clone(), twin.sender.clone());
    let reports = [case.finish().await, twin.finish().await];
    fx.judge(
        "archive-driver-loss: case client",
        &case_sender,
        &reports[0],
    )
    .await;
    fx.judge(
        "archive-driver-loss: twin client",
        &twin_sender,
        &reports[1],
    )
    .await;
    println!(
        "INGRESS_RECOVERY archive-driver-loss seconds={:.1}",
        fx.started.elapsed().as_secs_f64()
    );
}

/// No two ports of the fixture share an offset of its slot, for up to
/// ten lanes.
#[test]
fn the_ports_of_the_fixture_do_not_overlap() {
    let ports = Ports::of_process();
    let Ports(slot) = ports;
    let controls = (0..10).flat_map(|lane| (0..2).map(move |r| slot.port(10 * lane + r)));
    let replicas = (0..2).map(|id| Channels::new(ports, id)).flat_map(|c| {
        let [response, replay] = c.refetch_ports;
        [
            c.receipts_port,
            c.rpc_port,
            c.metrics_port,
            response,
            replay,
        ]
    });
    let drivers = (0..2).flat_map(|d| ports.archive(d));
    let all: Vec<u16> = controls.chain(replicas).chain(drivers).collect();
    let mut unique = all.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), all.len(), "two ports overlap: {all:?}");
}
