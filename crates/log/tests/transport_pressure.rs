//! Real transport recovery with independently stalled output streams.

#![cfg(all(feature = "docker-e2e", target_os = "linux"))]

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{AeronRuntime, PubHandle, RawFrame};
use kardamom_log::testing::AeronTestCluster;
use rkyv::util::AlignedVec;
use serde_json::json;
use tokio::sync::mpsc::UnboundedReceiver;

struct Actor {
    live: PubHandle,
    blocked: PubHandle,
    input: UnboundedReceiver<RawFrame>,
    runtime: AeronRuntime,
    channel: String,
    stream: i32,
}

impl Actor {
    fn new(dir: &str, index: usize, peer: &AeronRuntime) -> Self {
        let runtime = AeronRuntime::spawn_with_dir(dir).unwrap();
        let stream = 9000_i32.checked_add(i32::try_from(index).unwrap()).unwrap();
        let port = 45000_usize.checked_add(index).unwrap();
        let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}|term-length=65536");
        let (_, input) = runtime.open_subscription_raw(&channel, stream).unwrap();
        let live = peer.open_publication(&channel, stream).unwrap();
        let blocked = runtime
            .open_publication(&channel, stream.checked_add(1000).unwrap())
            .unwrap();
        Self {
            live,
            blocked,
            input,
            runtime,
            channel,
            stream,
        }
    }

    fn frame(sequence: u64) -> AlignedVec {
        let mut bytes = AlignedVec::new();
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&[0x5a; 1016]);
        bytes
    }

    async fn deliver(&mut self, sequence: u64) -> Duration {
        let started = Instant::now();
        self.live
            .publish_bytes_async(Self::frame(sequence))
            .await
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_millis(1500), self.input.recv())
            .await
            .expect("blocked outputs must not starve a live subscription")
            .unwrap();
        assert_eq!(frame.bytes, Self::frame(sequence).as_slice());
        started.elapsed()
    }

    async fn quiet_delivery(&mut self, sequence: u64) -> Duration {
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.deliver(sequence).await
    }

    async fn recover(&self) -> Duration {
        let publisher = self.blocked.clone();
        let pending =
            tokio::spawn(async move { publisher.publish_bytes_async(Self::frame(99999)).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!pending.is_finished());
        let started = Instant::now();
        let (_, mut input) = self
            .runtime
            .open_subscription_raw(&self.channel, self.stream.checked_add(1000).unwrap())
            .unwrap();
        pending.await.unwrap().unwrap();
        let frame = tokio::time::timeout(Duration::from_millis(1500), input.recv())
            .await
            .expect("output must recover when its subscriber returns")
            .unwrap();
        assert_eq!(frame.bytes, Self::frame(99999).as_slice());
        started.elapsed()
    }
}

struct Sample {
    started: Instant,
    ticks: u64,
}

impl Sample {
    fn ticks() -> u64 {
        let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
        let fields: Vec<_> = stat
            .rsplit_once(") ")
            .unwrap()
            .1
            .split_whitespace()
            .collect();
        let user: u64 = fields[11].parse().unwrap();
        let system: u64 = fields[12].parse().unwrap();
        user.checked_add(system).unwrap()
    }

    fn start() -> Self {
        Self {
            started: Instant::now(),
            ticks: Self::ticks(),
        }
    }

    fn report(self, phase: &str, mut latencies: Vec<Duration>) {
        let ticks = Self::ticks().checked_sub(self.ticks).unwrap();
        latencies.sort_unstable();
        let percentile = |p: usize| {
            latencies
                .get(latencies.len().checked_mul(p).unwrap() / 100)
                .map(Duration::as_secs_f64)
        };
        println!(
            "TRANSPORT_SAMPLE {}",
            json!({
                "phase":phase,"seconds":self.started.elapsed().as_secs_f64(),
                "process_cpu_ticks":ticks,"messages":latencies.len(),
                "p50_seconds":percentile(50),"p95_seconds":percentile(95),"p99_seconds":percentile(99)
            })
        );
    }
}

struct Scratch(Option<PathBuf>);

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Some(root) = &self.0 {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

struct Driver {
    cluster: AeronTestCluster,
    _scratch: Scratch,
}

impl Driver {
    async fn new() -> Self {
        kardamom_log::testing::require_docker().await;
        let cluster = AeronTestCluster::single_node().await.unwrap();
        let remove = std::env::var("TESTCONTAINERS_COMMAND").as_deref() != Ok("keep");
        let scratch = Scratch(
            std::env::var("KARDAMOM_AERON_DIR")
                .err()
                .filter(|_| remove)
                .map(|_| cluster.aeron_dir_host(0).parent().unwrap().to_path_buf()),
        );
        Self {
            cluster,
            _scratch: scratch,
        }
    }
}

struct Fixture {
    // Fields close runtime threads before the driver and remove its files last.
    actors: Vec<Actor>,
    _peer: AeronRuntime,
    _driver: Driver,
    frames: NonZeroU64,
}

impl Fixture {
    async fn new() -> Self {
        let driver = Driver::new().await;
        let dir = driver.cluster.aeron_dir_host(0).to_str().unwrap();
        let peer = AeronRuntime::spawn_with_dir(dir).unwrap();
        let count: NonZeroUsize = std::env::var("KARDAMOM_PROFILE_RUNTIMES")
            .unwrap_or_else(|_| "8".into())
            .parse()
            .unwrap();
        assert!(count.get() <= 64, "fixture supports at most 64 runtimes");
        let frames: NonZeroU64 = std::env::var("KARDAMOM_PROFILE_FRAMES")
            .unwrap_or_else(|_| "256".into())
            .parse()
            .unwrap();
        assert!(frames.get() <= 4096);
        Self {
            frames,
            actors: (0..count.get())
                .map(|index| Actor::new(dir, index, &peer))
                .collect(),
            _peer: peer,
            _driver: driver,
        }
    }

    async fn traffic(&mut self, phase: &str) {
        let sample = Sample::start();
        let mut latencies = Vec::new();
        for sequence in 1..=self.frames.get() {
            latencies.push(self.actors[0].deliver(sequence).await);
        }
        sample.report(phase, latencies);
    }

    async fn quiet_arrivals(&mut self, phase: &str) {
        let sample = Sample::start();
        let mut latencies = Vec::new();
        for sequence in 100_000..100_064 {
            latencies.push(self.actors[0].quiet_delivery(sequence).await);
        }
        sample.report(phase, latencies);
    }

    async fn run(&mut self) {
        for actor in &mut self.actors {
            actor.deliver(0).await;
        }
        let idle = Sample::start();
        tokio::time::sleep(Duration::from_secs(3)).await;
        idle.report("idle", Vec::new());
        self.quiet_arrivals("quiet_arrivals_connected").await;
        self.traffic("connected").await;
        let blocked: Vec<_> = self
            .actors
            .iter()
            .map(|actor| {
                let publisher = actor.blocked.clone();
                tokio::spawn(async move { publisher.publish_bytes_async(Actor::frame(999)).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(blocked.iter().all(|task| !task.is_finished()));
        let quiet = Sample::start();
        tokio::time::sleep(Duration::from_secs(2)).await;
        quiet.report("outputs_unavailable_idle", Vec::new());
        self.quiet_arrivals("quiet_arrivals_outputs_unavailable")
            .await;
        self.traffic("outputs_unavailable").await;
        assert!(
            blocked.iter().all(|task| !task.is_finished()),
            "fault must persist through the delivery sample"
        );
        for task in blocked {
            let error = task.await.unwrap().unwrap_err();
            assert!(error.to_string().contains("NOT_CONNECTED (-1)"), "{error}");
        }
        let recovery = Sample::start();
        let mut latencies = Vec::new();
        for actor in &self.actors {
            latencies.push(actor.recover().await);
        }
        recovery.report("output_rejoin", latencies);
        self.traffic("recovered").await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux Docker and one disposable Aeron media driver"]
async fn unavailable_outputs_preserve_live_delivery_and_recover() {
    let mut fixture = Fixture::new().await;
    tokio::time::timeout(Duration::from_secs(45), fixture.run())
        .await
        .unwrap();
}
