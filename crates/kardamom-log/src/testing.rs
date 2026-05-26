//! In-memory pub/sub fakes used by other crates' unit tests.
//!
//! Gated behind `#[cfg(any(test, feature = "testing"))]`. Importers add this
//! crate as `kardamom-log = { workspace = true, features = ["testing"] }`
//! to `[dev-dependencies]`.
//!
//! These fakes are intentionally simple: an `Arc<Mutex<Vec<bytes>>>` per
//! stream id, no Aeron involvement. They preserve **per-publisher FIFO**
//! order (sufficient for unit-testing components that consume the channel)
//! but do not model Aeron's concurrent-pub interleaving. Tests that need to
//! verify behavior under realistic interleaving go through the real Aeron
//! Docker harness (`docker_e2e.rs`), not these fakes.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::error::LogError;
use kardamom_types::{BPosition, FsyncWatermark};

/// Map from `(channel, stream_id)` to shared per-stream state.
type StreamMap = HashMap<(String, i32), Arc<Mutex<StreamState>>>;

/// In-memory bus shared by all `FakePublication` / `FakeSubscription` handles
/// that target the same `(channel, stream_id)` pair. Clone is cheap (an `Arc`).
#[derive(Clone, Default)]
pub struct FakeBus {
    streams: Arc<Mutex<StreamMap>>,
}

#[derive(Default)]
struct StreamState {
    /// Append-only log of (offset, bytes). Subscribers track their read cursor.
    log: Vec<(i64, Vec<u8>)>,
    /// Next byte offset (mimics Aeron's stream position).
    next_offset: i64,
}

impl FakeBus {
    pub fn new() -> Self {
        Self::default()
    }

    fn stream(&self, channel: &str, stream_id: i32) -> Arc<Mutex<StreamState>> {
        let mut g = self.streams.lock().unwrap();
        g.entry((channel.to_string(), stream_id))
            .or_insert_with(|| Arc::new(Mutex::new(StreamState::default())))
            .clone()
    }
}

/// Drop-in for `rusteron_client::AeronPublication` (the shared / concurrent
/// variant) in tests. Named `Concurrent` historically — keep the name so
/// existing test code doesn't churn.
pub struct FakeConcurrentPublication {
    state: Arc<Mutex<StreamState>>,
}

impl FakeConcurrentPublication {
    pub fn offer(&self, bytes: &[u8]) -> i64 {
        let mut g = self.state.lock().unwrap();
        let off = g.next_offset;
        g.log.push((off, bytes.to_vec()));
        g.next_offset += bytes.len() as i64;
        g.next_offset
    }

    /// Encode `msg` with rkyv, append it to the stream, and return the
    /// fragment's *start* `BPosition` (matches the real publisher's
    /// convention used by [`crate::publisher::offer`]).
    fn publish<T>(&self, msg: &T) -> Result<BPosition, LogError>
    where
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        let bytes =
            rkyv::to_bytes::<rancor::Error>(msg).map_err(|e| LogError::Codec(e.to_string()))?;
        let off = self.offer(bytes.as_slice());
        let frag_start = off - bytes.len() as i64;
        let header = FakeHeader::from_offset(frag_start);
        Ok(BPosition {
            term_id: header.term_id(),
            term_offset: header.term_offset(),
        })
    }
}

/// Mimics `rusteron_client::Header` enough for our consumers.
#[derive(Clone, Copy, Debug)]
pub struct FakeHeader {
    term_id: i32,
    term_offset: i32,
}

impl FakeHeader {
    pub fn from_offset(off: i64) -> Self {
        const TERM_LEN: i64 = 16 * 1024 * 1024;
        Self {
            term_id: (off / TERM_LEN) as i32,
            term_offset: (off % TERM_LEN) as i32,
        }
    }
    pub fn term_id(&self) -> i32 {
        self.term_id
    }
    pub fn term_offset(&self) -> i32 {
        self.term_offset
    }
}

/// Drop-in for `rusteron_client::Subscription` in tests.
pub struct FakeSubscription {
    state: Arc<Mutex<StreamState>>,
    cursor: usize,
}

impl FakeSubscription {
    /// Mirrors the real subscriber's `poll(&mut self, callback, fragment_limit)`.
    pub fn poll<F: FnMut(&[u8], FakeHeader)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        let g = self.state.lock().unwrap();
        let mut delivered = 0;
        while delivered < fragment_limit && self.cursor < g.log.len() {
            let (off, ref bytes) = g.log[self.cursor];
            let header = FakeHeader::from_offset(off);
            f(bytes.as_slice(), header);
            self.cursor += 1;
            delivered += 1;
        }
        delivered
    }
}

/// High-level fake publication that consumers can use in place of
/// `TxOrderingPublisher` / `ChannelCPublisher` / `ReceiptCachePublisher`.
pub struct FakePublication {
    pub_handle: FakeConcurrentPublication,
}

impl FakePublication {
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            pub_handle: FakeConcurrentPublication {
                state: bus.stream(channel, stream_id),
            },
        }
    }

    pub fn publish<T>(&self, msg: &T) -> Result<BPosition, LogError>
    where
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        self.pub_handle.publish(msg)
    }
}

/// High-level fake subscription for owned-value reads.
pub struct FakeTypedSubscription<T> {
    sub: FakeSubscription,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FakeTypedSubscription<T>
where
    T: Archive + 'static,
    T::Archived: Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            sub: FakeSubscription {
                state: bus.stream(channel, stream_id),
                cursor: 0,
            },
            _marker: std::marker::PhantomData,
        }
    }

    pub fn poll<F: FnMut(T, BPosition)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: FakeHeader| {
                if let Ok(v) = rkyv::from_bytes::<T, rancor::Error>(bytes) {
                    f(
                        v,
                        BPosition {
                            term_id: header.term_id(),
                            term_offset: header.term_offset(),
                        },
                    );
                }
            },
            fragment_limit,
        )
    }
}

// ============================================================================
// TxData and TxOrdering typed fakes.
//
// Per:
//   - TxData[i] is an Aeron *exclusive* publication carrying full
//     TxEnvelopes. M of them (one per sequencer), each its own stream.
//   - TxOrdering is the canonical orderer: Aeron *concurrent* multi-publisher
//     carrying TxOrderingMessage records (TxRef | BoundaryStart), ~16-32 B.
//
// At the in-memory-fake level, "exclusive vs concurrent" collapses (we only
// model one publisher at a time, so there is no CAS-cursor contention to
// simulate). The distinction is *type-level*: the tx_data pub/sub pair is
// parameterised by `TxEnvelope` and constructs single-producer streams keyed
// by `sequencer_id`; the tx_ordering pub/sub pair is parameterised by
// `TxOrderingMessage` and shares one stream. Tests that need to verify real
// concurrent-pub interleaving must use the docker e2e harness.
// ============================================================================

use kardamom_types::{TxEnvelope, TxOrderingMessage, TxRef};

/// TxData[i]: per-sequencer **exclusive** publication of full
/// `TxEnvelope` bytes. In production this maps to
/// `Aeron::add_exclusive_publication`; the fake just preserves FIFO.
///
/// `FakeChannelAPublication::new` requires the caller to hand in the
/// `sequencer_id`. The fake keeps the id so callers can build a `TxRef`
/// pointing at the produced `BPosition` without having to track the id
/// separately.
pub struct FakeChannelAPublication {
    sequencer_id: u8,
    pub_handle: FakeConcurrentPublication,
}

impl FakeChannelAPublication {
    /// Open the tx_data[i] pub on `bus`, using the channel URI/stream-id
    /// convention "<channel>"/"<stream_id>". A real wiring uses
    /// `kardamom_log::config::ChannelsConfig::tx_dattx_dattx_dattx_dattx_dattx_dattx_dattx_dattx_data_channel_template` to derive
    /// per-sequencer URIs.
    pub fn open(bus: &FakeBus, sequencer_id: u8, channel: &str, stream_id: i32) -> Self {
        Self {
            sequencer_id,
            pub_handle: FakeConcurrentPublication {
                state: bus.stream(channel, stream_id),
            },
        }
    }

    pub fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    /// Publish a `TxEnvelope` and return its fragment-start `BPosition` on
    /// tx_data[i]. Sequencers pass this `BPosition` into [`TxRef::new`]
    /// before writing to tx_ordering.
    pub fn publish(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        self.pub_handle.publish(env)
    }
}

/// TxData[i]: per-sequencer subscription returning `(BPosition, TxEnvelope)`.
///
/// Executors run M+1 of these (one per tx_data) plus one tx_ordering
/// subscription. Per they buffer A messages keyed by `BPosition`
/// until the corresponding `TxRef` arrives on tx_ordering.
pub struct FakeTxDataSubscription {
    sub: FakeSubscription,
}

impl FakeTxDataSubscription {
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            sub: FakeSubscription {
                state: bus.stream(channel, stream_id),
                cursor: 0,
            },
        }
    }

    /// Poll and invoke `f` with `(BPosition, TxEnvelope)` per fragment.
    /// `BPosition` is the fragment *start* — the same value a sequencer
    /// emitted as `TxRef.tx_data_position`.
    pub fn poll<F: FnMut(BPosition, TxEnvelope)>(
        &mut self,
        mut f: F,
        fragment_limit: usize,
    ) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: FakeHeader| {
                if let Ok(env) = rkyv::from_bytes::<TxEnvelope, rancor::Error>(bytes) {
                    f(
                        BPosition {
                            term_id: header.term_id(),
                            term_offset: header.term_offset(),
                        },
                        env,
                    );
                }
            },
            fragment_limit,
        )
    }
}

/// TxOrdering: canonical orderer, **concurrent** multi-publisher carrying
/// [`TxOrderingMessage`] records (TxRef | BoundaryStart). In production this
/// maps to `Aeron::add_publication` (the shared / concurrent variant).
pub struct FakeChannelBPublication {
    pub_handle: FakeConcurrentPublication,
}

impl FakeChannelBPublication {
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            pub_handle: FakeConcurrentPublication {
                state: bus.stream(channel, stream_id),
            },
        }
    }

    /// Publish a [`TxRef`] onto tx_ordering. Returns the fragment's start
    /// position (the canonical B-position).
    pub fn publish_ref(&self, r: &TxRef) -> Result<BPosition, LogError> {
        self.publish_message(&TxOrderingMessage::TxRef(*r))
    }

    /// Publish a [`kardamom_types::BlockBoundaryStart`] onto tx_ordering
    /// (sealer-emitted). Returns the boundary's canonical position.
    pub fn publish_boundary(
        &self,
        b: &kardamom_types::BlockBoundaryStart,
    ) -> Result<BPosition, LogError> {
        self.publish_message(&TxOrderingMessage::BoundaryStart(b.clone()))
    }

    fn publish_message(&self, m: &TxOrderingMessage) -> Result<BPosition, LogError> {
        self.pub_handle.publish(m)
    }
}

/// TxOrdering subscription returning `(BPosition, TxOrderingMessage)` per
/// fragment. The B-position is the canonical L2 tx ordering identifier
/// (system invariant I1).
pub struct FakeTxOrderingSubscription {
    sub: FakeSubscription,
}

impl FakeTxOrderingSubscription {
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            sub: FakeSubscription {
                state: bus.stream(channel, stream_id),
                cursor: 0,
            },
        }
    }

    pub fn poll<F: FnMut(BPosition, TxOrderingMessage)>(
        &mut self,
        mut f: F,
        fragment_limit: usize,
    ) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: FakeHeader| {
                if let Ok(m) = rkyv::from_bytes::<TxOrderingMessage, rancor::Error>(bytes) {
                    f(
                        BPosition {
                            term_id: header.term_id(),
                            term_offset: header.term_offset(),
                        },
                        m,
                    );
                }
            },
            fragment_limit,
        )
    }
}

/// In-memory fake fsync-watermark stream: an `Arc<Mutex<HashMap<recorder_id, VecDeque>>>`.
/// Lease/aggregator tests publish into one of these and poll on the other side.
#[derive(Clone, Default)]
pub struct FakeFsyncWatermarkStream {
    inner: Arc<Mutex<HashMap<u8, VecDeque<FsyncWatermark>>>>,
}

impl FakeFsyncWatermarkStream {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&self, w: FsyncWatermark) {
        let mut g = self.inner.lock().unwrap();
        g.entry(w.recorder_id).or_default().push_back(w);
    }

    pub fn drain(&self, recorder_id: u8) -> Vec<FsyncWatermark> {
        let mut g = self.inner.lock().unwrap();
        g.get_mut(&recorder_id)
            .map(|q| q.drain(..).collect())
            .unwrap_or_default()
    }
}

// ============================================================================
// AeronTestCluster — testcontainers-driven real Aeron for e2e tests.
//
// Other crates depend on `kardamom-log` with `features = ["docker-e2e"]` and
// reuse this struct from their own `tests/` directory. Public API:
//
//   let cluster = AeronTestCluster::single_node().await?;
//   let endpoint = cluster.archive_control_endpoint(0).await;
//   // ... build LogConfig pointing at endpoint, run scenario ...
//   drop(cluster); // tears down container
//
// Gated behind `docker-e2e` (which implies `testing`) so the testcontainers
// dependency doesn't bleed into hosts without a Docker daemon.
// ============================================================================

#[cfg(feature = "docker-e2e")]
mod docker {
    use std::time::Duration;

    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage, ImageExt};

    /// Tag we use for the locally-built Aeron image.
    const AERON_IMAGE_NAME: &str = "kardamom-aeron";
    const AERON_IMAGE_TAG: &str = "test";

    /// Reusable Aeron e2e harness. Each instance owns one or more real Aeron
    /// containers and exposes the host ports the Rust code should connect to.
    pub struct AeronTestCluster {
        nodes: Vec<ContainerAsync<GenericImage>>,
    }

    impl AeronTestCluster {
        /// Bring up a single Aeron node (Media Driver + Archive in one JVM).
        pub async fn single_node() -> Result<Self, Box<dyn std::error::Error>> {
            ensure_image_built().await?;
            let image = GenericImage::new(AERON_IMAGE_NAME, AERON_IMAGE_TAG)
                .with_exposed_port(8010_u16.udp())
                .with_exposed_port(8011_u16.udp())
                .with_exposed_port(8020_u16.udp())
                .with_exposed_port(8021_u16.udp())
                .with_wait_for(WaitFor::message_on_stdout("ArchiveAgent: started"));

            let node = image.with_shm_size(256 * 1024 * 1024).start().await?;

            Ok(Self { nodes: vec![node] })
        }

        /// Bring up `n` Aeron nodes for multi-recorder tests.
        pub async fn multi_node(n: usize) -> Result<Self, Box<dyn std::error::Error>> {
            ensure_image_built().await?;
            let mut nodes = Vec::with_capacity(n);
            for _ in 0..n {
                let image = GenericImage::new(AERON_IMAGE_NAME, AERON_IMAGE_TAG)
                    .with_exposed_port(8010_u16.udp())
                    .with_exposed_port(8011_u16.udp())
                    .with_exposed_port(8020_u16.udp())
                    .with_exposed_port(8021_u16.udp())
                    .with_wait_for(WaitFor::message_on_stdout("ArchiveAgent: started"));
                let node = image.with_shm_size(256 * 1024 * 1024).start().await?;
                nodes.push(node);
            }
            Ok(Self { nodes })
        }

        /// "host:port" the test should pass as the Aeron Archive control
        /// channel endpoint for node `i`.
        pub async fn archive_control_endpoint(&self, i: usize) -> String {
            let port = self.nodes[i]
                .get_host_port_ipv4(8010_u16.udp())
                .await
                .unwrap();
            format!("127.0.0.1:{port}")
        }

        pub async fn archive_response_endpoint(&self, i: usize) -> String {
            let port = self.nodes[i]
                .get_host_port_ipv4(8011_u16.udp())
                .await
                .unwrap();
            format!("127.0.0.1:{port}")
        }

        pub fn len(&self) -> usize {
            self.nodes.len()
        }

        pub fn is_empty(&self) -> bool {
            self.nodes.is_empty()
        }

        /// Stop node `i` (simulates recorder failure for quorum tests).
        pub async fn stop(&mut self, i: usize) -> Result<(), Box<dyn std::error::Error>> {
            self.nodes[i].stop().await?;
            Ok(())
        }
    }

    /// `docker build`s the Aeron image once per test run, idempotent.
    /// Shells out to the docker CLI because testcontainers doesn't have a
    /// "build if missing" helper. Cached layers make this fast on repeat runs.
    async fn ensure_image_built() -> Result<(), Box<dyn std::error::Error>> {
        use tokio::process::Command;
        let image_ref = format!("{AERON_IMAGE_NAME}:{AERON_IMAGE_TAG}");
        let out = Command::new("docker")
            .args(["image", "inspect", &image_ref])
            .output()
            .await?;
        if out.status.success() {
            return Ok(());
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let ctx = format!("{manifest_dir}/docker/aeron");
        let status = Command::new("docker")
            .args(["build", "-t", &image_ref, &ctx])
            .status()
            .await?;
        if !status.success() {
            return Err(format!("docker build failed (status {status:?})").into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }
}

#[cfg(feature = "docker-e2e")]
pub use docker::AeronTestCluster;
