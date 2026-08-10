//! Configuration types for the log subsystem.
//!
//! Loaded from TOML at process start; passed to the supervisor and the
//! quorum aggregator. No global state.
//!
//! Every struct is `#[serde(default, deny_unknown_fields)]`: a TOML file may
//! specify any subset (e.g. only `[channels]`) and each missing field falls
//! back to its built-in default, while unknown keys are rejected so typos in
//! operator-rendered configs fail loudly. [`LogConfig::from_toml_path`] is the
//! loader the service binaries use behind `--log-config`.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::LogError;

/// Static identifier for this host's recorder. Must be unique across N recorders.
pub type RecorderId = u8;

// `Default` is derived: it composes the per-struct `Default` impls below
// (each section defaults independently, which is what lets a TOML file
// specify only `[channels]` and inherit the rest).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    pub recorder_id: RecorderId,
    pub aeron: AeronConfig,
    pub channels: ChannelsConfig,
    pub quorum: QuorumConfig,
}

impl LogConfig {
    /// Load a `LogConfig` from a TOML file. Any field the file omits falls
    /// back to [`Default`]; unknown fields are rejected. Errors carry the
    /// path so misconfigured deployments fail fast with a useful message.
    pub fn from_toml_path(path: &Path) -> Result<Self, LogError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| LogError::Config(format!("read log-config {}: {e}", path.display())))?;
        let cfg: Self = toml::from_str(&raw)
            .map_err(|e| LogError::Config(format!("parse log-config {}: {e}", path.display())))?;
        cfg.channels
            .validate()
            .map_err(|e| LogError::Config(format!("invalid log-config {}: {e}", path.display())))?;
        Ok(cfg)
    }

    /// Resolve the effective `LogConfig` for a service binary: load it from
    /// `path` if `--log-config` was supplied, otherwise use the built-in
    /// (single-host IPC) defaults. This is the single entry point every
    /// channel-using binary calls so the fallback behaviour is uniform.
    pub fn resolve(path: Option<&Path>) -> Result<Self, LogError> {
        match path {
            Some(p) => Self::from_toml_path(p),
            None => Ok(Self::default()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AeronConfig {
    /// Directory the Media Driver uses for its shared-memory ring buffers.
    /// Must be on tmpfs for low latency. Default: `/dev/shm/aeron-kardamom`.
    pub aeron_dir: PathBuf,

    /// Directory the Archive uses for its segment files.
    pub archive_dir: PathBuf,

    /// Path to the Aeron Media Driver binary (jar or native). Spawned by supervisor.
    pub media_driver_cmd: Vec<String>,

    /// Path to the Aeron Archive runner. Spawned by supervisor.
    pub archive_cmd: Vec<String>,

    /// Aeron Archive `fileSyncLevel` for segment data files.
    /// 0 = no fsync (page cache only), 1 = fdatasync per frame, 2 = fsync per frame.
    /// Default 1: per-frame fdatasync gives byte-durable recording positions on
    /// PLP NVMe, at the cost of a per-frame fdatasync round-trip.
    pub file_sync_level: u8,

    /// Aeron Archive `catalog.fileSyncLevel` for the recording catalog metadata file.
    /// Default 1: the catalog is tiny and updated infrequently, so fsync is cheap.
    pub catalog_file_sync_level: u8,

    /// Archive control **request** channel — where a client (e.g.
    /// `kardamom-recorder`) sends commands to the Archive. Defaults to `aeron:ipc`:
    /// the recorder is always co-located with its node's ArchivingMediaDriver
    /// and shares its `aeron.dir`, so control rides the local IPC channel over
    /// the shared media driver. This is both simpler and avoids the UDP control
    /// handshake (whose response can't reliably route back to a co-located
    /// client). Only the recorder connects an `AeronArchive`, so this is unused
    /// by the pipeline services.
    pub archive_control_request_channel: String,

    /// Archive control **response** channel. Also `aeron:ipc` — responses ride
    /// the shared media driver back to the recorder.
    pub archive_control_response_channel: String,

    /// Remote durability-archive control endpoints (`host:port`) whose
    /// archives record the **tx_data** streams (the ingress nodes — tx_data is
    /// multicast and every ingress archive records every publisher's shard
    /// streams, so each entry is a full mirror; consumers rotate through them
    /// on failure). Used by the join-miss archive refetch
    /// ([`crate::refetch`]); empty (the default) disables refetch.
    #[serde(default)]
    pub tx_data_archive_endpoints: Vec<String>,

    /// Same for the **tx_deposits** stream (the da-watcher's node records it).
    #[serde(default)]
    pub tx_deposits_archive_endpoints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChannelsConfig {
    /// TxData[i]: per-sequencer **exclusive** publisher of full
    /// `TxEnvelope` bytes. One stream per sequencer. URI template
    /// substitutes `{sid}` with the sequencer id (e.g.
    /// `"aeron:ipc?alias=a-{sid}"`); stream id is
    /// `tx_data_stream_id_base + sequencer_id`.
    pub tx_data_channel_template: String,
    pub tx_data_stream_id_base: i32,

    /// TxOrdering: canonical orderer carrying tiny `TxOrderingMessage`
    /// records (TxRef + sealer-emitted boundary markers). Recorded.
    ///
    /// In CLUSTER-ONLY mode the live tx_ordering transport is the Aeron
    /// Cluster (Raft) orderer, not this Aeron channel. `tx_ordering_channel`
    /// and `tx_ordering_stream_id` remain because the surviving pieces still
    /// reference them: the archive **recorder** (durable-watermark path) and
    /// the offline archive **reader** record/replay tx_ordering frames on this
    /// `tx_ordering_stream_id`, and the in-crate `TxOrderingPublisher` /
    /// `TxOrderingSubscriber` (unit-test fakes' real-Aeron counterparts) open
    /// this `tx_ordering_channel`.
    pub tx_ordering_channel: String,
    pub tx_ordering_stream_id: i32,

    /// TxReceipts: receipts + block boundaries. Not recorded.
    ///
    /// Single-host / IPC default: one shared channel (`tx_receipts_channel`)
    /// that the lone executor publishes to and ingress subscribes to directly.
    pub tx_receipts_channel: String,
    pub tx_receipts_stream_id: i32,

    /// TxReceipts MDS (multi-host fan-in). When `tx_receipts_control_channel`
    /// is non-empty, receipts use a **multi-destination subscription**: each
    /// executor replica publishes to its own UDP endpoint
    /// (`tx_receipts_endpoint_host:tx_receipts_endpoint_base_port + replica`),
    /// and ingress opens ONE `control-mode=manual` subscription
    /// (`tx_receipts_control_channel`) and attaches each executor endpoint as a
    /// destination (discovered via Consul), deduping receipts by tx hash. Empty
    /// (the default) keeps the single-channel IPC path above.
    #[serde(default)]
    pub tx_receipts_control_channel: String,
    /// Host (ingress's receive address) every executor replica sends receipts
    /// to in MDS mode. Empty unless MDS is enabled.
    #[serde(default)]
    pub tx_receipts_endpoint_host: String,
    /// Base UDP port for per-replica receipt endpoints; replica `i` uses
    /// `base_port + 2*i` (receipts) / `+ 2*i + 1` (boundaries). 0 unless MDS.
    #[serde(default)]
    pub tx_receipts_endpoint_base_port: i32,
    /// Egress/ingress NIC subnet pinned on the per-replica receipt/boundary
    /// endpoints (e.g. `192.168.56.0/24`), appended as `|interface=...`. Like
    /// every other UDP channel, the unicast endpoints MUST pin the interface or
    /// the executor's publish picks the wrong source NIC and the connection to
    /// ingress never forms (`NOT_CONNECTED` forever). Empty ⇒ no interface pin
    /// (single-host/loopback).
    #[serde(default)]
    pub tx_receipts_endpoint_interface: String,
    /// Number of executor replicas whose endpoints ingress attaches to its MDS
    /// subscription at startup (`tx_receipts_endpoint(0..N)`).
    ///
    /// This is the **static-membership fallback** for the Consul watch the MDS
    /// design ultimately wants: today ingress simply attaches replicas `0..N`
    /// once at startup. It works because the executor job is a fixed `count`
    /// with `distinct_hosts`, so replica indices are stable `0..N`. A replica
    /// that restarts keeps its index and its endpoint, so the static attach
    /// stays correct across restarts.
    ///
    /// TODO(consul-watch): register each executor as a Consul service
    /// (`executor-receipts`) and have ingress watch it, calling
    /// `add_destination`/`remove_destination` on membership change instead of
    /// this static count. Until then this must match the executor job `count`.
    /// 0 (the default) is fine when MDS is disabled.
    #[serde(default)]
    pub tx_receipts_executor_count: u32,

    /// TxErrors: sequencer-emitted rejection signals (duplicate / past-nonce
    /// today; more variants in the future). RAM only, not recorded —
    /// operational signal, not canonical state.
    pub tx_errors_channel: String,
    pub tx_errors_stream_id: i32,

    /// TxDeposits: DA watcher publishes full `Deposit` envelopes here; the M
    /// sequencers subscribe and republish a `DepositRef` onto `tx_ordering`
    /// so the canonical order interleaves L1 deposits with regular L2 txs.
    /// RAM only.
    pub tx_deposits_channel: String,
    pub tx_deposits_stream_id: i32,

    /// TxRemoteEpochs: the interop watcher publishes one `RemoteEpochRecord`
    /// per peer-chain origin block that carried cross-chain messages; the M
    /// sequencers subscribe and republish each verbatim onto `tx_ordering` as
    /// a remote-origin-advancing record. `tx_deposits` for a peer Kardamom
    /// chain instead of L1 — separate stream because the two origins advance
    /// independently and a stalled peer must not hold up L1 deposits. RAM
    /// only (`docs/specs/interop-outbox-messaging-spec.md`).
    pub tx_remote_epochs_channel: String,
    pub tx_remote_epochs_stream_id: i32,

    /// TxBal: the per-block BAL (Block Access List — the executor's
    /// `BlockDelta`: account/storage/code mutations + receipts for a sealed
    /// block). EVERY executor replica publishes one `BlockDelta` per block on
    /// this same group/stream (validators may see one copy per replica —
    /// harmless: inserts are idempotent overwrites keyed by block number);
    /// validators subscribe and cross-check their independent re-execution
    /// against it. RAM only.
    pub tx_bal_channel: String,
    pub tx_bal_stream_id: i32,

    /// TxOrdering per-recorder fsync watermark publication, parameterized by
    /// recorder_id. e.g. "aeron:ipc?alias=fsync-wm-b-{rid}".
    pub fsync_watermark_channel_template: String,
    pub fsync_watermark_stream_id: i32,

    /// TxData per-sequencer fsync watermark publication.: each
    /// tx_data has its own fsync sidecar publishing
    /// `fsynced_tx_data_position[i]` to its own watermark stream. URI template
    /// substitutes `{sid}` with the sequencer id. Stream id is
    /// `fsync_watermark_tx_data_stream_id_base + sequencer_id`.
    pub fsync_watermark_tx_data_channel_template: String,
    pub fsync_watermark_tx_data_stream_id_base: i32,

    /// Aggregated quorum watermark (tx_ordering).
    pub quorum_watermark_channel: String,
    pub quorum_watermark_stream_id: i32,
}

impl ChannelsConfig {
    /// TxData[i] URI for a given sequencer (`{sid}` substituted).
    pub fn tx_data_channel(&self, sequencer_id: u8) -> String {
        self.tx_data_channel_template
            .replace("{sid}", &sequencer_id.to_string())
    }

    /// True when receipts use the multi-destination-subscription (fan-in) path
    /// — i.e. a `tx_receipts_control_channel` is configured. False = the
    /// single-channel IPC default (`tx_receipts_channel`).
    pub fn tx_receipts_mds_enabled(&self) -> bool {
        !self.tx_receipts_control_channel.is_empty()
    }

    /// Cross-field invariants serde cannot express; called by
    /// [`LogConfig::from_toml_path`] so a misconfigured deployment fails at
    /// load time with a message instead of misbehaving later.
    ///
    /// MDS receipt fan-in computes per-replica ports as `base + 2*i (+ 1)` in
    /// UNSIGNED arithmetic — a zero/negative base (the field stays `i32` for
    /// TOML-friendly signed parsing) would otherwise wrap silently into a
    /// nonsense endpoint port, and a base too close to 65535 would overflow
    /// the valid port range for the highest replica.
    pub fn validate(&self) -> Result<(), String> {
        if self.tx_receipts_mds_enabled() {
            let base = self.tx_receipts_endpoint_base_port;
            let highest = i64::from(base) + 2 * i64::from(self.tx_receipts_executor_count) + 1;
            if base <= 0 || highest > i64::from(u16::MAX) {
                return Err(format!(
                    "tx_receipts_endpoint_base_port ({base}) invalid with MDS enabled \
                     (tx_receipts_control_channel set): need 0 < base and \
                     base + 2*tx_receipts_executor_count + 1 <= 65535"
                ));
            }
        }
        Ok(())
    }

    /// The UDP endpoint executor replica `replica_idx` publishes its RECEIPT
    /// stream (`tx_receipts_stream_id`) to — and that ingress attaches as an MDS
    /// destination on its receipt subscription. Single source of truth on both
    /// sides. `None` when MDS is disabled.
    ///
    /// Receipts and boundaries get DISTINCT ports (interleaved `base + 2*r` /
    /// `base + 2*r + 1`): a `control-mode=manual` subscription *binds* its
    /// destination's UDP socket, and ingress runs two manual subs (receipts +
    /// boundaries), so a shared endpoint would collide ("Address already in use").
    pub fn tx_receipts_endpoint(&self, replica_idx: u32) -> Option<String> {
        if !self.tx_receipts_mds_enabled() {
            return None;
        }
        Some(self.tx_receipts_uri(self.tx_receipts_endpoint_base_port as u32 + 2 * replica_idx))
    }

    /// Build a per-replica receipt/boundary endpoint URI, pinning the egress NIC
    /// via `|interface=...` when configured (required multi-host — see
    /// `tx_receipts_endpoint_interface`).
    fn tx_receipts_uri(&self, port: u32) -> String {
        let mut uri = format!(
            "aeron:udp?endpoint={}:{port}",
            self.tx_receipts_endpoint_host
        );
        if !self.tx_receipts_endpoint_interface.is_empty() {
            uri.push_str("|interface=");
            uri.push_str(&self.tx_receipts_endpoint_interface);
        }
        uri
    }

    /// The UDP endpoint executor replica `replica_idx` publishes its block-
    /// BOUNDARY side-stream (`tx_receipts_stream_id + 1`) to — distinct from the
    /// receipt endpoint (see [`tx_receipts_endpoint`](Self::tx_receipts_endpoint))
    /// so the two ingress manual subscriptions don't bind the same socket.
    pub fn tx_receipts_boundary_endpoint(&self, replica_idx: u32) -> Option<String> {
        if !self.tx_receipts_mds_enabled() {
            return None;
        }
        Some(self.tx_receipts_uri(self.tx_receipts_endpoint_base_port as u32 + 2 * replica_idx + 1))
    }

    /// TxData[i] stream id (`tx_data_stream_id_base + sequencer_id`).
    pub fn tx_data_stream_id(&self, sequencer_id: u8) -> i32 {
        self.tx_data_stream_id_base + sequencer_id as i32
    }

    /// Per-recorder tx_ordering fsync watermark URI (`{rid}` substituted).
    pub fn fsync_watermark_channel(&self, recorder_id: u8) -> String {
        self.fsync_watermark_channel_template
            .replace("{rid}", &recorder_id.to_string())
    }

    /// Per-sequencer tx_data fsync watermark URI (`{sid}` substituted).
    pub fn fsync_watermark_tx_data_channel(&self, sequencer_id: u8) -> String {
        self.fsync_watermark_tx_data_channel_template
            .replace("{sid}", &sequencer_id.to_string())
    }

    /// Per-sequencer tx_data fsync watermark stream id.
    pub fn fsync_watermark_tx_data_stream_id(&self, sequencer_id: u8) -> i32 {
        self.fsync_watermark_tx_data_stream_id_base + sequencer_id as i32
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuorumConfig {
    /// Total recorders.
    pub n: usize,
    /// Required for quorum (Q ≤ N). Default Q=2 for N=3.
    pub q: usize,
}

// Per-struct `Default` impls (rather than one monolithic `LogConfig::default`)
// so each section can be defaulted independently — this is what lets a TOML
// file specify only `[channels]` and inherit the rest. `LogConfig` itself
// derives `Default`, composing these.

impl Default for AeronConfig {
    fn default() -> Self {
        Self {
            aeron_dir: PathBuf::from("/dev/shm/aeron-kardamom"),
            archive_dir: PathBuf::from("/var/lib/kardamom/archive"),
            media_driver_cmd: vec!["aeron-media-driver".into()],
            archive_cmd: vec!["aeron-archive".into()],
            file_sync_level: 1,
            catalog_file_sync_level: 1,
            archive_control_request_channel: "aeron:ipc".into(),
            archive_control_response_channel: "aeron:ipc".into(),
            tx_data_archive_endpoints: Vec::new(),
            tx_deposits_archive_endpoints: Vec::new(),
        }
    }
}

impl Default for ChannelsConfig {
    fn default() -> Self {
        // Defaults are all IPC so single-host deployments (the
        // in-container test runs on Linux, the `just aeron-driver-up`
        // path runs on macOS) work out of the box. Multi-host
        // production deployments override the {tx_ordering,
        // tx_receipts, fsync_watermark, quorum_watermark} channels
        // to UDP unicast or UDP multicast at the operator's discretion.
        // macOS in particular cannot route UDP multicast over
        // loopback, so the IPC defaults are required for local e2e.
        Self {
            tx_data_channel_template: "aeron:ipc?alias=a-{sid}".into(),
            tx_data_stream_id_base: 2000,
            tx_ordering_channel: "aeron:ipc?alias=tx-ordering".into(),
            tx_ordering_stream_id: 1001,
            tx_receipts_channel: "aeron:ipc?alias=tx-receipts".into(),
            tx_receipts_stream_id: 1002,
            // MDS disabled by default (single-host IPC uses tx_receipts_channel
            // above). The cluster's channels.toml sets these to enable fan-in.
            tx_receipts_control_channel: String::new(),
            tx_receipts_endpoint_host: String::new(),
            tx_receipts_endpoint_base_port: 0,
            tx_receipts_endpoint_interface: String::new(),
            tx_receipts_executor_count: 0,
            tx_errors_channel: "aeron:ipc?alias=tx-errors".into(),
            // 1003 collides with `tx_receipts_stream_id + 1` (the
            // BlockBoundary side-stream); Aeron IPC routes by
            // stream_id (alias is purely a debug label), so a
            // subscriber on tx-errors/1003 receives the executor's
            // BlockBoundary frames and rkyv-decodes them as TxError.
            // 1015 sits comfortably between the receipt block (1002,
            // 1003) and the fsync-watermark block (1010).
            tx_errors_stream_id: 1015,
            tx_deposits_channel: "aeron:ipc?alias=tx-deposits".into(),
            tx_deposits_stream_id: 1016,
            // 1017 adjoins tx_deposits (1016), the stream this mirrors, and is
            // clear of every other block: receipts (1002,1003), BAL (1004),
            // fsync (1010), tx_errors (1015), quorum (1020).
            tx_remote_epochs_channel: "aeron:ipc?alias=tx-remote-epochs".into(),
            tx_remote_epochs_stream_id: 1017,
            // 1004 sits in the free range between the receipt block (1002,1003)
            // and the fsync-watermark block (1010); BAL is another executor
            // output so it lives near receipts.
            tx_bal_channel: "aeron:ipc?alias=tx-bal".into(),
            tx_bal_stream_id: 1004,
            fsync_watermark_channel_template: "aeron:ipc?alias=fsync-wm-{rid}".into(),
            fsync_watermark_stream_id: 1010,
            fsync_watermark_tx_data_channel_template: "aeron:ipc?alias=fsync-wm-a-{sid}".into(),
            fsync_watermark_tx_data_stream_id_base: 1030,
            quorum_watermark_channel: "aeron:ipc?alias=quorum-watermark".into(),
            quorum_watermark_stream_id: 1020,
        }
    }
}

impl Default for QuorumConfig {
    fn default() -> Self {
        Self { n: 3, q: 2 }
    }
}

#[cfg(test)]
mod tests;
