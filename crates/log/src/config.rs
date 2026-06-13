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
        toml::from_str(&raw)
            .map_err(|e| LogError::Config(format!("parse log-config {}: {e}", path.display())))
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
    /// `kardamom-recorder`) sends commands to the Archive. For the cluster's
    /// ArchivingMediaDriver image this is the archive's control endpoint
    /// (`aeron:udp?endpoint=localhost:8010`); single-host/local setups can use
    /// IPC. Only the recorder connects an `AeronArchive`, so this is unused by
    /// the pipeline services.
    pub archive_control_request_channel: String,

    /// Archive control **response** channel — where the Archive sends replies.
    /// `endpoint=localhost:0` lets the OS pick an ephemeral UDP port.
    pub archive_control_response_channel: String,
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
    pub tx_ordering_channel: String,
    pub tx_ordering_stream_id: i32,

    /// TxReceipts: receipts + block boundaries. Not recorded.
    pub tx_receipts_channel: String,
    pub tx_receipts_stream_id: i32,

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
            archive_control_request_channel: "aeron:udp?endpoint=localhost:8010".into(),
            archive_control_response_channel: "aeron:udp?endpoint=localhost:0".into(),
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
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        f.write_all(contents.as_bytes()).expect("write");
        f.flush().expect("flush");
        f
    }

    #[test]
    fn empty_file_yields_defaults() {
        let f = write_tmp("");
        let cfg = LogConfig::from_toml_path(f.path()).expect("load empty");
        // Bit-for-bit the built-in defaults.
        let d = LogConfig::default();
        assert_eq!(cfg.recorder_id, d.recorder_id);
        assert_eq!(
            cfg.channels.tx_ordering_channel,
            d.channels.tx_ordering_channel
        );
        assert_eq!(cfg.quorum.n, d.quorum.n);
        assert_eq!(cfg.aeron.file_sync_level, d.aeron.file_sync_level);
    }

    #[test]
    fn partial_channels_section_inherits_other_fields() {
        // Only one channel field set; everything else must default.
        let f = write_tmp(
            r#"
            [channels]
            tx_ordering_channel = "aeron:udp?endpoint=239.192.56.11:40010"
            tx_ordering_stream_id = 1001
            "#,
        );
        let cfg = LogConfig::from_toml_path(f.path()).expect("load partial");
        assert_eq!(
            cfg.channels.tx_ordering_channel,
            "aeron:udp?endpoint=239.192.56.11:40010"
        );
        // Untouched channel fields fall back to IPC defaults.
        assert_eq!(
            cfg.channels.tx_receipts_channel,
            "aeron:ipc?alias=tx-receipts"
        );
        assert_eq!(cfg.channels.tx_data_stream_id_base, 2000);
        // Untouched sections fall back wholesale.
        assert_eq!(cfg.recorder_id, 0);
        assert_eq!(cfg.quorum, QuorumConfig::default());
    }

    #[test]
    fn recorder_id_and_quorum_override() {
        let f = write_tmp(
            r#"
            recorder_id = 2
            [quorum]
            n = 5
            q = 3
            "#,
        );
        let cfg = LogConfig::from_toml_path(f.path()).expect("load");
        assert_eq!(cfg.recorder_id, 2);
        assert_eq!(cfg.quorum.n, 5);
        assert_eq!(cfg.quorum.q, 3);
    }

    #[test]
    fn unknown_field_is_rejected() {
        let f = write_tmp(
            r#"
            [channels]
            tx_ordering_channLE = "typo"
            "#,
        );
        let err = LogConfig::from_toml_path(f.path()).expect_err("typo must be rejected");
        assert!(matches!(err, LogError::Config(_)), "got {err:?}");
    }

    #[test]
    fn missing_file_is_a_config_error() {
        let err = LogConfig::from_toml_path(Path::new("/no/such/log-config.toml"))
            .expect_err("missing file");
        assert!(matches!(err, LogError::Config(_)), "got {err:?}");
    }

    #[test]
    fn resolve_none_is_default() {
        let cfg = LogConfig::resolve(None).expect("resolve none");
        assert_eq!(
            cfg.channels.tx_ordering_channel,
            LogConfig::default().channels.tx_ordering_channel
        );
    }

    #[test]
    fn round_trips_through_toml() {
        // A fully-serialized config must parse back identically — guards the
        // serde attrs against a field that serializes but won't deserialize.
        let original = LogConfig::default();
        let s = toml::to_string(&original).expect("serialize");
        let f = write_tmp(&s);
        let back = LogConfig::from_toml_path(f.path()).expect("reparse");
        assert_eq!(
            back.channels.quorum_watermark_stream_id,
            original.channels.quorum_watermark_stream_id
        );
        assert_eq!(
            back.aeron.archive_control_request_channel,
            original.aeron.archive_control_request_channel
        );
    }
}
