//! Configuration types for the log subsystem.
//!
//! Loaded from TOML at process start; passed to the supervisor and the
//! quorum aggregator. No global state.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Static identifier for this host's recorder. Must be unique across N recorders.
pub type RecorderId = u8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogConfig {
    pub recorder_id: RecorderId,
    pub aeron: AeronConfig,
    pub channels: ChannelsConfig,
    pub quorum: QuorumConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuorumConfig {
    /// Total recorders.
    pub n: usize,
    /// Required for quorum (Q ≤ N). Default Q=2 for N=3.
    pub q: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            recorder_id: 0,
            aeron: AeronConfig {
                aeron_dir: PathBuf::from("/dev/shm/aeron-kardamom"),
                archive_dir: PathBuf::from("/var/lib/kardamom/archive"),
                media_driver_cmd: vec!["aeron-media-driver".into()],
                archive_cmd: vec!["aeron-archive".into()],
                file_sync_level: 1,
                catalog_file_sync_level: 1,
            },
            // Defaults are all IPC so single-host deployments (the
            // in-container test runs on Linux, the `just aeron-driver-up`
            // path runs on macOS) work out of the box. Multi-host
            // production deployments override the {tx_ordering,
            // tx_receipts, fsync_watermark, quorum_watermark} channels
            // to UDP unicast or UDP multicast at the operator's discretion.
            // macOS in particular cannot route UDP multicast over
            // loopback, so the IPC defaults are required for local e2e.
            channels: ChannelsConfig {
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
            },
            quorum: QuorumConfig { n: 3, q: 2 },
        }
    }
}
