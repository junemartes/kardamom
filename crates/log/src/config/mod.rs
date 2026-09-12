//! Configuration types for the log subsystem.
//!
//! Loaded from TOML at process start and passed to the Aeron transport and
//! the recorder. There is no global state.
//!
//! Every struct is `#[serde(default, deny_unknown_fields)]`. A TOML file
//! may specify any subset (for example only `[channels]`), and each
//! missing field falls back to its built-in default. Unknown keys are
//! rejected, so typos in operator-rendered configs fail loudly.
//! [`LogConfig::from_toml_path`] is the loader the service binaries use
//! behind `--log-config`.

mod discovery;

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub use discovery::{DiscoveryConfig, InterfaceSelector};

use crate::error::LogError;

/// Static identifier for this host's recorder. Must be unique across the
/// N recorders.
pub type RecorderId = u8;

/// A positive UDP base port. Parses from TOML through [`TryFrom<i32>`],
/// which rejects zero and negative values at load time. A field of this
/// type carries the "must be a positive port" guarantee itself, so callers
/// need no runtime check and no unsigned cast.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "i32", into = "i32")]
pub struct BasePort(std::num::NonZeroU16);

impl BasePort {
    /// The port number.
    #[must_use]
    pub fn get(self) -> u16 {
        self.0.get()
    }
}

impl TryFrom<i32> for BasePort {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let port = u16::try_from(value)
            .map_err(|_| format!("base port ({value}) must be between 1 and 65535"))?;
        std::num::NonZeroU16::new(port)
            .map(BasePort)
            .ok_or_else(|| "base port (0) must be a positive port number".to_string())
    }
}

/// The TOML form of `tx_receipts_endpoint_base_port`: absent or `0` is
/// "unset", any other value must be a valid [`BasePort`].
fn base_port_or_unset<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<BasePort>, D::Error> {
    let raw = Option::<i32>::deserialize(d)?;
    raw.filter(|v| *v != 0)
        .map(BasePort::try_from)
        .transpose()
        .map_err(serde::de::Error::custom)
}

impl From<BasePort> for i32 {
    fn from(value: BasePort) -> Self {
        i32::from(value.0.get())
    }
}

impl std::fmt::Display for BasePort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A non-empty Aeron channel URI (`aeron:ipc?...`, `aeron:udp?...`).
/// Parsed once at the config boundary (TOML deserialization), so a blank
/// channel string fails config load, not a runtime `add_publication`/
/// `add_subscription` call deep in a service's startup path.
///
/// `Deref<Target = str>` lets every existing `&cfg.some_channel` call
/// site keep working unchanged through deref coercion; callers that need
/// an owned `String` (for example a runtime command struct field) use
/// [`Self::as_str`] or `.to_string()`.
///
/// `tx_receipts_control_channel` is the one channel field that stays a
/// plain `String`: an empty string there is the "MDS off" sentinel (see
/// `ChannelsConfig::tx_receipts_mds_enabled`), not a URI. The two
/// `*_channel_template` fields also stay `String`: a template like
/// `"aeron:ipc?alias=a-{sid}"` is not a valid URI until `{sid}` is
/// substituted.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ChannelUri(String);

impl ChannelUri {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Build a `ChannelUri` from a runtime-constructed, already-known
    /// non-empty string (for example one assembled with `format!` from a
    /// resolved endpoint), skipping the empty-string check
    /// [`TryFrom<String>`] applies to config-file input. Named, rather
    /// than a plain `From<String>`, so trusting the caller is visible at
    /// the call site.
    #[must_use]
    pub fn new_trusted(uri: String) -> Self {
        Self(uri)
    }
}

impl TryFrom<String> for ChannelUri {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err("a channel URI must not be empty".to_string());
        }
        Ok(Self(value))
    }
}

impl From<ChannelUri> for String {
    fn from(value: ChannelUri) -> Self {
        value.0
    }
}

/// Trusts the literal: every call site is a compile-time default in this
/// module, already reviewed as a valid, non-empty URI. TOML-sourced
/// values go through the fallible [`TryFrom<String>`] above instead.
impl From<&str> for ChannelUri {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::ops::Deref for ChannelUri {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ChannelUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lets a test (or any other caller) compare a `ChannelUri` field against
/// a string literal directly, without `.as_str()`.
impl PartialEq<str> for ChannelUri {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for ChannelUri {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

// `Default` is derived. It composes the per-struct `Default` impls below.
// Each section defaults independently, which is what lets a TOML file
// specify only `[channels]` and inherit the rest.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    pub recorder_id: RecorderId,
    pub aeron: AeronConfig,
    pub channels: ChannelsConfig,
    pub discovery: DiscoveryConfig,
}

impl LogConfig {
    /// Load a `LogConfig` from a TOML file. Any field the file omits falls
    /// back to [`Default`]. Unknown fields are rejected. Errors carry the
    /// path, so misconfigured deployments fail fast with a useful message.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` cannot be read, if its contents do
    /// not parse as valid `LogConfig` TOML (including an unknown key),
    /// or if the parsed config fails [`ChannelsConfig::validate`] or
    /// [`DiscoveryConfig::validate`].
    pub fn from_toml_path(path: &Path) -> Result<Self, LogError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| LogError::Config(format!("read log-config {}: {e}", path.display())))?;
        let cfg: Self = toml::from_str(&raw)
            .map_err(|e| LogError::Config(format!("parse log-config {}: {e}", path.display())))?;
        cfg.channels
            .validate()
            .and_then(|()| cfg.discovery.validate())
            .map_err(|e| LogError::Config(format!("invalid log-config {}: {e}", path.display())))?;
        Ok(cfg)
    }

    /// Resolve the effective `LogConfig` for a service binary. Loads it
    /// from `path` if `--log-config` was supplied, otherwise uses the
    /// built-in single-host IPC defaults. This is the single entry point
    /// every channel-using binary calls, so the fallback behavior is
    /// uniform.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` is `Some` and loading it fails (see
    /// [`from_toml_path`](Self::from_toml_path)).
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

    /// Path to the Aeron Media Driver binary (jar or native). Spawned by
    /// the supervisor.
    pub media_driver_cmd: Vec<String>,

    /// Path to the Aeron Archive runner. Spawned by the supervisor.
    pub archive_cmd: Vec<String>,

    /// Aeron Archive `fileSyncLevel` for segment data files.
    /// 0 means no fsync (page cache only), 1 means fdatasync per frame,
    /// 2 means fsync per frame. Default 1: per-frame fdatasync gives
    /// byte-durable recording positions on PLP `NVMe`, at the cost of a
    /// per-frame fdatasync round trip.
    pub file_sync_level: u8,

    /// Aeron Archive `catalog.fileSyncLevel` for the recording catalog
    /// metadata file. Default 1: the catalog is tiny and updated rarely,
    /// so fsync is cheap.
    pub catalog_file_sync_level: u8,

    /// Archive control request channel: where a client (for example
    /// `kardamom-recorder`) sends commands to the Archive. Defaults to
    /// `aeron:ipc`. The recorder always sits next to its node's
    /// `ArchivingMediaDriver` and shares its `aeron.dir`, so control rides
    /// the local IPC channel over the shared media driver. This is both
    /// simpler and avoids the UDP control handshake, whose response cannot
    /// reliably route back to a co-located client. Only the recorder
    /// connects an `AeronArchive`, so the pipeline services do not use this.
    pub archive_control_request_channel: ChannelUri,

    /// Archive control response channel. Also `aeron:ipc`; responses ride
    /// the shared media driver back to the recorder.
    pub archive_control_response_channel: ChannelUri,

    /// Remote durability-archive control endpoints (`host:port`) whose
    /// archives record the `tx_data` streams (the ingress nodes). `tx_data` is
    /// multicast, and every ingress archive records every publisher's
    /// shard streams, so each entry is a full mirror. Consumers rotate
    /// through them on failure. Used by the join-miss archive refetch
    /// ([`crate::refetch`]). Empty (the default) disables refetch.
    #[serde(default)]
    pub tx_data_archive_endpoints: Vec<String>,

    /// Same as above for the `tx_deposits` stream (the da-watcher's node
    /// records it).
    #[serde(default)]
    pub tx_deposits_archive_endpoints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChannelsConfig {
    /// `TxData`[i]: per-sequencer exclusive publisher of full `TxEnvelope`
    /// bytes. One stream per sequencer. The URI template substitutes
    /// `{sid}` with the sequencer id (for example
    /// `"aeron:ipc?alias=a-{sid}"`). The stream id is
    /// `tx_data_stream_id_base + sequencer_id`.
    pub tx_data_channel_template: String,
    pub tx_data_stream_id_base: i32,

    /// `TxReceipts`: receipts and block boundaries. Not recorded.
    ///
    /// Single-host/IPC default: one shared channel (`tx_receipts_channel`)
    /// that the lone executor publishes to, and that ingress subscribes to
    /// directly.
    pub tx_receipts_channel: ChannelUri,
    pub tx_receipts_stream_id: i32,

    /// `TxReceipts` MDS (multi-host fan-in). When
    /// `tx_receipts_control_channel` is not empty, receipts use a
    /// multi-destination subscription: each executor replica publishes to
    /// its own UDP endpoint
    /// (`tx_receipts_endpoint_host:tx_receipts_endpoint_base_port + replica`),
    /// and ingress opens one `control-mode=manual` subscription
    /// (`tx_receipts_control_channel`) and attaches each executor endpoint
    /// as a destination (discovered through Consul), deduping receipts by
    /// tx hash. Empty (the default) keeps the single-channel IPC path above.
    #[serde(default)]
    pub tx_receipts_control_channel: String,
    /// Host (ingress's receive address) every executor replica sends
    /// receipts to in MDS mode. Empty unless MDS is enabled.
    #[serde(default)]
    pub tx_receipts_endpoint_host: String,
    /// Base UDP port for per-replica receipt endpoints. Replica `i` uses
    /// `base_port + 2*i` for receipts and `+ 2*i + 1` for boundaries.
    /// `None` unless MDS is enabled: the key is absent, or it is `0`, the
    /// value the deployed `channels.toml` renders when MDS is off. A
    /// negative value, or a value past 65535, is a config error, MDS on or
    /// off. With MDS on, [`Self::validate`] requires a port.
    #[serde(default, deserialize_with = "base_port_or_unset")]
    pub tx_receipts_endpoint_base_port: Option<BasePort>,
    /// Egress/ingress NIC subnet pinned on the per-replica receipt and
    /// boundary endpoints (for example `192.168.56.0/24`), appended as
    /// `|interface=...`. Like every other UDP channel, the unicast
    /// endpoints must pin the interface. Otherwise the executor's publish
    /// picks the wrong source NIC, and the connection to ingress never
    /// forms (`NOT_CONNECTED` forever). Empty means no interface pin
    /// (single-host or loopback).
    #[serde(default)]
    pub tx_receipts_endpoint_interface: String,
    /// Number of executor replicas whose endpoints ingress attaches to its
    /// MDS subscription at startup (`tx_receipts_endpoint(0..N)`).
    ///
    /// This is a static membership count, not a service watch. Ingress
    /// attaches replicas `0..N` once at startup. This works because the
    /// executor job runs a fixed `count` with `distinct_hosts`, so replica
    /// indices stay stable at `0..N`. A replica that restarts keeps its
    /// index and its endpoint, so the static attach stays correct across
    /// restarts. This value must match the executor job `count`. `None`
    /// (the default) is fine when MDS is disabled.
    ///
    /// Migration: a config written when this field was a plain `u32`
    /// may spell "no known count" as `tx_receipts_executor_count = 0`.
    /// That now fails to load (`0` is not a `NonZeroU32`); omit the key
    /// instead, which defaults to `None` and means the same thing.
    #[serde(default)]
    pub tx_receipts_executor_count: Option<std::num::NonZeroU32>,

    /// `TxErrors`: sequencer-emitted rejection signals (duplicate or
    /// past-nonce today; more variants may follow). RAM only, not
    /// recorded: an operational signal, not canonical state.
    pub tx_errors_channel: ChannelUri,
    pub tx_errors_stream_id: i32,

    /// `TxDeposits`: the DA watcher publishes full `Deposit` envelopes here.
    /// The M sequencers subscribe and republish a `DepositRef` onto
    /// `tx_ordering`, so the canonical order interleaves L1 deposits with
    /// regular L2 transactions. RAM only.
    pub tx_deposits_channel: ChannelUri,
    pub tx_deposits_stream_id: i32,

    /// `TxRemoteEpochs`: the interop watcher publishes one `RemoteEpochRecord`
    /// for each peer-chain origin block that carries cross-chain messages.
    /// The M sequencers subscribe and republish each record onto
    /// `tx_ordering` as a remote-origin-advancing record. This is like
    /// `tx_deposits`, but for a peer Kardamom chain instead of L1. It uses a
    /// separate stream because the two origins advance independently, and a
    /// stalled peer must not hold up L1 deposits. RAM only.
    pub tx_remote_epochs_channel: ChannelUri,
    pub tx_remote_epochs_stream_id: i32,

    /// `TxBal`: the per-block BAL (Block Access List; the executor's
    /// `BlockDelta` of account, storage, and code mutations plus receipts
    /// for a sealed block). Every executor replica publishes one
    /// `BlockDelta` per block on this same group/stream. Validators may
    /// see one copy per replica, which is harmless, because inserts are
    /// idempotent overwrites keyed by block number. Validators subscribe
    /// and cross-check their independent re-execution against it. RAM only.
    pub tx_bal_channel: ChannelUri,
    pub tx_bal_stream_id: i32,

    /// Per-recorder fsync watermark stream, parameterized by
    /// `recorder_id`, for example "aeron:ipc?alias=fsync-wm-{rid}". The
    /// ingress subscribes to it for the local-fsync ack policies.
    pub fsync_watermark_channel_template: String,
    pub fsync_watermark_stream_id: i32,
}

impl ChannelsConfig {
    /// `TxData`[i] URI for a given sequencer (`{sid}` substituted).
    #[must_use]
    pub fn tx_data_channel(&self, sequencer_id: u8) -> String {
        self.tx_data_channel_template
            .replace("{sid}", &sequencer_id.to_string())
    }

    /// True when receipts use the multi-destination-subscription (fan-in)
    /// path, meaning a `tx_receipts_control_channel` is configured. False
    /// means the single-channel IPC default (`tx_receipts_channel`).
    #[must_use]
    pub fn tx_receipts_mds_enabled(&self) -> bool {
        !self.tx_receipts_control_channel.is_empty()
    }

    /// Cross-field invariants serde cannot express. Called by
    /// [`LogConfig::from_toml_path`], so a misconfigured deployment fails
    /// at load time with a message instead of misbehaving later.
    ///
    /// [`BasePort`] already rules out a zero or negative base at parse
    /// time. This still checks the one bound the type cannot carry alone:
    /// MDS receipt fan-in computes per-replica ports as `base + 2*i (+
    /// 1)`, so a base too close to 65535 would push the highest replica's
    /// port past the valid range.
    ///
    /// # Errors
    ///
    /// Returns an error if MDS is enabled and
    /// `tx_receipts_endpoint_base_port` is unset, or does not leave room
    /// for every replica's receipt and boundary port under 65535; or if
    /// `tx_data_stream_id_base` or `tx_receipts_stream_id` sits too close
    /// to `i32::MAX` for its `+ sequencer_id` or `+ 1` derived id to stay
    /// in range.
    pub fn validate(&self) -> Result<(), String> {
        if self.tx_receipts_mds_enabled() {
            let Some(base) = self.tx_receipts_endpoint_base_port else {
                return Err("tx_receipts_endpoint_base_port must be set when \
                     tx_receipts_control_channel enables MDS"
                    .to_string());
            };
            // Unset means no receipt fan-out: 0 replicas need 0 extra
            // ports, so `base + 1` alone bounds the highest port.
            let executor_count = self
                .tx_receipts_executor_count
                .map_or(0, std::num::NonZeroU32::get);
            let highest = i64::from(base.get()) + 2 * i64::from(executor_count) + 1;
            if highest > i64::from(u16::MAX) {
                return Err(format!(
                    "tx_receipts_endpoint_base_port ({base}) invalid with MDS enabled \
                     (tx_receipts_control_channel set): \
                     base + 2*tx_receipts_executor_count + 1 <= 65535"
                ));
            }
        }
        // `tx_data_stream_id` adds a `u8` sequencer id (at most 255) to
        // the base. Leaving 255 of headroom below `i32::MAX` means that
        // add never overflows.
        if self.tx_data_stream_id_base > i32::MAX - 255 {
            return Err(format!(
                "tx_data_stream_id_base ({}) too close to i32::MAX: \
                 tx_data_stream_id(sequencer_id) would overflow",
                self.tx_data_stream_id_base
            ));
        }
        // `tx_receipts_boundary_stream_id` adds 1 to `tx_receipts_stream_id`.
        if self.tx_receipts_stream_id == i32::MAX {
            return Err("tx_receipts_stream_id must not be i32::MAX: \
                 tx_receipts_boundary_stream_id() would overflow"
                .to_string());
        }
        Ok(())
    }

    /// The UDP endpoint executor replica `replica_idx` publishes its
    /// receipt stream (`tx_receipts_stream_id`) to, and that ingress
    /// attaches as an MDS destination on its receipt subscription. This is
    /// the single source of truth on both sides. Returns `None` when MDS
    /// is disabled.
    ///
    /// Receipts and boundaries get distinct ports (interleaved `base + 2*r`
    /// and `base + 2*r + 1`). A `control-mode=manual` subscription binds
    /// its destination's UDP socket, and ingress runs two manual
    /// subscriptions (receipts and boundaries), so a shared endpoint would
    /// collide ("Address already in use").
    #[must_use]
    pub fn tx_receipts_endpoint(&self, replica_idx: u32) -> Option<String> {
        if !self.tx_receipts_mds_enabled() {
            return None;
        }
        self.tx_receipts_slot(replica_idx, 0)
    }

    /// The UDP endpoint executor replica `replica_idx` publishes its
    /// block-boundary side-stream (`tx_receipts_stream_id + 1`) to. This
    /// is distinct from the receipt endpoint (see
    /// [`tx_receipts_endpoint`](Self::tx_receipts_endpoint)), so the two
    /// ingress manual subscriptions do not bind the same socket.
    #[must_use]
    pub fn tx_receipts_boundary_endpoint(&self, replica_idx: u32) -> Option<String> {
        if !self.tx_receipts_mds_enabled() {
            return None;
        }
        self.tx_receipts_slot(replica_idx, 1)
    }

    /// Shared body of [`tx_receipts_endpoint`](Self::tx_receipts_endpoint)
    /// and
    /// [`tx_receipts_boundary_endpoint`](Self::tx_receipts_boundary_endpoint):
    /// `slot` is `0` for the receipt port and `1` for the boundary port.
    /// The caller has already checked `tx_receipts_mds_enabled`.
    ///
    /// `validate` proves `base + 2*executor_count + 1 <= 65535` whenever
    /// MDS is enabled through [`LogConfig::from_toml_path`], but a
    /// directly built `ChannelsConfig` (for example in tests) can skip
    /// that check, so this still uses `checked_add` and returns `None` on
    /// overflow instead of wrapping.
    fn tx_receipts_slot(&self, replica_idx: u32, slot: u32) -> Option<String> {
        let base = self.tx_receipts_endpoint_base_port?;
        let port = replica_idx
            .checked_mul(2)
            .and_then(|r| u32::from(base.get()).checked_add(r))
            .and_then(|p| p.checked_add(slot))
            .and_then(|p| u16::try_from(p).ok())?;
        Some(self.tx_receipts_uri(port))
    }

    /// Build a per-replica receipt/boundary endpoint URI, pinning the
    /// egress NIC with `|interface=...` when configured (required for
    /// multi-host; see `tx_receipts_endpoint_interface`).
    fn tx_receipts_uri(&self, port: u16) -> String {
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

    /// `TxData`[i] stream id (`tx_data_stream_id_base + sequencer_id`).
    ///
    /// `validate` proves `tx_data_stream_id_base <= i32::MAX - 255` for a
    /// loaded config, so this add never overflows there. A directly built
    /// `ChannelsConfig` (as in some tests) can skip that check;
    /// `saturating_add` avoids silently wrapping into a negative stream
    /// id.
    #[must_use]
    pub fn tx_data_stream_id(&self, sequencer_id: u8) -> i32 {
        self.tx_data_stream_id_base
            .saturating_add(i32::from(sequencer_id))
    }

    /// `TxReceipts` block-boundary side-stream id
    /// (`tx_receipts_stream_id + 1`). `validate` proves
    /// `tx_receipts_stream_id != i32::MAX` for a loaded config, so this
    /// add never overflows there.
    #[must_use]
    pub fn tx_receipts_boundary_stream_id(&self) -> i32 {
        self.tx_receipts_stream_id.saturating_add(1)
    }

    /// Per-recorder fsync watermark URI (`{rid}` substituted).
    #[must_use]
    pub fn fsync_watermark_channel(&self, recorder_id: u8) -> String {
        self.fsync_watermark_channel_template
            .replace("{rid}", &recorder_id.to_string())
    }
}

// Per-struct `Default` impls, rather than one monolithic
// `LogConfig::default`, so each section can be defaulted independently.
// This is what lets a TOML file specify only `[channels]` and inherit the
// rest. `LogConfig` itself derives `Default`, composing these.

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
        // Defaults are all IPC, so single-host deployments (the
        // in-container test runs on Linux, the `just aeron-driver-up`
        // path runs on macOS) work out of the box. Multi-host
        // deployments override the channels to UDP. macOS in particular
        // cannot route UDP multicast over loopback, so the IPC defaults
        // are required for local e2e.
        Self {
            tx_data_channel_template: "aeron:ipc?alias=a-{sid}".into(),
            tx_data_stream_id_base: 2000,
            tx_receipts_channel: "aeron:ipc?alias=tx-receipts".into(),
            tx_receipts_stream_id: 1002,
            // MDS is disabled by default (single-host IPC uses
            // tx_receipts_channel above). The cluster's channels.toml sets
            // these to enable fan-in.
            tx_receipts_control_channel: String::new(),
            tx_receipts_endpoint_host: String::new(),
            tx_receipts_endpoint_base_port: None,
            tx_receipts_endpoint_interface: String::new(),
            tx_receipts_executor_count: None,
            tx_errors_channel: "aeron:ipc?alias=tx-errors".into(),
            // 1003 collides with `tx_receipts_stream_id + 1` (the
            // BlockBoundary side-stream). Aeron IPC routes by stream_id
            // (the alias is only a debug label), so a subscriber on
            // tx-errors/1003 would receive the executor's BlockBoundary
            // frames and rkyv-decode them as TxError. 1015 sits comfortably
            // between the receipt block (1002, 1003) and the
            // fsync-watermark block (1010).
            tx_errors_stream_id: 1015,
            tx_deposits_channel: "aeron:ipc?alias=tx-deposits".into(),
            tx_deposits_stream_id: 1016,
            // 1017 sits next to tx_deposits (1016), the stream it mirrors. It
            // stays clear of every other block: receipts (1002, 1003), BAL
            // (1004), fsync (1010), tx_errors (1015).
            tx_remote_epochs_channel: "aeron:ipc?alias=tx-remote-epochs".into(),
            tx_remote_epochs_stream_id: 1017,
            // 1004 sits in the free range between the receipt block
            // (1002, 1003) and the fsync-watermark block (1010). BAL is
            // another executor output, so it lives near receipts.
            tx_bal_channel: "aeron:ipc?alias=tx-bal".into(),
            tx_bal_stream_id: 1004,
            fsync_watermark_channel_template: "aeron:ipc?alias=fsync-wm-{rid}".into(),
            fsync_watermark_stream_id: 1010,
        }
    }
}

#[cfg(test)]
mod tests;
