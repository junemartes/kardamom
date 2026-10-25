//! The flags of the ingress service. The `kardamom-ingress` binary parses
//! them, and so does the `ingress` subcommand of a process that runs the
//! service inside itself.

use std::net::SocketAddr;
use std::num::{NonZeroU8, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

use crate::config::{DEFAULT_DUPLICATE_CACHE_CAPACITY, DEFAULT_MAX_TX_BYTES};

/// The settings of one ingress service. The process flags (the config
/// files, the Aeron directory, the metrics address and the host id) stay
/// with the process that runs the service.
#[derive(Debug, Clone, clap::Args)]
pub struct IngressSettings {
    /// JSON-RPC bind address. Defaults to 127.0.0.1:8545.
    #[arg(long, default_value = "127.0.0.1:8545")]
    pub(crate) jsonrpc_bind: SocketAddr,
    /// Recorder id for the local fsync watermark subscription. Used when
    /// `ack_policy` includes a local-fsync gate. Defaults to 0.
    #[arg(long, default_value_t = 0)]
    pub(crate) recorder_id: u8,
    /// Number of executor replicas to attach to the `tx_receipts` MDS
    /// (fan-in) subscription at startup. Used only when MDS is enabled,
    /// with `tx_receipts_control_channel` set. When unset on the CLI,
    /// this falls back to `channels.tx_receipts_executor_count` from the
    /// log config.
    ///
    /// Static-membership fallback: ingress attaches replicas `0..N` once
    /// at startup. Membership is static for the life of the process.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    pub(crate) executor_count: Option<NonZeroU32>,
    /// The active shard count (M). The ingress opens every lane of the
    /// lane plane, and the identity map routes senders to the first M.
    /// So M must be 1, 2, 4, or 8. Defaults to 8. `--shard-map` replaces
    /// the identity map. Zero is rejected at parse time.
    #[arg(long, default_value = "8")]
    pub(crate) shards: NonZeroU8,
    /// A TOML file with the versioned vslot-to-lane map: `version = N`
    /// and `table = [256 lanes]`. It replaces the identity map. A resize
    /// re-renders this file and restarts the ingress. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.2.
    #[arg(long, env = "KARDAMOM_SHARD_MAP")]
    pub(crate) shard_map: Option<PathBuf>,
    /// Records each per-lane `tx_data` publication to the Aeron Archive,
    /// so the executor can replay full transaction envelopes on crash
    /// recovery, through `kardamom_log::replay`. Off by default, since
    /// single-host IPC has no archive. The cluster sets this on the node
    /// where the `ArchivingMediaDriver` runs.
    #[arg(long, env = "KARDAMOM_ARCHIVE_DURABILITY", default_value_t = false)]
    pub(crate) archive_durability: bool,
    /// Durability gate before acking a submit. Mirrors `AckPolicy`:
    ///   - `on-offer`: releases as soon as the receipt arrives. Lowest
    ///     latency, weakest guarantee.
    ///   - `on-local-fsync`: waits for this node's recorder fsync
    ///     watermark.
    ///   - `on-quorum`: waits for Q of N recorders to fsync.
    ///   - `on-local-fsync-and-quorum`: waits for both.
    ///
    /// Defaults to `on-offer`, because no process in the deployed
    /// topology runs a `QuorumAggregator` yet: nothing publishes the
    /// quorum watermark, so a quorum-gated policy would park every submit
    /// forever.
    #[arg(long, default_value = "on-offer")]
    pub(crate) ack_policy: AckPolicyArg,
    /// The stable identity of this ingress replica. An active/active
    /// deployment runs N replicas. This id namespaces `correlation_id`,
    /// so `(replica, sequence)` stays unique, and it is stamped as a
    /// metric label. Defaults to 0.
    #[arg(long, env = "KARDAMOM_INGRESS_ID", default_value_t = 0)]
    pub(crate) ingress_id: u16,
    /// Publish an `accepted` record on `tx_events` for each transaction
    /// that this ingress publishes on `tx_data`
    /// (`docs/specs/2026-10-25-client-events.md`). Best effort: the submit
    /// never waits for it.
    #[arg(long, env = "KARDAMOM_TX_EVENTS", value_enum, default_value_t = TxEventsArg::Off)]
    pub(crate) tx_events: TxEventsArg,
    /// This node's cluster-egress endpoint, `ip:port`, in cluster mode.
    /// Overrides `[cluster] egress_channel` as
    /// `aeron:udp?endpoint=<ip:port>`, the cluster client's per-node
    /// response channel. The Nomad job injects this per node. The egress
    /// tap of the service uses it.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    pub(crate) cluster_egress_endpoint: Option<String>,
    /// Max concurrent JSON-RPC connections. A submission parks its
    /// connection until the receipt arrives, so this value must
    /// comfortably exceed the offered rate times the receipt latency. See
    /// `IngressConfig::rpc_max_connections`.
    #[arg(
        long = "rpc-max-connections",
        env = "KARDAMOM_RPC_MAX_CONNECTIONS",
        default_value_t = 8192
    )]
    pub(crate) rpc_max_connections: u32,
    /// L2 chain id returned by `eth_chainId`. This is purely informational
    /// for clients, since the ingress recovers senders from the tx's own
    /// EIP-155 signature. But tooling that queries `eth_chainId` before
    /// signing needs this value to match the executor's `--chain-id`.
    /// EIP-155 forbids chain id 0, so this is rejected at parse time.
    #[arg(long, env = "KARDAMOM_CHAIN_ID", default_value = "1")]
    pub(crate) chain_id: NonZeroU64,
    /// Max time, in milliseconds, that a submit parks waiting for its
    /// receipt and ack gate before the client gets a `-32000` timeout.
    /// This bounds every `eth_sendRawTransaction` call. A nonce-gap tx
    /// that never becomes executable shows up as exactly this timeout.
    #[arg(
        long = "pending-receipt-timeout-ms",
        env = "KARDAMOM_PENDING_RECEIPT_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    pub(crate) pending_receipt_timeout_ms: u64,

    /// How far past the newest block boundary this proxy stamps a
    /// transaction's inclusion deadline. The sealer refuses an offer once
    /// its own block number passes that deadline, which is what makes its
    /// dedup window exact. It must equal the sealer's
    /// `-Dkardamom.cluster.inclusionHorizonBlocks`: the proxy stamps with
    /// it and the sealer holds an id until the deadline passes. See
    /// `docs/agents/offer-inclusion-deadline-spec.md`.
    #[arg(
        long = "inclusion-horizon-blocks",
        env = "KARDAMOM_INCLUSION_HORIZON_BLOCKS",
        default_value = "64"
    )]
    pub(crate) inclusion_horizon_blocks: NonZeroU64,
    /// The largest `raw_tx` that a submit accepts, in bytes. The ingress
    /// refuses a larger transaction with a `-32602` error that states the
    /// size and the cap. See `IngressConfig::max_tx_bytes`.
    #[arg(
        long = "max-tx-bytes",
        env = "KARDAMOM_MAX_TX_BYTES",
        default_value_t = DEFAULT_MAX_TX_BYTES
    )]
    pub(crate) max_tx_bytes: NonZeroUsize,
    /// The executor query endpoints, `http://host:port`, comma
    /// separated. `eth_getBalance` and `eth_getTransactionCount` ask one
    /// of them when the local account layer misses. Empty means no
    /// query: a cold address then gets an error.
    #[arg(
        long = "executor-query-endpoints",
        env = "KARDAMOM_EXECUTOR_QUERY_ENDPOINTS",
        value_delimiter = ','
    )]
    pub(crate) executor_query_endpoints: Vec<String>,
    /// The endpoints of `eth_getTransactionReceipt` on a cache miss: the
    /// egress (`http://host:port`). An answer that is no receipt then asks
    /// the executor query endpoints. Empty: the executor query endpoints
    /// only.
    #[arg(
        long = "receipt-query-endpoints",
        env = "KARDAMOM_RECEIPT_QUERY_ENDPOINTS",
        value_delimiter = ','
    )]
    pub(crate) receipt_query_endpoints: Vec<String>,
    /// Whether the submit path rejects an unfunded sender from the local
    /// account layer. `false` publishes every submit, as before the
    /// layer existed.
    #[arg(
        long = "admission-checks",
        env = "KARDAMOM_ADMISSION_CHECKS",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    pub(crate) admission_checks: bool,
    /// The bound on accounts resident in the local layer. Defaults to
    /// `LiveAccountsConfig::default`.
    #[arg(
        long = "live-accounts-capacity",
        env = "KARDAMOM_LIVE_ACCOUNTS_CAPACITY"
    )]
    pub(crate) live_accounts_capacity: Option<NonZeroUsize>,
    /// How long a local account entry stays after its last write, in
    /// ms. Defaults to `LiveAccountsConfig::default`.
    #[arg(long = "live-accounts-ttl-ms", env = "KARDAMOM_LIVE_ACCOUNTS_TTL_MS")]
    pub(crate) live_accounts_ttl_ms: Option<NonZeroU64>,
    /// The ack at admission: `eth_sendRawTransaction` answers when the v2
    /// sequencer that owns the sender takes the transaction into its
    /// mempool, as on Ethereum. The answer comes on this ingress's
    /// `tx_verdicts` stream, and the ingress reads no receipts. The ingress
    /// id must be at most 255. The local account layer then gets no rows,
    /// so `--admission-checks` must be `false`: the v2 sequencers check
    /// each sender.
    #[arg(
        long,
        env = "KARDAMOM_INGRESS_VERDICTS",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    pub(crate) verdicts: bool,
    /// With `--verdicts`, the time a submission waits for a verdict, in ms.
    #[arg(long, env = "KARDAMOM_INGRESS_ACK_TIMEOUT_MS", default_value_t = 2_000)]
    pub(crate) ack_timeout_ms: u64,
    /// With `--verdicts`, the sequencers of each lane that send a verdict.
    /// A rejection of every one of them answers at once.
    #[arg(long, env = "KARDAMOM_INGRESS_TWINS", default_value_t = std::num::NonZeroU8::new(2).unwrap_or(std::num::NonZeroU8::MIN))]
    pub(crate) twins: std::num::NonZeroU8,
    /// When `eth_sendRawTransaction` answers:
    ///   - `receipt`: with the receipt, or with `--verdicts` on the verdict
    ///     of the sequencer.
    ///   - `validated`: when this ingress validated the transaction and
    ///     published it on `tx_data`. It waits for no receipt and no
    ///     verdict, so it takes no `--verdicts`.
    #[arg(long, env = "KARDAMOM_INGRESS_ACK", default_value = "receipt")]
    pub(crate) ack: AckArg,
    /// With `--ack validated`, the hashes of recent transactions that the
    /// duplicate cache holds. A copy of a held hash answers at once, with
    /// no signature recovery and no second publication.
    #[arg(
        long = "duplicate-cache-entries",
        env = "KARDAMOM_INGRESS_DUPLICATE_CACHE_ENTRIES",
        default_value_t = DEFAULT_DUPLICATE_CACHE_CAPACITY
    )]
    pub(crate) duplicate_cache_entries: NonZeroUsize,
}

impl IngressSettings {
    /// The durability gate of the submit path.
    #[must_use]
    pub fn ack_policy(&self) -> kardamom_types::AckPolicy {
        self.ack_policy.clone().into()
    }

    /// The id that namespaces the correlation ids of this ingress.
    #[must_use]
    pub fn ingress_id(&self) -> u16 {
        self.ingress_id
    }
}

/// The values of `--tx-events`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum TxEventsArg {
    On,
    Off,
}

/// The values of `--ack`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum AckArg {
    Receipt,
    Validated,
}

#[derive(Clone, Debug, clap::ValueEnum)]
#[allow(clippy::enum_variant_names)] // Mirrors `types::AckPolicy` one to one.
pub(crate) enum AckPolicyArg {
    OnOffer,
    OnLocalFsync,
    OnQuorum,
    OnLocalFsyncAndQuorum,
}

impl From<AckPolicyArg> for kardamom_types::AckPolicy {
    fn from(a: AckPolicyArg) -> Self {
        match a {
            AckPolicyArg::OnOffer => Self::OnOffer,
            AckPolicyArg::OnLocalFsync => Self::OnLocalFsync,
            AckPolicyArg::OnQuorum => Self::OnQuorum,
            AckPolicyArg::OnLocalFsyncAndQuorum => Self::OnLocalFsyncAndQuorum,
        }
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
