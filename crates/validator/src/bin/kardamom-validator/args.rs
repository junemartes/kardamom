//! CLI + file-config surface of `kardamom-validator`.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;

use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use clap::Parser;
use kardamom_engine::bin_support::{StateDurabilityArg, TxSourceArg};
use kardamom_engine::reader::cluster::ClusterConfig;
use kardamom_validator::interop::{
    DEFAULT_FEED_MAX_SUBSCRIPTIONS, DEFAULT_FEED_MAX_SUBSCRIPTIONS_PER_DEST, RetentionBlocks,
};
use kardamom_validator::parallel::BatchSize;

/// Default `--chain-id`. Also `resolve_genesis`'s "no explicit override"
/// sentinel — see the field's doc.
const DEFAULT_CHAIN_ID: NonZeroU64 = NonZeroU64::new(1).expect("compile-time constant");
/// Default `--validation-batch-size`.
const DEFAULT_VALIDATION_BATCH_SIZE: BatchSize =
    BatchSize::new(NonZeroUsize::new(8).expect("compile-time constant"));
/// Default `--attester-post-interval`.
const DEFAULT_ATTESTER_POST_INTERVAL: PostInterval =
    PostInterval::new(NonZeroU64::new(1).expect("compile-time constant"));
/// Default `--feed-retention-blocks`.
const DEFAULT_FEED_RETENTION_BLOCKS: RetentionBlocks =
    RetentionBlocks::new(NonZeroU64::new(1024).expect("compile-time constant"));
/// Top-level config the `kardamom-validator` binary deserializes from
/// `--config`. Same `[cluster]` section shape as the executor's.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub(crate) struct ValidatorFileConfig {
    /// Aeron Cluster (Raft) sealer client config. `tx_ordering` always comes
    /// from the cluster egress; there is no non-cluster path.
    pub(crate) cluster: ClusterConfig,
}

#[derive(Debug, Clone, Parser)]
#[command(
    name = "kardamom-validator",
    version,
    about = "kardamom validator node"
)]
pub(crate) struct Args {
    /// Path to the TOML config file. Its presence is checked; tuning uses flags.
    /// Only `--clear-verdict` runs without it.
    #[arg(long, required_unless_present = "clear_verdict")]
    pub(crate) config: Option<PathBuf>,
    /// Clear the divergence verdict beside `--state-dir` and exit. A
    /// validator that runs halted on that verdict sees the removal within
    /// its poll interval and resumes from its cursor. Nothing else starts:
    /// no exporter, no Aeron. Run it from the validator's allocation, so
    /// the state directory is the same one.
    #[arg(long, default_value_t = false)]
    pub(crate) clear_verdict: bool,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    pub(crate) log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    pub(crate) aeron_dir: Option<PathBuf>,
    /// Number of executor replicas whose `tx_receipts` endpoints to attach,
    /// when `tx_receipts` MDS is enabled. Falls back to
    /// `channels.tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    pub(crate) executor_count: Option<u32>,
    /// L2 chain id (used for revm). `1` (the default) is also the "no
    /// explicit chain id" sentinel `resolve_genesis` checks against, so a
    /// genesis file's own chain id always wins unless this is set.
    #[arg(long, default_value_t = DEFAULT_CHAIN_ID)]
    pub(crate) chain_id: NonZeroU64,
    /// Path to a genesis TOML (schema: `kardamom_types::Genesis`).
    #[arg(long)]
    pub(crate) chain: Option<PathBuf>,
    /// Directory for the libmdbx state database. The validator keeps its own.
    #[arg(
        long,
        env = "KARDAMOM_STATE_DIR",
        default_value = "/opt/kardamom/validator-state"
    )]
    pub(crate) state_dir: PathBuf,
    /// State durability mode.
    #[arg(long, value_enum, default_value_t = StateDurabilityArg::Durable)]
    pub(crate) state_durability: StateDurabilityArg,
    /// Serve read-only state queries on this address: the committed
    /// nonce, balance and receipt, and a block's transaction references
    /// (`kardamom_getBlockRefs`) the batcher reads when the sealer no
    /// longer retains the block. The same endpoint as the executor's flag
    /// of this name. Off when unset.
    #[arg(long, env = "KARDAMOM_NONCE_QUERY_ADDR")]
    pub(crate) nonce_query_addr: Option<std::net::SocketAddr>,
    /// Local checkpoint staging dir for the replay-unavailable fallback.
    /// Peer checkpoints are fetched here and adopted on the next start.
    /// The validator never creates checkpoints, since its state is
    /// derived; this is an adoption-only directory.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_DIR")]
    pub(crate) checkpoint_dir: Option<PathBuf>,
    /// Executor checkpoint-serve addresses (`host:port`, comma-separated),
    /// to fetch from when the cluster refuses replay because the cursor is
    /// below the retention floor. Blocks through an adopted checkpoint are
    /// unverified by this validator. The trustless alternative is a
    /// rebuild from L1 (kardamom-reconstruct).
    #[arg(long, env = "KARDAMOM_CHECKPOINT_PEERS", value_delimiter = ',')]
    pub(crate) checkpoint_peers: Vec<String>,
    /// Enable the state-trie shadow-check. Every N blocks, recompute the
    /// world state root by a full rebuild, and stop on a mismatch with the
    /// incremental walker; this is a canary against trie bugs. When
    /// absent, only the incremental walker runs. `1` means every block.
    /// This costs a full rebuild on the sampled blocks.
    #[arg(long, env = "KARDAMOM_TRIE_SHADOW_CHECK")]
    pub(crate) trie_shadow_check: Option<NonZeroU64>,
    /// UDP endpoint (`host:port`) on this node where refetched `tx_data` and
    /// `tx_deposits` fragments land: join-miss recovery from the remote
    /// durability archives. See the executor's flag of the same name. When
    /// unset, refetch is disabled, and a lost envelope is fatal after the
    /// join timeout. `tx_ordering` recovery is the cluster client's replay,
    /// not this path.
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    pub(crate) replay_destination_endpoint: Option<String>,
    /// UDP endpoint (`host:port`) on this node for the refetch client's
    /// archive-control responses. Required alongside
    /// `--replay-destination-endpoint` for refetch to engage.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    pub(crate) archive_control_response_endpoint: Option<String>,
    /// This node's cluster-egress endpoint `ip:port`. Sets or overrides the
    /// `[cluster]` `egress_channel` as `aeron:udp?endpoint=<ip:port>`. The
    /// Nomad job injects this per node as `${meta.node_ip}:${NOMAD_HOST_PORT_egress}`.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    pub(crate) cluster_egress_endpoint: Option<String>,
    /// This consumer's voter id at the sealer. With an id, the consumer asks
    /// the sealer to void an entry whose `tx_data` every archive refuses, and
    /// drops the entry when the void record arrives. With no id, it stops at
    /// such an entry. The id must be in the sealer's
    /// `kardamom.cluster.voidVoters` list, and each consumer has its own id.
    #[arg(long, env = "KARDAMOM_VOID_VOTER_ID")]
    pub(crate) void_voter_id: Option<u8>,
    /// Where the validator reads the transaction bytes. `tx-data` joins the
    /// `tx_data` lanes, as an executor does. `exec-stream` reads the
    /// executor stream (`exec_txs`), checks each record against the
    /// canonical hash, refetches a miss from an executor archive by
    /// locator, and never votes: it drops an entry only on its void record.
    #[arg(long, value_enum, env = "KARDAMOM_TX_SOURCE", default_value_t = TxSourceArg::TxData)]
    pub(crate) tx_source: TxSourceArg,
    /// The executor query endpoints (`http://host:port`, comma-separated)
    /// that the `exec-stream` source asks for a locator on a miss
    /// (`kardamom_getExecLocator`). Empty turns the archive refetch off.
    #[arg(long, env = "KARDAMOM_EXECUTOR_QUERY_ENDPOINTS", value_delimiter = ',')]
    pub(crate) executor_query_endpoints: Vec<String>,
    /// Address for the Prometheus /metrics HTTP listener. Port 9007, since
    /// 9006 is the ingress default; running both locally with defaults
    /// must not compete for one socket. See docs/observability.md.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9007")]
    pub(crate) metrics_addr: std::net::SocketAddr,
    /// `/ready` passes while no divergence verdict stands and the
    /// committed block is at most this many blocks behind the sealer's
    /// head.
    #[arg(long, env = "KARDAMOM_READY_LAG_BLOCKS", default_value_t = 8)]
    pub(crate) ready_lag_blocks: u32,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    pub(crate) host_id: String,

    // --- L1 access: the epoch check and the output attester. ---
    /// L1 JSON-RPC endpoint. With `--lockbox`, the epoch check reads L1
    /// here. With `--output-oracle` and `--attester-key`, the attester
    /// posts withdrawal outputs here.
    #[arg(long, env = "KARDAMOM_L1_RPC_URL", value_parser = parse_l1_rpc_url)]
    pub(crate) l1_rpc_url: Option<reqwest::Url>,
    /// Address of the deployed `WithdrawalOutputOracle` proxy.
    #[arg(long, env = "KARDAMOM_OUTPUT_ORACLE")]
    pub(crate) output_oracle: Option<alloy_primitives::Address>,
    /// Address of the deployed `ETHLockbox` proxy. With `--l1-rpc-url`,
    /// this turns on epoch verification: every epoch on the canonical
    /// stream is re-derived from L1, and a mismatch is a divergence.
    /// Without it, the validator still checks the origin sequence (rules
    /// 1-2, which need no L1) but cannot check an epoch's contents.
    #[arg(long, env = "KARDAMOM_LOCKBOX")]
    pub(crate) lockbox: Option<alloy_primitives::Address>,
    /// Attester private key: raw hex, or `env:VAR` to read it from the
    /// environment, the deployer's key convention. Must be the oracle's
    /// permissioned `attester`. Resolved into a signer at parse time.
    #[arg(long, env = "KARDAMOM_ATTESTER_KEY", value_parser = AttesterKey::parse)]
    pub(crate) attester_key: Option<AttesterKey>,
    /// Post one L1 output per this many L2 blocks.
    /// Re-execute each block as seeded parallel batches, driven by the
    /// EIP-7928 BAL. Falls back
    /// to sequential re-execution per block when claims are unavailable or
    /// the block contains deposits, so liveness never depends on the BAL.
    #[arg(long, env = "KARDAMOM_PARALLEL_VALIDATION", default_value_t = false)]
    pub(crate) parallel_validation: bool,
    /// Spool anchored prover inputs to this directory: one frame per
    /// block, with witness, MPT proofs, records, and BAL, plus the
    /// expected public outputs. This is the zkVM prover's queue (spec
    /// 3c). It runs entirely off the hot path. Blocks the spool cannot
    /// pin a pre-state snapshot for are dropped with a counter, never
    /// awaited. Requires the trie-aware writer, which is the default.
    #[arg(long, env = "KARDAMOM_PROVE_BATCHES")]
    pub(crate) prove_batches: Option<std::path::PathBuf>,
    /// Transactions per parallel batch: this is the scheduling
    /// granularity, independent of the BAL's attribution granularity.
    /// Only meaningful at wire granularity K = 1. At K > 1 (the K=8 wire
    /// default), batches are chunk-aligned to the frame's K, and the
    /// worker count below is the real parallelism control.
    #[arg(long, env = "KARDAMOM_VALIDATION_BATCH_SIZE", default_value_t = DEFAULT_VALIDATION_BATCH_SIZE)]
    pub(crate) validation_batch_size: BatchSize,
    /// Worker threads in the parallel-validation pool. 0 means auto
    /// (`min(available_parallelism, 8)`). Hard-capped at 40, since the
    /// mdbx reader-slot budget (`MAX_READERS = 64`) reserves the rest for
    /// the exec thread, RPC, and compaction.
    #[arg(long, env = "KARDAMOM_VALIDATION_WORKERS", default_value_t = WorkerCount::Auto)]
    pub(crate) validation_workers: WorkerCount,

    #[arg(long, env = "KARDAMOM_ATTESTER_POST_INTERVAL", default_value_t = DEFAULT_ATTESTER_POST_INTERVAL)]
    pub(crate) attester_post_interval: PostInterval,

    // --- Interop serving surfaces (the outbox and attestation feeds) -----
    /// Enable the interop feed server on this address (`host:port`; port 0
    /// picks one). Off by default. When set, the validator extracts outbox
    /// messages from its re-executed receipts (cross-checked against BAL
    /// claims) and serves `kardamom_subscribeOutbox` +
    /// `kardamom_subscribeAttestations` over WS — the public-validator role.
    #[arg(long, env = "KARDAMOM_SERVE_FEED")]
    pub(crate) serve_feed: Option<std::net::SocketAddr>,
    /// How many blocks of outbox messages / attestations the feed retains.
    /// A subscriber whose cursor falls below the window is told `Lagged`;
    /// deeper backfill is a v2 concern (the data is in DA).
    #[arg(long, env = "KARDAMOM_FEED_RETENTION_BLOCKS", default_value_t = DEFAULT_FEED_RETENTION_BLOCKS)]
    pub(crate) feed_retention_blocks: RetentionBlocks,
    /// Cap on live feed subscriptions of both kinds (outbox and
    /// attestations) across all clients. A subscribe over the cap gets an
    /// RPC error. Default 256.
    #[arg(
        long,
        env = "KARDAMOM_FEED_MAX_SUBSCRIPTIONS",
        default_value_t = DEFAULT_FEED_MAX_SUBSCRIPTIONS
    )]
    pub feed_max_subscriptions: NonZeroUsize,
    /// Cap on live outbox subscriptions for one destination chain. A
    /// subscribe over the cap gets an RPC error. Default 8.
    #[arg(
        long,
        env = "KARDAMOM_FEED_MAX_SUBSCRIPTIONS_PER_DEST",
        default_value_t = DEFAULT_FEED_MAX_SUBSCRIPTIONS_PER_DEST
    )]
    pub feed_max_subscriptions_per_dest: NonZeroUsize,
    /// File the feed server's actually-bound address is written to
    /// (`host:port` + newline), for harnesses that pass port 0.
    #[arg(long, env = "KARDAMOM_SERVE_FEED_ADDR_FILE")]
    pub(crate) serve_feed_addr_file: Option<PathBuf>,
}

/// The flags of the L1 output attester, when it is on.
#[derive(Debug, Clone)]
pub(crate) struct AttesterArgs {
    pub(crate) l1_rpc_url: reqwest::Url,
    pub(crate) oracle: alloy_primitives::Address,
    pub(crate) key: AttesterKey,
}

impl Args {
    /// The attester's flags. `--output-oracle` and `--attester-key` turn
    /// the attester on together, and it then needs `--l1-rpc-url` too.
    /// `--l1-rpc-url` alone serves the epoch check and leaves the
    /// attester off.
    ///
    /// # Errors
    ///
    /// Returns an error if only one of `--output-oracle` and
    /// `--attester-key` is given, or if both are given without
    /// `--l1-rpc-url`.
    pub(crate) fn attester(&self) -> Result<Option<AttesterArgs>> {
        match (self.output_oracle, self.attester_key.clone()) {
            (None, None) => Ok(None),
            (Some(oracle), Some(key)) => {
                let l1_rpc_url = self.l1_rpc_url.clone().context(
                    "attestation needs --l1-rpc-url with --output-oracle and --attester-key",
                )?;
                Ok(Some(AttesterArgs {
                    l1_rpc_url,
                    oracle,
                    key,
                }))
            }
            _ => anyhow::bail!(
                "attestation needs --output-oracle and --attester-key together (got only one)"
            ),
        }
    }
}

/// The attester's private key, parsed once at the CLI boundary: `env:VAR`
/// (the deployer's key convention) is read from the environment
/// immediately, and the result is parsed into a signer. Downstream code
/// (`Written::spawn_attester`) takes the resolved signer directly, and
/// never re-parses the raw flag value.
// `PrivateKeySigner`'s own `Debug` impl prints only the address and chain
// id, never the key material, so deriving `Debug` here (required for
// `Args`'s own derive) does not log the secret.
#[derive(Debug, Clone)]
pub(crate) struct AttesterKey(PrivateKeySigner);

impl AttesterKey {
    #[must_use]
    pub(crate) fn into_signer(self) -> PrivateKeySigner {
        self.0
    }

    /// # Errors
    ///
    /// Returns an error (as a `String`, clap's custom-parser convention)
    /// if `key` names an unset `env:VAR`, or its resolved value is not a
    /// valid private key.
    fn parse(key: &str) -> std::result::Result<Self, String> {
        resolve_attester_key(key)
            .map(Self)
            .map_err(|e| e.to_string())
    }
}

/// Resolve the attester key flag: raw hex, or `env:VAR`, the deployer's
/// key convention, read from the environment; then parse it into a signer.
fn resolve_attester_key(key: &str) -> Result<PrivateKeySigner> {
    let raw = match key.strip_prefix("env:") {
        Some(var) => {
            std::env::var(var).with_context(|| format!("read attester key from env var {var}"))?
        }
        None => key.to_string(),
    };
    let hex = raw.trim().trim_start_matches("0x");
    PrivateKeySigner::from_str(hex).context("parse attester private key")
}

/// `--l1-rpc-url`'s `clap` value parser: parses once at the CLI boundary,
/// so `Written::spawn_attester` takes an already-valid `reqwest::Url`
/// instead of parsing the flag's raw string itself.
fn parse_l1_rpc_url(s: &str) -> std::result::Result<reqwest::Url, String> {
    s.parse::<reqwest::Url>().map_err(|e| e.to_string())
}

/// Blocks between L1 output posts, parsed once at the CLI boundary. A
/// `NonZeroU64` wrapper by name: distinguishes "how many blocks between
/// posts" from any other `NonZeroU64` this crate threads (for example
/// [`kardamom_validator::interop::RetentionBlocks`]) at every call site
/// that takes one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostInterval(NonZeroU64);

impl PostInterval {
    #[must_use]
    pub(crate) const fn new(n: NonZeroU64) -> Self {
        Self(n)
    }

    #[must_use]
    pub(crate) fn get(self) -> NonZeroU64 {
        self.0
    }
}

impl FromStr for PostInterval {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl std::fmt::Display for PostInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Worker threads in the parallel-validation pool, parsed once at the CLI
/// boundary: `--validation-workers 0` (or unset) is `Auto`, resolved to
/// `min(available_parallelism, 8)` at the one call site
/// (`build_block_exec`); anything else is a caller-fixed `Fixed` count,
/// hard-capped at 40 there. The `0`-means-auto sentinel is parsed into
/// this type once, instead of `build_block_exec` re-deriving "auto" from
/// a bare `usize` via `NonZeroUsize::new(..).is_none()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerCount {
    Auto,
    Fixed(NonZeroUsize),
}

impl FromStr for WorkerCount {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let n: usize = s.parse()?;
        Ok(NonZeroUsize::new(n).map_or(Self::Auto, Self::Fixed))
    }
}

impl std::fmt::Display for WorkerCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "0"),
            Self::Fixed(n) => write!(f, "{n}"),
        }
    }
}

#[cfg(test)]
mod tests;
