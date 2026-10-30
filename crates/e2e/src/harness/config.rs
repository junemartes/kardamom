//! Stack configuration: [`StackConfig`] and the L2 genesis choice.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

use super::services::IngressOptions;

/// The dev chain id (`deploy/cluster/config/genesis/dev.toml`).
pub const DEV_CHAIN_ID: NonZeroU64 = NonZeroU64::new(412_346).unwrap();

/// Default shard count: [`StackConfig::default`]. Matches the deployed
/// cluster's shard count.
const DEFAULT_SHARDS: NonZeroU32 = NonZeroU32::new(2).unwrap();

const DEFAULT_CLUSTER_TICK_MS: NonZeroU64 = NonZeroU64::new(250).unwrap();

/// Stack settings a scenario can tune. The defaults match the deployed
/// shape where it matters (shards=2, like the cluster), and use a
/// test-friendly value where it does not (250 ms boundary ticks).
#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool below is an independent, orthogonal bring-up toggle (not a state \
               machine), and scenarios set only the ones they need with \
               ..StackConfig::default(); a flags enum would not reduce the call-site noise"
)]
pub struct StackConfig {
    pub shards: NonZeroU32,
    pub sealer_members: NonZeroUsize,
    pub cluster_tick_ms: NonZeroU64,
    pub ingress: IngressOptions,
    /// Also run `kardamom-validator` (used by the consistency and
    /// divergence-detection tests). Off by default: the nonce and RPC
    /// scenarios do not need it, and stacks stay lighter without it.
    pub validator: bool,
    /// Run the validator with `--parallel-validation` (the deployed
    /// cluster's mode: seeded BAL batches on the whole-block path). The
    /// consistency and two-stacks scenarios set it so the validator's
    /// verdict covers the whole-block arm, not just the streaming one.
    pub validator_parallel: bool,
    /// Run the validator with `--serve-feed` (the public-validator role):
    /// outbox extraction + the WS feed surfaces. The bound address lands
    /// in `<root>/validator-feed.addr`; see
    /// [`crate::harness::LocalStack::validator_feed_url`].
    pub validator_serve_feed: bool,
    /// The sealer's remote-origin allowlist
    /// (`-Dkardamom.cluster.remoteOrigins`): the peer chain ids whose
    /// kind-5 records the cluster seals. Empty disables interop, and every
    /// remote-origin record is rejected. The xchain scenarios set it.
    pub remote_origins: Vec<u64>,
    /// Trie shadow-check cadence for the validator. `Some(1)` checks every
    /// block, the semantics-suite default. The cluster runs with 8.
    pub trie_shadow_check: Option<NonZeroU64>,
    /// The L2 chain id. Defaults to [`DEV_CHAIN_ID`] (what every genesis TOML
    /// in the repo declares). A different id makes launch materialise a
    /// patched copy of the genesis into the stack root (same alloc + predeploy
    /// blobs, overridden `chain_id` line) — the two-stack interop scenario
    /// needs a distinct chain B without duplicating predeploy bytecode in a
    /// sibling TOML (the deployer's genesis drift guard stays pointed at the
    /// canonical files).
    pub chain_id: NonZeroU64,
    /// Which L2 genesis to run. Bridge scenarios need
    /// [`Genesis::DevWithdrawals`] for the `L2ToL1MessagePasser` predeploy.
    pub genesis: Genesis,
    /// Bring up anvil, the bridge contracts, the da-watcher, and, when a
    /// validator runs, its L1 output attester.
    pub l1: bool,
    /// Record `tx_data` into the shared Aeron archive (the ingress
    /// `--archive-durability` flag) and turn on the consumers' join-miss
    /// refetch. Crash recovery needs this: a restarted consumer replays
    /// canonical records from its persisted cursor, but the envelopes for
    /// those records were published while it was down, so only the
    /// archive still has them. This costs a recorder-startup barrier at
    /// bring-up, so it is opt-in.
    pub archive_durability: bool,
    /// Route the validator's L1 reads through the mock verified endpoint
    /// (`l1_verified`), the way production routes them through a light
    /// client. The da-watcher keeps talking to anvil directly. Only the
    /// verifier's view is interposed, so a fault isolates to verification
    /// and does not also corrupt the epochs being produced.
    pub verified_l1: bool,
    /// Priority fees on every role: the sequencer's tip and fee checks,
    /// the sealer's ordering window, and the executor's fee schedule
    /// (the `[fees]` fragment appended to the genesis), as the deploy's
    /// one `PRIORITY_FEES` value sets them.
    pub priority_fees: bool,
    /// The local canary's accounts, Anvil #34 to #37
    /// (`deploy/cluster/config/genesis/canary.toml`, appended to the
    /// genesis), as the deploy funds them for `CANARY_LOCAL=1`.
    pub canary_genesis: bool,
    /// Also run `kardamom-notifier`, the transaction status feed. The
    /// feed's WebSocket URL is [`crate::harness::LocalStack::notifier_ws_url`].
    pub notifier: bool,
    /// The scenario sends transactions that cannot pay, so that the
    /// executor writes skip receipts. The v2 loop refuses such a
    /// transaction before the sealer, so the sequencers keep the v1 loop
    /// under `KARDAMOM_E2E_ADMISSION=1`.
    pub unpayable_spends: bool,
    /// The scenario reads the local account layer of the ingress, which
    /// only the full receipt stream fills. The ingress keeps `tx_receipts`
    /// under `KARDAMOM_E2E_VERDICTS=1`.
    pub ingress_rows: bool,
    /// The carry window rollout: the member feed, the egress, and the
    /// batcher. `None` runs none of them.
    pub carry: Option<CarryOptions>,
    /// The read path: a Redis container and a state mirror next to the
    /// executor, which writes checkpoints for the mirror's rebuild.
    pub read_path: bool,
    /// With `read_path`: every sequencer reads that Redis in its nonce
    /// lookup before it asks the executor, as in the deployment.
    pub sequencer_cache: bool,
    /// Every sequencer runs with `--first-floor-from-lookup`: a sender
    /// gets its first floor only from a lookup. The sequencer binary must
    /// have the `floor-seams` feature.
    #[cfg(feature = "floor-seams")]
    pub first_floor_from_lookup: bool,
    /// Run the sequencers on the v2 loop, as `KARDAMOM_E2E_ADMISSION=1`
    /// does for the whole suite.
    pub v2_loop: bool,
}

/// The parts of the stack that the carry window rollout needs.
///
/// - Each sealer member serves its admin endpoint (`/status`, `/metrics`)
///   and keeps `retention` frames in its replay window.
/// - Member 0 runs the member feed on the shared media driver, so the
///   executor can read it in carry mode. The feed uses fixed stream ids on
///   IPC, so only one member of a host can feed one driver.
/// - The sequencers run the v2 loop: only the v2 loop offers a transaction
///   with its bytes.
/// - The executor forwards its frames on `exec_frames`, and the egress
///   stores them.
/// - With `StackConfig::l1`, a live batcher posts to the settlement
///   contract through a fake DA proxy.
#[derive(Clone, Copy, Debug)]
pub struct CarryOptions {
    /// `-Dkardamom.cluster.retention` on every member.
    pub retention: NonZeroU32,
    /// The carry mode from genesis. Without it, the chain starts on the
    /// reference rules, and the carry window comes with upgrade
    /// transactions, as on a live chain. With it:
    ///
    /// - Every member starts with the features 3, 4 and 5 at genesis
    ///   (`-Dkardamom.cluster.genesisFeatures=3,4,5`), and takes a periodic
    ///   snapshot every [`GENESIS_SNAPSHOT_INTERVAL_S`] seconds.
    /// - The L2 genesis holds the activation words of the same features
    ///   (`genesis_features = [3, 4, 5]`).
    /// - Each sequencer starts at the floor 5.
    /// - The executor reads the member feed and the egress from its first
    ///   block, and the validator and the batcher read the egress.
    pub genesis: bool,
}

/// The periodic snapshot interval of a member on a chain in carry mode
/// from genesis, in seconds. A restart before the first snapshot needs
/// the bring-up to end well before it, and a restart from a snapshot
/// needs it to come during the scenario.
pub const GENESIS_SNAPSHOT_INTERVAL_S: u64 = 90;

/// The genesis features of a chain in carry mode from genesis, as the
/// sealer setting and the L2 genesis TOML spell them.
pub(crate) const GENESIS_FEATURES: &str = "3,4,5";

/// The L2 genesis a stack boots from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Genesis {
    /// `deploy/cluster/config/genesis/dev.toml`: 18 prefunded dev accounts,
    /// with no withdrawal predeploy. This is what the deployed cluster
    /// runs.
    ClusterDev,
    /// `chains/dev-withdrawals.toml`: the `L2ToL1MessagePasser` predeploy
    /// at `0x42…16`, but only account #0 is prefunded.
    DevWithdrawals,
    /// `chains/dev-interop.toml` — the cross-chain `Outbox`/`Inbox`
    /// predeploys at `0x42…E0`/`0x42…E1`, only account #0 prefunded. The
    /// xchain scenario needs it.
    DevInterop,
}

impl Genesis {
    pub(super) fn path(self, repo: &std::path::Path) -> PathBuf {
        match self {
            Genesis::ClusterDev => repo.join("deploy/cluster/config/genesis/dev.toml"),
            Genesis::DevWithdrawals => repo.join("chains/dev-withdrawals.toml"),
            Genesis::DevInterop => repo.join("chains/dev-interop.toml"),
        }
    }

    /// The fee schedule fragment the deploy appends with priority fees
    /// on: the same file the executor and validator jobs render.
    pub(super) fn fees_fragment(repo: &std::path::Path) -> PathBuf {
        repo.join("deploy/cluster/config/genesis/fees.toml")
    }

    /// The local canary's fragment: the four accounts of its ring, the
    /// same file the executor and validator jobs append for it.
    pub(super) fn canary_fragment(repo: &std::path::Path) -> PathBuf {
        repo.join("deploy/cluster/config/genesis/canary.toml")
    }
}

impl StackConfig {
    /// Whether the stack runs the carry mode from genesis
    /// ([`CarryOptions::genesis`]).
    #[must_use]
    pub fn carry_genesis(&self) -> bool {
        self.carry.is_some_and(|c| c.genesis)
    }
}

impl Default for StackConfig {
    fn default() -> Self {
        Self {
            shards: DEFAULT_SHARDS,
            sealer_members: NonZeroUsize::MIN,
            cluster_tick_ms: DEFAULT_CLUSTER_TICK_MS,
            ingress: IngressOptions::default(),
            validator: false,
            validator_parallel: false,
            validator_serve_feed: false,
            remote_origins: Vec::new(),
            trie_shadow_check: Some(NonZeroU64::MIN),
            chain_id: DEV_CHAIN_ID,
            genesis: Genesis::ClusterDev,
            l1: false,
            archive_durability: false,
            verified_l1: false,
            priority_fees: false,
            canary_genesis: false,
            notifier: false,
            unpayable_spends: false,
            ingress_rows: false,
            carry: None,
            read_path: false,
            sequencer_cache: false,
            #[cfg(feature = "floor-seams")]
            first_floor_from_lookup: false,
            v2_loop: false,
        }
    }
}
