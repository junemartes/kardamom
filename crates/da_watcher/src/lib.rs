//! DA watcher: an async task that takes the records of the `l1_blocks`
//! stream, one per finalized L1 block, and publishes each record's epoch,
//! with its [`kardamom_types::Deposit`]s, on the dedicated `tx_deposits`
//! Aeron channel. The L1 follower (`kardamom-l1-indexer`) reads L1 and
//! derives the epochs; the watcher has no L1 access.
//!
//! Sequencers subscribe to `tx_deposits`, derive a [`kardamom_types::DepositRef`]
//! `(source_hash, deposit_position)`, and emit that ref on the canonical
//! `tx_ordering` channel. This mirrors the existing `tx_data → TxRef on
//! tx_ordering` flow for regular L2 transactions. Executors consume
//! `tx_ordering` and resolve deposits from the `tx_deposits` archive by
//! `deposit_position`.
//!
//! ## Layering
//!
//! - [`feed::BlockFeed`]: the watcher's view of `l1_blocks`: the live
//!   records, and the history of a block range from the stream's archives.
//!   The binary reads the Aeron stream; tests use a scripted fake.
//! - [`source::L1Source`]: an async trait for the L1 reads of the L1
//!   follower and the validator (`finalized_block_number`, `block_ids`,
//!   `headers`, `logs`), with production on an alloy provider
//!   ([`rpc_source::RpcL1Source`]). The watcher does not use it.
//! - [`sources::L1Sources`]: the set of sources a follower runs on. Two
//!   sources must agree on a block or a log query, or the light client
//!   serves it; a source that fails or lies rotates out for a backoff.
//! - [`publisher::EpochPublisher`]: the sink for the
//!   [`kardamom_types::Deposit`] records the watcher emits. Production wraps
//!   `kardamom_log::aeron_live::TxDepositsPublisherHandle`. Tests use the
//!   in-memory fake in [`publisher::fakes`].
//! - [`watcher::L1Watcher`]: the watcher state. `process_once` is one
//!   pass: it takes the records that arrived, in order, checks each
//!   record's parent link, publishes its epoch on `tx_deposits`, and
//!   advances the cursor. `spawn` wraps it in a loop that selects on the
//!   next record, the sealer's origin, the re-publish deadline, and a
//!   housekeeping tick. Returns a [`watcher::WatcherHandle`].
//! - [`cursor::CursorFile`]: the durable cursor of a watcher. The L1
//!   watcher keeps the sealer's confirmed L1 origin there
//!   ([`l1_cursor::L1Cursor`]), so a restart resumes after it and checks
//!   the parent link again.
//! - `window::Window`: the epochs the L1 watcher published that no sealer
//!   boundary confirmed yet. The watcher publishes them again when no
//!   boundary confirms one in time, so an epoch lost between the publish
//!   and the commit is not lost for good.
//!
//! ## Semantics
//!
//! The cursor lifecycle, OP source-hash derivation, and address aliasing
//! follow the deposit-monitor contract. The follower derives the epochs
//! with the same [`kardamom_types::epoch::derive_epoch`]; this crate
//! publishes them to the Aeron `tx_deposits` channel.
//!
//! Out of scope for this crate:
//! - Executor deposit execution (mint pre-credit plus the inner EVM call).
//!   That lives downstream, in `executor`: the executor consumes
//!   `tx_ordering`, dedups `DepositRef` by `source_hash`, resolves the
//!   `Deposit` from `tx_deposits`, and runs the deposit.
//! - Reorg handling. The stream carries finalized blocks only, so a record
//!   never changes.
//! - L1-attributes / system txs (OP `is_system_transaction = true`).
//!
//! ## Interop
//!
//! [`interop`] is the second source adapter that the spec's §6 asks this
//! crate to grow. It uses the same watch-derive-publish shape, with a peer
//! Kardamom chain as the origin instead of L1. It mirrors the layering
//! above, seam for seam: `RemoteChainSource` matches [`source::L1Source`],
//! `RemoteEpochPublisher` matches [`publisher::EpochPublisher`], and
//! `interop::InteropWatcher` matches [`watcher::L1Watcher`]. It
//! shares nothing else. The derivation rule lives in
//! `kardamom_types::xchain`, for the same reason the deposit rule lives in
//! `kardamom_types::epoch`.

// The `#[async_trait]` and `#[rpc]` macros expand trait methods to functions
// that carry a bare `#[must_use]` and return a pinned boxed future, which is
// `#[must_use]` on its own. The lint fires in the macros' output, so the crate
// allows it here.
#![allow(clippy::double_must_use)]
pub mod boundaries;
pub mod cursor;
pub mod feed;
pub mod interop;
pub mod l1_cursor;
pub mod metrics;
pub mod publisher;
pub mod rpc_source;
pub mod source;
pub mod sources;
pub mod watcher;
mod window;

// The deposit-derivation rule lives in `kardamom_types::epoch`, so the
// verifier shares it. A second copy would verify nothing.
pub use boundaries::BoundaryFeed;
pub use cursor::{CursorError, CursorFile};
pub use feed::BlockFeed;
pub use kardamom_types::epoch::{
    DepositLog, LockboxLog, UpgradeLog, alias_l1_address, source_hash, source_hash_system,
};
pub use l1_cursor::{L1Cursor, L1CursorError};
pub use publisher::{EpochPublisher, PublishError};
pub use rpc_source::RpcL1Source;
pub use source::{L1Header, L1Source, L1SourceError};
pub use sources::{L1Endpoints, L1Sources, SourceHalt};
pub use watcher::{
    DaWatcherConfig, L1ResumeAfter, L1Watcher, MonitorError, ResumeAfterError, START_WAIT,
    WatcherHandle,
};
