//! DA watcher: an async task that tails finalized L1 blocks, decodes
//! `DepositInitiated` events from the per-L2 `ETHLockbox` proxy, and
//! republishes each one on the dedicated `tx_deposits` Aeron channel as a
//! [`kardamom_types::Deposit`].
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
//! - [`source::L1Source`]: an async trait for the two L1 reads the watcher
//!   needs (`finalized_block_number`, `lockbox_logs`). The trait is the seam
//!   for tests (a mock impl) and for production
//!   ([`rpc_source::RpcL1Source`], backed by an alloy provider).
//! - [`publisher::EpochPublisher`]: the sink for the
//!   [`kardamom_types::Deposit`] records the watcher emits. Production wraps
//!   `kardamom_log::aeron_live::TxDepositsPublisherHandle`. Tests use the
//!   in-memory fake in [`publisher::fakes`].
//! - [`watcher::process_once`]: a pure, single-pass function for one tick.
//!   It reads the finalized tip, fetches logs in `(cursor, tip]`, builds a
//!   `Deposit` from each log, publishes on `tx_deposits`, and advances the
//!   cursor.
//! - [`watcher::spawn`]: wraps `process_once` in a `tokio::time::interval`
//!   loop with structured logging. Returns a [`watcher::WatcherHandle`].
//!
//! ## Semantics
//!
//! The cursor lifecycle, `NotFinalized` handling, per-log error
//! continuation, OP source-hash derivation, and address aliasing follow the
//! deposit-monitor contract. This crate owns the L1-side logic and
//! publishes to the Aeron `tx_deposits` channel.
//!
//! Out of scope for this crate:
//! - Executor deposit execution (mint pre-credit plus the inner EVM call).
//!   That lives downstream, in `executor`: the executor consumes
//!   `tx_ordering`, dedups `DepositRef` by `source_hash`, resolves the
//!   `Deposit` from `tx_deposits`, and runs the deposit.
//! - Reorg handling. Finalized blocks do not reorg in normal Ethereum
//!   operation; the watcher trusts finality.
//! - L1-attributes / system txs (OP `is_system_transaction = true`).
//!
//! ## Interop
//!
//! [`interop`] is the second source adapter that the spec's §6 asks this
//! crate to grow. It uses the same watch-derive-publish shape, with a peer
//! Kardamom chain as the origin instead of L1. It mirrors the layering
//! above, seam for seam: `RemoteChainSource` matches [`source::L1Source`],
//! `RemoteEpochPublisher` matches [`publisher::EpochPublisher`], and
//! `interop::watcher::process_once` matches [`watcher::process_once`]. It
//! shares nothing else. The derivation rule lives in
//! `kardamom_types::xchain`, for the same reason the deposit rule lives in
//! `kardamom_types::epoch`.

pub mod interop;
pub mod metrics;
pub mod publisher;
pub mod rpc_source;
pub mod source;
pub mod watcher;

// The deposit-derivation rule lives in `kardamom_types::epoch`, so the
// verifier shares it. A second copy would verify nothing.
pub use kardamom_types::epoch::{
    DepositLog, LockboxLog, UpgradeLog, alias_l1_address, deposit_from_log, source_hash,
    source_hash_system, upgrade_from_log,
};
pub use publisher::{EpochPublisher, PublishError};
pub use rpc_source::RpcL1Source;
pub use source::{L1Source, L1SourceError};
pub use watcher::{DaWatcherConfig, MonitorError, WatcherHandle, process_once, spawn};
