//! DA watcher: an async task that tails finalized L1 blocks, decodes
//! `DepositInitiated` events from the per-L2 `ETHLockbox` proxy, and
//! republishes each one onto the dedicated `tx_deposits` Aeron channel as a
//! [`kardamom_types::Deposit`].
//!
//! Sequencers subscribe to `tx_deposits`, derive a [`kardamom_types::DepositRef`]
//! `(source_hash, deposit_position)`, and emit that ref on the canonical
//! `tx_ordering` channel — mirroring the existing
//! `tx_data → TxRef on tx_ordering` flow for regular L2 txs. Executors
//! consume `tx_ordering` and resolve deposits from the `tx_deposits` archive
//! by `deposit_position`.
//!
//! ## Layering
//!
//! - [`source::L1Source`] — async trait abstracting the two L1 reads the
//!   watcher needs (`finalized_block_number`, `deposit_logs`). The trait is
//!   the seam for tests (mock impl) and production
//!   ([`rpc_source::RpcL1Source`] — alloy-provider-backed).
//! - [`publisher::EpochPublisher`] — sink for the
//!   [`kardamom_types::Deposit`] records the watcher emits. Production wraps
//!   `kardamom_log::aeron_live::TxDepositsPublisherHandle`; tests use the
//!   in-memory fake in [`publisher::fakes`].
//! - [`watcher::process_once`] — pure, single-pass function (one tick). The
//!   integration shape: read finalized tip, fetch logs in `(cursor, tip]`,
//!   build a `Deposit` from each log, publish on `tx_deposits`, advance
//!   cursor.
//! - [`watcher::spawn`] — wraps `process_once` in a `tokio::time::interval`
//!   loop with structured logging. Returns a [`watcher::WatcherHandle`].
//!
//! ## Spec inheritance
//!
//! Semantics (cursor lifecycle, NotFinalized handling, per-log error
//! continuation, OP source-hash derivation, address aliasing) are inherited
//! from `docs/agents/l1-deposit-monitor-spec.md` shipped on PR #10. This
//! crate is the new-architecture port of that work: it preserves the
//! L1-side logic and replaces the in-memory `Node::submit_deposit_transaction`
//! call with publishing to the Aeron `tx_deposits` channel.
//!
//! Out of scope for this crate:
//! - Executor deposit-execution (mint pre-credit + inner EVM call). That
//!   lives downstream in `executor`: the executor consumes
//!   `tx_ordering`, dedups `DepositRef` by `source_hash`, resolves the
//!   `Deposit` from `tx_deposits`, and runs the deposit. The wiring is a
//!   separate follow-up.
//! - Reorg handling. Finalized blocks do not reorg in normal Ethereum
//!   operation; the watcher trusts finality.
//! - L1-attributes / system txs (OP `is_system_transaction = true`).
//!
//! ## Interop
//!
//! [`interop`] is the second source adapter the spec's §6 asks this crate to
//! grow: the same watch-derive-publish shape with a PEER KARDAMOM CHAIN as
//! the origin instead of L1. It mirrors the layering above seam for seam
//! (`RemoteChainSource` ↔ [`source::L1Source`], `RemoteEpochPublisher` ↔
//! [`publisher::EpochPublisher`], `interop::watcher::process_once` ↔
//! [`watcher::process_once`]) and shares nothing else — the derivation rule
//! lives in `kardamom_types::xchain` for the same reason the deposit rule
//! lives in `kardamom_types::epoch`.

pub mod interop;
pub mod metrics;
pub mod publisher;
pub mod rpc_source;
pub mod source;
pub mod watcher;

// The deposit-derivation rule moved to `kardamom_types::epoch` so the
// VERIFIER can share it — a second copy would verify nothing (see
// docs/agents/l1-origin-deposit-derivation-spec.md). Re-exported here so
// existing callers keep working.
pub use kardamom_types::epoch::{DepositLog, alias_l1_address, deposit_from_log, source_hash};
pub use publisher::{EpochPublisher, PublishError};
pub use rpc_source::RpcL1Source;
pub use source::{L1Source, L1SourceError};
pub use watcher::{DaWatcherConfig, MonitorError, WatcherHandle, process_once, spawn};
