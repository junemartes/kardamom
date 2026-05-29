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
//! - [`publisher::DepositPublisher`] — sink for the
//!   [`kardamom_types::Deposit`] records the watcher emits. Production wraps
//!   `kardamom_log::TxDepositsPublisher`; tests use the in-memory fake in
//!   [`publisher::fakes`].
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

pub mod derive;
pub mod metrics;
pub mod publisher;
pub mod rpc_source;
pub mod source;
pub mod watcher;

pub use derive::{alias_l1_address, source_hash};
pub use publisher::{DepositPublisher, PublishError};
pub use rpc_source::RpcL1Source;
pub use source::{DepositLog, L1Source, L1SourceError};
pub use watcher::{DaWatcherConfig, MonitorError, WatcherHandle, process_once, spawn};
