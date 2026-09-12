//! `TxData` / `tx_ordering` reader threads and join buffer.
//!
//! `tx_ordering` carries only ~16-32 B [`TxOrderingMessage`] records (`TxRef`
//! or `BoundaryStart`). The full envelope bytes live on the per-lane
//! **`tx_data`** archives. The lane plane has a fixed size
//! (`kardamom_types::shard_map::LANE_COUNT`, 8). Below, M is that lane
//! count. An idle lane has a reader thread that blocks and never inserts.
//!
//! This module owns the M+1 reader thread topology:
//!
//! ```text
//!   ┌────────────────┐
//!   │ tx_data[0]   │──┐                                 ┌────────────┐
//!   │ reader thread  │  │   DashMap<(sid,tx_data_position),     │ exec thread│
//!   ├────────────────┤  │     TxEnvelope> "join buffer"   │ (revm)     │
//!   │ tx_data[1]   │──┤◄────insert────────────────────► │            │
//!   │ reader thread  │  │              ▲                  └────────────┘
//!   ├────────────────┤  │              │ lookup+remove          ▲
//!   │      …         │  │              │                        │ (BPosition,
//!   ├────────────────┤  │       ┌──────┴──────────┐             │  TxEnvelope)
//!   │ tx_data[M-1] │──┘       │ tx_ordering reader│─────────────┘
//!   └────────────────┘          │     thread      │ (also forwards
//!                               └─────────────────┘  BlockBoundaryStart
//!                                                    inline)
//! ```
//!
//! Each `tx_data` reader thread serves one Aeron subscription (`tx_data`[i]).
//! `rusteron_client::Aeron` is `!Send + !Sync`, so each subscription must own
//! its own Aeron client on its own OS thread. The reader only inserts each
//! fragment into the shared [`JoinBuffer`].
//!
//! The single `tx_ordering` reader pulls [`TxOrderingMessage`] records in
//! canonical order (system invariant I1). For each:
//!
//! - `TxRef`: look up `(sequencer_id, tx_data_position)` in the join buffer.
//!   If present, send `(b_position, TxEnvelope)` to the exec thread. If
//!   absent (a few µs of A-publisher lag), spin with a bounded backoff up to
//!   [`ReaderConfig::join_timeout`]. Beyond that, return
//!   [`ExecutorError::JoinTimeout`]: something is wrong upstream.
//! - `BoundaryStart`: forward as-is.
//!
//! The `BPosition` handed to exec is the `tx_ordering` position, the canonical
//! L2 position, not the `tx_data` position. Downstream consumers keep keying
//! on this.

pub mod cluster;
mod join;
mod ports;
mod threads;

pub use join::{JoinBuffer, ReaderConfig};
pub use ports::{
    EpochObserver, ExecSink, JoinRecovery, JoinRecoveryError, JoinRecoveryFactory, NoEpochCheck,
    NoRemoteEpochCheck, RemoteEpochObserver, SinkClosed, TxDataSubscription,
    TxOrderingSubscription,
};
pub use threads::{ReaderToExec, TxDataReader, TxOrderingInputs, TxOrderingReader};

#[cfg(test)]
mod tests;
