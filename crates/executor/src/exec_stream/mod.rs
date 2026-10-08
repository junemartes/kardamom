//! The executor stream (`exec_txs`): each transaction that this executor
//! joins, in canonical order.
//!
//! The `tx_ordering` reader sends each joined record, and a progress mark
//! after each message, on a bounded channel to one publisher thread. That
//! thread owns every part of the stream:
//!
//! - the recorded publication, an IPC publication that the archive on this
//!   node records. The publisher offers each record until the publication
//!   takes it. A slow or absent archive blocks the publisher, the channel
//!   fills, and the reader blocks: this executor stalls, and it never
//!   executes a record that its archive could not take;
//! - the live publication, a lossy publication for the consumers;
//! - the locator log ([`LocatorLog`]), which maps a canonical index to a
//!   position in a recording;
//! - the recorded cursor, the highest canonical index whose records the
//!   archive has written. It never passes the recording position.
//!
//! A recorder thread holds the local recording of this session and reads
//! its recording position. The executor serves only after that recording
//! is active.

mod cursor;
mod locators;
mod metrics;
mod open;
mod publisher;

#[cfg(test)]
mod tests;

pub use locators::{Locator, LocatorLog};
pub use metrics::ExecStreamMetrics;
pub use open::{ExecStream, ExecStreamConfig, ExecStreamThreads};
