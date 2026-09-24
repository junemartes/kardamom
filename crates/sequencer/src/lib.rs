//! Sequencer subsystem for the kardamom rollup.
//!
//! The sequencer is stateless. The in-memory `next_nonce` map is a cache,
//! and the sequencer can rebuild it from canonical sources. A cold sender
//! starts at nonce 0. In the warm steady state, the `tx_data` tail gives
//! visibility: every matched envelope advances the sender's nonce. Two
//! sources recover committed floors out of band: the receipt-floor resync
//! (`crate::resync`) and the executor nonce lookup (`crate::lookup`). The
//! sequencer holds no state-DB reader.
//!
//! Topology (see `docs/specs/dynamic-sequencer-sizing.md`):
//!   - The ingress routes a sender by virtual slot,
//!     `vslot = keccak256(sender)[..8] % 256`, through a versioned map from
//!     vslot to `tx_data` lane (`kardamom_types::shard_map`).
//!   - Each active lane has two racing replicas. Both read the lane's
//!     `tx_data` stream, both run the same state machine, and both offer the
//!     same refs. The Aeron Cluster dedups by canonical id, first seen.
//!     There is no preferred replica, no lease, and no routing table in a
//!     sequencer.
//!   - A replica serves a vslot set. A resize moves vslots between lanes:
//!     the gaining replica reads the old lane too, warms up in shadow mode,
//!     and then publishes in parallel with the old shard until the ingress
//!     switches maps and the old shard drains.
//!
//! ## Sender trust
//!
//! The ingress proxy recovers the sender during batched secp256k1 verification
//! and writes it into `TxEnvelope.sender` as a typed `Address`, never an
//! `Option`. This crate trusts the field unconditionally: there is no
//! fallback path, no `recover_signer()` call, and no paranoid-check mode.
//! The sequencer does zero secp256k1 work on the hot path.

pub mod config;
pub mod epoch;
pub mod error;
#[cfg(any(test, feature = "testing"))]
pub mod fakes;
pub mod inbound;
pub mod lookup;
pub mod metrics;
mod nonce_decode;
pub mod outbound;
pub mod partition;
pub(crate) mod pending;
pub mod pump;
pub mod remote_epoch;
pub mod resync;
pub(crate) mod sender;
pub mod sequencer;
pub mod shutdown;
pub(crate) mod state;
#[cfg(any(test, feature = "testing"))]
pub mod testkit;
mod unconfirmed;

pub use config::{BackpressurePolicy, SequencerConfig};
pub use epoch::EpochSubscriber;
pub use error::SequencerError;
pub use remote_epoch::RemoteEpochSubscriber;
pub use sequencer::{Sequencer, Shutdown};

// Re-export shared types so external callers can write
// `kardamom_sequencer::TxError` without a separate dependency line.
pub use ::kardamom_types::{Deposit, DepositRef, TxError, TxErrorReason};
