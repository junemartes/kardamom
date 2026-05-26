//! S2 sequencer subsystem for the kardamom rollup.
//!
//! Stateless sequencer: in-memory `next_nonce` map is treated as a cache,
//! reconstructable from canonical sources (state DB for cold senders, plus
//! tx_data tail for warm steady-state visibility — the latter is a
//! follow-up; today the sequencer falls back to state DB on cache miss).
//!
//! Topology:
//!   - Proxy shards senders by address (`keccak(sender) % M`).
//!   - Each shard has an ordered group of K sequencers: one **preferred**,
//!     the rest **followers**. Proxy forwards txs to the preferred; if no
//!     ack within ~1ms, retries to the next follower and promotes it.
//!   - Sequencers themselves are symmetric. No primary/standby distinction,
//!     no lease — the "preferred" pointer lives in the proxy's routing
//!     table, not in any sequencer's state.
//!
//! ## Sender trust
//!
//! The proxy (S1) recovers the sender during batched secp256k1 verification and
//! writes it into `TxEnvelope.sender` (typed `Address`, never `Option`). This
//! crate trusts the field unconditionally — there is no fallback path, no
//! `recover_signer()` call, and no paranoid-check mode. The sequencer performs
//! zero secp256k1 work on the hot path.

pub mod config;
pub mod error;
pub mod inbound;
pub mod metrics;
pub mod outbound;
pub mod partition;
pub mod pending;
pub mod sender;
pub mod sequencer;
pub mod state;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use config::{BackpressurePolicy, SequencerConfig};
pub use error::SequencerError;
pub use sequencer::{Sequencer, Shutdown};

// Re-export shared types so external callers can write
// `kardamom_sequencer::TxError` without a separate dependency line.
pub use ::kardamom_types::{TxError, TxErrorReason};
