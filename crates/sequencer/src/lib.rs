//! S2 sequencer subsystem for the kardamom rollup.
//!
//! Stateless sequencer: in-memory `next_nonce` map is treated as a cache,
//! reconstructable from canonical sources. Cold senders seed at nonce 0;
//! warm steady-state visibility comes from the tx_data tail (every matched
//! envelope advances the sender's nonce), and committed floors are recovered
//! out of band by the receipt-floor resync (`crate::resync`). The sequencer
//! holds no state-DB reader.
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
pub mod epoch;
pub mod error;
pub mod inbound;
pub mod metrics;
pub mod outbound;
pub mod partition;
pub mod pending;
pub mod resync;
pub mod sender;
pub mod sequencer;
pub mod state;

pub use config::{BackpressurePolicy, SequencerConfig};
pub use epoch::{EpochSubscriber, process_epoch};
pub use error::SequencerError;
pub use sequencer::{Sequencer, Shutdown};

// Re-export shared types so external callers can write
// `kardamom_sequencer::TxError` without a separate dependency line.
pub use ::kardamom_types::{Deposit, DepositRef, TxError, TxErrorReason};
