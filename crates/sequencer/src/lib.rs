//! Sequencer subsystem for the kardamom rollup.
//!
//! The sequencer is stateless. The in-memory `next_nonce` map is a cache,
//! and the sequencer can rebuild it from canonical sources. A cold sender
//! starts at nonce 0. In the warm steady state, the tx_data tail gives
//! visibility: every matched envelope advances the sender's nonce. The
//! receipt-floor resync (`crate::resync`) recovers committed floors out of
//! band. The sequencer holds no state-DB reader.
//!
//! Topology:
//!   - The proxy shards senders by address (`keccak(sender) % M`).
//!   - Each shard has an ordered group of K sequencers: one **preferred**
//!     sequencer, and the rest are followers. The proxy forwards
//!     transactions to the preferred sequencer. If no ack arrives within
//!     about 1 ms, the proxy retries the next follower and promotes it.
//!   - Sequencers are symmetric. There is no primary/standby distinction
//!     and no lease. The "preferred" pointer lives in the proxy's routing
//!     table, not in any sequencer's state.
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
pub mod inbound;
pub mod metrics;
mod nonce_decode;
pub mod outbound;
pub mod partition;
pub mod pending;
pub mod resync;
pub mod sender;
pub mod sequencer;
pub mod shutdown;
pub mod state;
mod unconfirmed;

pub use config::{BackpressurePolicy, SequencerConfig};
pub use epoch::{EpochSubscriber, process_epoch};
pub use error::SequencerError;
pub use sequencer::{Sequencer, Shutdown};

// Re-export shared types so external callers can write
// `kardamom_sequencer::TxError` without a separate dependency line.
pub use ::kardamom_types::{Deposit, DepositRef, TxError, TxErrorReason};
