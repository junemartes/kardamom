//! S2 sequencer subsystem for the kardamom rollup.
//!
//! One process per ingress partition (default M=8). Each process exclusively
//! owns a sender slice (`keccak(sender)[..8] % M`), maintains per-sender
//! next-nonce state, and publishes canonical-ordered raw `TxEnvelope` messages
//! into Aeron channel B. A hot-standby sibling tails B and takes over on lease
//! expiry, inheriting the in-lockstep `next_nonce` map so no sender's nonce
//! check resets to zero.
//!
//! ## D-Sh3 (sender trust)
//!
//! The proxy (S1) recovers the sender during batched secp256k1 verification and
//! writes it into `TxEnvelope.sender` (typed `Address`, never `Option`). This
//! crate trusts the field unconditionally — there is no fallback path, no
//! `recover_signer()` call, and no paranoid-check mode. The sequencer performs
//! zero secp256k1 work on the hot path.

pub mod config;
pub mod duplicate;
pub mod error;
pub mod inbound;
pub mod lease;
pub mod metrics;
pub mod outbound;
pub mod partition;
pub mod pending;
pub mod primary;
pub mod sender;
pub mod standby;
pub mod state;

// Re-exports are added incrementally per Task 2+. The empty modules above
// compile cleanly as empty Rust files; each task fills one in and may add
// its own re-export here.
