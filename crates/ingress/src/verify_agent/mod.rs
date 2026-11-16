//! The verify agent of the validated ack: one thread that recovers the
//! sender of each transaction, checks its nonce, and offers it on its lane
//! of `tx_data`, with no task hop. See
//! `docs/specs/2026-10-23-hot-path-agents.md`, section 5.
//!
//! The handler of a submit stays on tokio. It does the intake checks, the
//! hash, the duplicate cache and the decode, then pushes the submit into
//! the ring of the agent and awaits its answer:
//!
//! ```text
//! handler (tokio): intake, hash, duplicate cache, decode -> push
//!   ring (ArrayQueue) -> agent (one thread):
//!     protocol limits -> recovery -> nonce (local layer) -> offer on its lane
//!     -> answer: the hash and the sender, or the error
//! ```
//!
//! - Two thread wakes for each submit: the push wakes a parked agent, and
//!   the answer wakes the handler. The tokio path has four.
//! - A refused offer waits in a [`kardamom_log::parked::ParkedQueue`] with
//!   the deadline of the Aeron thread, then answers `txpool is full`.
//! - A local miss admits. The agent never waits for Redis: the filler
//!   reads the row of a missed sender in the background (`fill`).
//! - The agent offers on lent publications (`LentPublication`), so it
//!   needs no hop to the Aeron thread.

mod agent;
mod doorbell;
mod fill;
mod lane;

pub use agent::VerifyItem;
pub(crate) use agent::{VerifyAgent, VerifyAgentHandle};
pub use doorbell::Doorbell;
pub(crate) use fill::{AccountFiller, MISS_CAPACITY};
pub use lane::{Admitted, AgentLanes, Answer, LaneOffer};

#[cfg(all(test, not(feature = "loom")))]
mod tests;
