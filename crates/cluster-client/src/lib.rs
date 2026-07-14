//! Minimal Rust-native Aeron Cluster **client**.
//!
//! Aeron's Raft Consensus Module is JVM-only and there is no first-party C/C++
//! cluster client to bind (rusteron vendors `aeron-cluster` as Java only). But
//! the *client session protocol* is just SBE messages over ordinary Aeron
//! pub/sub, and rusteron gives us that transport. This crate implements the
//! client side of that protocol directly:
//!
//! ```text
//!   client --ingress-->  [SessionConnectRequest] [SessionMessageHeader|payload]* [SessionKeepAlive]*
//!   client <--egress--   [SessionEvent(OK|REDIRECT)] [SessionMessageHeader|payload]* [NewLeaderEvent]*
//! ```
//!
//! ## Layers
//!
//! - [`protocol`] — pure SBE codec for the client-facing cluster messages
//!   (schema id = 111, `io.aeron.cluster.codecs`). No native deps.
//! - [`session`] — the sans-IO [`session::SessionDriver`] state machine
//!   (connect handshake, keep-alive, `NewLeaderEvent` redirect, app framing).
//!
//! Both layers are deterministically unit-testable. The live transport (driving
//! an Aeron ingress publication + egress subscription) layers on top in
//! `kardamom-cluster-adapter`'s `live` module — it owns the IO, so this crate
//! stays sans-IO and the protocol is testable without a media driver.

pub mod protocol;
pub mod session;
