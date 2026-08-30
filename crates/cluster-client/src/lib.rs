//! Minimal Rust-native Aeron Cluster client.
//!
//! Aeron's Raft Consensus Module is JVM-only, and there is no first-party
//! C/C++ cluster client to bind (rusteron vendors `aeron-cluster` as Java
//! only). But the client session protocol is just SBE messages over
//! ordinary Aeron pub/sub, and rusteron gives us that transport. This crate
//! implements the client side of that protocol directly:
//!
//! ```text
//!   client --ingress-->  [SessionConnectRequest] [SessionMessageHeader|payload]* [SessionKeepAlive]*
//!   client <--egress--   [SessionEvent(OK|REDIRECT)] [SessionMessageHeader|payload]* [NewLeaderEvent]*
//! ```
//!
//! ## Layers
//!
//! - [`bytes`]: `Option`-returning little-endian read primitives, shared
//!   with `kardamom-cluster-adapter`'s app-envelope codec. Each codec maps
//!   a miss to its own error type.
//! - [`protocol`]: a pure SBE codec for the client-facing cluster messages
//!   (schema id 111, `io.aeron.cluster.codecs`). It has no native
//!   dependencies.
//! - [`session`]: the sans-IO [`session::SessionDriver`] state machine. It
//!   handles the connect handshake, keep-alive, `NewLeaderEvent` redirects,
//!   and app framing.
//!
//! Both layers can be unit-tested deterministically. The live transport,
//! which drives an Aeron ingress publication and egress subscription,
//! layers on top in `kardamom-cluster-adapter`'s `live` module. That module
//! owns the IO, so this crate stays sans-IO, and the protocol can be tested
//! without a media driver.

pub mod bytes;
pub mod protocol;
pub mod session;
