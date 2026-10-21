//! The egress: the stage after the executors.
//!
//! Every executor sends the canonical frames that it reads on
//! `exec_frames`. The egress merges the copies into one canonical stream
//! ([`merge`]), compares the copies, and keeps the stream on disk
//! ([`store`]). The readers after the executors (the batcher, the
//! validator, the clients) read the egress, never an earlier stage.

pub mod http;
pub mod merge;
pub mod metrics;
pub mod owner;
pub mod store;
