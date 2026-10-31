//! The egress: the stage after the executors.
//!
//! Every executor sends the canonical frames that it reads on
//! `exec_frames`. The egress merges the copies into one canonical stream
//! ([`merge`]), compares the copies, and keeps the stream on disk
//! ([`store`]). The readers after the executors (the batcher, the
//! validator, the clients) read the egress, never an earlier stage.
//!
//! The store keeps every block that L1 does not hold for good yet
//! ([`l1_heads`]): the posted head comes from the `l1_blocks` stream of
//! the L1 follower, and the final head from the output oracle on L1
//! ([`final_head`]).
//!
//! The egress also keeps the drops that the sequencers report ([`drops`]),
//! so a client asks one place for the status of its transaction.

pub mod catchup;
pub mod drops;
pub mod final_head;
pub mod http;
pub mod l1_heads;
pub mod merge;
pub mod metrics;
pub mod owner;
pub mod receipts;
pub mod ref_mark;
pub mod store;
#[cfg(test)]
pub(crate) mod test_support;
