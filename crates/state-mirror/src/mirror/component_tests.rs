//! Real mirror, checkpoint and Redis; only receipt delivery and Redis ACLs are controlled.

mod coverage_gap;
mod recovery;
#[allow(
    dead_code,
    reason = "the Sentinel fixture of the cache crate; this crate uses its total-loss part"
)]
#[path = "../../../cache/tests/sentinel_recovery/support.rs"]
mod sentinels;
mod support;
mod total_loss;
