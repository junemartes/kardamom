//! `ClusterWatermark`: the durable position that the ingress's on-quorum ack
//! gate consumes. It comes from cluster egress progress.
//!
//! A record reaches egress only after the leader's replicated state machine
//! processes it. This only happens once a Raft quorum commits the record. So
//! a relayed record (or a boundary) on egress is a quorum-durability signal:
//! the durable canonical count is the highest value seen so far. This
//! replaces the old standalone sealer's archive-recording-position
//! watermark. The message it sends to ingress is unchanged: a monotonic
//! count.

/// Monotonic durable-count watermark.
#[derive(Debug, Clone, Default)]
pub struct ClusterWatermark {
    durable_count: u64,
}

impl ClusterWatermark {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe a relayed record's 0-based `index`. A record at index `i` means
    /// `i + 1` canonical records are now durable.
    pub fn observe_record(&mut self, index: u64) -> u64 {
        self.durable_count = self.durable_count.max(index + 1);
        self.durable_count
    }

    /// Observe a boundary's `end_tx_idx` (already a canonical count).
    pub fn observe_boundary(&mut self, end_tx_idx: u64) -> u64 {
        self.durable_count = self.durable_count.max(end_tx_idx);
        self.durable_count
    }

    /// Current durable canonical count (never regresses).
    pub fn position(&self) -> u64 {
        self.durable_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_index_advances_count_by_one() {
        let mut w = ClusterWatermark::new();
        assert_eq!(w.observe_record(0), 1);
        assert_eq!(w.observe_record(1), 2);
        assert_eq!(w.position(), 2);
    }

    #[test]
    fn never_regresses() {
        let mut w = ClusterWatermark::new();
        w.observe_record(9); // count = 10
        assert_eq!(w.observe_record(3), 10); // stale, ignored
        assert_eq!(w.observe_boundary(5), 10); // stale, ignored
        assert_eq!(w.observe_boundary(20), 20); // advances
    }
}
