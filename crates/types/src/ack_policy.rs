//! Acknowledgment policy: how long the ingress proxy waits before returning
//! success to a client that submitted a transaction.
//!
//! Picks one of four points along the durability ladder:
//!
//!   * `OnOffer` — waits for receipt arrival only. Survives a process crash.
//!   * `OnLocalFsync` — waits for this node's recorder to fsync the byte.
//!     Survives single-node power loss.
//!   * `OnQuorum` (the default) — waits for Q-of-N recorders to fsync the
//!     byte. Survives a coordinated process crash, but not a data-center loss.
//!   * `OnLocalFsyncAndQuorum` — waits for both of the above, whichever is
//!     slower. This is the strictest of the four options.
//!
//! In the typical healthy case (N=3, Q=2), the local node's own watermark
//! counts toward the quorum. So `OnQuorum` implies local fsync, and
//! `OnLocalFsyncAndQuorum` behaves the same as `OnQuorum` in steady state.
//! The "and" variant differs only when local fsync lags the quorum. This can
//! happen for a short time around recorder restarts.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub enum AckPolicy {
    /// No durability gate beyond receipt arrival. Lowest latency, weakest guarantee.
    OnOffer,
    /// Wait until this node's recorder has fsynced past the tx's B-position.
    OnLocalFsync,
    /// Wait until Q-of-N recorders have fsynced past the tx's B-position.
    #[default]
    OnQuorum,
    /// Wait until both this node's local fsync and the Q-of-N quorum catch
    /// up. This is stricter than either alone during transient states.
    OnLocalFsyncAndQuorum,
}

impl AckPolicy {
    /// Returns true if the ack gate must subscribe to the recorder's
    /// `FsyncWatermark` stream.
    pub fn requires_local_fsync(self) -> bool {
        matches!(self, Self::OnLocalFsync | Self::OnLocalFsyncAndQuorum)
    }

    /// Returns true if the ack gate must subscribe to the shared
    /// `QuorumWatermark` stream.
    pub fn requires_quorum(self) -> bool {
        matches!(self, Self::OnQuorum | Self::OnLocalFsyncAndQuorum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_on_quorum() {
        assert_eq!(AckPolicy::default(), AckPolicy::OnQuorum);
    }

    #[test]
    fn requires_flags_match_table() {
        for (policy, local, quorum) in [
            (AckPolicy::OnOffer, false, false),
            (AckPolicy::OnLocalFsync, true, false),
            (AckPolicy::OnQuorum, false, true),
            (AckPolicy::OnLocalFsyncAndQuorum, true, true),
        ] {
            assert_eq!(policy.requires_local_fsync(), local, "{policy:?}");
            assert_eq!(policy.requires_quorum(), quorum, "{policy:?}");
        }
    }
}
