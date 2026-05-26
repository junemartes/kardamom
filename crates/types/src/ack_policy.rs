//! Acknowledgment policy: how long the ingress proxy waits before returning
//! success to a client that submitted a transaction.
//!
//! Picks one of four points along the durability ladder:
//!
//! | variant                  | waits for                                    | survives                          |
//! |--------------------------|----------------------------------------------|-----------------------------------|
//! | `OnOffer`                | receipt arrival only                         | process crash                     |
//! | `OnLocalFsync`           | this node's recorder having fsynced the byte | single-node power loss            |
//! | `OnQuorum` *(default)*   | Q-of-N recorders having fsynced the byte     | coordinated process crash, not DC |
//! | `OnLocalFsyncAndQuorum`  | both of the above (whichever is slower)      | belt-and-suspenders               |
//!
//! In the typical N=3, Q=2 healthy case the local node's own watermark counts
//! toward the quorum, so `OnQuorum` implies local fsync — making
//! `OnLocalFsyncAndQuorum` equivalent to `OnQuorum` in steady state. The
//! "and" variant only differs when local lags quorum, which can happen
//! transiently around recorder restarts.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub enum AckPolicy {
    /// No durability gate beyond receipt arrival. Lowest latency, weakest guarantee.
    OnOffer,
    /// Wait until this node's recorder has fsynced past the tx's B-position.
    OnLocalFsync,
    /// Wait until Q-of-N recorders have fsynced past the tx's B-position.
    #[default]
    OnQuorum,
    /// Wait until both this node's local fsync *and* the Q-of-N quorum have
    /// caught up. Stricter than either alone in transient states.
    OnLocalFsyncAndQuorum,
}

impl AckPolicy {
    /// Whether the ack gate needs to subscribe to the local recorder's
    /// per-recorder `FsyncWatermark` stream.
    pub fn requires_local_fsync(self) -> bool {
        matches!(self, Self::OnLocalFsync | Self::OnLocalFsyncAndQuorum)
    }

    /// Whether the ack gate needs to subscribe to the shared `QuorumWatermark`
    /// stream.
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
