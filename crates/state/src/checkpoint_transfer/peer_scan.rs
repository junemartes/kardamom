//! The fetch over a list of peers: probe every peer, then download one
//! image, from the peer with the furthest restore point.

use std::path::Path;

use alloy_primitives::B256;
use tracing::{info, warn};

use super::{CheckpointFetch, CheckpointHead, FetchRequest, fetch_latest_checkpoint};
use crate::checkpoint::{BlockRange, CheckpointInfo};

/// One fetch over a list of peers: the peers, the local checkpoint
/// directory, the range of the restore, and the chain of the node.
pub(super) struct PeerScan<'a> {
    pub(super) peers: &'a [String],
    pub(super) checkpoints_dir: &'a Path,
    pub(super) range: BlockRange,
    pub(super) expected_genesis: Option<B256>,
}

/// The image that one peer offers, read from its response head before
/// any download.
struct PeerOffer<'a> {
    peer: &'a str,
    head: CheckpointHead,
}

impl PeerOffer<'_> {
    /// The rank of the offer. A higher rank is a better offer. The first
    /// key is the restore point: the end of the log of the peer after its
    /// image. The restore applies that log, and the range check reads the
    /// same end. The second key is the image block: of two equal restore
    /// points, the newer image leaves less log to fetch and apply.
    fn rank(&self) -> (u64, u64) {
        (self.head.log_end, self.head.block)
    }
}

impl<'a> PeerScan<'a> {
    /// Probe every peer, then fetch from the best offer. The sort is
    /// stable, so of two equal offers the one earlier in the list wins.
    pub(super) fn best(&self) -> Option<CheckpointInfo> {
        let mut offers: Vec<PeerOffer<'a>> = self
            .peers
            .iter()
            .filter_map(|peer| self.probe(peer))
            .collect();
        offers.sort_by_key(|offer| std::cmp::Reverse(offer.rank()));
        offers.iter().find_map(|offer| self.take(offer))
    }

    /// Read the offer of `peer`, with no body download. A peer that does
    /// not answer, or offers nothing that serves the range, gives `None`.
    fn probe(&self, peer: &'a str) -> Option<PeerOffer<'a>> {
        let request = FetchRequest { range: self.range }.line();
        let head = CheckpointFetch::connect(peer, &request, self.expected_genesis)
            .and_then(|mut fetch| fetch.offer(self.range))
            .inspect_err(|e| warn!(peer, error = %e, "checkpoint probe of peer failed"))
            .ok()??;
        info!(
            peer,
            block = head.block,
            log_end = head.log_end,
            "checkpoint peer offers an image"
        );
        Some(PeerOffer { peer, head })
    }

    /// Fetch the checkpoint of `offer`, and the block log after it.
    /// `None` when the fetch fails, or when the base and the local log
    /// do not reach the range.
    fn take(&self, offer: &PeerOffer) -> Option<CheckpointInfo> {
        let peer = offer.peer;
        let base = fetch_latest_checkpoint(
            peer,
            self.checkpoints_dir,
            self.range,
            self.expected_genesis,
        )
        .inspect_err(|e| warn!(peer, error = %e, "checkpoint fetch from peer failed"))
        .ok()??;
        if let Err(e) =
            super::log_fetch::fetch_log(peer, self.checkpoints_dir, &base, self.range.max)
        {
            warn!(peer, error = %e, "the block log fetch from peer failed; the base restores alone");
        }
        if !base.reaches(self.range) {
            warn!(
                peer,
                block = base.block,
                "the fetched checkpoint and its log do not reach the range; trying the next peer"
            );
            return None;
        }
        Some(base)
    }
}
