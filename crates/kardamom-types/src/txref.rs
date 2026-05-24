//! `TxRef` — the tiny canonical-orderer record carried on channel B in the
//! split (channel A + channel B) architecture (D-Sh12, spec D11 / §2.3).
//!
//! Channel B no longer carries full [`TxEnvelope`]s; it carries
//! `(sequencer_id, position_a)` pairs (~16 B per record) plus the occasional
//! [`BlockBoundaryStart`] from the sealer. The full transaction bytes live on
//! the per-sequencer **channel A** archive at `position_a`.
//!
//! See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.3 and
//! `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh12.

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Reference to a transaction stored on a per-sequencer channel A archive.
///
/// On the wire (channel B): ~16 B. The canonical L2 ordering is determined by
/// the B-Archive position of the containing record; this type carries the
/// (sequencer, A-position) pointer that downstream consumers (executor,
/// batcher) use to fetch the full [`TxEnvelope`] from the appropriate channel
/// A.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), compare(PartialEq))]
pub struct TxRef {
    /// Identifier of the channel-A publisher that wrote the underlying
    /// [`TxEnvelope`]. Matches the sequencer's partition id (`0..M`).
    pub sequencer_id: u8,
    /// Aeron position on channel A[sequencer_id] where the underlying
    /// envelope starts.
    pub position_a: BPosition,
}

impl TxRef {
    pub const fn new(sequencer_id: u8, position_a: BPosition) -> Self {
        Self {
            sequencer_id,
            position_a,
        }
    }
}
