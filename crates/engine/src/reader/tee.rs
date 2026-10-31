//! [`FrameTee`]: the canonical frames that a reader delivers, in canonical
//! order, for the egress.
//!
//! A reader gets the frames of its source out of order: a live frame can
//! come before the replay that fills the gap in front of it, and a frame
//! can come twice. The tee holds the bytes of each record frame by its
//! canonical index, and of each boundary frame by its block, as they come.
//! When the reader delivers an item, the tee sends the bytes of that item
//! and forgets the older ones. So the egress gets each frame once, byte for
//! byte as the source sent it, in the order that the reader executes.
//!
//! After a restart, the tee also forwards the committed history that the
//! reader checks. It sends this history only after the full comparison
//! succeeds, so the new copy can chain from a boundary the egress holds.
//!
//! Each message is `[seq:u64][frame]`. The sequence counts every forwarded
//! frame, also one that the tee drops, so the egress sees a gap in this
//! copy as a jump of the sequence.
//!
//! The tee never waits: a full queue drops the frame, and the egress fills
//! the gap from the copy of another executor.

use std::collections::BTreeMap;

use crossbeam_channel::{Sender, TrySendError};
use kardamom_cluster_adapter::wire::{EGRESS_KIND_BOUNDARY, EGRESS_KIND_RELAYED};
use kardamom_types::{BPosition, TxOrderingMessage};

use super::Ordered;

/// The bytes of the sequence in front of each frame.
const SEQ_LEN: usize = 8;

/// The held frames of one reader, and the queue to the egress publisher.
pub struct FrameTee {
    out: Sender<Vec<u8>>,
    records: BTreeMap<u64, Vec<u8>>,
    boundaries: BTreeMap<u64, Vec<u8>>,
    /// The sequence of the next message.
    seq: u64,
}

impl FrameTee {
    /// A tee that sends the delivered frames to `out`.
    #[must_use]
    pub fn new(out: Sender<Vec<u8>>) -> Self {
        Self {
            out,
            records: BTreeMap::new(),
            boundaries: BTreeMap::new(),
            seq: 0,
        }
    }

    /// Hold one frame of the source until the reader delivers its item. The
    /// key is the u64 after the kind byte. A frame that is not a record or
    /// a boundary carries nothing for the egress.
    fn hold(&mut self, frame: &[u8]) {
        let Some((&kind, key)) = frame.split_first() else {
            return;
        };
        let Some(key) = key.first_chunk::<8>().map(|k| u64::from_le_bytes(*k)) else {
            return;
        };
        let held = match kind {
            EGRESS_KIND_RELAYED => &mut self.records,
            EGRESS_KIND_BOUNDARY => &mut self.boundaries,
            _ => return,
        };
        held.insert(key, frame.to_vec());
    }

    /// The reader delivered `item` at `position`: send its frame, and forget
    /// the frames of the same kind below it, which the reader delivered or
    /// dropped as copies.
    fn release(&mut self, position: BPosition, item: &Ordered) {
        if let Some(frame) = self.take(position, item) {
            self.send(&frame);
        }
    }

    fn take(&mut self, position: BPosition, item: &Ordered) -> Option<Vec<u8>> {
        let (held, key) = match item {
            Ordered::Message(TxOrderingMessage::BoundaryStart(b)) => {
                (&mut self.boundaries, b.block_number)
            }
            Ordered::Message(_) | Ordered::Tx(_) => (&mut self.records, position.as_index()),
        };
        let mut kept = held.split_off(&key);
        let frame = kept.remove(&key);
        *held = kept;
        frame
    }

    /// Send `frame` behind the next sequence number.
    fn send(&mut self, frame: &[u8]) {
        let mut message = Vec::with_capacity(SEQ_LEN + frame.len());
        message.extend_from_slice(&self.seq.to_le_bytes());
        message.extend_from_slice(frame);
        // One message for each forwarded frame: a u64 does not wrap.
        self.seq += 1;
        if let Err(TrySendError::Full(_)) = self.out.try_send(message) {
            metrics::counter!(crate::metrics::TEE_DROPPED_TOTAL).increment(1);
        }
    }
}

/// The tee of one reader: off, or forwarding through a [`FrameTee`].
#[derive(Default)]
pub(crate) struct Forward(Option<FrameTee>);

impl Forward {
    pub(crate) fn on(tee: FrameTee) -> Self {
        Self(Some(tee))
    }

    /// Hold one frame of the source. Off, this does nothing.
    pub(crate) fn hold(&mut self, frame: &[u8]) {
        if let Some(tee) = self.0.as_mut() {
            tee.hold(frame);
        }
    }

    pub(crate) fn take(&mut self, position: BPosition, item: &Ordered) -> Option<Vec<u8>> {
        self.0.as_mut()?.take(position, item)
    }

    pub(crate) fn send(&mut self, frame: &[u8]) {
        if let Some(tee) = self.0.as_mut() {
            tee.send(frame);
        }
    }

    /// The reader delivers `out`: forward its frame, and pass `out` on.
    pub(crate) fn release(&mut self, out: (BPosition, Ordered)) -> (BPosition, Ordered) {
        if let Some(tee) = self.0.as_mut() {
            tee.release(out.0, &out.1);
        }
        out
    }
}

#[cfg(test)]
#[path = "tee_tests.rs"]
mod tests;
