//! The invariants every follower case ends on: the stream carried only
//! true records, contiguous through a block; the da-watcher published
//! each epoch once, in order, with no gap; the archive holds every posted
//! batch with its payload.

use std::collections::BTreeMap;

use kardamom_l1_indexer::L1Block;
use kardamom_types::EpochRecord;

use super::rig::Rig;

impl Rig {
    /// Every record on the stream is the chain's record of its block, and
    /// the records cover `1..=through` with no gap. Copies are allowed: a
    /// follower that fails before its cursor write publishes a range again.
    pub(crate) fn assert_stream_true_through(&self, through: u64) {
        let records = self.stream.records();
        let by_number: BTreeMap<u64, &L1Block> = records
            .iter()
            .map(|record| (record.number, record))
            .collect();
        for record in &records {
            assert_eq!(
                *record,
                self.l1.record(record.number),
                "a published record of block {} is not the chain's",
                record.number
            );
        }
        let numbers: Vec<u64> = by_number.keys().copied().collect();
        let expected: Vec<u64> = (1..=through).collect();
        assert_eq!(
            numbers.get(..expected.len()),
            Some(expected.as_slice()),
            "the stream must carry every block through {through}"
        );
    }

    /// The highest block on the stream.
    pub(crate) fn stream_head(&self) -> Option<u64> {
        self.stream.head()
    }

    /// The da-watcher published each epoch of `2..=through` once, in block
    /// order, each the chain's epoch, and nothing past `through`. Block 1
    /// is the record a first start anchors at.
    pub(crate) fn assert_epochs_once_through(&mut self, through: u64) {
        let expected: Vec<EpochRecord> = (2..=through).map(|n| self.l1.record(n).epoch).collect();
        assert_eq!(self.epochs(), expected.as_slice());
    }

    /// The archive holds batches `1..=last` with their payloads, and its
    /// cursor names `last`.
    pub(crate) fn assert_archive_through(&self, last: u64) {
        (1..=last).for_each(|index| {
            let entry = self
                .store
                .batch(index)
                .unwrap()
                .unwrap_or_else(|| panic!("the archive lacks batch {index}"));
            let payload = self.store.payload(&entry.da_cert).unwrap().unwrap();
            assert_eq!(payload, format!("payload of batch {index}").into_bytes());
        });
        assert_eq!(self.store.batch(last + 1).unwrap(), None);
        assert_eq!(self.store.cursor().unwrap().last_batch, Some(last));
    }
}
