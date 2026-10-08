//! The executor stream record: one transaction that an executor joined.
//!
//! An executor publishes one record for each [`TxRef`] that it joins, in
//! canonical order. A consumer of transaction bytes reads these records and
//! does not join `tx_data` itself. A consumer accepts a record at `index`
//! only when `keccak256(envelope.raw_tx)` equals the `tx_hash` of the
//! [`TxRef`] that the canonical order carries at `index`.

use rkyv::{Archive, Deserialize, Serialize};

use crate::envelope::TxEnvelope;
use crate::txref::TxRef;

/// The transaction that an executor joined at one canonical index.
///
/// The record carries only `TxRef` entries. Epochs, deposits, remote
/// epochs, boundaries and voids are in the canonical order already.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct ExecTxRecord {
    /// The canonical index of the [`TxRef`].
    pub index: u64,
    /// The reference as the canonical order carried it. An executor that
    /// fetches this record puts the envelope into its `tx_data` join buffer
    /// under the key of this reference.
    pub tx_ref: TxRef,
    /// The envelope that this executor joined for `tx_ref`.
    pub envelope: TxEnvelope,
}
