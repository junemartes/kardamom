//! Kardamom L1 batcher.
//!
//! Offline, archive-driven (S0): reads the canonical L2 stream from
//! the on-disk Aeron archives, groups it into per-block batches, packs them
//! into EIP-4844 blobs (KAR1 + zstd framing, no state-root field /!), and posts them to the `KardamomL2Settlement` data-availability
//! sink contract on L1.
//!
//! ## split data/ordering topology
//!
//! After the batcher reads from `M + 1` archives, not one:
//!
//! - **TxOrdering archive** carries the canonical orderer payload — only
//!   `TxOrderingMessage` records (`TxRef + BoundaryStart`). Tiny per-record.
//! - **Per-sequencer tx_data archives** carry the bulk `TxEnvelope` bytes.
//!   One per sequencer; opened on demand by [`multi_archive_reader`].
//!
//! [`multi_archive_reader::MultiArchiveReader`] is the glue: it walks B in
//! canonical order, resolving each `TxRef` against the appropriate tx_data
//! position index, and yields [`multi_archive_reader::ResolvedRecord`]s the
//! existing [`batch::BatchAccumulator`] can consume as-is.
//!
//! See `` for the full task plan and
//! `` / / for
//! the cross-cutting decisions that shape this crate.

pub mod archive_reader;
pub mod batch;
pub mod batcher;
pub mod blob;
pub mod compress;
pub mod da_store;
pub mod error;
pub mod frame;
pub mod l1;
pub mod live;
pub mod multi_archive_reader;
pub mod prover_submit;
pub mod recon;
pub mod rereplicate;
pub mod settlement;

pub use batch::{BatchAccumulator, ClosedBlock, RecordedTx};
pub use batcher::{Batcher, MockSender, PostedBatch, Sender};
pub use da_store::{BlobSource, FsBlobStore};
pub use error::BatcherError;
pub use frame::{BlockFrame, Kar1Payload, TxFrame};
pub use l1::{BatchDescriptor, post_batch, read_posted_batches, recover_blocks};
pub use multi_archive_reader::{MultiArchiveConfig, MultiArchiveReader, ResolvedRecord};
pub use prover_submit::{SubmitOutcome, submit_next_proof};
pub use rereplicate::{
    HealReport, MirrorReport, diff_mirror, heal_from_mirror, mirror_archive, verify_mirror,
};
