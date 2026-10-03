//! Kardamom L1 batcher.
//!
//! This is the offline, archive-driven design. It reads the canonical L2
//! stream from the on-disk Aeron archives. It groups the stream into
//! per-block batches. It frames the batches (KAR1 format with zstd, no
//! state-root field), disperses them to EigenDA through its proxy, and
//! posts each certificate to the `KardamomL2Settlement` data-availability
//! sink contract on L1.
//!
//! ## Split data/ordering topology
//!
//! The batcher reads from `M + 1` archives, not one:
//!
//! - The **`TxOrdering` archive** carries the canonical orderer payload. It
//!   holds only `TxOrderingMessage` records (`TxRef + BoundaryStart`). Each
//!   record is small.
//! - The **per-sequencer `tx_data` archives** carry the bulk `TxEnvelope`
//!   bytes. There is one archive per sequencer. [`multi_archive_reader`]
//!   opens each archive on demand.
//!
//! [`multi_archive_reader::MultiArchiveReader`] connects the two archives. It
//! walks the ordering archive in canonical order. For each `TxRef`, it looks
//! up the position in the matching `tx_data` index. It yields
//! [`multi_archive_reader::ResolvedRecord`]s. The existing
//! [`batch::BatchAccumulator`] can consume these records as-is.
//!
//! See the project task plan and design docs for the cross-cutting decisions
//! that shape this crate.

pub mod archive_reader;
pub mod batch;
pub mod batcher;
pub mod compress;
pub mod da;
pub mod da_store;
pub mod error;
pub mod indexer;
pub mod l1;
pub mod live;
pub mod multi_archive_reader;
pub mod optimistic;
pub mod prover_submit;
pub mod recon;
pub mod rereplicate;
pub mod settlement;
#[cfg(any(test, feature = "test-support", feature = "docker-e2e"))]
pub mod testkit;
#[cfg(any(test, feature = "test-support", feature = "docker-e2e"))]
pub mod testkit_da;

pub use batch::{BatchAccumulator, ClosedBlock, RecordedTx};
pub use batcher::{Batcher, MockSender, PostedBatch, Sender};
pub use da::{DaProxy, PayloadSource};
pub use error::BatcherError;
// The KAR1 codec lives with the shared types: the executors and the
// validator store every block in this form, and the batcher reads it back.
pub use kardamom_types::kar1 as frame;
pub use kardamom_types::kar1::{BlockFrame, Kar1Payload, TxFrame};
pub use l1::{BatchDescriptor, post_batch, read_posted_batches, recover_blocks};
pub use multi_archive_reader::{MultiArchiveConfig, MultiArchiveReader, ResolvedRecord};
pub use optimistic::{BatchClaimer, BatchWatcher, ClaimOutcome, WatchOutcome};
pub use prover_submit::{ProofSubmitter, SubmitOutcome};
pub use rereplicate::{
    HealReport, MirrorReport, diff_mirror, heal_from_mirror, mirror_archive, verify_mirror,
};
