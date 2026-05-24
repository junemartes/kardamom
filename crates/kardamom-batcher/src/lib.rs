//! Kardamom L1 batcher.
//!
//! Offline, archive-driven (S0 D-Sh10): reads the canonical L2 stream from
//! the on-disk Aeron archives, groups it into per-block batches, packs them
//! into EIP-4844 blobs (KAR1 + zstd framing, no state-root field per S0
//! D-Sh11), and posts them to the `KardamomL2Settlement` data-availability
//! sink contract on L1.
//!
//! ## D-Sh12 split data/ordering topology
//!
//! After D-Sh12 the batcher reads from `M + 1` archives, not one:
//!
//! - **Channel B archive** carries the canonical orderer payload — only
//!   `ChannelBMessage` records (`TxRef + BoundaryStart`). Tiny per-record.
//! - **Per-sequencer channel A archives** carry the bulk `TxEnvelope` bytes.
//!   One per sequencer; opened on demand by [`multi_archive_reader`].
//!
//! [`multi_archive_reader::MultiArchiveReader`] is the glue: it walks B in
//! canonical order, resolving each `TxRef` against the appropriate channel-A
//! position index, and yields [`multi_archive_reader::ResolvedRecord`]s the
//! existing [`batch::BatchAccumulator`] can consume as-is.
//!
//! See `docs/plans/2026-05-23-S7-l1-batcher.md` for the full task plan and
//! `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh10 / D-Sh11 / D-Sh12 for
//! the cross-cutting decisions that shape this crate.

pub mod archive_reader;
pub mod batch;
pub mod batcher;
pub mod blob;
pub mod compress;
pub mod error;
pub mod frame;
pub mod multi_archive_reader;
pub mod recon;
pub mod settlement;

#[cfg(feature = "aeron-live")]
pub mod archive_live;

pub use batch::{BatchAccumulator, ClosedBlock, RecordedTx};
pub use batcher::{Batcher, MockSender, PostedBatch, Sender};
pub use error::BatcherError;
pub use frame::{BlockFrame, Kar1Payload, TxFrame};
pub use multi_archive_reader::{MultiArchiveConfig, MultiArchiveReader, ResolvedRecord};
