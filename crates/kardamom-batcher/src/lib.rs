//! Kardamom L1 batcher.
//!
//! Offline, archive-driven (S0 D-Sh10): reads `TxEnvelope` + `BlockBoundaryStart`
//! records from channel B's on-disk Aeron Archive segment files, groups them
//! into per-block batches, packs them into EIP-4844 blobs (KAR1 + zstd framing,
//! no state-root field per S0 D-Sh11), and posts them to the
//! `KardamomL2Settlement` data-availability sink contract on L1.
//!
//! See `docs/plans/2026-05-23-S7-l1-batcher.md` for the full task plan and
//! `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh10 / D-Sh11 for the
//! cross-cutting decisions that shape this crate.

pub mod archive_reader;
pub mod batch;
pub mod batcher;
pub mod blob;
pub mod compress;
pub mod error;
pub mod frame;
pub mod recon;
pub mod settlement;

#[cfg(feature = "aeron-live")]
pub mod archive_live;

pub use batch::{BatchAccumulator, ClosedBlock, RecordedTx};
pub use batcher::{Batcher, MockSender, PostedBatch, Sender};
pub use error::BatcherError;
pub use frame::{BlockFrame, Kar1Payload, TxFrame};
