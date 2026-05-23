//! Pure data types and traits shared across the kardamom subsystems.
//!
//! No I/O. No Aeron. No libmdbx. Everything in this crate is `#[no_std]`-
//! friendly in spirit (we still use `alloc` for `Vec`/`Bytes`).
//!
//! Wire types (`TxEnvelope`, `Receipt`, `BlockBoundary*`, `CachedReceipt`,
//! `FsyncWatermark`, `QuorumWatermark`, `BlockDelta`) derive
//! `#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]`. Consumers
//! that need zero-copy access use `rkyv::access::<Archived<T>>(bytes)`;
//! consumers that need an owned value call `rkyv::deserialize`.

pub mod boundary;
pub mod delta;
pub mod envelope;
pub mod position;
pub mod receipt;
pub mod state;
pub mod watermark;

pub use boundary::{BlockBoundary, BlockBoundaryStart};
pub use delta::{AccountChange, BlockDelta, StorageChange};
pub use envelope::TxEnvelope;
pub use position::BPosition;
pub use receipt::{CachedReceipt, Receipt, WireLog};
pub use state::{SnapshotSource, StateDatabase, StateError};
pub use watermark::{FsyncWatermark, QuorumWatermark};
