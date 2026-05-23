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
//!
//! ## rkyv + alloy-primitives
//!
//! alloy's `Address`, `B256`, and `U256` do not derive `rkyv::Archive`
//! upstream. We bridge them via the [`wire`] module's `with` adapters and use
//! `#[rkyv(with = wire::AddressBytes)]` style attributes on fields. This keeps
//! the public type ergonomic (`pub sender: Address`) while making rkyv happy.

pub mod boundary;
pub mod delta;
pub mod envelope;
pub mod position;
pub mod receipt;
pub mod state;
pub mod watermark;
pub mod wire;

pub use boundary::{BlockBoundary, BlockBoundaryStart};
pub use delta::{AccountChange, BlockDelta, StorageChange};
pub use envelope::TxEnvelope;
pub use position::BPosition;
pub use receipt::{CachedReceipt, Receipt, WireLog};
pub use state::{SnapshotSource, StateDatabase, StateError};
pub use watermark::{FsyncWatermark, QuorumWatermark};
