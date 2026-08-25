//! Pure data types and traits shared across the kardamom subsystems.
//!
//! No I/O. No Aeron. No libmdbx. Everything in this crate is `#[no_std]`-
//! friendly in spirit (we still use `alloc` for `Vec`/`Bytes`).
//!
//! Wire types (`TxEnvelope`, `Receipt`, `BlockBoundary*`,
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

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod ack_policy;
pub mod boundary;
pub mod delta;
pub mod deposit;
pub mod envelope;
pub mod epoch;
pub mod genesis;
pub mod limits;
pub mod position;
pub mod prover;
pub mod receipt;
pub mod state;
pub mod tx_error;
pub mod tx_ordering;
pub mod txref;
pub mod watermark;
pub mod wire;
pub mod withdrawals;
pub mod witness;

pub use ack_policy::AckPolicy;
pub use boundary::{BlockBoundary, BlockBoundaryStart};
pub use delta::{AccountChange, BalFrame, BlockDelta, CodeEntry, StorageChange};
pub use deposit::{Deposit, DepositRef};
pub use envelope::TxEnvelope;
pub use epoch::{DepositLog, EpochError, EpochRecord, derive_epoch};
pub use genesis::{AllocEntry, Genesis, GenesisError};
pub use position::{BPosition, TxDataLoc};
pub use prover::{
    BatchProverInput, BatchPublicOutputs, BlockRecordsDigest, ProverInput, ProverRecord,
    PublicOutputs, batch_records_commitment,
};
pub use receipt::{Receipt, TX_TYPE_DEPOSIT, TX_TYPE_LEGACY, WireLog, tx_type_of};
pub use state::{SnapshotSource, StateDatabase, StateError};
pub use tx_error::{TxError, TxErrorReason};
pub use tx_ordering::TxOrderingMessage;
pub use txref::TxRef;
pub use watermark::{FsyncWatermark, QuorumWatermark};
pub use witness::{ExecutionWitness, WitnessAccount, WitnessProofs, WitnessSlot};
