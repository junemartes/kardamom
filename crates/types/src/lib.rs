//! Pure data types and traits shared across the kardamom subsystems.
//!
//! No I/O. No Aeron. No libmdbx. This crate is `#[no_std]`-friendly in
//! spirit. It still uses `alloc` for `Vec` and `Bytes`.
//!
//! Wire types (`TxEnvelope`, `Receipt`, `BlockBoundary*`,
//! `FsyncWatermark`, `QuorumWatermark`, `BlockDelta`) derive
//! `#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]`. A consumer
//! that needs zero-copy access uses `rkyv::access::<Archived<T>>(bytes)`. A
//! consumer that needs an owned value calls `rkyv::deserialize`.
//!
//! ## rkyv + alloy-primitives
//!
//! alloy's `Address`, `B256`, and `U256` types do not derive `rkyv::Archive`
//! upstream. This crate bridges them with the [`wire`] module's `with`
//! adapters, using attributes like `#[rkyv(with = wire::AddressBytes)]` on
//! fields. This keeps the public type easy to use (`pub sender: Address`)
//! and keeps rkyv working.

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
pub mod upgrades;
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
pub use upgrades::{CHAIN_STATE, SYSTEM_UPGRADER, encode_set_feature};
pub use watermark::{FsyncWatermark, QuorumWatermark};
pub use witness::{ExecutionWitness, WitnessAccount, WitnessProofs, WitnessSlot};
