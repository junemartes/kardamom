# kardamom-types

Pure data types and traits shared across the kardamom subsystems. Per
**D-Sh1** in `docs/plans/2026-05-23-S0-shared-decisions.md`, every wire type
that crosses an Aeron channel or a libmdbx boundary lives here, derives
`rkyv::{Archive, Serialize, Deserialize}` (D-Sh2), and is consumed by S1, S2,
S3, S4, S5, S6, S7.

This crate has **no I/O dependencies** — no Aeron, no libmdbx, no
alloy-provider, no jsonrpsee. If you find yourself wanting to add one,
you have the wrong crate.

## Owned types

- `BPosition` — canonical L2 tx identifier (Aeron position)
- `TxEnvelope` — raw tx + correlation id + sender + tx_hash (sender and tx_hash always populated; D-Sh3, D-Sh4)
- `Receipt`, `WireLog` — per-tx execution receipt + log entry
- `CachedReceipt` — receipt-cache channel message
- `BlockBoundaryStart`, `BlockBoundary` — block markers (no state root; D-Sh11)
- `FsyncWatermark`, `QuorumWatermark` — durability accounting
- `BlockDelta`, `AccountChange`, `StorageChange`, `CodeEntry` — block-write payload (executor → state writer)
- `StateDatabase`, `SnapshotSource` — state-access traits

## rkyv ↔ alloy adapters

alloy-primitives types (`Address`, `B256`, `U256`) and `bytes::Bytes` do not
derive `rkyv::Archive` upstream. The `wire` module provides field-level
`with` adapters that archive each as a fixed-size byte array. Annotate
fields with `#[rkyv(with = wire::AddressBytes)]` etc.; the public field
types remain ergonomic (`pub sender: Address`).
