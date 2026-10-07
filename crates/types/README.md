# kardamom-types

Pure data types and traits shared across the kardamom subsystems. Every
wire type that crosses an Aeron channel or a libmdbx boundary lives here
and derives `rkyv::{Archive, Serialize, Deserialize}`. Every role crate
uses these types.

This crate has **no I/O dependencies** — no Aeron, no libmdbx, no
alloy-provider, no jsonrpsee. If you find yourself wanting to add one,
you have the wrong crate.

## Owned types

- `BPosition` — canonical L2 tx identifier (Aeron position)
- `TxEnvelope` — raw tx + correlation id + sender + tx_hash + inclusion deadline (sender and tx_hash always populated; `max_inclusion_block` is the last block the sealer may order the tx into, and `i64::MAX` in the ingress means no deadline)
- `Receipt`, `WireLog` — per-tx execution receipt + log entry
- `BlockBoundaryStart`, `BlockBoundary` — block markers (no state root; `BlockBoundary` carries the block's `base_fee` and `gas_used`)
- `TxError`, `TxErrorReason` — sequencer rejection signal on `tx_errors`; the reasons are `DuplicatedTx`, `Evicted`, `Expired`, `PastDeadline`, `FeeInvalid`, `FeeTooLow`, `InsufficientFunds`, `DaLag` (`sealed_head`, `posted_head`, `budget_blocks`: the sealer refused the tx on its DA-lag guard) and `RecordLag` (`sealed_index`, `recorded_index`, `budget`: the sealer refused the tx on its record-lag guard); `TxErrorReason::word` gives the reason word that a client sees
- `TxStatus`, `TxStage` — one step of a tx on the `tx_status` stream (`Offered`, `Sealed`, `Executed`, `Rejected`); `TxStageKind` is the stage without its payload
- `FeeSchedule` — the genesis `[fees]` section: `base_fee_initial` and `beneficiary`
- `BlockFees` — the base fee and the tip beneficiary of one block; `BlockFees::NONE` is the chain with no schedule
- `TxFees` — the fee fields of one tx: the ordering bid, the effective tip rate, the tip amount and the worst-case cost
- `VoidRecord` — the sealer's decision to remove the entry at one canonical index (`index`, `tx_hash`); it travels as `TxOrderingMessage::Void`
- `service` module — the halt and lifecycle types: `Halt`, `HaltCause`, `RecoveryId`, `Clears`, `HaltRef`, `Pause`, `PauseReason`, `ServiceState` and `ServiceEvent`; they derive rkyv, and `ServiceEvent` crosses the `events` stream
- `FsyncWatermark`, `QuorumWatermark` — durability accounting
- `BlockDelta`, `AccountChange`, `StorageChange`, `CodeEntry` — block-write payload (executor → state writer)
- `StateDatabase`, `SnapshotSource` — state-access traits

## rkyv ↔ alloy adapters

alloy-primitives types (`Address`, `B256`, `U256`) and `bytes::Bytes` do not
derive `rkyv::Archive` upstream. The `wire` module provides field-level
`with` adapters that archive each as a fixed-size byte array. Annotate
fields with `#[rkyv(with = wire::AddressBytes)]` etc.; the public field
types remain ergonomic (`pub sender: Address`).
