//! (Removed) Quorum fsync-watermark aggregator.
//!
//! The Q-of-N quorum aggregator (`QuorumState`, `run_quorum_loop`,
//! `QuorumAggregator`) and the per-recorder fsync watermark fan-in lived here.
//! They have been removed in favour of the **archive-at-the-sealer**
//! durability model: a single Aeron Archive co-located with the sealer records
//! the sealer's tx_ordering MDC publication, and the sealer publishes that
//! recording's byte-durable position as the single
//! [`kardamom_types::QuorumWatermark`] (now meaning "the one durable
//! watermark", not a Q-of-N aggregate) on `quorum_watermark_channel`. Ingress
//! gates its must-deliver ack on that single position via the unchanged
//! `--ack-policy on-quorum` path.
//!
//! See [`crate::recorder::run_durable_watermark_loop`] (the producer, driven
//! by the sealer) and `kardamom-sealer --archive-durability` (the wiring).
//!
//! This module is intentionally left empty; it is retained as a landing place
//! for the durability-model documentation above and to keep the `pub mod
//! watermark;` path stable for downstream `use` sites that referenced it.
