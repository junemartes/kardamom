//! (Removed) Quorum fsync-watermark aggregator.
//!
//! The Q-of-N quorum aggregator (`QuorumState`, `run_quorum_loop`,
//! `QuorumAggregator`) and the per-recorder fsync watermark fan-in lived here.
//! The archive-at-the-sealer durability model replaces them. A single Aeron
//! Archive next to the sealer records the sealer's tx_ordering MDC
//! publication. The sealer publishes the recording's byte-durable position as
//! the single [`kardamom_types::QuorumWatermark`] (now "the one durable
//! watermark", not a Q-of-N aggregate) on `quorum_watermark_channel`. Ingress
//! gates its must-deliver ack on that single position through the unchanged
//! `--ack-policy on-quorum` path.
//!
//! See [`crate::recorder::run_durable_watermark_loop`] (the producer, driven
//! by the sealer) and `kardamom-sealer --archive-durability` (the wiring).
//!
//! This module has no code. It keeps the durability-model documentation above
//! and keeps the `pub mod watermark;` path stable for downstream `use` sites.
