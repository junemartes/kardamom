//! Parity and stop tests for the seeded parallel engine (`engine.rs`):
//! parallel batches against sequential ground truth, forged claims,
//! deposits, and the K > 1 quantized chunk views.

mod dispatch;
mod fixtures;
mod forged;
mod parity;
