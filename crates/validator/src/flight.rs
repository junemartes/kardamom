//! Receipt-divergence flight recorder.
//!
//! The claim-check path dumps its inputs at the point of failure
//! (`parallel::dump_divergence_inputs`). But a receipt write-set-hash
//! mismatch fires later, on the commit thread, after the block's records
//! and claims are dropped. Without this ring, such a mismatch leaves one
//! log line and nothing to replay. The ring keeps the last few blocks'
//! canonical records and claim indexes; this is cheap, since record
//! payloads are refcounted `Bytes`. The receipt sink dumps the whole ring
//! plus both receipts, field by field, the moment a mismatch is proven.

use std::collections::VecDeque;
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};

use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::block_env::ExecEnv;
use kardamom_types::Receipt;

use crate::parallel::ClaimIndex;

/// How many recent blocks the ring retains. The mismatching tx is almost
/// always in the newest block, since receipts stream right behind
/// execution, but pipelined commits mean the receipt check can trail by a
/// few blocks.
const RING_CAP: usize = 6;

struct BlockCapture {
    block: u64,
    granularity: NonZeroU16,
    /// The exec env the block ran under, such as the boundary timestamp.
    /// The prover spool re-executes with capture under the same env.
    env: ExecEnv,
    records: Vec<BufferedRecord>,
    /// `None` when the block validated on the sequential path, so claims
    /// never arrived. The records alone still let the block replay offline.
    claims: Option<Arc<ClaimIndex>>,
}

/// Shared ring of recent block inputs. The block-exec strategy pushes to
/// it; [`crate::ValidatorReceiptSink`] dumps it on a receipt mismatch.
#[derive(Default)]
pub struct FlightRing {
    ring: Mutex<VecDeque<BlockCapture>>,
}

impl FlightRing {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<BlockCapture>> {
        crate::lock_recover(&self.ring)
    }

    /// Record a block's inputs. Call once per block, before execution.
    pub fn push(
        &self,
        block: u64,
        granularity: NonZeroU16,
        env: ExecEnv,
        records: &[BufferedRecord],
        claims: Option<Arc<ClaimIndex>>,
    ) {
        let mut g = self.lock();
        if g.len() >= RING_CAP {
            g.pop_front();
        }
        g.push_back(BlockCapture {
            block,
            granularity,
            env,
            records: records.to_vec(),
            claims,
        });
    }

    /// Get one block's inputs back out of the ring; this is the prover
    /// spool's feed. Cheap, since record payloads are refcounted `Bytes`.
    pub fn records_for(&self, block: u64) -> Option<(NonZeroU16, ExecEnv, Vec<BufferedRecord>)> {
        let g = self.lock();
        g.iter()
            .find(|c| c.block == block)
            .map(|c| (c.granularity, c.env, c.records.clone()))
    }

    /// Serialize the ring and both receipts for offline replay. This is
    /// best-effort: failures only log. Recorder trouble must never mask
    /// the divergence stop.
    pub fn dump_receipt_divergence(&self, local: &Receipt, published: &Receipt) {
        let file_name = format!(
            "receipt-divergence-{}-{}.json",
            local.block_number,
            local.tx_idx.as_index()
        );
        let payload = self.snapshot_json(local, published);
        match write_flight_dump(&file_name, &payload) {
            Ok(path) => {
                tracing::error!(path = %path.display(), "receipt-divergence inputs dumped for offline replay");
            }
            Err(e) => tracing::warn!(error = %e, "receipt-divergence dump failed"),
        }
    }

    /// Build the dump payload: both receipts plus the ring's recent block
    /// inputs. The lock guard's scope ends here, before the caller writes
    /// the file.
    fn snapshot_json(&self, local: &Receipt, published: &Receipt) -> serde_json::Value {
        let g = self.lock();
        serde_json::json!({
            "local": receipt_json(local),
            "published": receipt_json(published),
            "ring": g.iter().map(|c| serde_json::json!({
                "block": c.block,
                "granularity": c.granularity,
                "records": crate::parallel::records_json(&c.records),
                "claims": c.claims.as_deref().map(crate::parallel::claims_json),
            })).collect::<Vec<_>>(),
        })
    }
}

/// Write `payload` as pretty JSON to `file_name`, under the flight-dump
/// directory (`KARDAMOM_FLIGHT_DIR`, defaulting to `/opt/kardamom/state`;
/// overridable so tests can check the file without `/opt` existing).
/// Shared by this ring's receipt-divergence dump and
/// [`crate::parallel::dump_divergence_inputs`]'s claim-path dump — both
/// write into the same directory, so one offline tool reads either.
/// Returns the path written, for the caller's own log line; the caller
/// decides how to log a write failure, since the two dumps' log fields
/// differ (one names a block, the other does not).
pub(crate) fn write_flight_dump(
    file_name: &str,
    payload: &serde_json::Value,
) -> std::io::Result<std::path::PathBuf> {
    let dir = std::env::var("KARDAMOM_FLIGHT_DIR").unwrap_or_else(|_| "/opt/kardamom/state".into());
    let path = std::path::Path::new(&dir).join(file_name);
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(payload).unwrap_or_default(),
    )?;
    Ok(path)
}

fn receipt_json(r: &Receipt) -> serde_json::Value {
    serde_json::json!({
        "tx_idx": r.tx_idx.as_index(),
        "tx_hash": format!("{:?}", r.tx_hash),
        "tx_type": r.tx_type,
        "status": r.status,
        "gas_used": r.gas_used,
        "cumulative_gas_used": r.cumulative_gas_used,
        "write_set_hash": format!("{:?}", r.write_set_hash),
        "from": format!("{:?}", r.from),
        "to": r.to.map(|a| format!("{a:?}")),
        "contract_address": r.contract_address.map(|a| format!("{a:?}")),
        "nonce": r.nonce,
        "block_number": r.block_number,
        "transaction_index": r.transaction_index,
        "effective_gas_price": r.effective_gas_price,
        "logs": r.logs.iter().map(|l| serde_json::json!({
            "address": format!("{:?}", l.address),
            "topics": l.topics.iter().map(|t| format!("{t:?}")).collect::<Vec<_>>(),
            "data": alloy_primitives::hex::encode(&l.data),
        })).collect::<Vec<_>>(),
    })
}
