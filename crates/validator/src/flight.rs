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
    granularity: u16,
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

    /// Record a block's inputs. Call once per block, before execution.
    pub fn push(
        &self,
        block: u64,
        granularity: u16,
        env: ExecEnv,
        records: &[BufferedRecord],
        claims: Option<Arc<ClaimIndex>>,
    ) {
        let mut g = self.ring.lock().expect("flight ring poisoned");
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
    pub fn records_for(&self, block: u64) -> Option<(u16, ExecEnv, Vec<BufferedRecord>)> {
        let g = self.ring.lock().expect("flight ring poisoned");
        g.iter()
            .find(|c| c.block == block)
            .map(|c| (c.granularity, c.env, c.records.clone()))
    }

    /// Serialize the ring and both receipts for offline replay. This is
    /// best-effort: failures only log. Recorder trouble must never mask
    /// the divergence stop.
    pub fn dump_receipt_divergence(&self, local: &Receipt, published: &Receipt) {
        // This uses the same directory as the claim-path dumper. It can
        // be overridden, so tests can check the file without /opt existing.
        let dir =
            std::env::var("KARDAMOM_FLIGHT_DIR").unwrap_or_else(|_| "/opt/kardamom/state".into());
        let dir = std::path::Path::new(&dir);
        let path = dir.join(format!(
            "receipt-divergence-{}-{}.json",
            local.block_number,
            local.tx_idx.as_index()
        ));
        let g = self.ring.lock().expect("flight ring poisoned");
        let payload = serde_json::json!({
            "local": receipt_json(local),
            "published": receipt_json(published),
            "ring": g.iter().map(|c| serde_json::json!({
                "block": c.block,
                "granularity": c.granularity,
                "records": crate::parallel::records_json(&c.records),
                "claims": c.claims.as_deref().map(crate::parallel::claims_json),
            })).collect::<Vec<_>>(),
        });
        drop(g);
        match std::fs::write(
            &path,
            serde_json::to_vec_pretty(&payload).unwrap_or_default(),
        ) {
            Ok(()) => {
                tracing::error!(path = %path.display(), "receipt-divergence inputs dumped for offline replay");
            }
            Err(e) => tracing::warn!(error = %e, "receipt-divergence dump failed"),
        }
    }
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
