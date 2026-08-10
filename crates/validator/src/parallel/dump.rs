//! Serialize a diverging block's inputs for offline replay. Best-effort:
//! failures only log. Format: JSON envelope with hex payloads — small
//! (one block), self-contained, versioned by field presence.

use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;

use super::claims::ClaimIndex;

/// Serializable projection of a block's canonical records — shared by the
/// claim-path dump below and the receipt-path flight ring
/// (`crate::flight`): both dumps must stay replayable by the same offline
/// tooling.
pub(crate) fn records_json(records: &[BufferedRecord]) -> Vec<serde_json::Value> {
    records
        .iter()
        .map(|r| match r {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => serde_json::json!({
                "kind": "tx",
                "raw": alloy_primitives::hex::encode(&envelope.raw_tx),
                "sender": format!("{:?}", envelope.sender),
                "hash": format!("{:?}", envelope.tx_hash),
                "correlation_id": envelope.correlation_id,
                "idx": tx_idx.0,
                "pos": position.as_index(),
            }),
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => serde_json::json!({
                "kind": "deposit",
                "source_hash": format!("{:?}", deposit.source_hash),
                "from": format!("{:?}", deposit.from),
                "to": deposit.to.map(|a| format!("{a:?}")),
                "mint": deposit.mint.to_string(),
                "value": deposit.value.to_string(),
                "gas_limit": deposit.gas_limit,
                "input": alloy_primitives::hex::encode(&deposit.input),
                "idx": tx_idx.0,
                "pos": position.as_index(),
            }),
        })
        .collect()
}

/// Serializable projection of a claim index (same sharing rationale as
/// [`records_json`]).
pub(crate) fn claims_json(claims: &ClaimIndex) -> serde_json::Value {
    serde_json::json!({
        "claims_storage": claims.storage.iter().map(|((a, k), w)| serde_json::json!({
            "addr": format!("{a:?}"), "slot": format!("{k:?}"),
            "writes": w.iter().map(|(i, v)| (i, v.to_string())).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "claims_balance": claims.balance.iter().map(|(a, w)| serde_json::json!({
            "addr": format!("{a:?}"),
            "writes": w.iter().map(|(i, v)| (i, v.to_string())).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "claims_nonce": claims.nonce.iter().map(|(a, w)| serde_json::json!({
            "addr": format!("{a:?}"),
            "writes": w.clone(),
        })).collect::<Vec<_>>(),
        "claims_code": claims.code.iter().map(|(a, w)| serde_json::json!({
            "addr": format!("{a:?}"),
            "writes": w.iter().map(|(i, c)| (i, alloy_primitives::hex::encode(c))).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

pub(super) fn dump_divergence_inputs(
    block: u64,
    records: &[BufferedRecord],
    claims: &ClaimIndex,
    parent: Option<&PendingDelta>,
    granularity: u16,
    err: &ExecutorError,
) {
    // Same dir as the flight recorder's receipt-divergence dumper;
    // overridable so tests (and dev hosts) can assert the artifact without
    // /opt existing.
    let dir = std::env::var("KARDAMOM_FLIGHT_DIR").unwrap_or_else(|_| "/opt/kardamom/state".into());
    let path = std::path::Path::new(&dir).join(format!("divergence-{block}.json"));
    let mut payload = serde_json::json!({
        "block": block,
        "granularity": granularity,
        "error": format!("{err:?}"),
        "records": records_json(records),
        "parent_accounts": parent.map(|p| p.accounts.iter().map(|(a, (n, b, c))| serde_json::json!({
            "addr": format!("{a:?}"), "nonce": n, "balance": b.to_string(), "code_hash": format!("{c:?}"),
        })).collect::<Vec<_>>()),
        "parent_storage": parent.map(|p| p.storage.iter().map(|((a, k), v)| serde_json::json!({
            "addr": format!("{a:?}"), "slot": format!("{k:?}"), "value": v.to_string(),
        })).collect::<Vec<_>>()),
    });
    if let (serde_json::Value::Object(p), serde_json::Value::Object(c)) =
        (&mut payload, claims_json(claims))
    {
        p.extend(c);
    }
    match std::fs::write(
        &path,
        serde_json::to_vec_pretty(&payload).unwrap_or_default(),
    ) {
        Ok(()) => {
            tracing::error!(block, path = %path.display(), "divergence inputs dumped for offline replay")
        }
        Err(e) => tracing::warn!(block, error = %e, "divergence dump failed"),
    }
}
