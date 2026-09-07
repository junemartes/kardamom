//! Serialize a diverging block's inputs for offline replay. This is
//! best-effort: failures only log. The format is a JSON envelope with hex
//! payloads. It covers one block, is self-contained, and is versioned by
//! field presence.

use std::num::NonZeroU16;

use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;

use super::claims::ClaimIndex;

/// Serializable projection of a block's canonical records. The claim-path
/// dump below and the receipt-path flight ring (`crate::flight`) share
/// this: both dumps must stay replayable by the same offline tooling.
pub(crate) fn records_json(records: &[BufferedRecord]) -> Vec<serde_json::Value> {
    records
        .iter()
        .map(|r| match r {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => tx_json(*tx_idx, envelope, *position),
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => deposit_json(*tx_idx, deposit, *position),
            BufferedRecord::XChain {
                tx_idx,
                origin_chain_id,
                message,
                position,
            } => xchain_json(*tx_idx, *origin_chain_id, message, *position),
        })
        .collect()
}

fn tx_json(
    tx_idx: kardamom_engine::TxIndex,
    envelope: &kardamom_types::TxEnvelope,
    position: kardamom_types::BPosition,
) -> serde_json::Value {
    serde_json::json!({
        "kind": "tx",
        "raw": alloy_primitives::hex::encode(&envelope.raw_tx),
        "sender": format!("{:?}", envelope.sender),
        "hash": format!("{:?}", envelope.tx_hash),
        "correlation_id": envelope.correlation_id,
        "idx": tx_idx.0,
        "pos": position.as_index(),
    })
}

fn deposit_json(
    tx_idx: kardamom_engine::TxIndex,
    deposit: &kardamom_types::Deposit,
    position: kardamom_types::BPosition,
) -> serde_json::Value {
    serde_json::json!({
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
    })
}

fn xchain_json(
    tx_idx: kardamom_engine::TxIndex,
    origin_chain_id: u64,
    message: &kardamom_types::xchain::XChainMessage,
    position: kardamom_types::BPosition,
) -> serde_json::Value {
    serde_json::json!({
        "kind": "xchain",
        "origin_chain_id": origin_chain_id,
        "source_hash": format!("{:?}", message.source_hash),
        "seq": message.seq,
        "origin_sender": format!("{:?}", message.origin_sender),
        "target": format!("{:?}", message.target),
        "value": message.value.to_string(),
        "gas_limit": message.gas_limit,
        "input": alloy_primitives::hex::encode(&message.input),
        "idx": tx_idx.0,
        "pos": position.as_index(),
    })
}

/// Serializable projection of a claim index. Same sharing reason as
/// [`records_json`].
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
    granularity: NonZeroU16,
    err: &ExecutorError,
) {
    let file_name = format!("divergence-{block}.json");
    let mut payload = serde_json::json!({
        "block": block,
        "granularity": granularity.get(),
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
    match crate::flight::write_flight_dump(&file_name, &payload) {
        Ok(path) => {
            tracing::error!(block, path = %path.display(), "divergence inputs dumped for offline replay");
        }
        Err(e) => tracing::warn!(block, error = %e, "divergence dump failed"),
    }
}
