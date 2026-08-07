//! RPC golden vectors — W2 of `docs/agents/l1-client-suite-port-spec.md`,
//! the hive `rpc-compat` analog.
//!
//! Each `.io` file under `crates/e2e/vectors/rpc/` is a sequence of
//! request/expectation exchanges frozen against the v0 RPC contract:
//!
//! ```text
//! # comment
//! >> {"method": "eth_chainId", "params": []}
//! << {"result": "${CHAIN_ID_HEX}"}
//! ```
//!
//! Expectations are `{"result": …}` or `{"error": {"code": …, "message": …}}`.
//! `${NAME}` tokens are substituted before sending/matching (chain id,
//! signed raw txs, addresses); the matchers `${ANY}` (any value) and
//! `${HEX}` (any `0x…` string) survive into comparison. Object comparison is
//! SUBSET: keys listed in the expectation must match, extra actual keys are
//! allowed — pin what the contract promises, tolerate additions. Hex-string
//! comparison is case-insensitive (alloy checksums addresses).
//!
//! Vectors are `include_str!`-embedded so both targets ship them inside
//! their binaries — no fixture paths on the Target-C runner.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Bytes, Signature, TxKind, U256};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::BTreeMap;

use super::Target;
use crate::harness::l2::{self, RpcError};

const VECTORS: &[(&str, &str)] = &[
    (
        "chain_meta",
        include_str!("../../vectors/rpc/chain_meta.io"),
    ),
    (
        "error_contract",
        include_str!("../../vectors/rpc/error_contract.io"),
    ),
    (
        "protocol_limits",
        include_str!("../../vectors/rpc/protocol_limits.io"),
    ),
    (
        "submit_receipt",
        include_str!("../../vectors/rpc/submit_receipt.io"),
    ),
];

pub struct Params {
    /// Funded dev-mnemonic index for the happy-path submit vector. The
    /// account must be fresh (nonce 0) — the established per-case account
    /// convention.
    pub sender: usize,
    /// Transfer recipient (any address; also mnemonic-derived).
    pub recipient: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            sender: 0,
            recipient: 1,
        }
    }
}

pub async fn run(t: &Target, p: Params) -> Result<()> {
    let subs = build_substitutions(t, &p)?;
    for (file, text) in VECTORS {
        for (i, (req, expect)) in parse(text)
            .with_context(|| format!("parse vectors/rpc/{file}.io"))?
            .into_iter()
            .enumerate()
        {
            let req = substitute(req, &subs);
            let expect = substitute(expect, &subs);
            let method = req["method"]
                .as_str()
                .with_context(|| format!("{file}[{i}]: request has no method"))?;
            let params: Vec<Value> = req["params"].as_array().cloned().unwrap_or_default();

            let outcome = t.rpc.raw_call(method, &params).await;
            let actual = match outcome.result {
                Ok(v) => json!({ "result": v }),
                Err(RpcError::Call { code, message }) => {
                    json!({ "error": { "code": code, "message": message } })
                }
                Err(RpcError::Transport(e)) => {
                    bail!("{file}[{i}] {method}: transport error (the contract forbids these): {e}")
                }
            };
            matches(&expect, &actual)
                .with_context(|| format!("{file}[{i}] {method}: got {actual}"))?;
        }
    }
    Ok(())
}

fn build_substitutions(t: &Target, p: &Params) -> Result<BTreeMap<String, Value>> {
    let signers = l2::dev_signers(p.sender.max(p.recipient) as u32 + 1)?;
    let sender = &signers[p.sender];
    let recipient = signers[p.recipient].address;

    let valid = l2::sign_transfer(sender, t.chain_id, 0, recipient, 1)?;

    // Deterministically unrecoverable: r = 0 is outside the scalar field, so
    // decode succeeds and recovery fails — the exact path the sig-verify
    // error covers.
    let badsig = {
        let tx = TxLegacy {
            chain_id: Some(t.chain_id),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(recipient),
            value: U256::ONE,
            input: Bytes::new(),
        };
        let sig = Signature::new(U256::ZERO, U256::ONE, false);
        encode_2718(tx.into_signed(sig))
    };

    // A block's worth of gas — valid RLP + signature, over the per-tx cap.
    let overcap = {
        let mut tx = TxLegacy {
            chain_id: Some(t.chain_id),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 30_000_000,
            to: TxKind::Call(recipient),
            value: U256::ONE,
            input: Bytes::new(),
        };
        let sig = sender
            .signer
            .sign_transaction_sync(&mut tx)
            .context("sign overcap tx")?;
        encode_2718(tx.into_signed(sig))
    };

    // Minimal type-3 envelope; only needs to decode (rejection precedes
    // sig-verify), but sign it properly anyway.
    let type3 = {
        let mut tx = alloy_consensus::TxEip4844 {
            chain_id: t.chain_id,
            nonce: 0,
            gas_limit: 21_000,
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 0,
            to: recipient,
            value: U256::ONE,
            access_list: Default::default(),
            blob_versioned_hashes: vec![alloy_primitives::B256::repeat_byte(0x01)],
            max_fee_per_blob_gas: 1,
            input: Bytes::new(),
        };
        let sig = sender
            .signer
            .sign_transaction_sync(&mut tx)
            .context("sign type3 tx")?;
        encode_2718(tx.into_signed(sig))
    };

    let hex = |b: &[u8]| format!("0x{}", hex::encode(b));
    Ok(BTreeMap::from([
        ("CHAIN_ID_HEX".into(), json!(format!("0x{:x}", t.chain_id))),
        ("SENDER".into(), json!(hex(sender.address.as_slice()))),
        ("RECIPIENT".into(), json!(hex(recipient.as_slice()))),
        ("RAW_TX_VALID".into(), json!(hex(&valid.raw))),
        ("TX_HASH_VALID".into(), json!(format!("{:#x}", valid.hash))),
        ("RAW_TX_BADSIG".into(), json!(hex(&badsig))),
        ("RAW_TX_OVERCAP".into(), json!(hex(&overcap))),
        ("RAW_TX_TYPE3".into(), json!(hex(&type3))),
    ]))
}

fn encode_2718<T: Encodable2718>(signed: T) -> Vec<u8> {
    let mut out = Vec::new();
    signed.encode_2718(&mut out);
    out
}

/// Parse `>> request` / `<< expectation` line pairs; `#` comments and blank
/// lines are skipped. Every `>>` must be followed by a `<<`.
fn parse(text: &str) -> Result<Vec<(Value, Value)>> {
    let mut out = Vec::new();
    let mut pending: Option<Value> = None;
    for (ln, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(req) = line.strip_prefix(">> ") {
            ensure!(
                pending.is_none(),
                "line {}: request without expectation",
                ln + 1
            );
            pending = Some(serde_json::from_str(req).with_context(|| format!("line {}", ln + 1))?);
        } else if let Some(exp) = line.strip_prefix("<< ") {
            let req = pending
                .take()
                .with_context(|| format!("line {}: expectation without request", ln + 1))?;
            out.push((
                req,
                serde_json::from_str(exp).with_context(|| format!("line {}", ln + 1))?,
            ));
        } else {
            bail!("line {}: expected '>> ', '<< ', or comment", ln + 1);
        }
    }
    ensure!(pending.is_none(), "trailing request without expectation");
    Ok(out)
}

/// Replace whole-string `${NAME}` tokens (matchers `${ANY}`/`${HEX}` are not
/// in the map and survive into `matches`).
fn substitute(v: Value, subs: &BTreeMap<String, Value>) -> Value {
    match v {
        Value::String(s) => {
            if let Some(sub) = s
                .strip_prefix("${")
                .and_then(|r| r.strip_suffix('}'))
                .and_then(|name| subs.get(name))
            {
                return sub.clone();
            }
            Value::String(s)
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(|i| substitute(i, subs)).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, val)| (k, substitute(val, subs)))
                .collect(),
        ),
        other => other,
    }
}

/// Recursive comparison: `${ANY}` matches anything, `${HEX}` any `0x…`
/// string, objects match as SUBSET of actual, arrays elementwise, hex
/// strings case-insensitively.
fn matches(expect: &Value, actual: &Value) -> Result<()> {
    match (expect, actual) {
        (Value::String(e), _) if e == "${ANY}" => Ok(()),
        (Value::String(e), Value::String(a)) if e == "${HEX}" => {
            ensure!(
                a.starts_with("0x") && a.len() > 2,
                "expected a 0x-hex string, got {a:?}"
            );
            Ok(())
        }
        (Value::String(e), Value::String(a)) if e.starts_with("0x") && a.starts_with("0x") => {
            ensure!(e.eq_ignore_ascii_case(a), "hex mismatch: want {e}, got {a}");
            Ok(())
        }
        (Value::Object(e), Value::Object(a)) => {
            for (k, ev) in e {
                let av = a
                    .get(k)
                    .with_context(|| format!("missing key {k:?} (want {ev})"))?;
                matches(ev, av).with_context(|| format!("at key {k:?}"))?;
            }
            Ok(())
        }
        (Value::Array(e), Value::Array(a)) => {
            ensure!(
                e.len() == a.len(),
                "array length: want {}, got {}",
                e.len(),
                a.len()
            );
            for (i, (ev, av)) in e.iter().zip(a).enumerate() {
                matches(ev, av).with_context(|| format!("at index {i}"))?;
            }
            Ok(())
        }
        _ => {
            ensure!(expect == actual, "want {expect}, got {actual}");
            Ok(())
        }
    }
}
