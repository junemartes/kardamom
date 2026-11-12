//! A deposit receipt must preserve its canonical source hash and ordering position.

use alloy_primitives::B256;
use anyhow::Context;
use serde::Serialize;
use serde_json::{Value, json};

use super::Rpc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct ReceiptPosition {
    block: u64,
    index: u64,
}

impl ReceiptPosition {
    fn parse(receipt: &Value, expected: B256) -> anyhow::Result<Option<Self>> {
        if receipt.is_null() {
            return Ok(None);
        }
        let hash = receipt["transactionHash"]
            .as_str()
            .context("receipt has no transactionHash")?
            .parse::<B256>()?;
        anyhow::ensure!(
            hash == expected,
            "receipt identity {hash} differs from deposit source {expected}"
        );
        anyhow::ensure!(
            receipt["status"] == "0x1",
            "deposit receipt fails: {receipt}"
        );
        Ok(Some(Self {
            block: Rpc::quantity(&receipt["blockNumber"])?,
            index: Rpc::quantity(&receipt["transactionIndex"])?,
        }))
    }
}

impl Rpc {
    pub(crate) async fn deposit_position(
        &self,
        source: B256,
    ) -> anyhow::Result<Option<ReceiptPosition>> {
        ReceiptPosition::parse(
            &self
                .call("eth_getTransactionReceipt", json!([format!("{source:#x}")]))
                .await?,
            source,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_identity_status_and_position_are_required() {
        let source = B256::repeat_byte(1);
        let mut receipt = json!({"transactionHash":source,"status":"0x1","blockNumber":"0x2","transactionIndex":"0x3"});
        assert_eq!(ReceiptPosition::parse(&Value::Null, source).unwrap(), None);
        assert_eq!(
            ReceiptPosition::parse(&receipt, source).unwrap(),
            Some(ReceiptPosition { block: 2, index: 3 })
        );
        assert!(ReceiptPosition::parse(&receipt, B256::repeat_byte(2)).is_err());
        receipt["status"] = json!("0x0");
        assert!(ReceiptPosition::parse(&receipt, source).is_err());
    }
}
