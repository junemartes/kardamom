//! Deposit evidence comes from the direct L1 receipt, outside the fault proxy.

use alloy_primitives::{Address, B256, keccak256};
use anyhow::Context;
use serde::Serialize;
use serde_json::{Value, json};

use super::{FINALITY_BLOCKS, L1, L1_OWNER, RECEIPT_BUDGET, quantity, selector};
use crate::poll;

#[derive(Debug, Serialize)]
pub(crate) struct Deposit {
    pub(crate) source_hash: B256,
    pub(crate) l1_block: u64,
    pub(crate) log_index: u64,
    pub(crate) nonce: u64,
}

impl Deposit {
    fn call(to: Address) -> String {
        let to = alloy_primitives::hex::encode(to);
        format!(
            "{}{to:0>64}{:064x}{:064x}{:064x}",
            selector("depositETH(address,uint64,bytes)"),
            200_000u64,
            96u64,
            0u64
        )
    }

    fn parse(receipt: &Value, lockbox: Address) -> anyhow::Result<Self> {
        anyhow::ensure!(
            receipt["status"] == "0x1",
            "depositETH fails on L1: {receipt}"
        );
        let event = format!(
            "{:#x}",
            keccak256("DepositInitiated(uint64,address,address,uint256,uint64,bytes)")
        );
        let logs: Vec<_> = receipt["logs"]
            .as_array()
            .context("deposit receipt has no logs")?
            .iter()
            .filter(|log| {
                log["address"]
                    .as_str()
                    .and_then(|address| address.parse::<Address>().ok())
                    == Some(lockbox)
                    && log["topics"][0].as_str() == Some(&event)
            })
            .collect();
        let [log] = logs.as_slice() else {
            anyhow::bail!("deposit receipt needs one lockbox deposit event");
        };
        let block_hash = log["blockHash"]
            .as_str()
            .context("deposit log has no block hash")?
            .parse::<B256>()?;
        let log_index = quantity(&log["logIndex"])?;
        Ok(Self {
            source_hash: kardamom_types::epoch::source_hash(block_hash, log_index),
            l1_block: quantity(&log["blockNumber"])?,
            log_index,
            nonce: quantity(&log["topics"][1])?,
        })
    }
}

impl L1 {
    pub(crate) async fn deposit(
        &self,
        lockbox: Address,
        to: Address,
        value: u64,
    ) -> anyhow::Result<Deposit> {
        let hash = self
            .rpc(
                "eth_sendTransaction",
                json!([{
                    "from": L1_OWNER, "to": format!("{lockbox:#x}"),
                    "data": Deposit::call(to), "value": format!("0x{value:x}"),
                }]),
            )
            .await?;
        let (receipt, _) = poll::until(RECEIPT_BUDGET, |_| async {
            let receipt = self
                .rpc("eth_getTransactionReceipt", json!([&hash]))
                .await?;
            Ok::<_, anyhow::Error>((!receipt.is_null()).then_some(receipt))
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("depositETH has no L1 receipt"))?;
        let evidence = Deposit::parse(&receipt, lockbox)?;
        self.mine(FINALITY_BLOCKS).await?;
        Ok(evidence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_deposit_call_has_the_dynamic_empty_bytes_tail() {
        let to = Address::repeat_byte(0xd4);
        let call = Deposit::call(to);
        assert_eq!(
            call,
            concat!(
                "0xf07d426b",
                "000000000000000000000000d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4",
                "0000000000000000000000000000000000000000000000000000000000030d40",
                "0000000000000000000000000000000000000000000000000000000000000060",
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
        );
        assert_eq!(call.len(), 2 + 8 + 4 * 64);
        assert_eq!(
            &call[10..74],
            format!("{:0>64}", alloy_primitives::hex::encode(to))
        );
        assert_eq!(u64::from_str_radix(&call[138..202], 16).unwrap(), 96);
        assert_eq!(u64::from_str_radix(&call[202..266], 16).unwrap(), 0);
    }

    #[test]
    fn an_unrelated_or_failed_l1_receipt_cannot_define_a_deposit_identity() {
        assert!(Deposit::parse(&json!({"status":"0x0","logs":[]}), Address::ZERO).is_err());
        assert!(Deposit::parse(&json!({"status":"0x1","logs":[]}), Address::ZERO).is_err());
    }
    #[test]
    fn direct_l1_logs_define_source_identity_and_reject_duplicate_events() {
        let lockbox = Address::repeat_byte(7);
        let event = keccak256("DepositInitiated(uint64,address,address,uint256,uint64,bytes)");
        let block_hash = B256::repeat_byte(1);
        let log = json!({"address":lockbox,"blockHash":block_hash,"blockNumber":"0x63",
            "logIndex":"0x4","topics":[event, format!("0x{:064x}",10)]});
        let mut receipt = json!({"status":"0x1","logs":[log.clone()]});
        let deposit = Deposit::parse(&receipt, lockbox).unwrap();
        assert_eq!(
            (deposit.l1_block, deposit.log_index, deposit.nonce),
            (99, 4, 10)
        );
        assert_eq!(
            deposit.source_hash,
            kardamom_types::epoch::source_hash(block_hash, 4)
        );
        assert!(Deposit::parse(&receipt, Address::ZERO).is_err());
        receipt["logs"] = json!([log.clone(), log]);
        assert!(Deposit::parse(&receipt, lockbox).is_err());
    }
}
