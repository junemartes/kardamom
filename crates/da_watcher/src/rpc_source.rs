//! Alloy-provider-backed implementation of [`crate::source::L1Source`].
//!
//! Two responsibilities only:
//!   * map `finalized_block_number()` → `eth_getBlockByNumber("finalized")`,
//!   * map `deposit_logs(...)` → `eth_getLogs(...)` filtered by the lockbox
//!     address and the `DepositInitiated` event signature, then ABI-decode
//!     each result into a [`DepositLog`].
//!
//! Ported verbatim from PR #10's `crates/node/src/l1_source_rpc.rs`. The
//! event signature is byte-pinned to the on-chain `ETHLockbox.sol` ABI by
//! the contracts' bytecode-hash CI check.

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{BlockNumberOrTag, Filter, Log as RpcLog};
use alloy_sol_types::{SolEvent, sol};
use async_trait::async_trait;

use crate::source::{DepositLog, L1Source, L1SourceError};

sol! {
    /// Mirror of `contracts/src/L1/ETHLockbox.sol::DepositInitiated`.
    /// The Rust-side wire signature must stay byte-identical with the Solidity
    /// declaration; CI's bytecode-hash pin catches drift on the contract side.
    #[derive(Debug)]
    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );
}

/// Wraps an alloy `Provider` and exposes the two L1 reads the watcher needs.
pub struct RpcL1Source<P> {
    provider: P,
}

impl<P> RpcL1Source<P> {
    /// Wrap an alloy `Provider` in the `L1Source` adapter.
    #[must_use]
    pub const fn new(provider: P) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P> L1Source for RpcL1Source<P>
where
    P: Provider + Send + Sync + 'static,
{
    async fn finalized_block_number(&self) -> Result<u64, L1SourceError> {
        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|e| L1SourceError::Provider(e.to_string()))?
            .ok_or(L1SourceError::NotFinalized)?;
        Ok(block.header.number)
    }

    async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError> {
        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await
            .map_err(|e| L1SourceError::Provider(e.to_string()))?
            .ok_or_else(|| {
                // The caller only ever asks for blocks at or below the
                // finalized tip, so a miss is a reorg or a lying provider —
                // not an expected "not yet" like NotFinalized.
                L1SourceError::Provider(format!("finalized L1 block {number} not found"))
            })?;
        Ok((block.header.hash, block.header.parent_hash))
    }

    async fn deposit_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<DepositLog>, L1SourceError> {
        let filter = Filter::new()
            .address(lockbox)
            .event_signature(DepositInitiated::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(|e| L1SourceError::Provider(e.to_string()))?;

        logs.iter().map(decode_deposit_log).collect()
    }
}

/// Decode a single `DepositInitiated` log into a [`DepositLog`].
///
/// Fails on:
///  * `topic[0] != DepositInitiated::SIGNATURE_HASH` → `Decode`,
///  * ABI-decode failure → `Decode`,
///  * `mint > u128::MAX` (the deposit type's `mint` field is `u128`) →
///    `Decode`,
///  * missing `block_hash` / `log_index` (only happens for pending logs,
///    which `eth_getLogs` with explicit block bounds will not return) →
///    `Decode`.
pub(crate) fn decode_deposit_log(log: &RpcLog) -> Result<DepositLog, L1SourceError> {
    let topic0 = log
        .topic0()
        .ok_or_else(|| L1SourceError::Decode("log has no topic[0]".to_string()))?;
    if *topic0 != DepositInitiated::SIGNATURE_HASH {
        return Err(L1SourceError::Decode(format!(
            "unexpected event signature: got {topic0:#x}, want {:#x}",
            DepositInitiated::SIGNATURE_HASH
        )));
    }

    let decoded = DepositInitiated::decode_log(&log.inner)
        .map_err(|e| L1SourceError::Decode(format!("DepositInitiated decode: {e}")))?;
    let evt = decoded.data;

    let mint = u256_to_u128(evt.mint)
        .ok_or_else(|| L1SourceError::Decode(format!("mint {} exceeds u128::MAX", evt.mint)))?;

    let block_hash = log
        .block_hash
        .ok_or_else(|| L1SourceError::Decode("log missing block_hash".to_string()))?;
    let log_index = log
        .log_index
        .ok_or_else(|| L1SourceError::Decode("log missing log_index".to_string()))?;
    let block_number = log
        .block_number
        .ok_or_else(|| L1SourceError::Decode("log missing block_number".to_string()))?;

    Ok(DepositLog {
        block_number,
        block_hash,
        log_index,
        from: evt.from,
        to: evt.to,
        mint,
        gas_limit: evt.gasLimit,
        data: evt.data,
    })
}

fn u256_to_u128(v: U256) -> Option<u128> {
    if v > U256::from(u128::MAX) {
        None
    } else {
        Some(v.to::<u128>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, Bytes, LogData, U256, address, b256};
    use alloy_rpc_types_eth::Log as RpcLog;
    use alloy_sol_types::SolValue;

    fn synthetic_log(mint: U256, gas_limit: u64, data: Bytes) -> RpcLog {
        let from = address!("00000000000000000000000000000000000000aa");
        let to = address!("00000000000000000000000000000000000000bb");
        // Topics: [sig, depositNonce(uint64), from(address), to(address)]
        let topics = vec![
            DepositInitiated::SIGNATURE_HASH,
            B256::from(U256::from(7u64)),
            B256::left_padding_from(from.as_slice()),
            B256::left_padding_from(to.as_slice()),
        ];
        // Non-indexed: (uint256 mint, uint64 gasLimit, bytes data).
        let body = (mint, gas_limit, data).abi_encode_sequence();
        let inner = alloy_primitives::Log {
            address: address!("0000000000000000000000000000000000C0DE01"),
            data: LogData::new(topics, Bytes::from(body)).expect("LogData"),
        };
        RpcLog {
            inner,
            block_hash: Some(b256!(
                "1111111111111111111111111111111111111111111111111111111111111111"
            )),
            block_number: Some(123),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: Some(4),
            removed: false,
        }
    }

    #[test]
    fn decodes_deposit_initiated_log_into_deposit_log() {
        let mint = U256::from(1_000u64);
        let data = Bytes::from(vec![0xde, 0xad]);
        let log = synthetic_log(mint, 50_000, data.clone());
        let out = decode_deposit_log(&log).expect("decode ok");
        assert_eq!(
            out.from,
            address!("00000000000000000000000000000000000000aa")
        );
        assert_eq!(out.to, address!("00000000000000000000000000000000000000bb"));
        assert_eq!(out.mint, 1_000u128);
        assert_eq!(out.gas_limit, 50_000);
        assert_eq!(out.data, data);
        assert_eq!(out.log_index, 4);
        assert_eq!(
            out.block_hash,
            b256!("1111111111111111111111111111111111111111111111111111111111111111")
        );
    }

    #[test]
    fn rejects_log_with_wrong_event_signature() {
        let mut log = synthetic_log(U256::from(1u64), 21_000, Bytes::new());
        let bad_topic = b256!("dead000000000000000000000000000000000000000000000000000000000000");
        let mut topics: Vec<B256> = log.inner.data.topics().to_vec();
        topics[0] = bad_topic;
        log.inner.data =
            LogData::new(topics, log.inner.data.data.clone()).expect("LogData rebuild");
        let err = decode_deposit_log(&log).expect_err("wrong sig must fail");
        assert!(
            matches!(err, L1SourceError::Decode(ref m) if m.contains("event signature")),
            "got {err:?}"
        );
    }

    #[test]
    fn rejects_log_with_mint_above_u128() {
        let too_big = U256::from(u128::MAX) + U256::from(1u64);
        let log = synthetic_log(too_big, 21_000, Bytes::new());
        let err = decode_deposit_log(&log).expect_err("oversize mint must fail");
        assert!(
            matches!(err, L1SourceError::Decode(ref m) if m.contains("u128::MAX")),
            "got {err:?}"
        );
    }
}
