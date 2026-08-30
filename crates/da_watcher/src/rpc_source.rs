//! Alloy-provider-backed implementation of [`crate::source::L1Source`].
//!
//! It has only two jobs:
//!   * map `finalized_block_number()` to `eth_getBlockByNumber("finalized")`,
//!   * map `lockbox_logs(...)` to `eth_getLogs(...)`, filtered by the
//!     lockbox address and the `DepositInitiated` and `UpgradeInitiated`
//!     event signatures, then ABI-decode each result into a [`LockboxLog`].
//!
//! Ported from `crates/node/src/l1_source_rpc.rs`. The contracts'
//! bytecode-hash CI check byte-pins the event signature to the on-chain
//! `ETHLockbox.sol` ABI.

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{BlockNumberOrTag, Filter, Log as RpcLog};
use alloy_sol_types::{SolEvent, sol};
use async_trait::async_trait;

use kardamom_types::epoch::UpgradeLog;

use crate::source::{DepositLog, L1Source, L1SourceError, LockboxLog};

sol! {
    /// Mirror of `contracts/src/L1/ETHLockbox.sol::DepositInitiated`.
    /// The Rust-side wire signature must stay byte-identical with the
    /// Solidity declaration. CI's bytecode-hash pin catches drift on the
    /// contract side.
    #[derive(Debug)]
    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );

    /// Mirror of `contracts/src/L1/ETHLockbox.sol::UpgradeInitiated`, the
    /// upgrade transaction. `activationTimestamp` is in epoch milliseconds.
    #[derive(Debug)]
    event UpgradeInitiated(
        uint64 indexed upgradeNonce, uint256 indexed featureId, uint64 activationTimestamp
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
                // finalized tip. So a miss means a reorg or a lying
                // provider, not an expected "not yet" like NotFinalized.
                L1SourceError::Provider(format!("finalized L1 block {number} not found"))
            })?;
        Ok((block.header.hash, block.header.parent_hash))
    }

    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LockboxLog>, L1SourceError> {
        // One query for both event kinds, using a topic0 set, not two round
        // trips. Two separate queries could succeed and fail independently,
        // which could let an epoch derive with its deposits but without its
        // upgrade.
        let filter = Filter::new()
            .address(lockbox)
            .event_signature(vec![
                DepositInitiated::SIGNATURE_HASH,
                UpgradeInitiated::SIGNATURE_HASH,
            ])
            .from_block(from_block)
            .to_block(to_block);

        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(|e| L1SourceError::Provider(e.to_string()))?;

        logs.iter().map(decode_lockbox_log).collect()
    }
}

/// Decode one lockbox log, dispatching on `topic[0]`.
///
/// An unrecognized topic0 is a hard `Decode` error, not a skip. The filter
/// asked for exactly two signatures, so any other topic means the provider
/// ignored the filter. Silently dropping it would derive an epoch that
/// disagrees with L1.
pub(crate) fn decode_lockbox_log(log: &RpcLog) -> Result<LockboxLog, L1SourceError> {
    let topic0 = *log
        .topic0()
        .ok_or_else(|| L1SourceError::Decode("log has no topic[0]".to_string()))?;
    if topic0 == DepositInitiated::SIGNATURE_HASH {
        decode_deposit_log(log).map(LockboxLog::Deposit)
    } else if topic0 == UpgradeInitiated::SIGNATURE_HASH {
        decode_upgrade_log(log).map(LockboxLog::Upgrade)
    } else {
        Err(L1SourceError::Decode(format!(
            "unexpected event signature: got {topic0:#x}, want {:#x} or {:#x}",
            DepositInitiated::SIGNATURE_HASH,
            UpgradeInitiated::SIGNATURE_HASH
        )))
    }
}

/// Decode a single `UpgradeInitiated` log into an [`UpgradeLog`].
pub(crate) fn decode_upgrade_log(log: &RpcLog) -> Result<UpgradeLog, L1SourceError> {
    let topic0 = log
        .topic0()
        .ok_or_else(|| L1SourceError::Decode("log has no topic[0]".to_string()))?;
    if *topic0 != UpgradeInitiated::SIGNATURE_HASH {
        return Err(L1SourceError::Decode(format!(
            "unexpected event signature: got {topic0:#x}, want {:#x}",
            UpgradeInitiated::SIGNATURE_HASH
        )));
    }

    let decoded = UpgradeInitiated::decode_log(&log.inner)
        .map_err(|e| L1SourceError::Decode(format!("UpgradeInitiated decode: {e}")))?;
    let evt = decoded.data;

    let block_hash = log
        .block_hash
        .ok_or_else(|| L1SourceError::Decode("log missing block_hash".to_string()))?;
    let log_index = log
        .log_index
        .ok_or_else(|| L1SourceError::Decode("log missing log_index".to_string()))?;
    let block_number = log
        .block_number
        .ok_or_else(|| L1SourceError::Decode("log missing block_number".to_string()))?;

    Ok(UpgradeLog {
        block_number,
        block_hash,
        log_index,
        feature_id: evt.featureId,
        activation_timestamp: evt.activationTimestamp,
    })
}

/// Decode a single `DepositInitiated` log into a [`DepositLog`].
///
/// This function returns `Decode` when:
///  * `topic[0] != DepositInitiated::SIGNATURE_HASH`,
///  * the ABI decode fails,
///  * `mint > u128::MAX` (the deposit type's `mint` field is `u128`),
///  * `block_hash` or `log_index` is missing. This happens only for a
///    pending log, which `eth_getLogs` with explicit block bounds does not
///    return.
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

    fn synthetic_upgrade_log(feature: u64, activation: u64, log_index: u64) -> RpcLog {
        // topics: [sig, upgradeNonce(uint64), featureId(uint256)]
        let topics = vec![
            UpgradeInitiated::SIGNATURE_HASH,
            B256::from(U256::from(3u64)),
            B256::from(U256::from(feature)),
        ];
        // Non-indexed: (uint64 activationTimestamp).
        let body = (activation,).abi_encode_sequence();
        let inner = alloy_primitives::Log {
            address: address!("0000000000000000000000000000000000C0DE01"),
            data: LogData::new(topics, Bytes::from(body)).expect("LogData"),
        };
        RpcLog {
            inner,
            block_hash: Some(b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            )),
            block_number: Some(456),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    #[test]
    fn decodes_upgrade_initiated_log() {
        let log = synthetic_upgrade_log(9, 1_700_000_000_250, 2);
        let out = decode_upgrade_log(&log).expect("decode ok");
        assert_eq!(out.feature_id, U256::from(9u64));
        assert_eq!(out.activation_timestamp, 1_700_000_000_250);
        assert_eq!(out.log_index, 2);
        assert_eq!(out.block_number, 456);
    }

    /// One filtered query returns both kinds interleaved. The dispatcher
    /// must route by topic0, not by position or count.
    #[test]
    fn dispatches_both_event_kinds_on_topic0() {
        let dep = synthetic_log(U256::from(5u64), 21_000, Bytes::new());
        let upg = synthetic_upgrade_log(1, 0, 7);

        assert!(matches!(
            decode_lockbox_log(&dep).expect("deposit"),
            LockboxLog::Deposit(_)
        ));
        assert!(matches!(
            decode_lockbox_log(&upg).expect("upgrade"),
            LockboxLog::Upgrade(_)
        ));
    }

    /// A provider that ignores the topic filter must be caught, not skipped.
    /// Silently dropping an unknown log would derive an epoch that
    /// disagrees with L1, while looking healthy.
    #[test]
    fn dispatcher_rejects_an_unfiltered_third_event() {
        let mut log = synthetic_log(U256::from(1u64), 21_000, Bytes::new());
        let bad = b256!("beef000000000000000000000000000000000000000000000000000000000000");
        let mut topics: Vec<B256> = log.inner.data.topics().to_vec();
        topics[0] = bad;
        log.inner.data = LogData::new(topics, log.inner.data.data.clone()).expect("rebuild");

        let err = decode_lockbox_log(&log).expect_err("unknown topic0 must fail");
        assert!(
            matches!(err, L1SourceError::Decode(ref m) if m.contains("event signature")),
            "got {err:?}"
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
