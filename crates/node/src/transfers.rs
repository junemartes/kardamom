//! `traceTransfers` inspector for `eth_simulateV1`.
//!
//! Captures every ETH movement (top-level value, internal call value,
//! selfdestruct) and emits a synthetic ERC-20 `Transfer(address,address,uint256)`
//! log. Real logs from the EVM are collected through the same buffer so that
//! synthetic and real logs interleave in execution order.

use alloy_primitives::{Address, B256, Bytes, Log, LogData, U256};
use alloy_rpc_types_eth::Log as RpcLog;
use revm::Inspector;
use revm::context::{ContextTr, JournalTr};
use revm::interpreter::interpreter::EthInterpreter;
use revm::interpreter::{CallInputs, CallOutcome, CallValue};

use crate::simulate::TRANSFER_SIG;

/// Inspector that buffers logs (real + synthetic transfer events) in the
/// order they're emitted. After execution, [`Self::into_rpc_logs`] returns
/// the merged sequence with stable indices.
#[derive(Default)]
pub(crate) struct TransferInspector {
    logs: Vec<Log>,
}

impl TransferInspector {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Emit a phantom transfer event after the top-level execution, modelling
    /// the priority-fee payment from `payer` to `beneficiary`.
    pub(crate) fn record_coinbase_payment(
        &mut self,
        payer: Address,
        beneficiary: Address,
        amount: U256,
    ) {
        if beneficiary == Address::ZERO || amount.is_zero() {
            return;
        }
        self.logs
            .push(transfer_log(payer, payer, beneficiary, amount));
    }

    /// Consume the inspector and return the captured logs, formatted for RPC.
    /// Indices are assigned in capture order so they're stable across the
    /// merged real+synthetic stream.
    pub(crate) fn into_rpc_logs(self) -> Vec<RpcLog> {
        self.logs
            .into_iter()
            .enumerate()
            .map(|(i, log)| RpcLog {
                inner: log,
                log_index: Some(i as u64),
                ..Default::default()
            })
            .collect()
    }
}

impl<CTX> Inspector<CTX, EthInterpreter> for TransferInspector
where
    CTX: ContextTr<Journal: JournalTr>,
{
    fn call(&mut self, _context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        if let CallValue::Transfer(value) = inputs.value
            && !value.is_zero()
        {
            self.logs.push(transfer_log(
                inputs.caller,
                inputs.caller,
                inputs.target_address,
                value,
            ));
        }
        None
    }

    fn log(&mut self, _context: &mut CTX, log: Log) {
        self.logs.push(log);
    }

    fn selfdestruct(&mut self, contract: Address, target: Address, value: U256) {
        if value.is_zero() {
            return;
        }
        self.logs
            .push(transfer_log(contract, contract, target, value));
    }
}

/// `event Transfer(address indexed from, address indexed to, uint256 value)`
fn transfer_log(emitter: Address, from: Address, to: Address, value: U256) -> Log {
    let data = LogData::new_unchecked(
        vec![TRANSFER_SIG, address_topic(from), address_topic(to)],
        Bytes::from(value.to_be_bytes::<32>().to_vec()),
    );
    Log {
        address: emitter,
        data,
    }
}

fn address_topic(addr: Address) -> B256 {
    let mut topic = [0u8; 32];
    topic[12..].copy_from_slice(addr.as_slice());
    B256::from(topic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_topic_pads_left_with_zeros() {
        let addr = Address::from([0x11u8; 20]);
        let topic = address_topic(addr);
        assert_eq!(&topic.as_slice()[..12], &[0u8; 12]);
        assert_eq!(&topic.as_slice()[12..], &[0x11u8; 20]);
    }

    #[test]
    fn coinbase_payment_skipped_when_beneficiary_is_zero() {
        let mut insp = TransferInspector::new();
        insp.record_coinbase_payment(Address::from([1u8; 20]), Address::ZERO, U256::from(100u64));
        assert!(insp.into_rpc_logs().is_empty());
    }

    #[test]
    fn coinbase_payment_skipped_when_amount_is_zero() {
        let mut insp = TransferInspector::new();
        insp.record_coinbase_payment(
            Address::from([1u8; 20]),
            Address::from([2u8; 20]),
            U256::ZERO,
        );
        assert!(insp.into_rpc_logs().is_empty());
    }

    #[test]
    fn coinbase_payment_emits_transfer_when_both_nonzero() {
        let payer = Address::from([1u8; 20]);
        let bene = Address::from([2u8; 20]);
        let mut insp = TransferInspector::new();
        insp.record_coinbase_payment(payer, bene, U256::from(7u64));
        let logs = insp.into_rpc_logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].inner.address, payer);
        assert_eq!(logs[0].inner.data.topics()[0], TRANSFER_SIG);
        assert_eq!(logs[0].inner.data.topics()[1], address_topic(payer));
        assert_eq!(logs[0].inner.data.topics()[2], address_topic(bene));
        let mut expected = [0u8; 32];
        expected[31] = 7;
        assert_eq!(logs[0].inner.data.data.as_ref(), &expected[..]);
    }
}
