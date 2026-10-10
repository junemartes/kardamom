//! The canary's contracts on the wire: the creation input, the call
//! input, and the events, encoded by hand for the few shapes the probes
//! use.

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};

use crate::rpc::Receipt;

/// The gas limit of a counter deploy.
pub const DEPLOY_GAS: u64 = 1_000_000;
/// The gas limit of a counter write: a cold slot and an event, with room.
pub const INCREMENT_GAS: u64 = 100_000;

/// The counter of the `contract` probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counter(pub Address);

impl Counter {
    /// The creation input: the bytecode and the ABI encoding of the
    /// `address[] writers` argument.
    #[must_use]
    pub fn creation(writers: &[Address]) -> Bytes {
        let code = kardamom_deployer::embedded::canary_counter_creation();
        let words = [U256::from(32), U256::from(writers.len())]
            .into_iter()
            .chain(writers.iter().map(|w| U256::from_be_slice(w.as_slice())));
        let input: Vec<u8> = code
            .iter()
            .copied()
            .chain(words.flat_map(|w| w.to_be_bytes::<32>()))
            .collect();
        Bytes::from(input)
    }

    /// The input of `increment()`.
    #[must_use]
    pub fn increment() -> Bytes {
        Bytes::copy_from_slice(&keccak256("increment()")[..4])
    }

    /// The count an `Incremented` event of this counter in `receipt`
    /// reports.
    #[must_use]
    pub fn count_in(self, receipt: &Receipt) -> Option<U256> {
        let topic: B256 = keccak256("Incremented(address,uint256)");
        receipt
            .logs
            .iter()
            .find(|log| log.address == self.0 && log.topics.first() == Some(&topic))
            .and_then(|log| log.data.get(..32))
            .map(U256::from_be_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::Log;

    #[test]
    fn the_creation_input_ends_with_the_writers_array() {
        let writers = [Address::repeat_byte(1), Address::repeat_byte(2)];
        let input = Counter::creation(&writers);
        let tail = &input[input.len() - 128..];
        assert_eq!(U256::from_be_slice(&tail[..32]), U256::from(32));
        assert_eq!(U256::from_be_slice(&tail[32..64]), U256::from(2));
        assert_eq!(&tail[76..96], writers[0].as_slice());
        assert_eq!(&tail[108..128], writers[1].as_slice());
    }

    #[test]
    fn the_count_comes_from_this_counters_event() {
        let counter = Counter(Address::repeat_byte(9));
        let log = |address| Log {
            address,
            topics: vec![keccak256("Incremented(address,uint256)"), B256::ZERO],
            data: Bytes::from(U256::from(7).to_be_bytes::<32>().to_vec()),
        };
        let receipt = Receipt {
            status: alloy_primitives::U64::from(1),
            block_number: None,
            contract_address: None,
            logs: vec![log(Address::repeat_byte(8)), log(counter.0)],
        };
        assert_eq!(counter.count_in(&receipt), Some(U256::from(7)));
        assert_eq!(Counter(Address::ZERO).count_in(&receipt), None);
    }
}
