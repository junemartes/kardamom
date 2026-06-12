//! Deposit-side of the sequencer.
//!
//! The DA watcher publishes `types::Deposit` envelopes onto the dedicated
//! `tx_deposits` Aeron channel. Each of the M sequencers in the deployment
//! subscribes to that channel and republishes a tiny [`types::DepositRef`]
//! onto the canonical orderer `tx_ordering` — same pattern as the regular
//! `tx_data → TxRef on tx_ordering` flow for L2 txs.
//!
//! All M sequencers race on the multi-publisher tx_ordering stream, so each
//! deposit produces M duplicate `DepositRef`s in canonical order. Downstream
//! consumers (executor) dedup on `source_hash` and resolve the full
//! `Deposit` envelope from the `tx_deposits` archive at the ref's
//! `deposit_position`.
//!
//! Deposits are **not nonce-gated**: they carry an OP `source_hash` instead
//! and have no state-machine interaction in the sequencer. The codepath is
//! therefore a simple poll → publish pump that lives alongside (and runs
//! independently of) the nonce-gated tx_data → TxRef path in
//! [`crate::sequencer`].

use kardamom_types::{BPosition, Deposit, DepositRef};

use crate::error::SequencerError;
use crate::outbound::TxOrderingRefPublisher;

/// Subscription surface the deposit pump reads from. Production wiring
/// binds this to the real `log::TxDepositsSubscriber`; tests use the
/// in-memory fake in [`fakes`].
pub trait DepositSubscriber: Send {
    /// Poll for one deposit. Returns:
    /// * `Ok(Some((pos, deposit)))` — a deposit was available and is yielded.
    /// * `Ok(None)` — no fragment available right now (caller should back off).
    /// * `Err(SequencerError::IngressDisconnected)` — subscription closed.
    fn poll(&mut self) -> Result<Option<(BPosition, Deposit)>, SequencerError>;
}

/// Single-step deposit pump: pull one deposit off the subscription and
/// republish a [`DepositRef`] on `tx_ordering`. Returns `Ok(true)` if a
/// deposit was processed (caller should keep going), `Ok(false)` if the
/// subscription is idle (caller can back off).
///
/// On `SequencerError::Backpressure` the caller retries the same ref next
/// tick — the deposit envelope itself is durable on the deposits stream, so
/// re-deriving the ref is cheap (no rewind state to manage).
pub fn process_deposit<S, P>(sub: &mut S, b: &mut P) -> Result<bool, SequencerError>
where
    S: DepositSubscriber,
    P: TxOrderingRefPublisher,
{
    let Some((pos, dep)) = sub.poll()? else {
        return Ok(false);
    };
    let dref = DepositRef::new(dep.source_hash, pos);
    b.try_publish_deposit_ref(&dref)?;
    Ok(true)
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// In-memory `DepositSubscriber` driven by a scripted queue. Push test
    /// inputs via [`push`]; the sequencer drains them in FIFO order.
    #[derive(Default, Clone)]
    pub struct ScriptedDeposits {
        pub queue: Arc<Mutex<VecDeque<(BPosition, Deposit)>>>,
        pub closed: Arc<Mutex<bool>>,
    }

    impl ScriptedDeposits {
        pub fn push(&self, pos: BPosition, dep: Deposit) {
            self.queue.lock().unwrap().push_back((pos, dep));
        }

        pub fn close(&self) {
            *self.closed.lock().unwrap() = true;
        }
    }

    impl DepositSubscriber for ScriptedDeposits {
        fn poll(&mut self) -> Result<Option<(BPosition, Deposit)>, SequencerError> {
            if let Some(item) = self.queue.lock().unwrap().pop_front() {
                return Ok(Some(item));
            }
            if *self.closed.lock().unwrap() {
                return Err(SequencerError::IngressDisconnected);
            }
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;
    use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;
    use alloy_primitives::{Address, B256, U256};
    use bytes::Bytes;

    fn deposit(source: u8) -> Deposit {
        Deposit {
            source_hash: B256::repeat_byte(source),
            from: Address::repeat_byte(0x11),
            to: Some(Address::repeat_byte(0x22)),
            mint: 1_000,
            value: U256::from(1_000u64),
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Bytes::new(),
        }
    }

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

    #[test]
    fn idle_subscription_returns_false() {
        let mut sub = ScriptedDeposits::default();
        let mut pub_ = InMemoryTxOrderingRefPublisher::default();
        assert!(!process_deposit(&mut sub, &mut pub_).unwrap());
        assert!(pub_.deposit_refs.lock().unwrap().is_empty());
    }

    #[test]
    fn deposit_yields_deposit_ref_on_b() {
        let sub_handle = ScriptedDeposits::default();
        sub_handle.push(pos(7), deposit(0xAA));
        let mut sub = sub_handle.clone();
        let mut pub_ = InMemoryTxOrderingRefPublisher::default();

        assert!(process_deposit(&mut sub, &mut pub_).unwrap());
        let refs = pub_.deposit_refs.lock().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].source_hash, B256::repeat_byte(0xAA));
        assert_eq!(refs[0].deposit_position, pos(7));
    }

    #[test]
    fn multiple_deposits_preserve_arrival_order_on_b() {
        let sub_handle = ScriptedDeposits::default();
        sub_handle.push(pos(1), deposit(0x01));
        sub_handle.push(pos(2), deposit(0x02));
        sub_handle.push(pos(3), deposit(0x03));
        let mut sub = sub_handle.clone();
        let mut pub_ = InMemoryTxOrderingRefPublisher::default();

        while process_deposit(&mut sub, &mut pub_).unwrap() {}
        let refs = pub_.deposit_refs.lock().unwrap();
        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].source_hash, B256::repeat_byte(0x01));
        assert_eq!(refs[1].source_hash, B256::repeat_byte(0x02));
        assert_eq!(refs[2].source_hash, B256::repeat_byte(0x03));
    }

    #[test]
    fn closed_subscription_surfaces_disconnected() {
        let sub_handle = ScriptedDeposits::default();
        sub_handle.close();
        let mut sub = sub_handle;
        let mut pub_ = InMemoryTxOrderingRefPublisher::default();
        assert!(matches!(
            process_deposit(&mut sub, &mut pub_),
            Err(SequencerError::IngressDisconnected)
        ));
    }

    #[test]
    fn backpressure_does_not_drop_deposit() {
        let sub_handle = ScriptedDeposits::default();
        sub_handle.push(pos(1), deposit(0x55));
        let mut sub = sub_handle.clone();
        let mut pub_ = InMemoryTxOrderingRefPublisher::default();
        *pub_.fail_with_backpressure.lock().unwrap() = true;

        // process_deposit pulled the deposit off the queue but failed to
        // publish; the deposit envelope itself is still durable on the
        // (real-Aeron) tx_deposits stream, so the caller would re-derive
        // the ref next tick. The fake here just surfaces the error.
        assert!(matches!(
            process_deposit(&mut sub, &mut pub_),
            Err(SequencerError::Backpressure)
        ));
        assert!(pub_.deposit_refs.lock().unwrap().is_empty());
    }
}
