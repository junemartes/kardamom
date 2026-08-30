//! Sender accessor for ingress tx frames.
//!
//! The ingress proxy recovers the sender during batched secp256k1 verification.
//! It writes the sender into [`kardamom_types::TxEnvelope::sender`] as a
//! typed `Address`, never an `Option`. The sequencer trusts this value
//! unconditionally: no fallback, no `recover_signer()`, and no
//! paranoid-check mode. The sequencer does zero secp256k1 work on the hot
//! path. This keeps the §3 nonce-check budget at 3 microseconds or less per
//! transaction.
//!
//! This module is a thin accessor. It exists for two reasons:
//!  1. It makes the trust assumption explicit at call sites. `sender_of(env)`
//!     reads better than `env.sender`.
//!  2. It gives one place to add a debug-only panic if the field is ever
//!     `Address::ZERO` in test builds. That would show the proxy regressed.

use alloy_primitives::Address;
use kardamom_types::TxEnvelope;

/// Return the proxy-populated sender for a [`TxEnvelope`].
///
/// This function is `#[inline]`, so the compiler folds it into the caller.
/// The `debug_assert!` check is removed in release builds.
#[inline]
pub fn sender_of(envelope: &TxEnvelope) -> Address {
    debug_assert!(
        envelope.sender != Address::ZERO,
        "TxEnvelope.sender must be populated by the proxy; got Address::ZERO"
    );
    envelope.sender
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kardamom_types::TxEnvelope;

    fn env_with_sender(sender: Address) -> TxEnvelope {
        TxEnvelope {
            correlation_id: 7,
            raw_tx: Bytes::from_static(b"raw"),
            sender,
            tx_hash: Default::default(),
        }
    }

    #[test]
    fn returns_proxy_populated_sender_directly() {
        let s = Address::repeat_byte(0xAB);
        let env = env_with_sender(s);
        assert_eq!(sender_of(&env), s);
    }

    #[test]
    fn returns_distinct_senders_for_distinct_envelopes() {
        let a = env_with_sender(Address::repeat_byte(1));
        let b = env_with_sender(Address::repeat_byte(2));
        assert_ne!(sender_of(&a), sender_of(&b));
    }
}
