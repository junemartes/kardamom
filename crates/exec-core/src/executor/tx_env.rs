//! Envelope decoding, and `revm::TxEnv` derivation. This covers both
//! signed 2718 envelopes ([`tx_env_from_alloy`]) and OP-style deposits
//! ([`tx_env_from_deposit`]).

use alloy_consensus::Transaction;
use alloy_eips::eip2718::{Decodable2718, Typed2718};
use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_types::Deposit;
use kardamom_types::xchain::{self, XChainMessage};
use revm::context::TxEnv;
use revm::primitives::TxKind;

use alloc::format;

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

/// A decoded 2718 envelope, ready for `TxEnv` derivation.
///
/// This newtype owns both halves of the old free-function pair
/// (`decode_alloy_envelope` and `tx_env_from_alloy`). It stays independent
/// of [`super::Executor`]. The Block-STM engine decodes off-thread, in
/// `prepare`, and hands the decoded value to a worker later.
#[derive(Debug, Clone)]
pub struct DecodedTx(pub alloy_consensus::TxEnvelope);

impl DecodedTx {
    /// Decode from the `raw_tx` bytes carried in a
    /// `kardamom_types::TxEnvelope`. The proxy already verified the
    /// signature. This method only needs the typed accessors.
    pub fn decode(raw_tx: &Bytes, tx_idx: TxIndex) -> Result<Self, ExecutorError> {
        let mut slice: &[u8] = raw_tx.as_ref();
        alloy_consensus::TxEnvelope::decode_2718(&mut slice)
            .map(Self)
            .map_err(|e| ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("decode raw_tx: {e}"),
            })
    }

    /// Convert into a revm `TxEnv`. `signer` is the sender the proxy
    /// populated. This method never recomputes it.
    pub fn tx_env(&self, signer: Address) -> TxEnv {
        tx_env_from_alloy(&self.0, signer)
    }
}

impl core::ops::Deref for DecodedTx {
    type Target = alloy_consensus::TxEnvelope;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Convert a recovered tx envelope into a `TxEnv`. `signer` is the sender
/// the proxy already populated; this function never recomputes it.
///
/// This uses a full struct literal on purpose. It used to end in
/// `..Default::default()`, which silently dropped `tx_type`, `access_list`,
/// `gas_priority_fee`, and `authorization_list`. That made 2930, 1559, and
/// 7702 txs execute with legacy semantics: no access-list warmth, no
/// priority-fee split, and set-code txs treated as plain calls. Every
/// field is now populated from the envelope. A revm field addition now
/// causes a compile error, forcing a decision instead of a silent default.
fn tx_env_from_alloy(alloy_env: &alloy_consensus::TxEnvelope, signer: Address) -> TxEnv {
    use revm::context_interface::either::Either;
    TxEnv {
        // The EIP-2718 type byte drives revm's per-type validity rules,
        // for example PRIORITY_GREATER_THAN_MAX_FEE and blob-field checks.
        tx_type: alloy_env.ty(),
        caller: signer,
        chain_id: alloy_env.chain_id(),
        nonce: alloy_env.nonce(),
        gas_limit: alloy_env.gas_limit(),
        value: alloy_env.value(),
        data: alloy_env.input().clone(),
        kind: match alloy_env.to() {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        // For 1559-family types, this field is the max fee. Revm derives
        // the effective price from basefee plus priority fee.
        gas_price: alloy_env
            .gas_price()
            .unwrap_or_else(|| alloy_env.max_fee_per_gas()),
        gas_priority_fee: alloy_env.max_priority_fee_per_gas(),
        access_list: alloy_env.access_list().cloned().unwrap_or_default(),
        // Type-3 never reaches here in production. Ingress rejects it,
        // and `max_blobs_per_tx = 0`. This maps it anyway, so derivation
        // stays total and the deterministic invalid-skip fires on the
        // right rule.
        blob_hashes: alloy_env
            .blob_versioned_hashes()
            .map(<[B256]>::to_vec)
            .unwrap_or_default(),
        max_fee_per_blob_gas: alloy_env.max_fee_per_blob_gas().unwrap_or_default(),
        authorization_list: alloy_env
            .authorization_list()
            .map(|l| l.iter().cloned().map(Either::Left).collect())
            .unwrap_or_default(),
    }
}

/// Build a `TxEnv` from a deposit envelope. Deposits never deduct fees
/// (`gas_price = 0`), do not check chain ID at the envelope layer, and
/// disable the nonce check through the caller's
/// `cfg.disable_nonce_check = true`.
pub(super) fn tx_env_from_deposit(dep: &Deposit) -> TxEnv {
    TxEnv {
        caller: dep.from,
        kind: match dep.to {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        value: dep.value,
        data: AlloyBytes::copy_from_slice(dep.input.as_ref()),
        gas_limit: dep.gas_limit,
        gas_price: 0,
        nonce: 0,
        chain_id: None,
        ..Default::default()
    }
}

/// Build a `TxEnv` for one cross-chain delivery. The call is fee-free
/// (`gas_price = 0`), carries no nonce, and its caller is the aliased
/// origin Outbox — the only address `Inbox.deliver` accepts. `gas_limit`
/// adds [`super::xchain::XCHAIN_DELIVERY_OVERHEAD`] on top of the
/// message's own budget, to also pay for the Inbox's own bookkeeping.
pub(super) fn tx_env_from_xchain(origin_chain_id: u64, message: &XChainMessage) -> TxEnv {
    TxEnv {
        caller: xchain::xchain_tx_sender(origin_chain_id),
        kind: TxKind::Call(xchain::INBOX),
        value: alloy_primitives::U256::ZERO,
        data: AlloyBytes::from(xchain::deliver_calldata(origin_chain_id, message)),
        gas_limit: message
            .gas_limit
            .saturating_add(super::xchain::XCHAIN_DELIVERY_OVERHEAD),
        gas_price: 0,
        nonce: 0,
        chain_id: None,
        ..Default::default()
    }
}

// -- tx_env_from_alloy typed-field mapping ----------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_primitives::{TxKind as APTxKind, U256, address};

    fn dummy_sig() -> alloy_primitives::Signature {
        alloy_primitives::Signature::new(U256::from(1), U256::from(1), false)
    }

    #[test]
    fn tx_env_maps_legacy_without_typed_fields() {
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce: 3,
            gas_price: 42,
            gas_limit: 21_000,
            to: APTxKind::Call(address!("0x1111111111111111111111111111111111111111")),
            value: U256::from(5),
            input: Default::default(),
        };
        let env: alloy_consensus::TxEnvelope = tx.into_signed(dummy_sig()).into();
        let te = tx_env_from_alloy(&env, Address::ZERO);
        assert_eq!(te.tx_type, 0);
        assert_eq!(te.gas_price, 42);
        assert_eq!(te.gas_priority_fee, None);
        assert!(te.access_list.iter().next().is_none());
        assert!(te.authorization_list.is_empty());
    }

    #[test]
    fn tx_env_maps_eip1559_priority_fee_and_access_list() {
        let al =
            alloy_eips::eip2930::AccessList(alloc::vec![alloy_eips::eip2930::AccessListItem {
                address: address!("0x2222222222222222222222222222222222222222"),
                storage_keys: alloc::vec![B256::ZERO],
            }]);
        let tx = alloy_consensus::TxEip1559 {
            chain_id: 1,
            nonce: 9,
            gas_limit: 100_000,
            max_fee_per_gas: 50,
            max_priority_fee_per_gas: 5,
            to: APTxKind::Call(address!("0x1111111111111111111111111111111111111111")),
            value: U256::ZERO,
            access_list: al,
            input: Default::default(),
        };
        let env: alloy_consensus::TxEnvelope = tx.into_signed(dummy_sig()).into();
        let te = tx_env_from_alloy(&env, Address::ZERO);
        assert_eq!(te.tx_type, 2);
        assert_eq!(te.gas_price, 50, "gas_price carries the MAX fee");
        assert_eq!(te.gas_priority_fee, Some(5));
        assert_eq!(te.access_list.iter().count(), 1);
    }

    #[test]
    fn tx_env_maps_eip7702_authorization_list() {
        let auth = alloy_eips::eip7702::Authorization {
            chain_id: U256::from(1),
            address: address!("0x3333333333333333333333333333333333333333"),
            nonce: 0,
        };
        let signed_auth = alloy_eips::eip7702::SignedAuthorization::new_unchecked(
            auth,
            0,
            U256::from(1),
            U256::from(1),
        );
        let tx = alloy_consensus::TxEip7702 {
            chain_id: 1,
            nonce: 1,
            gas_limit: 100_000,
            max_fee_per_gas: 50,
            max_priority_fee_per_gas: 2,
            to: address!("0x1111111111111111111111111111111111111111"),
            value: U256::ZERO,
            access_list: Default::default(),
            authorization_list: alloc::vec![signed_auth],
            input: Default::default(),
        };
        let env: alloy_consensus::TxEnvelope = tx.into_signed(dummy_sig()).into();
        let te = tx_env_from_alloy(&env, Address::ZERO);
        assert_eq!(te.tx_type, 4);
        assert_eq!(te.authorization_list.len(), 1);
        assert_eq!(te.gas_priority_fee, Some(2));
    }
}
