//! Pre-signed-transaction queue built from mnemonic-derived signers.

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;

#[derive(Debug, Clone)]
pub struct DerivedSigner {
    pub signer: PrivateKeySigner,
    pub address: Address,
}

/// Pre-sign `count` EIP-2718-encoded legacy value transfers, round-robining
/// across `signers`. Each signer's nonces start at `nonce_base` and increase
/// monotonically. Returns the raw bytes for each tx, ordered for round-robin
/// dispatch.
pub fn presign_transfers(
    signers: &[DerivedSigner],
    chain_id: u64,
    to: Address,
    value: U256,
    count: usize,
    nonce_base: u64,
) -> anyhow::Result<Vec<Bytes>> {
    let n = signers.len();
    if n == 0 {
        anyhow::bail!("at least one signer is required");
    }
    let txs_per_signer = count.div_ceil(n);
    let mut out = Vec::with_capacity(count);
    'outer: for nonce_offset in 0..txs_per_signer {
        for signer in signers {
            if out.len() == count {
                break 'outer;
            }
            let mut tx = TxLegacy {
                chain_id: Some(chain_id),
                nonce: nonce_base + nonce_offset as u64,
                gas_price: 1_000_000_000,
                gas_limit: 21_000,
                to: TxKind::Call(to),
                value,
                input: Bytes::new(),
            };
            let sig = signer
                .signer
                .sign_transaction_sync(&mut tx)
                .map_err(|e| anyhow::anyhow!("signing tx: {e}"))?;
            let envelope: TxEnvelope = tx.into_signed(sig).into();
            let mut bytes = Vec::with_capacity(110);
            envelope.encode_2718(&mut bytes);
            out.push(Bytes::from(bytes));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mnemonic;

    const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";

    #[test]
    fn presign_round_robins_across_signers() {
        let signers = mnemonic::derive_signers(ANVIL_PHRASE, 3).unwrap();
        let to = Address::from([0x11u8; 20]);
        let bytes = presign_transfers(&signers, 1, to, U256::from(1u64), 7, 0).unwrap();
        assert_eq!(bytes.len(), 7);
    }
}
