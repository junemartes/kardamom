//! BIP-39 mnemonic generation and BIP-32 signer derivation.
//!
//! Uses the English wordlist via `alloy_signer_local`'s `mnemonic`
//! feature, which itself wraps `coins-bip39`. Derivation path is the
//! standard Ethereum/Anvil m/44'/60'/0'/0/N.

use alloy_signer_local::{
    MnemonicBuilder,
    coins_bip39::{English, Mnemonic},
};

use crate::signers::DerivedSigner;

/// Generate a fresh BIP-39 mnemonic phrase using the English wordlist.
///
/// `word_count` must be one of 12, 15, 18, 21, 24. Other values error.
pub fn generate(word_count: u32) -> anyhow::Result<String> {
    let mut rng = rand::thread_rng();
    let mnemonic = Mnemonic::<English>::new_with_count(&mut rng, word_count as usize)
        .map_err(|e| anyhow::anyhow!("generating {word_count}-word mnemonic: {e}"))?;
    Ok(mnemonic.to_phrase())
}

/// Derive `count` signers from `phrase` along m/44'/60'/0'/0/N (the
/// Anvil/MetaMask default Ethereum derivation path).
pub fn derive_signers(phrase: &str, count: u32) -> anyhow::Result<Vec<DerivedSigner>> {
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..count {
        let path = format!("m/44'/60'/0'/0/{i}");
        let signer = MnemonicBuilder::<English>::default()
            .phrase(phrase)
            .derivation_path(&path)
            .map_err(|e| anyhow::anyhow!("derivation path {path}: {e}"))?
            .build()
            .map_err(|e| anyhow::anyhow!("deriving signer {i} from mnemonic: {e}"))?;
        out.push(DerivedSigner {
            address: signer.address(),
            signer,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, address};

    const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";

    #[test]
    fn generate_12_words_round_trips() {
        let phrase = generate(12).expect("generates");
        assert_eq!(phrase.split_whitespace().count(), 12);
        let signers = derive_signers(&phrase, 1).expect("derives");
        assert_eq!(signers.len(), 1);
    }

    #[test]
    fn generate_all_valid_word_counts() {
        for n in [12u32, 15, 18, 21, 24] {
            let phrase = generate(n).unwrap_or_else(|e| panic!("generate({n}): {e}"));
            assert_eq!(phrase.split_whitespace().count(), n as usize);
        }
    }

    #[test]
    fn generate_invalid_word_count_errors() {
        assert!(
            generate(13).is_err(),
            "13 words is not a valid BIP-39 length"
        );
    }

    #[test]
    fn derive_signers_matches_anvil_well_known_addresses() {
        let signers = derive_signers(ANVIL_PHRASE, 3).expect("derives");
        let expected: [Address; 3] = [
            address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266"),
            address!("70997970C51812dc3A010C7d01b50e0d17dc79C8"),
            address!("3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"),
        ];
        for (got, want) in signers.iter().zip(expected.iter()) {
            assert_eq!(got.address, *want);
        }
    }

    #[test]
    fn derive_signers_is_deterministic() {
        let a = derive_signers(ANVIL_PHRASE, 4).expect("a");
        let b = derive_signers(ANVIL_PHRASE, 4).expect("b");
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.address, y.address);
        }
    }
}
