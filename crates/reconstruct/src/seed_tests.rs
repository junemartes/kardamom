use alloy_primitives::{Address, B256, hex};
use alloy_signer_local::PrivateKeySigner;
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::recon::reconstruct;
use kardamom_engine::ReplayOutcome;

use super::{SealerSeed, SeedInput, SeedSender, SenderOrder};
use crate::reconstruct_state;
use crate::rules::{Activation, Boundary, SealerRules};
use crate::test_support::{CHAIN_ID, genesis, test_genesis, two_transfer_blocks};
use kardamom_types::upgrades::{FEATURE_CARRY_WINDOW, GenesisFeatures};

/// The seed file of [`golden_seed`]. `SealerSeedTest.java` parses the same
/// bytes, so the two sides of the layout cannot drift apart.
const GOLDEN: &str = concat!(
    "4b534544",
    "00000001",
    "0000000000064aba",
    "0000000000000007",
    "0000000000000013",
    "0000018bcfe568fa",
    "000000000000002a",
    "1111111111111111111111111111111111111111111111111111111111111111",
    "00000002",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "0000000000000003",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "0000000000000001",
);

/// The rules section of [`golden_rules_seed`], after the bytes of
/// [`GOLDEN`] with version 2: version 5, sealed origin 42, the genesis
/// activations of features 3 and 4, the carry window flag off, and the
/// activation of feature 5 at origin 40 with a time ahead.
/// `SealerSeedTest.java` parses the same bytes.
const GOLDEN_RULES: &str = concat!(
    "05",
    "000000000000002a",
    "01",
    "0000000000000000",
    "0000000000000001",
    "01",
    "0000000000000000",
    "0000000000000001",
    "00",
    "01",
    "0000000000000028",
    "0000018bcfe57b88",
);

/// The rules of a carry-born chain at origin 42 that sent feature 5 with a
/// time ahead at origin 40.
fn golden_rules() -> SealerRules {
    let mut rules = SealerRules::genesis(GenesisFeatures::CarryWindow);
    rules.on_activation(
        Activation {
            l1_origin: 40,
            feature_id: FEATURE_CARRY_WINDOW,
            time_ms: 1_700_000_005_000,
        },
        39,
        1_700_000_000_000,
    );
    rules.at_boundary(&Boundary {
        l1_origin: 42,
        sealed_ms: 1_700_000_000_000,
        l2_timestamp: 1_700_000_000_250,
    });
    rules
}

fn golden_rules_seed() -> SealerSeed {
    SealerSeed {
        rules: golden_rules(),
        ..golden_seed()
    }
}

fn golden_seed() -> SealerSeed {
    SealerSeed {
        chain_id: CHAIN_ID,
        block: 7,
        end_tx_idx: 19,
        l2_timestamp: 1_700_000_000_250,
        l1_origin: 42,
        state_root: B256::repeat_byte(0x11),
        senders: vec![
            SeedSender {
                address: Address::repeat_byte(0xAA),
                next_nonce: 3,
            },
            SeedSender {
                address: Address::repeat_byte(0xBB),
                next_nonce: 1,
            },
        ],
        rules: SealerRules::genesis(GenesisFeatures::None),
    }
}

/// A seed whose rules hold no activation keeps the bytes of version 1.
#[test]
fn the_seed_layout_matches_the_golden_bytes() {
    assert_eq!(hex::encode(golden_seed().encode().unwrap()), GOLDEN);
}

/// A seed whose rules hold an activation is version 2: the bytes of
/// version 1, then the rules.
#[test]
fn a_seed_with_rules_matches_the_golden_bytes() {
    let rules = golden_rules();
    assert_eq!((rules.version(), rules.carry_window()), (5, false));
    let expected = format!("{}00000002{}{GOLDEN_RULES}", &GOLDEN[..8], &GOLDEN[16..]);
    assert_eq!(hex::encode(golden_rules_seed().encode().unwrap()), expected);
}

#[test]
fn the_sender_order_puts_the_most_recent_sender_last() {
    let [a, b, c] = [0xA1, 0xB2, 0xC3].map(Address::repeat_byte);
    assert_eq!(SenderOrder::of([a, b, a, c]), SenderOrder(vec![b, a, c]));
    assert_eq!(SenderOrder::of([]), SenderOrder(vec![]));
}

/// A state rebuilt from the blobs of two transfer blocks, and the replay
/// outcome. The frames of the last block lose their cursor when
/// `version_2_head` is set, as a version 2 payload has none.
struct Rebuilt {
    dir: tempfile::TempDir,
    outcome: ReplayOutcome,
    user: Address,
}

impl Rebuilt {
    fn new(version_2_head: bool) -> Self {
        let signer = PrivateKeySigner::random();
        let (block1, block2) = two_transfer_blocks(
            &signer,
            Address::repeat_byte(0x01),
            Address::repeat_byte(0x02),
        );
        let batch = pack_blocks(&BatcherConfig::default(), &[block1, block2]).unwrap();
        let mut frames = reconstruct(&batch.payload).unwrap();
        if version_2_head {
            frames[1].cursor = None;
        }
        let dir = tempfile::tempdir().unwrap();
        let outcome = reconstruct_state(
            dir.path(),
            &test_genesis(&genesis(signer.address()), &[]),
            &frames,
        )
        .unwrap();
        Self {
            dir,
            outcome,
            user: signer.address(),
        }
    }

    fn seed(&self) -> Result<SealerSeed, crate::ReconstructError> {
        SeedInput {
            state_dir: self.dir.path(),
            chain_id: CHAIN_ID,
            outcome: &self.outcome,
            senders: &SenderOrder::of([self.user]),
            rules: &SealerRules::genesis(GenesisFeatures::None),
        }
        .seed()
    }
}

/// The seed of a state rebuilt from L1 payloads carries the rebuilt head:
/// block 2, its canonical end 3, its timestamp and origin, the root, and
/// the next nonce of the sender (three transfers sent, so nonce 3).
#[test]
fn a_seed_carries_the_rebuilt_head_and_the_next_nonce() {
    let rebuilt = Rebuilt::new(false);
    let seed = rebuilt.seed().unwrap();
    assert_eq!(
        seed,
        SealerSeed {
            chain_id: CHAIN_ID,
            block: 2,
            end_tx_idx: 3,
            l2_timestamp: 1_700_000_001,
            l1_origin: 0,
            state_root: rebuilt.outcome.state_root,
            senders: vec![SeedSender {
                address: rebuilt.user,
                next_nonce: 3,
            }],
            rules: SealerRules::genesis(GenesisFeatures::None),
        }
    );
    let path = rebuilt.dir.path().join("sealer.seed");
    seed.write(&path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), seed.encode().unwrap());
}

#[test]
fn a_seed_refuses_a_head_without_a_canonical_end() {
    let err = Rebuilt::new(true).seed().unwrap_err().to_string();
    assert!(err.contains("carries no canonical cursor"), "{err}");
}

#[test]
fn seed_version_matches_the_registry() {
    kardamom_formats::Registry::assert_versions(
        "sealer-seed",
        kardamom_formats::Versions {
            writes: super::VERSION,
            reads_min: super::VERSION,
            reads_max: super::RULES_VERSION,
        },
    );
}
