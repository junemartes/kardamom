//! The genesis state of each chain file: the pinned roots of the chains
//! without genesis features, and the activation words of a chain with them.

use alloy_primitives::{B256, U256, b256, keccak256};
use kardamom_state::{StateEnv, StateSnapshot};
use kardamom_types::epoch::{LockboxLog, UpgradeLog, derive_epoch};
use kardamom_types::upgrades::{
    CHAIN_STATE, GENESIS_ACTIVATION_MS, GenesisFeatures, SEALER_FEATURES, activation_slot,
};
use kardamom_types::{Genesis, StateDatabase};

use super::{ReplayBlock, ReplayGenesis, replay_blocks};
use crate::actor::test_support::fresh_env;

/// A genesis file of the repository, parsed and validated.
fn genesis(path: &str) -> Genesis {
    let full = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    let raw = std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("{path}: {e}"));
    let genesis: Genesis = toml::from_str(&raw).unwrap_or_else(|e| panic!("{path}: {e}"));
    genesis.validate().unwrap();
    genesis
}

/// The state root after `blocks` on `genesis`, and the env that holds it.
fn replayed(genesis: &Genesis, blocks: Vec<ReplayBlock>) -> (tempfile::TempDir, StateEnv, B256) {
    let (dir, env) = fresh_env();
    let root = replay_blocks(env.clone(), &ReplayGenesis::of(genesis), blocks)
        .unwrap()
        .state_root;
    (dir, env, root)
}

/// The code hash at [`CHAIN_STATE`] and the activation word of each carry
/// feature, in the state of `env`.
fn chain_state(env: &StateEnv) -> (Option<B256>, Vec<U256>) {
    let snap = StateSnapshot::open(env).unwrap();
    let code_hash = snap.basic(CHAIN_STATE).unwrap().map(|(_, _, hash)| hash);
    let words = SEALER_FEATURES
        .iter()
        .map(|id| snap.storage(CHAIN_STATE, activation_slot(*id)).unwrap())
        .collect();
    (code_hash, words)
}

/// A chain without `genesis_features` keeps the genesis state root and the
/// genesis digest that its nodes and checkpoints already hold. A change of
/// either one forks every live chain at genesis.
#[test]
fn a_genesis_without_features_keeps_its_root_and_digest() {
    let pinned = [
        (
            "deploy/cluster/config/genesis/dev.toml",
            b256!("0x0295730d035067499560bdf5a7b5870215e149fa2b5b67ae8d0462208d6c9e01"),
            b256!("0x13fa4ca8ce4001a3d25770d88043337051741845e9da24b2b8afe2c96f85b39f"),
        ),
        (
            "chains/dev.toml",
            b256!("0x6b1a900a7ad02add8f7f37595e189ce3dd3ca213139e609ddb00fd289d4fafbd"),
            b256!("0xb8e333280fd48718e1db69f3a5aef9756b8a57ab6c342725d801a519600fbec4"),
        ),
        (
            "chains/dev-withdrawals.toml",
            b256!("0x965469f8a813677f7149a1f66163ad2baa90e7f6357b587b8536e26f313a5ec8"),
            b256!("0x42a1647ff667f54f90c9420ed2b9ea338a0a7a31ce1c270aadb20dc47cfa6c82"),
        ),
        (
            "chains/dev-interop.toml",
            b256!("0xa9b8d9ce4f85512dccda50bc305372efa79070ee586bb45c8af4da607606218f"),
            b256!("0x80f6e6ce13c503a6691811211222f9bbc88152fc38a0d60c65a52b3a2d9452bd"),
        ),
    ];
    let got: Vec<_> = pinned
        .iter()
        .map(|(path, _, _)| {
            let genesis = genesis(path);
            assert_eq!(genesis.genesis_features, GenesisFeatures::None, "{path}");
            let (_dir, _env, root) = replayed(&genesis, Vec::new());
            (
                *path,
                root,
                kardamom_state::genesis_digest(&genesis.to_alloc()),
            )
        })
        .collect();
    assert_eq!(got, pinned);
}

/// A carry genesis holds what three upgrade transactions write on a chain
/// without the features: the `KardamomChainState` code, and for each
/// feature the word of `setFeature(feature, GENESIS_ACTIVATION_MS)`.
#[test]
fn a_carry_genesis_holds_the_words_of_the_upgrade_transactions() {
    let plain = genesis("deploy/cluster/config/genesis/dev.toml");
    let l1_hash = keccak256(1u64.to_be_bytes());
    let logs: Vec<LockboxLog> = SEALER_FEATURES
        .iter()
        .zip(0u64..)
        .map(|(feature, log_index)| {
            LockboxLog::Upgrade(UpgradeLog {
                block_number: 1,
                block_hash: l1_hash,
                log_index,
                feature_id: U256::from(*feature),
                activation_timestamp: GENESIS_ACTIVATION_MS,
            })
        })
        .collect();
    let upgraded = ReplayBlock {
        block_number: 1,
        l2_timestamp: 1_700_000_000_000,
        canonical_end: None,
        l1_epochs: vec![derive_epoch(1, l1_hash, &logs).unwrap()],
        remote_epochs: Vec::new(),
        txs: Vec::new(),
    };
    let (_d1, upgraded_env, _) = replayed(&plain, vec![upgraded]);

    let carry = Genesis {
        genesis_features: GenesisFeatures::CarryWindow,
        ..plain.clone()
    };
    let (_d2, carry_env, carry_root) = replayed(&carry, Vec::new());

    let words = vec![U256::from(GENESIS_ACTIVATION_MS); SEALER_FEATURES.len()];
    let (code_hash, upgraded_words) = chain_state(&upgraded_env);
    assert_eq!(
        upgraded_words, words,
        "the upgrade transactions write the words"
    );
    assert_eq!(chain_state(&carry_env), (code_hash, words));
    let (_d3, _env, plain_root) = replayed(&plain, Vec::new());
    assert_ne!(carry_root, plain_root);
}
