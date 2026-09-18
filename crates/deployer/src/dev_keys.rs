//! Anvil's deterministic dev accounts, and the workspace's own dev
//! addresses, in one place for e2e and integration tests. Anvil derives
//! these accounts from its standard test mnemonic; the keys are public and
//! for dev use only.

use alloy_primitives::{Address, address};

/// The kardamom factory owner. Anvil impersonates this account instead of
/// signing with its key.
pub const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
/// A placeholder L2 minter, for fixtures that do not exercise
/// minter-gated calls.
pub const L2_MINTER: Address = address!("00000000000000000000000000000000000000BE");

/// Anvil dev account #0: the withdrawal oracle's attester.
pub const ATTESTER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ATTESTER_ADDR: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// Anvil dev account #1: the withdrawal oracle's challenger.
pub const CHALLENGER_KEY: &str =
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
pub const CHALLENGER_ADDR: Address = address!("70997970C51812dc3A010C7d01b50e0d17dc79C8");
/// Anvil dev account #2: the batcher account. This must be a real, funded
/// key. The DA path sends genuine EIP-4844 blob transactions, which cannot
/// use an impersonated account.
pub const BATCHER_KEY: &str = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a";
pub const BATCHER_ADDR: Address = address!("3C44CdDdB6a900fa2b585dd299e03d12FA4293BC");
