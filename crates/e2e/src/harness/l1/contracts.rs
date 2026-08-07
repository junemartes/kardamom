//! The bridge contract bindings and the anvil dev accounts the L1 harness
//! signs (or impersonates) with. Pure declarations — all behaviour lives in
//! the parent module, which re-exports everything here.

use alloy_primitives::{Address, address};
use alloy_sol_types::sol;

/// Factory owner; impersonated on anvil rather than key-signed.
pub const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
/// The L2 minter authorized on the lockbox (unused by these scenarios, which
/// exercise the deposit/withdraw paths rather than minter-gated calls).
pub const L2_MINTER: Address = address!("00000000000000000000000000000000000000BE");

/// Anvil dev account #0 — the oracle's attester (and the account prefunded by
/// `chains/dev-withdrawals.toml` on L2).
pub const ATTESTER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ATTESTER_ADDR: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// Anvil dev account #1 — the oracle's challenger, and the EOA these
/// scenarios deposit from.
pub const DEPOSITOR_KEY: &str =
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
pub const DEPOSITOR_ADDR: Address = address!("70997970C51812dc3A010C7d01b50e0d17dc79C8");
/// Anvil dev account #2 — the batcher EOA. It must be a REAL funded key: the
/// DA path sends genuine EIP-4844 blob transactions, which cannot be
/// impersonated.
pub const BATCHER_KEY: &str = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a";
pub const BATCHER_ADDR: Address = address!("3C44CdDdB6a900fa2b585dd299e03d12FA4293BC");

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        struct WithdrawalTransaction {
            uint256 nonce;
            address sender;
            address target;
            uint256 value;
        }
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
        function outputOracle() external view returns (address);
        function finalizeWithdrawal(
            WithdrawalTransaction calldata wtx,
            uint256 outputIndex,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] calldata proof
        ) external;
        event DepositInitiated(
            uint64 indexed depositNonce,
            address indexed from,
            address indexed to,
            uint256 mint,
            uint64 gasLimit,
            bytes data
        );
    }

    #[sol(rpc)]
    contract WithdrawalOutputOracle {
        function outputCount() external view returns (uint256);
        function outputRootAt(uint256 index) external view returns (bytes32);
        function isFinalizable(uint256 index) external view returns (bool);
    }

    /// The L2 predeploy at `kardamom_types::withdrawals::MESSAGE_PASSER`.
    /// No Rust binding exists in the workspace, so declare one here.
    #[sol(rpc)]
    contract L2ToL1MessagePasser {
        function initiateWithdrawal(address target) external payable;
        event MessagePassed(
            uint256 indexed nonce,
            address indexed sender,
            address indexed target,
            uint256 value,
            bytes32 withdrawalHash
        );
    }
}
