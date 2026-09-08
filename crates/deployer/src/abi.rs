//! Shared `alloy-sol-types` bindings for the bridge contracts, for e2e and
//! integration tests across the workspace. One binding here, instead of one
//! per test file, keeps every caller's ABI in sync with the Solidity
//! source.

use alloy_sol_types::sol;

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
        /// The upgrade transaction. Only the factory owner may call this.
        /// `activationTimestamp` is in epoch milliseconds; 0 means
        /// immediately.
        function initiateUpgrade(uint256 featureId, uint64 activationTimestamp) external;
        function upgradeNonce() external view returns (uint64);
        event UpgradeInitiated(
            uint64 indexed upgradeNonce,
            uint256 indexed featureId,
            uint64 activationTimestamp
        );
    }

    #[sol(rpc)]
    contract WithdrawalOutputOracle {
        function deleteOutput(uint256 index) external;
        function outputCount() external view returns (uint256);
        function outputRootAt(uint256 index) external view returns (bytes32);
        function isFinalizable(uint256 index) external view returns (bool);
    }
}
