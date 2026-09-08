//! The bridge contract bindings, and the anvil dev accounts the L1
//! harness signs (or impersonates) with. The dev accounts and the
//! `ETHLockbox`/`WithdrawalOutputOracle` bindings come from
//! `kardamom_deployer`, the one place the workspace declares them. This
//! module adds only the `L2ToL1MessagePasser` binding, which has no
//! production Rust binding elsewhere. All behavior lives in the parent
//! module, which re-exports everything here.

use alloy_sol_types::sol;
pub use kardamom_deployer::abi::{ETHLockbox, WithdrawalOutputOracle};
pub use kardamom_deployer::dev_keys::{
    ATTESTER_ADDR, ATTESTER_KEY, BATCHER_ADDR, BATCHER_KEY, CHALLENGER_ADDR as DEPOSITOR_ADDR,
    CHALLENGER_KEY as DEPOSITOR_KEY, DEV_OWNER, L2_MINTER,
};

sol! {
    /// The L2 predeploy at `kardamom_types::withdrawals::MESSAGE_PASSER`.
    /// No Rust binding for it exists in the workspace, so this declares
    /// one here.
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
