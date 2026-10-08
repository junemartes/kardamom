//! Shared `alloy-sol-types` bindings for the bridge contracts, for e2e and
//! integration tests across the workspace. Each binding is generated from
//! the committed ABI in `contracts/abi`, which `just abi-check` keeps equal
//! to the Solidity source.

use alloy_sol_types::sol;

sol!(
    #[sol(rpc)]
    ETHLockbox,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/abi/ETHLockbox.json"
    )
);

sol!(
    #[sol(rpc)]
    WithdrawalOutputOracle,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/abi/WithdrawalOutputOracle.json"
    )
);
