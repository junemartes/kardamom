// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

/// Base contract for all kardamom L1 app implementations.
/// Gates UUPS upgrades on `msg.sender == FACTORY`, where FACTORY is the deterministic
/// kardamom factory proxy address (computed via Arachnid SingletonFactory + fixed
/// salts; see crates/deployer/src/addresses.rs).
abstract contract KardamomUUPSBase is Initializable, UUPSUpgradeable {
    /// Kardamom factory proxy address. Must match the address computed by
    /// `kardamom_deployer::addresses::factory_proxy_address(...)`. A cross-check
    /// test asserts this; if either drifts, the test fails.
    address internal constant FACTORY = 0xA082604471549EfeCA411Bf2555Ed10d09FCec27;

    error NotFactory();

    function _authorizeUpgrade(address) internal view override {
        if (msg.sender != FACTORY) revert NotFactory();
    }
}
