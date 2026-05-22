// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

/// Base contract for all kardamom L1 app implementations.
/// Gates UUPS upgrades on `msg.sender == FACTORY`, where FACTORY is the deterministic
/// kardamom factory proxy address (computed via ERC-7955 CREATE2 factory + fixed
/// salts + canonical dev owner; see crates/deployer/src/addresses.rs and the
/// factory_address_sync test).
abstract contract KardamomUUPSBase is Initializable, UUPSUpgradeable {
    /// Kardamom factory proxy address. Must match the address computed by
    /// `kardamom_deployer::addresses::factory_proxy_address(...)`. A cross-check
    /// test asserts this; if either drifts, the test fails.
    address internal constant FACTORY = 0x5044E450D9115c4b8D7e2AD792c730Fb92d13d37;

    error NotFactory();

    function _authorizeUpgrade(address) internal view override {
        if (msg.sender != FACTORY) revert NotFactory();
    }
}
