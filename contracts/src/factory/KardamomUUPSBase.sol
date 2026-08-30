// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

/// The base contract for all Kardamom L1 app implementations.
/// It allows a UUPS upgrade only when `msg.sender == FACTORY`. FACTORY is
/// the deterministic Kardamom factory proxy address, computed from the
/// ERC-7955 CREATE2 factory, fixed salts, and the canonical dev owner. See
/// `crates/deployer/src/addresses.rs` and the `factory_address_sync` test.
abstract contract KardamomUUPSBase is Initializable, UUPSUpgradeable {
    /// The Kardamom factory proxy address. It must match the address that
    /// `kardamom_deployer::addresses::factory_proxy_address(...)` computes.
    /// A cross-check test asserts this; if either value drifts, the test fails.
    address internal constant FACTORY = 0x2e4925D28F5F52086ff20aAd4981D68B1C87676E;

    error NotFactory();

    function _authorizeUpgrade(address) internal view override {
        if (msg.sender != FACTORY) revert NotFactory();
    }
}
