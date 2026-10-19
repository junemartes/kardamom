// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

/// The base contract for all Kardamom L1 app implementations.
/// It allows a UUPS upgrade only when `msg.sender == FACTORY`. FACTORY is
/// the Kardamom factory proxy that deploys the implementation. The deployer
/// appends `abi.encode(factory)` to the creation code, so each owner's
/// factory builds implementations that only that factory can upgrade.
abstract contract KardamomUUPSBase is Initializable, UUPSUpgradeable {
    /// The Kardamom factory proxy that has the upgrade authority. The
    /// constructor sets it. It is part of the implementation code, so a
    /// proxy reads the value of its current implementation.
    /// @custom:oz-upgrades-unsafe-allow state-variable-immutable
    address internal immutable FACTORY;

    error NotFactory();

    constructor(address factory_) {
        FACTORY = factory_;
    }

    /// @notice The Kardamom factory proxy that can upgrade this contract
    ///         and call its factory-only setters. The deployer reads it
    ///         after each deploy and each upgrade, and it compares it with
    ///         the factory of its owner.
    function factory() external view returns (address) {
        return FACTORY;
    }

    function _authorizeUpgrade(address) internal view override {
        if (msg.sender != FACTORY) revert NotFactory();
    }
}
