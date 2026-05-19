// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import {
    Ownable2StepUpgradeable
} from "@openzeppelin/contracts-upgradeable/access/Ownable2StepUpgradeable.sol";
import {
    OwnableUpgradeable
} from "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

import {IKardamomFactory} from "./IKardamomFactory.sol";

/// @notice UUPS-upgradeable contract registry and CREATE2 deployer.
/// Bootstrap pattern: deployed through the Arachnid SingletonFactory so its proxy
/// address is deterministic across chains. Its `initialize()` takes no arguments and
/// sets the owner to `tx.origin`, which must be the bootstrap EOA.
contract KardamomFactoryV1 is
    IKardamomFactory,
    Initializable,
    UUPSUpgradeable,
    Ownable2StepUpgradeable
{
    bytes32[] public ids;
    mapping(bytes32 => Entry) private _registry;

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    /// @notice Bootstrap initializer. Sets the operator to `tx.origin`, which must be
    /// the EOA that called the SingletonFactory. Encoded into proxy initData as a
    /// constant `abi.encodeWithSignature("initialize()")`, so the proxy address does
    /// not depend on which operator deploys.
    function initialize() external initializer {
        __UUPSUpgradeable_init();
        __Ownable_init(tx.origin);
        __Ownable2Step_init();
    }

    function _authorizeUpgrade(address) internal override onlyOwner {}

    // ---------- IKardamomFactory ----------

    function applyDeployments(DeploymentSpec[] calldata specs) external onlyOwner {
        uint256 n = specs.length;
        for (uint256 i = 0; i < n; i++) {
            DeploymentSpec calldata s = specs[i];
            if (s.action == Action.Deploy) {
                _deployUUPS(s);
            } else if (s.action == Action.Upgrade) {
                _upgradeUUPS(s);
            } else {
                revert UnknownAction();
            }
        }
    }

    function entry(bytes32 id) external view returns (Entry memory) {
        return _registry[id];
    }

    function idCount() external view returns (uint256) {
        return ids.length;
    }

    function idAt(uint256 i) external view returns (bytes32) {
        return ids[i];
    }

    function predictProxyAddress(bytes32 id) external view returns (address) {
        Entry storage e = _registry[id];
        return e.exists ? e.proxy : address(0);
    }

    function predictImplAddress(bytes32 implSalt, bytes calldata implInitcode)
        external
        view
        returns (address)
    {
        return _create2Address(implSalt, keccak256(implInitcode));
    }

    // ---------- internals ----------

    function _deployUUPS(DeploymentSpec calldata s) internal {
        if (_registry[s.id].exists) revert AlreadyDeployed(s.id);

        address impl = _create2(s.implInitcode, s.implSalt);
        if (impl.code.length == 0) revert Create2Failed();

        bytes memory proxyInitcode =
            abi.encodePacked(type(ERC1967Proxy).creationCode, abi.encode(impl, s.initData));
        bytes32 proxySalt = keccak256(abi.encode(s.id, "proxy"));
        address proxy = _create2(proxyInitcode, proxySalt);
        if (proxy.code.length == 0) revert Create2Failed();

        ids.push(s.id);
        _registry[s.id] = Entry({
            proxy: proxy,
            currentImpl: impl,
            version: 1,
            deployedAt: uint64(block.number),
            upgradedAt: uint64(block.number),
            exists: true
        });

        emit Deployed(s.id, proxy, impl, 1);
    }

    function _upgradeUUPS(DeploymentSpec calldata s) internal {
        Entry storage e = _registry[s.id];
        if (!e.exists) revert NotRegistered(s.id);

        address newImpl = _create2(s.implInitcode, s.implSalt);
        if (newImpl.code.length == 0) revert Create2Failed();
        if (newImpl == e.currentImpl) revert ImplCollision(newImpl);

        address oldImpl = e.currentImpl;
        UUPSUpgradeable(e.proxy).upgradeToAndCall(newImpl, s.initData);

        e.currentImpl = newImpl;
        e.version += 1;
        e.upgradedAt = uint64(block.number);

        emit Upgraded(s.id, oldImpl, newImpl, e.version);
    }

    function _create2(bytes memory initcode, bytes32 salt) internal returns (address addr) {
        assembly {
            addr := create2(0, add(initcode, 0x20), mload(initcode), salt)
        }
        if (addr == address(0)) revert Create2Failed();
    }

    function _create2Address(bytes32 salt, bytes32 initcodeHash) internal view returns (address) {
        return address(
            uint160(
                uint256(
                    keccak256(abi.encodePacked(bytes1(0xff), address(this), salt, initcodeHash))
                )
            )
        );
    }
}
