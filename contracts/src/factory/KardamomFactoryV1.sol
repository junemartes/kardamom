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
/// Bootstrap pattern: deployed through ERC-7955's permissionless CREATE2 factory so the
/// proxy address is the same across L1s for a given (compiled bytecode, owner) pair.
/// The proxy's initcode embeds `initialize(address owner)`, so the canonical address
/// depends on the owner — different envs (mainnet/testnet/dev) use different owners
/// and naturally land at different L1 addresses.
contract KardamomFactoryV1 is
    IKardamomFactory,
    Initializable,
    UUPSUpgradeable,
    Ownable2StepUpgradeable
{
    // Per-(l2ChainId) discovery.
    uint256[] public l2ChainIds;
    mapping(uint256 => bool) private _l2ChainIdSeen;

    // Per-(l2ChainId, contractId) registry.
    mapping(uint256 => bytes32[]) private _idsByChainId;
    mapping(uint256 => mapping(bytes32 => Entry)) private _registry;

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    /// @notice Bootstrap initializer. Owner is passed in; the bootstrap caller (whoever
    /// runs ERC-7955.deploy with this initcode) does not need to be the owner.
    function initialize(address owner) external initializer {
        __Ownable_init(owner);
        __Ownable2Step_init();
    }

    function _authorizeUpgrade(address) internal override onlyOwner {}

    // ---------- IKardamomFactory ----------

    /// @notice Process specs sequentially. Any single failure reverts the entire batch
    /// (standard Solidity transaction semantics — there is no partial-success state).
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

    function entry(uint256 l2ChainId, bytes32 id) external view returns (Entry memory) {
        return _registry[l2ChainId][id];
    }

    function l2ChainIdCount() external view returns (uint256) {
        return l2ChainIds.length;
    }

    function l2ChainIdAt(uint256 i) external view returns (uint256) {
        return l2ChainIds[i];
    }

    function idCount(uint256 l2ChainId) external view returns (uint256) {
        return _idsByChainId[l2ChainId].length;
    }

    function idAt(uint256 l2ChainId, uint256 i) external view returns (bytes32) {
        return _idsByChainId[l2ChainId][i];
    }

    function predictProxyAddress(uint256 l2ChainId, bytes32 id) external view returns (address) {
        Entry storage e = _registry[l2ChainId][id];
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
        if (_registry[s.l2ChainId][s.id].exists) revert AlreadyDeployed(s.l2ChainId, s.id);

        address impl = _resolveImpl(s);

        bytes memory proxyInitcode =
            abi.encodePacked(type(ERC1967Proxy).creationCode, abi.encode(impl, s.initData));
        bytes32 proxySalt = keccak256(abi.encode(s.l2ChainId, s.id, "proxy"));
        address proxy = _create2(proxyInitcode, proxySalt);
        if (proxy.code.length == 0) revert Create2Failed();

        if (!_l2ChainIdSeen[s.l2ChainId]) {
            _l2ChainIdSeen[s.l2ChainId] = true;
            l2ChainIds.push(s.l2ChainId);
        }
        _idsByChainId[s.l2ChainId].push(s.id);
        _registry[s.l2ChainId][s.id] = Entry({
            proxy: proxy,
            currentImpl: impl,
            version: 1,
            deployedAt: uint64(block.number),
            upgradedAt: uint64(block.number),
            exists: true
        });

        emit Deployed(s.l2ChainId, s.id, proxy, impl, 1);
    }

    function _upgradeUUPS(DeploymentSpec calldata s) internal {
        Entry storage e = _registry[s.l2ChainId][s.id];
        if (!e.exists) revert NotRegistered(s.l2ChainId, s.id);

        address newImpl = _resolveImpl(s);
        if (newImpl == e.currentImpl) revert ImplCollision(newImpl);

        address oldImpl = e.currentImpl;
        UUPSUpgradeable(e.proxy).upgradeToAndCall(newImpl, s.initData);

        e.currentImpl = newImpl;
        e.version += 1;
        e.upgradedAt = uint64(block.number);

        emit Upgraded(s.l2ChainId, s.id, oldImpl, newImpl, e.version);
    }

    /// Resolve the impl: reuse `s.targetImpl` if non-zero (verified to have code),
    /// otherwise CREATE2 from `s.implInitcode` + `s.implSalt`.
    function _resolveImpl(DeploymentSpec calldata s) internal returns (address impl) {
        if (s.targetImpl != address(0)) {
            if (s.targetImpl.code.length == 0) revert ImplNotDeployed(s.targetImpl);
            impl = s.targetImpl;
            return impl;
        }
        impl = _create2(s.implInitcode, s.implSalt);
        // Defense-in-depth: _create2 already reverts on addr==0, but a constructor
        // that returns no runtime code (e.g. SELFDESTRUCT in constructor) would still
        // produce a non-zero address with empty code.
        if (impl.code.length == 0) revert Create2Failed();
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
