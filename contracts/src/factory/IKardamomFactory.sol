// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

interface IKardamomFactory {
    enum Action {
        Deploy,
        Upgrade
    }

    /// One deployment or upgrade instruction. `targetImpl` lets multiple
    /// specs in one batch share a single, freshly deployed implementation.
    /// The first spec for an `(id, implSalt)` pair sets
    /// `targetImpl = address(0)`, so the factory runs CREATE2 for the
    /// implementation. Later specs set `targetImpl` to that address, to
    /// skip the redundant CREATE2 call.
    struct DeploymentSpec {
        uint256 l2ChainId;
        bytes32 id;
        Action action;
        bytes implInitcode;
        bytes initData;
        bytes32 implSalt;
        address targetImpl;
    }

    struct Entry {
        address proxy;
        address currentImpl;
        uint64 version;
        uint64 deployedAt;
        uint64 upgradedAt;
        bool exists;
    }

    event Deployed(
        uint256 indexed l2ChainId, bytes32 indexed id, address proxy, address impl, uint64 version
    );
    event Upgraded(
        uint256 indexed l2ChainId,
        bytes32 indexed id,
        address oldImpl,
        address newImpl,
        uint64 version
    );

    error AlreadyDeployed(uint256 l2ChainId, bytes32 id);
    error NotRegistered(uint256 l2ChainId, bytes32 id);
    error Create2Failed();
    error ImplCollision(address impl);
    error ImplNotDeployed(address targetImpl);
    error UnknownAction();

    function applyDeployments(DeploymentSpec[] calldata specs) external;
    function entry(uint256 l2ChainId, bytes32 id) external view returns (Entry memory);

    function l2ChainIdCount() external view returns (uint256);
    function l2ChainIdAt(uint256 i) external view returns (uint256);
    function idCount(uint256 l2ChainId) external view returns (uint256);
    function idAt(uint256 l2ChainId, uint256 i) external view returns (bytes32);

    function predictProxyAddress(uint256 l2ChainId, bytes32 id) external view returns (address);
    function predictImplAddress(bytes32 implSalt, bytes calldata implInitcode)
        external
        view
        returns (address);
}
