// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

interface IKardamomFactory {
    enum Action {
        Deploy,
        Upgrade
    }

    struct DeploymentSpec {
        bytes32 id;
        Action action;
        bytes implInitcode;
        bytes initData;
        bytes32 implSalt;
    }

    struct Entry {
        address proxy;
        address currentImpl;
        uint64 version;
        uint64 deployedAt;
        uint64 upgradedAt;
        bool exists;
    }

    event Deployed(bytes32 indexed id, address proxy, address impl, uint64 version);
    event Upgraded(bytes32 indexed id, address oldImpl, address newImpl, uint64 version);

    error AlreadyDeployed(bytes32 id);
    error NotRegistered(bytes32 id);
    error Create2Failed();
    error ImplCollision(address impl);
    error UnknownAction();

    function applyDeployments(DeploymentSpec[] calldata specs) external;
    function entry(bytes32 id) external view returns (Entry memory);
    function idCount() external view returns (uint256);
    function idAt(uint256 i) external view returns (bytes32);
    function predictProxyAddress(bytes32 id) external view returns (address);
    function predictImplAddress(bytes32 implSalt, bytes calldata implInitcode)
        external
        view
        returns (address);
}
