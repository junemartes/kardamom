// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {KardamomFactoryV1} from "../../src/factory/KardamomFactoryV1.sol";
import {IKardamomFactory} from "../../src/factory/IKardamomFactory.sol";

/// Minimal UUPS-upgradeable app impl used as a CREATE2 target in factory tests.
/// Must extend UUPSUpgradeable so that upgradeToAndCall dispatched through the proxy works.
contract DummyImpl is Initializable, UUPSUpgradeable {
    uint256 public x;

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(uint256 _x) external initializer {
        x = _x;
    }

    function _authorizeUpgrade(address) internal override {}
}

contract KardamomFactoryV1Test is Test {
    KardamomFactoryV1 factory;
    address owner = address(0xBEEF);

    bytes32 constant ID_DUMMY = keccak256("dummy");
    uint256 constant L2_A = 42;
    uint256 constant L2_B = 43;

    function setUp() public {
        KardamomFactoryV1 impl = new KardamomFactoryV1();
        bytes memory initData = abi.encodeWithSignature("initialize(address)", owner);
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        factory = KardamomFactoryV1(address(proxy));
    }

    function _deploySpec(uint256 l2ChainId, bytes32 id, uint64 version)
        internal
        pure
        returns (IKardamomFactory.DeploymentSpec memory)
    {
        bytes memory initcode = type(DummyImpl).creationCode;
        bytes32 implSalt = keccak256(abi.encode(id, "impl", version));
        bytes memory initData = abi.encodeWithSignature("initialize(uint256)", uint256(42));
        return IKardamomFactory.DeploymentSpec({
            l2ChainId: l2ChainId,
            id: id,
            action: IKardamomFactory.Action.Deploy,
            implInitcode: initcode,
            initData: initData,
            implSalt: implSalt,
            targetImpl: address(0)
        });
    }

    function _upgradeSpec(uint256 l2ChainId, bytes32 id, uint64 version, address targetImpl)
        internal
        pure
        returns (IKardamomFactory.DeploymentSpec memory)
    {
        bytes memory initcode = type(DummyImpl).creationCode;
        bytes32 implSalt = keccak256(abi.encode(id, "impl", version));
        return IKardamomFactory.DeploymentSpec({
            l2ChainId: l2ChainId,
            id: id,
            action: IKardamomFactory.Action.Upgrade,
            implInitcode: initcode,
            initData: "",
            implSalt: implSalt,
            targetImpl: targetImpl
        });
    }

    function test_owner_set_from_initialize_arg() public view {
        assertEq(factory.owner(), owner);
    }

    function test_initialize_only_once() public {
        vm.expectRevert();
        factory.initialize(address(0xCAFE));
    }

    function test_applyDeployments_deploys_and_registers() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](1);
        specs[0] = _deploySpec(L2_A, ID_DUMMY, 1);

        vm.prank(owner);
        factory.applyDeployments(specs);

        IKardamomFactory.Entry memory e = factory.entry(L2_A, ID_DUMMY);
        assertTrue(e.exists);
        assertEq(e.version, 1);
        assertGt(e.proxy.code.length, 0);
        assertGt(e.currentImpl.code.length, 0);
        assertEq(factory.l2ChainIdCount(), 1);
        assertEq(factory.l2ChainIdAt(0), L2_A);
        assertEq(factory.idCount(L2_A), 1);
        assertEq(factory.idAt(L2_A, 0), ID_DUMMY);
        assertEq(DummyImpl(e.proxy).x(), 42);
    }

    function test_two_l2s_same_id_get_distinct_proxies() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](2);
        specs[0] = _deploySpec(L2_A, ID_DUMMY, 1);
        specs[1] = _deploySpec(L2_B, ID_DUMMY, 1);
        // Second spec must NOT re-CREATE2 the same impl — share it via targetImpl.
        // Predict the impl address using the factory's helper.
        address sharedImpl = factory.predictImplAddress(specs[0].implSalt, specs[0].implInitcode);
        specs[1].targetImpl = sharedImpl;

        vm.prank(owner);
        factory.applyDeployments(specs);

        IKardamomFactory.Entry memory ea = factory.entry(L2_A, ID_DUMMY);
        IKardamomFactory.Entry memory eb = factory.entry(L2_B, ID_DUMMY);
        assertTrue(ea.exists);
        assertTrue(eb.exists);
        assertEq(ea.currentImpl, eb.currentImpl, "impl must be shared");
        assertEq(ea.currentImpl, sharedImpl, "impl must equal predicted shared address");
        assertTrue(ea.proxy != eb.proxy, "proxies must differ");
        assertEq(factory.l2ChainIdCount(), 2);
        assertEq(factory.idCount(L2_A), 1);
        assertEq(factory.idCount(L2_B), 1);
    }

    function test_targetImpl_with_no_code_reverts() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](1);
        specs[0] = _deploySpec(L2_A, ID_DUMMY, 1);
        // Bogus target (no code).
        specs[0].targetImpl = address(0xDEAD);

        vm.expectRevert(
            abi.encodeWithSelector(IKardamomFactory.ImplNotDeployed.selector, address(0xDEAD))
        );
        vm.prank(owner);
        factory.applyDeployments(specs);
    }

    function test_atomic_two_l2_upgrade_with_shared_impl() public {
        // Seed: deploy on L2_A and L2_B sharing v1 impl.
        IKardamomFactory.DeploymentSpec[] memory s1 = new IKardamomFactory.DeploymentSpec[](2);
        s1[0] = _deploySpec(L2_A, ID_DUMMY, 1);
        s1[1] = _deploySpec(L2_B, ID_DUMMY, 1);
        s1[1].targetImpl = factory.predictImplAddress(s1[0].implSalt, s1[0].implInitcode);
        vm.prank(owner);
        factory.applyDeployments(s1);

        // Upgrade both L2s to v2 with one shared impl.
        IKardamomFactory.DeploymentSpec[] memory s2 = new IKardamomFactory.DeploymentSpec[](2);
        s2[0] = _upgradeSpec(L2_A, ID_DUMMY, 2, address(0));
        // Predict the new shared impl, then reference it from the second spec.
        address newImpl = factory.predictImplAddress(s2[0].implSalt, s2[0].implInitcode);
        s2[1] = _upgradeSpec(L2_B, ID_DUMMY, 2, newImpl);

        vm.prank(owner);
        factory.applyDeployments(s2);

        IKardamomFactory.Entry memory ea = factory.entry(L2_A, ID_DUMMY);
        IKardamomFactory.Entry memory eb = factory.entry(L2_B, ID_DUMMY);
        assertEq(ea.version, 2);
        assertEq(eb.version, 2);
        assertEq(ea.currentImpl, eb.currentImpl, "upgrade impl must be shared");
        assertEq(ea.currentImpl, newImpl);
    }

    function test_deploying_same_id_twice_reverts() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](1);
        specs[0] = _deploySpec(L2_A, ID_DUMMY, 1);

        vm.prank(owner);
        factory.applyDeployments(specs);

        vm.expectRevert(
            abi.encodeWithSelector(IKardamomFactory.AlreadyDeployed.selector, L2_A, ID_DUMMY)
        );
        vm.prank(owner);
        factory.applyDeployments(specs);
    }

    function test_non_owner_cannot_apply() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](1);
        specs[0] = _deploySpec(L2_A, ID_DUMMY, 1);

        vm.expectRevert();
        factory.applyDeployments(specs);
    }

    function test_unregistered_upgrade_reverts() public {
        bytes32 ID_UNREG = keccak256("unregistered");
        IKardamomFactory.DeploymentSpec[] memory upgradeSpecs =
            new IKardamomFactory.DeploymentSpec[](1);
        upgradeSpecs[0] = _upgradeSpec(L2_A, ID_UNREG, 1, address(0));

        vm.expectRevert(
            abi.encodeWithSelector(IKardamomFactory.NotRegistered.selector, L2_A, ID_UNREG)
        );
        vm.prank(owner);
        factory.applyDeployments(upgradeSpecs);
    }

    function test_batch_atomicity_on_revert() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](2);
        specs[0] = _deploySpec(L2_A, ID_DUMMY, 1);
        specs[1] = _deploySpec(L2_A, ID_DUMMY, 2); // same (l2, id) — must revert

        vm.expectRevert(
            abi.encodeWithSelector(IKardamomFactory.AlreadyDeployed.selector, L2_A, ID_DUMMY)
        );
        vm.prank(owner);
        factory.applyDeployments(specs);

        assertEq(factory.l2ChainIdCount(), 0);
        assertEq(factory.idCount(L2_A), 0);
    }
}
