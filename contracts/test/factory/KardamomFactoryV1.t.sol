// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {KardamomFactoryV1} from "../../src/factory/KardamomFactoryV1.sol";
import {IKardamomFactory} from "../../src/factory/IKardamomFactory.sol";

/// Minimal app impl used as a CREATE2 target in factory tests.
contract DummyImpl {
    uint256 public x;

    function initialize(uint256 _x) external {
        x = _x;
    }
}

contract KardamomFactoryV1Test is Test {
    KardamomFactoryV1 factory;
    address owner = address(0xBEEF);

    bytes32 constant ID_DUMMY = keccak256("dummy");

    function setUp() public {
        // Deploy factory directly (not via SingletonFactory) for these unit tests; the
        // SingletonFactory path is exercised in the Rust e2e.
        vm.prank(owner, owner); // make tx.origin == owner
        KardamomFactoryV1 impl = new KardamomFactoryV1();
        // KardamomFactoryV1 constructor calls _disableInitializers, so the impl itself
        // cannot be used — wrap in an ERC1967Proxy.
        bytes memory initData = abi.encodeWithSignature("initialize()");
        vm.prank(owner, owner);
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        factory = KardamomFactoryV1(address(proxy));
    }

    function test_owner_is_tx_origin() public view {
        assertEq(factory.owner(), owner);
    }

    function test_initialize_only_once() public {
        vm.expectRevert();
        factory.initialize();
    }

    function _dummySpec(bytes32 id, uint64 version)
        internal
        pure
        returns (IKardamomFactory.DeploymentSpec memory)
    {
        bytes memory initcode = type(DummyImpl).creationCode;
        bytes32 implSalt = keccak256(abi.encode(id, "impl", version));
        bytes memory initData = abi.encodeWithSignature("initialize(uint256)", uint256(42));
        return IKardamomFactory.DeploymentSpec({
            id: id,
            action: IKardamomFactory.Action.Deploy,
            implInitcode: initcode,
            initData: initData,
            implSalt: implSalt
        });
    }

    function test_applyDeployments_deploys_and_registers() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](1);
        specs[0] = _dummySpec(ID_DUMMY, 1);

        vm.prank(owner);
        factory.applyDeployments(specs);

        IKardamomFactory.Entry memory e = factory.entry(ID_DUMMY);
        assertTrue(e.exists);
        assertEq(e.version, 1);
        assertEq(e.proxy.code.length > 0, true);
        assertEq(e.currentImpl.code.length > 0, true);
        assertEq(factory.idCount(), 1);
        assertEq(factory.idAt(0), ID_DUMMY);
        // Proxy actually delegates to DummyImpl initialized with x=42.
        assertEq(DummyImpl(e.proxy).x(), 42);
    }

    function test_deploying_same_id_twice_reverts() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](1);
        specs[0] = _dummySpec(ID_DUMMY, 1);

        vm.prank(owner);
        factory.applyDeployments(specs);

        vm.expectRevert(abi.encodeWithSelector(IKardamomFactory.AlreadyDeployed.selector, ID_DUMMY));
        vm.prank(owner);
        factory.applyDeployments(specs);
    }

    function test_non_owner_cannot_apply() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](1);
        specs[0] = _dummySpec(ID_DUMMY, 1);

        vm.expectRevert();
        factory.applyDeployments(specs);
    }

    function test_unregistered_upgrade_reverts() public {
        bytes32 ID_UNREG = keccak256("unregistered");
        IKardamomFactory.DeploymentSpec[] memory upgradeSpecs =
            new IKardamomFactory.DeploymentSpec[](1);
        upgradeSpecs[0] = IKardamomFactory.DeploymentSpec({
            id: ID_UNREG,
            action: IKardamomFactory.Action.Upgrade,
            implInitcode: type(DummyImpl).creationCode,
            initData: "",
            implSalt: keccak256(abi.encode(ID_UNREG, "impl", uint64(1)))
        });

        vm.expectRevert(abi.encodeWithSelector(IKardamomFactory.NotRegistered.selector, ID_UNREG));
        vm.prank(owner);
        factory.applyDeployments(upgradeSpecs);
    }

    function test_batch_atomicity_on_revert() public {
        IKardamomFactory.DeploymentSpec[] memory specs = new IKardamomFactory.DeploymentSpec[](2);
        specs[0] = _dummySpec(ID_DUMMY, 1);
        // Second spec: deploy under same id — must revert, undoing the first.
        specs[1] = _dummySpec(ID_DUMMY, 2);

        vm.expectRevert(abi.encodeWithSelector(IKardamomFactory.AlreadyDeployed.selector, ID_DUMMY));
        vm.prank(owner);
        factory.applyDeployments(specs);

        // Atomicity: idCount stays 0 even though spec[0] would have succeeded alone.
        assertEq(factory.idCount(), 0);
    }
}
