// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {ETHLockbox} from "../../src/L1/ETHLockbox.sol";
import {IWithdrawalOutputOracle} from "../../src/L1/ETHLockbox.sol";
import {KardamomUUPSBase} from "../../src/factory/KardamomUUPSBase.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

/// Minimal oracle double: one settable output slot.
contract MockOracle is IWithdrawalOutputOracle {
    mapping(uint256 => bytes32) public roots;
    mapping(uint256 => bool) public finalizable;

    function set(uint256 index, bytes32 root, bool isFinal) external {
        roots[index] = root;
        finalizable[index] = isFinal;
    }

    function outputRootAt(uint256 index) external view returns (bytes32) {
        return roots[index];
    }

    function isFinalizable(uint256 index) external view returns (bool) {
        return finalizable[index];
    }

    uint64 public finalizationWindow = 1 days;

    function setFinalizationWindow(uint64 w) external {
        finalizationWindow = w;
    }
}

contract ETHLockboxTest is Test {
    ETHLockbox lockbox;
    MockOracle oracle;
    address constant L2_MINTER = address(0xBEEF);

    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );
    event WithdrawalFinalized(
        bytes32 indexed withdrawalHash, address indexed target, uint256 value
    );

    function setUp() public {
        oracle = new MockOracle();
        ETHLockbox impl = new ETHLockbox();
        bytes memory initData =
            abi.encodeWithSelector(ETHLockbox.initialize.selector, L2_MINTER, address(oracle));
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        lockbox = ETHLockbox(payable(address(proxy)));
    }

    function test_initializer_stores_config() public view {
        assertEq(lockbox.l2Minter(), L2_MINTER);
        assertEq(lockbox.outputOracle(), address(oracle));
    }

    function test_impl_cannot_be_initialized_directly() public {
        ETHLockbox impl = new ETHLockbox();
        vm.expectRevert();
        impl.initialize(L2_MINTER, address(oracle));
    }

    function test_depositETH_increments_balance_and_emits() public {
        address alice = address(0xA11CE);
        vm.deal(alice, 10 ether);
        address to = address(0xB0B);

        vm.expectEmit(true, true, true, true);
        emit DepositInitiated(1, alice, to, 1 ether, 100_000, hex"");

        vm.prank(alice);
        lockbox.depositETH{value: 1 ether}(to, 100_000, hex"");

        assertEq(address(lockbox).balance, 1 ether);
        assertEq(lockbox.depositNonce(), 1);
    }

    function test_receive_reverts() public {
        (bool ok,) = address(lockbox).call{value: 1 ether}("");
        assertFalse(ok);
        assertEq(address(lockbox).balance, 0);
    }

    function test_zero_value_reverts() public {
        vm.expectRevert(ETHLockbox.ZeroDeposit.selector);
        lockbox.depositETH{value: 0}(address(0xB0B), 0, hex"");
    }

    // ---------------------------------------------------------------------
    // Withdrawal off-ramp
    // ---------------------------------------------------------------------

    uint8 constant OUTPUT_VERSION = 0;
    address constant L2_SENDER = address(0x5151);
    address constant L1_TARGET = address(0x7777);

    function _leaf(uint256 nonce, address target, uint256 value) internal pure returns (bytes32) {
        return keccak256(abi.encode(nonce, L2_SENDER, target, value));
    }

    /// Domain-separated tree hashing — mirrors ETHLockbox LEAF_DOMAIN/NODE_DOMAIN
    /// and kardamom_types::withdrawals.
    function _hashLeaf(bytes32 wh) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(bytes1(0x00), wh));
    }

    function _hashNode(bytes32 left, bytes32 right) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(bytes1(0x01), left, right));
    }

    /// Fund the lockbox as if deposits had filled it, and register a 2-leaf
    /// withdrawals tree [leaf0, sibling] whose leaf0 withdraws `value` to target.
    function _setupWithdrawal(uint256 value)
        internal
        returns (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        )
    {
        vm.deal(address(lockbox), 10 ether);
        wtx = ETHLockbox.WithdrawalTransaction({
            nonce: 0, sender: L2_SENDER, target: L1_TARGET, value: value
        });
        bytes32 leaf0 = _leaf(0, L1_TARGET, value);
        bytes32 sibling = _leaf(1, address(0xdead), 3 ether); // some other withdrawal
        // positional domain-separated tree; leaf0 is index 0, proof carries the
        // sibling as a tree node (i.e. already leaf-domain-hashed).
        withdrawalsRoot = _hashNode(_hashLeaf(leaf0), _hashLeaf(sibling));
        leafIndex = 0;
        proof = new bytes32[](1);
        proof[0] = _hashLeaf(sibling);
        stateRoot = keccak256("state-root-block-7");
    }

    function _outputRoot(bytes32 stateRoot, bytes32 withdrawalsRoot)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot));
    }

    function test_finalizeWithdrawal_pays_out() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        bytes32 wh = lockbox.hashWithdrawal(wtx);
        vm.expectEmit(true, true, false, true);
        emit WithdrawalFinalized(wh, L1_TARGET, 1 ether);

        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);

        assertEq(L1_TARGET.balance, 1 ether);
        assertEq(address(lockbox).balance, 9 ether);
        assertTrue(lockbox.finalizedWithdrawals(wh));
    }

    function test_finalizeWithdrawal_replay_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
        vm.expectRevert(ETHLockbox.AlreadyFinalized.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_not_finalizable_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        // window not elapsed / deleted
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), false);

        vm.expectRevert(ETHLockbox.NotFinalizable.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_bad_proof_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        proof[0] = keccak256("wrong-sibling");
        vm.expectRevert(ETHLockbox.BadInclusionProof.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_output_mismatch_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        // Oracle holds a different output root than the revealed preimage.
        oracle.set(0, keccak256("unrelated-output"), true);

        vm.expectRevert(ETHLockbox.OutputRootMismatch.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_single_leaf_tree() public {
        // A range with exactly one withdrawal: root == leaf-domain hash, empty proof.
        vm.deal(address(lockbox), 10 ether);
        ETHLockbox.WithdrawalTransaction memory wtx = ETHLockbox.WithdrawalTransaction({
            nonce: 0, sender: L2_SENDER, target: L1_TARGET, value: 2 ether
        });
        bytes32 withdrawalsRoot = _hashLeaf(_leaf(0, L1_TARGET, 2 ether));
        bytes32 stateRoot = keccak256("sr");
        bytes32[] memory proof = new bytes32[](0);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, 0, proof);
        assertEq(L1_TARGET.balance, 2 ether);
    }

    function test_finalizeWithdrawal_leaf_index_beyond_proof_depth_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        // Index 2 == 0b10: the low bit walks the same left-branch path as index
        // 0, and the high bit used to be silently ignored. It must now revert
        // (leafIndex is bound to the proof depth).
        vm.expectRevert(ETHLockbox.BadInclusionProof.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, 2, proof);
    }

    function test_raw_leaf_cannot_pose_as_internal_node() public {
        // With domain separation, a withdrawals root built WITHOUT the leaf
        // domain (the old format) must not verify: leaves and nodes live in
        // disjoint preimage spaces.
        vm.deal(address(lockbox), 10 ether);
        ETHLockbox.WithdrawalTransaction memory wtx = ETHLockbox.WithdrawalTransaction({
            nonce: 0, sender: L2_SENDER, target: L1_TARGET, value: 1 ether
        });
        bytes32 rawRoot = _leaf(0, L1_TARGET, 1 ether); // undomained single-leaf root
        bytes32 stateRoot = keccak256("sr");
        oracle.set(0, _outputRoot(stateRoot, rawRoot), true);

        vm.expectRevert(ETHLockbox.BadInclusionProof.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, rawRoot, 0, new bytes32[](0));
    }

    // ---------------------------------------------------------------------
    // V2 migration (wire the oracle into a pre-off-ramp proxy)
    // ---------------------------------------------------------------------

    /// Must match KardamomUUPSBase.FACTORY.
    address constant FACTORY = 0x2e4925D28F5F52086ff20aAd4981D68B1C87676E;

    function test_initializeV2_sets_oracle_factory_only() public {
        // A lockbox deployed deposit-only (zero oracle) — the pre-V2 fleet.
        ETHLockbox impl = new ETHLockbox();
        ETHLockbox depositOnly = ETHLockbox(
            payable(address(
                    new ERC1967Proxy(
                        address(impl),
                        abi.encodeWithSelector(
                            ETHLockbox.initialize.selector, L2_MINTER, address(0)
                        )
                    )
                ))
        );
        assertEq(depositOnly.outputOracle(), address(0));

        vm.expectRevert(KardamomUUPSBase.NotFactory.selector);
        depositOnly.initializeV2(address(oracle));

        vm.prank(FACTORY);
        depositOnly.initializeV2(address(oracle));
        assertEq(depositOnly.outputOracle(), address(oracle));

        // reinitializer(2) is one-shot.
        vm.prank(FACTORY);
        vm.expectRevert();
        depositOnly.initializeV2(address(0xDEAD));
    }

    // ---------------------------------------------------------------------
    // Upgrade transactions (L1-governed L2 feature flags)
    // ---------------------------------------------------------------------

    address constant UPGRADE_OWNER = address(0xA11CE);

    event UpgradeInitiated(
        uint64 indexed upgradeNonce, uint256 indexed featureId, uint64 activationTimestamp
    );

    /// Point `FACTORY.owner()` at `UPGRADE_OWNER`. The real factory is
    /// `Ownable2StepUpgradeable`; the lockbox only ever reads `owner()`.
    function _mockFactoryOwner() internal {
        vm.mockCall(FACTORY, abi.encodeWithSignature("owner()"), abi.encode(UPGRADE_OWNER));
    }

    function test_initiateUpgrade_emits_for_the_factory_owner() public {
        _mockFactoryOwner();

        vm.expectEmit(true, true, true, true, address(lockbox));
        emit UpgradeInitiated(1, 7, 1_700_000_000_250);

        vm.prank(UPGRADE_OWNER);
        lockbox.initiateUpgrade(7, 1_700_000_000_250);

        assertEq(lockbox.upgradeNonce(), 1);
    }

    function test_initiateUpgrade_rejects_a_stranger() public {
        _mockFactoryOwner();

        vm.expectRevert(ETHLockbox.NotUpgradeAuthority.selector);
        vm.prank(address(0xBAD));
        lockbox.initiateUpgrade(1, 0);

        // A rejected attempt must not consume a nonce — the nonce indexes the
        // authorized instruction series operators reason about.
        assertEq(lockbox.upgradeNonce(), 0);
    }

    /// The authority is the factory owner *at call time*, so rotating factory
    /// ownership rotates who may upgrade the chain. One root of trust.
    function test_initiateUpgrade_follows_factory_ownership() public {
        _mockFactoryOwner();
        vm.prank(UPGRADE_OWNER);
        lockbox.initiateUpgrade(1, 0);

        address rotated = address(0xB0B);
        vm.mockCall(FACTORY, abi.encodeWithSignature("owner()"), abi.encode(rotated));

        vm.expectRevert(ETHLockbox.NotUpgradeAuthority.selector);
        vm.prank(UPGRADE_OWNER);
        lockbox.initiateUpgrade(2, 0);

        vm.prank(rotated);
        lockbox.initiateUpgrade(2, 0);
        assertEq(lockbox.upgradeNonce(), 2);
    }

    function test_initiateUpgrade_nonce_is_monotonic() public {
        _mockFactoryOwner();
        vm.startPrank(UPGRADE_OWNER);
        for (uint64 i = 1; i <= 3; i++) {
            lockbox.initiateUpgrade(i, 0);
            assertEq(lockbox.upgradeNonce(), i);
        }
        vm.stopPrank();
    }

    /// An immediate upgrade (activation 0) must be expressible: unlike
    /// `depositETH`, an upgrade carries no value and must not be forced to.
    function test_initiateUpgrade_takes_no_value_and_allows_zero_activation() public {
        _mockFactoryOwner();
        vm.expectEmit(true, true, true, true, address(lockbox));
        emit UpgradeInitiated(1, 1, 0);
        vm.prank(UPGRADE_OWNER);
        lockbox.initiateUpgrade(1, 0);
        assertEq(address(lockbox).balance, 0);
    }

    function test_unauthorized_upgrade_reverts() public {
        ETHLockbox newImpl = new ETHLockbox();
        (bool ok,) = address(lockbox)
            .call(abi.encodeWithSignature("upgradeToAndCall(address,bytes)", address(newImpl), ""));
        assertFalse(ok, "non-factory upgrade must revert");
    }
}

// ---------------------------------------------------------------------------
// Egress cap: the total per window and the per-account share.
// ---------------------------------------------------------------------------

contract ETHLockboxEgressTest is Test {
    ETHLockbox lockbox;
    MockOracle oracle;
    address constant L2_MINTER = address(0xBEEF);
    /// Must match KardamomUUPSBase.FACTORY.
    address constant FACTORY = 0x2e4925D28F5F52086ff20aAd4981D68B1C87676E;
    uint8 constant OUTPUT_VERSION = 0;
    uint64 constant WINDOW = 1 days;

    event EgressLimitsUpdated(uint256 capPerWindow, uint256 accountCapPerWindow);

    function setUp() public {
        oracle = new MockOracle();
        oracle.setFinalizationWindow(WINDOW);
        ETHLockbox impl = new ETHLockbox();
        bytes memory initData =
            abi.encodeWithSelector(ETHLockbox.initialize.selector, L2_MINTER, address(oracle));
        lockbox = ETHLockbox(payable(address(new ERC1967Proxy(address(impl), initData))));
        vm.deal(address(lockbox), 100 ether);
        vm.warp(1_700_000_000);
    }

    function _hashLeaf(bytes32 wh) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(bytes1(0x00), wh));
    }

    /// Register a single-leaf tree for one withdrawal at output `index` and
    /// finalize it.
    function _finalize(uint256 index, uint256 nonce, address l2Sender, uint256 value) internal {
        _finalize(index, nonce, l2Sender, value, false);
    }

    /// With `expectCapRevert`, the finalize call itself must revert with
    /// `EgressCapExceeded`. The expectation is armed right before that call,
    /// after the helper's own external calls.
    function _finalize(
        uint256 index,
        uint256 nonce,
        address l2Sender,
        uint256 value,
        bool expectCapRevert
    ) internal {
        ETHLockbox.WithdrawalTransaction memory wtx = ETHLockbox.WithdrawalTransaction({
            nonce: nonce, sender: l2Sender, target: address(0x7777), value: value
        });
        bytes32 root = _hashLeaf(lockbox.hashWithdrawal(wtx));
        bytes32 stateRoot = keccak256(abi.encode("state", index));
        oracle.set(index, keccak256(abi.encodePacked(OUTPUT_VERSION, stateRoot, root)), true);
        if (expectCapRevert) vm.expectRevert(ETHLockbox.EgressCapExceeded.selector);
        lockbox.finalizeWithdrawal(wtx, index, stateRoot, root, 0, new bytes32[](0));
    }

    function test_setEgressLimits_factory_only() public {
        vm.expectRevert(KardamomUUPSBase.NotFactory.selector);
        lockbox.setEgressLimits(1 ether, 1 ether);

        vm.expectEmit(false, false, false, true);
        emit EgressLimitsUpdated(5 ether, 2 ether);
        vm.prank(FACTORY);
        lockbox.setEgressLimits(5 ether, 2 ether);
        assertEq(lockbox.egressCapPerWindow(), 5 ether);
        assertEq(lockbox.egressAccountCapPerWindow(), 2 ether);
    }

    function test_zero_caps_do_no_accounting() public {
        _finalize(0, 0, address(0xA1), 50 ether);
        assertEq(lockbox.egressUsed(lockbox.egressWindowId()), 0);
    }

    function test_account_share_is_enforced_per_window() public {
        vm.prank(FACTORY);
        lockbox.setEgressLimits(0, 2 ether);
        _finalize(0, 0, address(0xA1), 2 ether);
        _finalize(1, 1, address(0xA1), 1 wei, true);
        // Another account has its own share.
        _finalize(2, 2, address(0xA2), 2 ether);
        // The next window starts fresh. The rejected withdrawal is not consumed.
        vm.warp(block.timestamp + WINDOW);
        _finalize(1, 1, address(0xA1), 1 wei);
    }

    function test_total_cap_is_enforced_across_accounts() public {
        vm.prank(FACTORY);
        lockbox.setEgressLimits(3 ether, 2 ether);
        _finalize(0, 0, address(0xA1), 2 ether);
        _finalize(1, 1, address(0xA2), 1 ether);
        assertEq(lockbox.egressUsed(lockbox.egressWindowId()), 3 ether);
        _finalize(2, 2, address(0xA3), 1 wei, true);
        vm.warp(block.timestamp + WINDOW);
        _finalize(2, 2, address(0xA3), 1 wei);
    }

    function test_window_id_follows_the_oracle_window() public {
        assertEq(lockbox.egressWindowId(), block.timestamp / WINDOW);
        oracle.setFinalizationWindow(2 hours);
        assertEq(lockbox.egressWindowId(), block.timestamp / 2 hours);
    }
}
