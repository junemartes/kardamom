// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {WithdrawalOutputOracle} from "../../src/L1/WithdrawalOutputOracle.sol";
import {KardamomUUPSBase} from "../../src/factory/KardamomUUPSBase.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

contract WithdrawalOutputOracleTest is Test {
    WithdrawalOutputOracle oracle;
    address constant ATTESTER = address(0xA77E);
    address constant CHALLENGER = address(0xC4A1);
    uint64 constant WINDOW = 1 days;
    /// Must match KardamomUUPSBase.FACTORY (the rotation/upgrade authority).
    address constant FACTORY = 0xED26EF4bE626a43Ea01074435D80fC48Dc5040cD;

    event OutputProposed(
        uint256 indexed index,
        bytes32 indexed outputRoot,
        uint64 indexed l2BlockNumber,
        uint64 timestamp
    );
    event OutputDeleted(uint256 indexed index, bytes32 outputRoot);

    function setUp() public {
        WithdrawalOutputOracle impl = new WithdrawalOutputOracle();
        bytes memory initData = abi.encodeWithSelector(
            WithdrawalOutputOracle.initialize.selector, ATTESTER, CHALLENGER, WINDOW
        );
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        oracle = WithdrawalOutputOracle(address(proxy));
        vm.warp(1_700_000_000);
    }

    function test_initialize_stores_config() public view {
        assertEq(oracle.attester(), ATTESTER);
        assertEq(oracle.challenger(), CHALLENGER);
        assertEq(oracle.finalizationWindow(), WINDOW);
        assertEq(oracle.outputCount(), 0);
    }

    function test_proposeOutput_appends() public {
        bytes32 root = keccak256("out0");
        vm.expectEmit(true, true, true, true);
        emit OutputProposed(0, root, 10, uint64(block.timestamp));
        vm.prank(ATTESTER);
        uint256 idx = oracle.proposeOutput(root, 10);
        assertEq(idx, 0);
        assertEq(oracle.outputCount(), 1);
        assertEq(oracle.outputRootAt(0), root);
    }

    function test_proposeOutput_only_attester() public {
        vm.expectRevert(WithdrawalOutputOracle.NotAttester.selector);
        oracle.proposeOutput(keccak256("x"), 1);
    }

    function test_proposeOutput_must_advance_block() public {
        vm.startPrank(ATTESTER);
        oracle.proposeOutput(keccak256("a"), 10);
        vm.expectRevert(WithdrawalOutputOracle.NonMonotonicBlock.selector);
        oracle.proposeOutput(keccak256("b"), 10);
        vm.stopPrank();
    }

    function test_not_finalizable_during_window() public {
        vm.prank(ATTESTER);
        oracle.proposeOutput(keccak256("a"), 10);
        assertFalse(oracle.isFinalizable(0));
        vm.warp(block.timestamp + WINDOW - 1);
        assertFalse(oracle.isFinalizable(0));
        vm.warp(block.timestamp + 1);
        assertTrue(oracle.isFinalizable(0));
    }

    function test_deleteOutput_blocks_finalization() public {
        vm.prank(ATTESTER);
        oracle.proposeOutput(keccak256("a"), 10);

        vm.expectEmit(true, false, false, true);
        emit OutputDeleted(0, keccak256("a"));
        vm.prank(CHALLENGER);
        oracle.deleteOutput(0);

        vm.warp(block.timestamp + WINDOW + 1);
        assertFalse(oracle.isFinalizable(0)); // deleted stays non-finalizable
    }

    function test_deleteOutput_only_challenger() public {
        vm.prank(ATTESTER);
        oracle.proposeOutput(keccak256("a"), 10);
        vm.expectRevert(WithdrawalOutputOracle.NotChallenger.selector);
        oracle.deleteOutput(0);
    }

    function test_deleteOutput_after_window_reverts() public {
        vm.prank(ATTESTER);
        oracle.proposeOutput(keccak256("a"), 10);
        vm.warp(block.timestamp + WINDOW + 1);
        vm.prank(CHALLENGER);
        vm.expectRevert(WithdrawalOutputOracle.WindowElapsed.selector);
        oracle.deleteOutput(0);
    }

    function test_deleteOutput_twice_reverts() public {
        vm.prank(ATTESTER);
        oracle.proposeOutput(keccak256("a"), 10);
        vm.startPrank(CHALLENGER);
        oracle.deleteOutput(0);
        vm.expectRevert(WithdrawalOutputOracle.AlreadyDeleted.selector);
        oracle.deleteOutput(0);
        vm.stopPrank();
    }

    // ---------------------------------------------------------------------
    // Re-proposal after a challenge (deleted outputs leave the monotonicity
    // floor) — regression for the stranded-range deadlock.
    // ---------------------------------------------------------------------

    function test_repropose_same_block_after_delete() public {
        vm.prank(ATTESTER);
        oracle.proposeOutput(keccak256("bad"), 10);
        vm.prank(CHALLENGER);
        oracle.deleteOutput(0);

        // The corrected output for the SAME range must be proposable.
        vm.prank(ATTESTER);
        uint256 idx = oracle.proposeOutput(keccak256("good"), 10);
        assertEq(idx, 1);
        assertEq(oracle.outputRootAt(1), keccak256("good"));
        // Indices stay stable; the deleted record remains.
        assertEq(oracle.outputCount(), 2);

        // And its withdrawals become finalizable after the window.
        vm.warp(block.timestamp + WINDOW);
        assertFalse(oracle.isFinalizable(0)); // deleted forever
        assertTrue(oracle.isFinalizable(1));
    }

    function test_monotonicity_floor_is_latest_non_deleted() public {
        vm.startPrank(ATTESTER);
        oracle.proposeOutput(keccak256("a"), 10);
        oracle.proposeOutput(keccak256("b"), 20);
        vm.stopPrank();
        vm.prank(CHALLENGER);
        oracle.deleteOutput(1); // floor falls back to block 10

        vm.startPrank(ATTESTER);
        vm.expectRevert(WithdrawalOutputOracle.NonMonotonicBlock.selector);
        oracle.proposeOutput(keccak256("c"), 10); // still covered by output 0
        oracle.proposeOutput(keccak256("c"), 15); // above the non-deleted floor
        vm.stopPrank();
        assertEq(oracle.outputRootAt(2), keccak256("c"));
    }

    // ---------------------------------------------------------------------
    // Init validation + factory-gated key rotation
    // ---------------------------------------------------------------------

    function _initData(address att, address chal, uint64 window)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodeWithSelector(WithdrawalOutputOracle.initialize.selector, att, chal, window);
    }

    function test_initialize_rejects_zero_attester() public {
        WithdrawalOutputOracle impl = new WithdrawalOutputOracle();
        vm.expectRevert(WithdrawalOutputOracle.ZeroAddress.selector);
        new ERC1967Proxy(address(impl), _initData(address(0), CHALLENGER, WINDOW));
    }

    function test_initialize_rejects_zero_challenger() public {
        WithdrawalOutputOracle impl = new WithdrawalOutputOracle();
        vm.expectRevert(WithdrawalOutputOracle.ZeroAddress.selector);
        new ERC1967Proxy(address(impl), _initData(ATTESTER, address(0), WINDOW));
    }

    function test_initialize_rejects_zero_window() public {
        WithdrawalOutputOracle impl = new WithdrawalOutputOracle();
        vm.expectRevert(WithdrawalOutputOracle.ZeroWindow.selector);
        new ERC1967Proxy(address(impl), _initData(ATTESTER, CHALLENGER, 0));
    }

    function test_setAttester_rotates_only_via_factory() public {
        address newAttester = address(0xA77E2);
        vm.expectRevert(KardamomUUPSBase.NotFactory.selector);
        oracle.setAttester(newAttester);
        vm.prank(ATTESTER); // not even the current attester may rotate itself
        vm.expectRevert(KardamomUUPSBase.NotFactory.selector);
        oracle.setAttester(newAttester);

        vm.prank(FACTORY);
        oracle.setAttester(newAttester);
        assertEq(oracle.attester(), newAttester);

        // Old key is locked out; the new one proposes.
        vm.prank(ATTESTER);
        vm.expectRevert(WithdrawalOutputOracle.NotAttester.selector);
        oracle.proposeOutput(keccak256("x"), 1);
        vm.prank(newAttester);
        oracle.proposeOutput(keccak256("x"), 1);
    }

    function test_setChallenger_rotates_only_via_factory() public {
        address newChallenger = address(0xC4A12);
        vm.expectRevert(KardamomUUPSBase.NotFactory.selector);
        oracle.setChallenger(newChallenger);

        vm.prank(FACTORY);
        oracle.setChallenger(newChallenger);
        assertEq(oracle.challenger(), newChallenger);

        vm.prank(ATTESTER);
        oracle.proposeOutput(keccak256("x"), 1);
        vm.prank(CHALLENGER);
        vm.expectRevert(WithdrawalOutputOracle.NotChallenger.selector);
        oracle.deleteOutput(0);
        vm.prank(newChallenger);
        oracle.deleteOutput(0);
    }

    function test_setters_reject_zero_values() public {
        vm.startPrank(FACTORY);
        vm.expectRevert(WithdrawalOutputOracle.ZeroAddress.selector);
        oracle.setAttester(address(0));
        vm.expectRevert(WithdrawalOutputOracle.ZeroAddress.selector);
        oracle.setChallenger(address(0));
        vm.expectRevert(WithdrawalOutputOracle.ZeroWindow.selector);
        oracle.setFinalizationWindow(0);
        vm.stopPrank();
    }

    function test_setFinalizationWindow_via_factory() public {
        vm.prank(FACTORY);
        oracle.setFinalizationWindow(2 days);
        assertEq(oracle.finalizationWindow(), 2 days);
    }
}
