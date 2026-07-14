// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {WithdrawalOutputOracle} from "../../src/L1/WithdrawalOutputOracle.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

contract WithdrawalOutputOracleTest is Test {
    WithdrawalOutputOracle oracle;
    address constant ATTESTER = address(0xA77E);
    address constant CHALLENGER = address(0xC4A1);
    uint64 constant WINDOW = 1 days;

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
}
