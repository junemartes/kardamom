// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {L2ToL1MessagePasser} from "../../src/L2/L2ToL1MessagePasser.sol";

contract L2ToL1MessagePasserTest is Test {
    L2ToL1MessagePasser passer;

    event MessagePassed(
        uint256 indexed nonce,
        address indexed sender,
        address indexed target,
        uint256 value,
        bytes32 withdrawalHash
    );

    function setUp() public {
        passer = new L2ToL1MessagePasser();
    }

    function test_initiateWithdrawal_records_and_emits() public {
        address alice = address(0xA11CE);
        address target = address(0x7777);
        vm.deal(alice, 5 ether);

        bytes32 expected = passer.hashWithdrawal(0, alice, target, 1 ether);
        vm.expectEmit(true, true, true, true);
        emit MessagePassed(0, alice, target, 1 ether, expected);

        vm.prank(alice);
        passer.initiateWithdrawal{value: 1 ether}(target);

        assertTrue(passer.sentMessages(expected));
        assertEq(passer.messageNonce(), 1);
        assertEq(address(passer).balance, 1 ether); // locked on L2
    }

    function test_nonce_increments_per_withdrawal() public {
        address alice = address(0xA11CE);
        vm.deal(alice, 5 ether);
        vm.startPrank(alice);
        passer.initiateWithdrawal{value: 1 ether}(address(0x1));
        passer.initiateWithdrawal{value: 1 ether}(address(0x2));
        vm.stopPrank();
        assertEq(passer.messageNonce(), 2);
    }

    function test_zero_value_reverts() public {
        vm.expectRevert(L2ToL1MessagePasser.ZeroWithdrawal.selector);
        passer.initiateWithdrawal{value: 0}(address(0x1));
    }

    function test_hashWithdrawal_matches_abi_encode() public view {
        bytes32 got = passer.hashWithdrawal(7, address(0xBEEF), address(0xCAFE), 3 ether);
        bytes32 want = keccak256(abi.encode(uint256(7), address(0xBEEF), address(0xCAFE), 3 ether));
        assertEq(got, want);
    }
}
