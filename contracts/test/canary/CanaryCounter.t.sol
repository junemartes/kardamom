// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {CanaryCounter} from "../../src/canary/CanaryCounter.sol";

/// The count and the balance move together, and only a writer moves them.
contract CanaryCounterTest is Test {
    CanaryCounter counter;
    address writer = address(0xA11CE);
    address other = address(0xB0B);

    event Incremented(address indexed writer, uint256 count);

    function setUp() public {
        address[] memory writers = new address[](1);
        writers[0] = writer;
        counter = new CanaryCounter(writers);
        vm.deal(writer, 1 ether);
        vm.deal(other, 1 ether);
    }

    function test_a_write_adds_one_and_one_wei() public {
        vm.expectEmit(true, false, false, true);
        emit Incremented(writer, 1);
        vm.prank(writer);
        assertEq(counter.increment{value: 1}(), 1);
        vm.prank(writer);
        assertEq(counter.increment{value: 1}(), 2);
        assertEq(counter.count(), 2);
        assertEq(address(counter).balance, 2);
    }

    function test_a_non_writer_cannot_write() public {
        vm.prank(other);
        vm.expectRevert(abi.encodeWithSelector(CanaryCounter.NotWriter.selector, other));
        counter.increment{value: 1}();
    }

    function test_a_write_needs_exactly_one_wei() public {
        vm.prank(writer);
        vm.expectRevert(abi.encodeWithSelector(CanaryCounter.WrongValue.selector, 0));
        counter.increment();
        vm.prank(writer);
        vm.expectRevert(abi.encodeWithSelector(CanaryCounter.WrongValue.selector, 2));
        counter.increment{value: 2}();
    }

    function test_a_plain_transfer_reverts() public {
        vm.prank(other);
        (bool ok,) = address(counter).call{value: 1}("");
        assertFalse(ok);
        assertEq(address(counter).balance, 0);
    }

    function testFuzz_balance_equals_count(uint8 writes) public {
        for (uint256 i = 0; i < writes; ++i) {
            vm.prank(writer);
            counter.increment{value: 1}();
        }
        assertEq(address(counter).balance, counter.count());
        assertEq(counter.count(), writes);
    }
}
