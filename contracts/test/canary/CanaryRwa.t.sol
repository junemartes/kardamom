// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {CanaryRwa} from "../../src/canary/CanaryRwa.sol";

/// The owner mints and burns; only allowed addresses send and receive.
contract CanaryRwaTest is Test {
    CanaryRwa token;
    address alice = address(0xA11CE);
    address bob = address(0xB0B);
    address mallory = address(0xBAD);

    event Supply(uint256 totalSupply);

    function setUp() public {
        address[] memory ring = new address[](2);
        ring[0] = alice;
        ring[1] = bob;
        token = new CanaryRwa(ring);
    }

    function test_the_token_names_itself() public view {
        assertEq(token.name(), "Kardamom Canary Asset");
        assertEq(token.symbol(), "KCA");
        assertEq(token.decimals(), 18);
        assertEq(token.owner(), address(this));
    }

    function test_mint_and_burn_report_the_supply() public {
        vm.expectEmit(false, false, false, true);
        emit Supply(5 ether);
        token.mint(alice, 5 ether);
        vm.expectEmit(false, false, false, true);
        emit Supply(3 ether);
        token.burn(alice, 2 ether);
        assertEq(token.totalSupply(), 3 ether);
        assertEq(token.balanceOf(alice), 3 ether);
    }

    function test_only_the_owner_mints_and_burns() public {
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(CanaryRwa.NotOwner.selector, alice));
        token.mint(alice, 1);
        token.mint(alice, 1);
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(CanaryRwa.NotOwner.selector, alice));
        token.burn(alice, 1);
    }

    function test_a_transfer_off_the_allowlist_reverts() public {
        token.mint(alice, 10);
        vm.prank(alice);
        assertTrue(token.transfer(bob, 4));
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(CanaryRwa.NotAllowed.selector, mallory));
        token.transfer(mallory, 1);
        vm.expectRevert(abi.encodeWithSelector(CanaryRwa.NotAllowed.selector, mallory));
        token.mint(mallory, 1);
    }

    function test_a_removed_holder_cannot_send_but_can_be_burned() public {
        token.mint(alice, 10);
        token.setAllowed(alice, false);
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(CanaryRwa.NotAllowed.selector, alice));
        token.transfer(bob, 1);
        token.burn(alice, 10);
        assertEq(token.totalSupply(), 0);
    }

    function testFuzz_supply_is_mints_less_burns(uint96 minted, uint96 burned) public {
        vm.assume(burned <= minted);
        token.mint(bob, minted);
        token.burn(bob, burned);
        assertEq(token.totalSupply(), uint256(minted) - burned);
    }
}
