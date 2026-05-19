// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {ETHLockbox} from "../../src/L1/ETHLockbox.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

contract ETHLockboxTest is Test {
    ETHLockbox lockbox;
    address constant L2_MINTER = address(0xBEEF);

    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );

    function setUp() public {
        ETHLockbox impl = new ETHLockbox();
        bytes memory initData = abi.encodeWithSelector(ETHLockbox.initialize.selector, L2_MINTER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        lockbox = ETHLockbox(payable(address(proxy)));
    }

    function test_initializer_stores_l2Minter() public view {
        assertEq(lockbox.l2Minter(), L2_MINTER);
    }

    function test_impl_cannot_be_initialized_directly() public {
        ETHLockbox impl = new ETHLockbox();
        vm.expectRevert();
        impl.initialize(L2_MINTER);
    }

    function test_initial_depositNonce_is_zero() public view {
        assertEq(lockbox.depositNonce(), 0);
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

    function test_depositNonce_increments_monotonically() public {
        address alice = address(0xA11CE);
        address bob = address(0xB0B);
        vm.deal(alice, 5 ether);
        vm.deal(bob, 5 ether);

        vm.prank(alice);
        lockbox.depositETH{value: 1 ether}(address(0x1), 0, hex"");

        vm.prank(bob);
        lockbox.depositETH{value: 1 ether}(address(0x2), 0, hex"");

        vm.prank(alice);
        lockbox.depositETH{value: 1 ether}(address(0x3), 0, hex"");

        assertEq(lockbox.depositNonce(), 3);
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

    function test_unauthorized_upgrade_reverts() public {
        ETHLockbox newImpl = new ETHLockbox();
        // Not the factory — should revert NotFactory.
        (bool ok,) = address(lockbox)
            .call(abi.encodeWithSignature("upgradeToAndCall(address,bytes)", address(newImpl), ""));
        assertFalse(ok, "non-factory upgrade must revert");
    }
}
