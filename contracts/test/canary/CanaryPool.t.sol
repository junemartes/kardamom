// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {CanaryRwa} from "../../src/canary/CanaryRwa.sol";
import {CanaryPool} from "../../src/canary/CanaryPool.sol";

/// The pool's swaps and liquidity follow the constant-product formula,
/// keep the product of the reserves from falling, and keep the reserves
/// equal to the balances.
contract CanaryPoolTest is Test {
    CanaryRwa token;
    CanaryPool pool;
    address lp = address(0x1D);
    address trader = address(0x7A);

    function setUp() public {
        address[] memory ring = new address[](2);
        ring[0] = lp;
        ring[1] = trader;
        token = new CanaryRwa(ring);
        pool = new CanaryPool(token);
        token.setAllowed(address(pool), true);
        token.mint(lp, 1_000_000 ether);
        token.mint(trader, 1_000 ether);
        vm.deal(lp, 1 ether);
        vm.deal(trader, 1 ether);
        vm.prank(lp);
        token.approve(address(pool), type(uint256).max);
        vm.prank(trader);
        token.approve(address(pool), type(uint256).max);
        vm.prank(lp);
        pool.addLiquidity{value: 0.002 ether}(100_000 ether);
    }

    function assertSynced() internal view {
        assertEq(pool.reserveEth(), address(pool).balance);
        assertEq(pool.reserveToken(), token.balanceOf(address(pool)));
    }

    function test_the_first_liquidity_gets_a_share_per_wei() public view {
        assertEq(pool.totalShares(), 0.002 ether);
        assertEq(pool.shares(lp), 0.002 ether);
        assertSynced();
    }

    function test_a_swap_pays_the_formula_and_keeps_the_product() public {
        uint256 eth = pool.reserveEth();
        uint256 tok = pool.reserveToken();
        uint256 expected = (0.00001 ether * 997 * tok) / (eth * 1000 + 0.00001 ether * 997);
        vm.prank(trader);
        uint256 out = pool.swapEthForToken{value: 0.00001 ether}(0);
        assertEq(out, expected);
        assertGe(pool.reserveEth() * pool.reserveToken(), eth * tok);
        assertSynced();
        uint256 back = pool.getAmountOut(out, pool.reserveToken(), pool.reserveEth());
        vm.prank(trader);
        assertEq(pool.swapTokenForEth(out, 0), back);
        assertLt(back, 0.00001 ether, "a round trip pays the fee twice");
        assertSynced();
        assertEq(pool.seq(), 3);
    }

    function test_slippage_and_zero_amounts_revert() public {
        vm.prank(trader);
        vm.expectRevert();
        pool.swapEthForToken{value: 1}(type(uint256).max);
        vm.prank(trader);
        vm.expectRevert(CanaryPool.ZeroAmount.selector);
        pool.swapTokenForEth(0, 0);
    }

    function test_liquidity_round_trip_returns_the_share_of_each_reserve() public {
        uint256 eth = pool.reserveEth();
        uint256 tok = pool.reserveToken();
        uint256 supply = pool.totalShares();
        uint256 addEth = 0.0001 ether;
        uint256 addTok = addEth * tok / eth + 1;
        vm.prank(lp);
        uint256 minted = pool.addLiquidity{value: addEth}(addTok);
        uint256 a = addEth * supply / eth;
        uint256 b = addTok * supply / tok;
        assertEq(minted, a < b ? a : b);
        uint256 eth2 = pool.reserveEth();
        uint256 tok2 = pool.reserveToken();
        uint256 supply2 = pool.totalShares();
        vm.prank(lp);
        (uint256 ethOut, uint256 tokOut) = pool.removeLiquidity(minted);
        assertEq(ethOut, minted * eth2 / supply2);
        assertEq(tokOut, minted * tok2 / supply2);
        assertSynced();
    }

    function test_more_shares_than_held_revert() public {
        vm.prank(trader);
        vm.expectRevert(abi.encodeWithSelector(CanaryPool.NotEnoughShares.selector, 0, 1));
        pool.removeLiquidity(1);
    }

    function test_a_holder_off_the_allowlist_cannot_buy() public {
        address outsider = address(0xBAD);
        vm.deal(outsider, 1 ether);
        vm.prank(outsider);
        vm.expectRevert(abi.encodeWithSelector(CanaryRwa.NotAllowed.selector, outsider));
        pool.swapEthForToken{value: 0.00001 ether}(0);
    }

    function testFuzz_the_product_never_falls(uint64 amountIn, bool ethIn) public {
        vm.assume(amountIn > 1000);
        uint256 before = pool.reserveEth() * pool.reserveToken();
        vm.deal(trader, uint256(amountIn) + 1 ether);
        token.mint(trader, amountIn);
        vm.prank(trader);
        if (ethIn) {
            pool.swapEthForToken{value: amountIn}(0);
        } else {
            pool.swapTokenForEth(amountIn, 0);
        }
        assertGe(pool.reserveEth() * pool.reserveToken(), before);
        assertSynced();
    }
}
