// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {KardamomL2Settlement} from "../../src/L2/KardamomL2Settlement.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

contract KardamomL2SettlementTest is Test {
    KardamomL2Settlement settlement;
    address constant BATCHER = address(0xBA7);

    bytes32 constant RC = keccak256("records");

    event BatchPosted(
        uint64 indexed batchIndex,
        bytes32[] blobHashes,
        uint64 l2BlockStart,
        uint64 l2BlockEnd,
        bytes32 recordsCommitment
    );

    function setUp() public {
        KardamomL2Settlement impl = new KardamomL2Settlement();
        bytes memory initData =
            abi.encodeWithSelector(KardamomL2Settlement.initialize.selector, BATCHER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        settlement = KardamomL2Settlement(address(proxy));
    }

    function test_initializer_stores_batcher() public view {
        assertEq(settlement.l1Batcher(), BATCHER);
        assertEq(settlement.lastBatchIndex(), 0);
    }

    function test_impl_cannot_be_initialized_directly() public {
        KardamomL2Settlement impl = new KardamomL2Settlement();
        vm.expectRevert();
        impl.initialize(BATCHER);
    }

    function test_only_batcher_can_post() public {
        bytes32[] memory hashes = _hashes(1);
        vm.expectRevert(KardamomL2Settlement.NotBatcher.selector);
        settlement.postBatch(0, hashes, 1, 1, RC);
    }

    function test_post_emits_event_and_advances_index() public {
        bytes32[] memory hashes = _hashes(2);
        vm.prank(BATCHER);
        vm.expectEmit(true, false, false, true);
        emit BatchPosted(1, hashes, 10, 15, RC);
        settlement.postBatch(0, hashes, 10, 15, RC);
        assertEq(settlement.lastBatchIndex(), 1);
        (uint64 s0, uint64 e0, bytes32 rc) = settlement.batches(1);
        assertEq(s0, 10);
        assertEq(e0, 15);
        assertEq(rc, RC);
    }

    function test_post_rejects_stale_prev_index() public {
        bytes32[] memory hashes = _hashes(1);
        vm.prank(BATCHER);
        settlement.postBatch(0, hashes, 0, 0, RC);
        // lastBatchIndex is now 1; reposting with prevBatchIndex=0 must revert.
        vm.prank(BATCHER);
        vm.expectRevert(KardamomL2Settlement.StaleBatchIndex.selector);
        settlement.postBatch(0, hashes, 1, 1, RC);
    }

    function test_post_rejects_future_prev_index() public {
        bytes32[] memory hashes = _hashes(1);
        vm.prank(BATCHER);
        vm.expectRevert(KardamomL2Settlement.StaleBatchIndex.selector);
        settlement.postBatch(5, hashes, 0, 0, RC);
    }

    function test_post_rejects_empty_blob_array() public {
        bytes32[] memory empty = new bytes32[](0);
        vm.prank(BATCHER);
        vm.expectRevert(KardamomL2Settlement.EmptyBlobs.selector);
        settlement.postBatch(0, empty, 0, 0, RC);
    }

    function test_post_rejects_inverted_block_range() public {
        bytes32[] memory hashes = _hashes(1);
        vm.prank(BATCHER);
        vm.expectRevert(KardamomL2Settlement.BadBlockRange.selector);
        settlement.postBatch(0, hashes, 10, 5, RC);
    }

    function test_index_advances_monotonically_across_three_posts() public {
        bytes32[] memory h = _hashes(1);
        vm.startPrank(BATCHER);
        settlement.postBatch(0, h, 0, 0, RC);
        settlement.postBatch(1, h, 1, 2, RC);
        settlement.postBatch(2, h, 3, 5, RC);
        vm.stopPrank();
        assertEq(settlement.lastBatchIndex(), 3);
    }

    function test_unauthorized_upgrade_reverts() public {
        KardamomL2Settlement newImpl = new KardamomL2Settlement();
        (bool ok,) = address(settlement)
            .call(abi.encodeWithSignature("upgradeToAndCall(address,bytes)", address(newImpl), ""));
        assertFalse(ok, "non-factory upgrade must revert");
    }

    function testFuzz_index_advances_on_each_post(uint8 n) public {
        n = uint8(bound(n, 1, 32));
        bytes32[] memory h = _hashes(1);
        vm.startPrank(BATCHER);
        for (uint64 i = 0; i < n; i++) {
            settlement.postBatch(i, h, i, i, RC);
        }
        vm.stopPrank();
        assertEq(settlement.lastBatchIndex(), n);
    }

    function _hashes(uint256 n) internal pure returns (bytes32[] memory out) {
        out = new bytes32[](n);
        for (uint256 i = 0; i < n; i++) {
            out[i] = bytes32(uint256(0xC0FFEE) + i);
        }
    }
}
