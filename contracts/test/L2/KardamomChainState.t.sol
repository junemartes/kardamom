// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {KardamomChainState} from "../../src/L2/KardamomChainState.sol";

contract KardamomChainStateTest is Test {
    KardamomChainState cs;

    /// Must match `kardamom_types::upgrades::SYSTEM_UPGRADER`. The Rust-side
    /// cross-check lives in `crates/deployer/tests/chainstate_genesis_predeploy.rs`.
    address constant SYSTEM_UPGRADER = 0x454156dAb0518B9244CC7Ff1b0FfFf6c7E031B6D;

    event FeatureScheduled(uint256 indexed featureId, uint256 activationTimestamp);

    function setUp() public {
        cs = new KardamomChainState();
        // Timestamps on this chain are epoch-MILLISECONDS.
        vm.warp(1_700_000_000_000);
    }

    // ---------------------------------------------------------------------
    // Authorization
    // ---------------------------------------------------------------------

    function test_setFeature_rejects_everyone_but_the_system_upgrader() public {
        vm.expectRevert(KardamomChainState.NotSystemUpgrader.selector);
        cs.setFeature(1, 0);

        vm.expectRevert(KardamomChainState.NotSystemUpgrader.selector);
        vm.prank(address(0xBAD));
        cs.setFeature(1, 0);

        assertEq(cs.activationOf(1), 0);
    }

    function test_system_upgrader_can_schedule() public {
        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(1, 1_700_000_005_000);
        assertEq(cs.activationOf(1), 1_700_000_005_000);
    }

    // ---------------------------------------------------------------------
    // Activation semantics
    // ---------------------------------------------------------------------

    function test_zero_activation_resolves_to_the_current_block_timestamp() public {
        vm.expectEmit(true, true, true, true, address(cs));
        emit FeatureScheduled(1, block.timestamp);

        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(1, 0);

        // Resolved, not stored as 0 — otherwise "scheduled" and "never
        // scheduled" would be indistinguishable.
        assertEq(cs.activationOf(1), block.timestamp);
        assertTrue(cs.isActive(1));
    }

    function test_future_activation_is_scheduled_but_not_yet_active() public {
        uint64 t = uint64(block.timestamp + 4_000);
        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(1, t);

        assertEq(cs.activationOf(1), t);
        assertFalse(cs.isActive(1));

        // Exactly at T the feature is active: the predicate is `>=`.
        vm.warp(t);
        assertTrue(cs.isActive(1));
    }

    function test_past_activation_is_immediately_active() public {
        uint64 t = uint64(block.timestamp - 1);
        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(1, t);
        assertTrue(cs.isActive(1));
    }

    function test_unscheduled_feature_is_never_active() public view {
        assertEq(cs.activationOf(99), 0);
        assertFalse(cs.isActive(99));
    }

    function test_features_are_independent() public {
        vm.startPrank(SYSTEM_UPGRADER);
        cs.setFeature(1, 0);
        cs.setFeature(2, uint64(block.timestamp + 10_000));
        vm.stopPrank();

        assertTrue(cs.isActive(1));
        assertFalse(cs.isActive(2));
        assertFalse(cs.isActive(3));
    }

    /// Rescheduling forward suspends an active feature. Documented v1 behaviour
    /// (the authority is trusted); pinned so a future rollback design has to
    /// change it deliberately.
    function test_rescheduling_forward_suspends_an_active_feature() public {
        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(1, 0);
        assertTrue(cs.isActive(1));

        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(1, uint64(block.timestamp + 10_000));
        assertFalse(cs.isActive(1));
    }

    // ---------------------------------------------------------------------
    // Health beacon (protocol-written, never EVM-written)
    // ---------------------------------------------------------------------

    function test_health_beacon_starts_empty() public view {
        (uint64 count, uint64 blockNumber, uint64 ts) = cs.health();
        assertEq(count, 0);
        assertEq(blockNumber, 0);
        assertEq(ts, 0);
    }

    /// The engine writes the packed word directly into the block delta; this
    /// pins the unpacking against that layout. Mirrors
    /// `kardamom_exec_core::features::pack_beacon`.
    function test_health_unpacks_the_packed_beacon_word() public {
        uint64 count = 42;
        uint64 blockNumber = 1234;
        uint64 ts = 1_700_000_000_250;
        uint256 packed = uint256(count) | (uint256(blockNumber) << 64) | (uint256(ts) << 128);

        // Slot 1 — `healthBeacon`, as pinned by the storage-layout check.
        vm.store(address(cs), bytes32(uint256(1)), bytes32(packed));

        assertEq(cs.healthBeacon(), packed);
        (uint64 c, uint64 b, uint64 t) = cs.health();
        assertEq(c, count);
        assertEq(b, blockNumber);
        assertEq(t, ts);
    }

    function test_health_unpacking_is_exact_at_field_maxima() public {
        // Every field saturated: proves the shifts/masks do not bleed across
        // field boundaries.
        uint256 packed = uint256(type(uint64).max) | (uint256(type(uint64).max) << 64)
            | (uint256(type(uint64).max) << 128);
        vm.store(address(cs), bytes32(uint256(1)), bytes32(packed));
        (uint64 c, uint64 b, uint64 t) = cs.health();
        assertEq(c, type(uint64).max);
        assertEq(b, type(uint64).max);
        assertEq(t, type(uint64).max);
    }

    /// No Solidity path may write the beacon: it is protocol state.
    function test_no_function_writes_the_beacon() public {
        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(1, 0);
        assertEq(cs.healthBeacon(), 0);
    }

    // ---------------------------------------------------------------------
    // Storage layout — the engine addresses these slots by hand
    // ---------------------------------------------------------------------

    function test_activation_lives_in_the_mapping_at_slot_zero() public {
        vm.prank(SYSTEM_UPGRADER);
        cs.setFeature(7, 1_700_000_009_000);

        // keccak256(pad32(featureId) ++ pad32(0)) — what
        // `kardamom_exec_core::features::activation_slot` computes.
        bytes32 slot = keccak256(abi.encode(uint256(7), uint256(0)));
        assertEq(uint256(vm.load(address(cs), slot)), 1_700_000_009_000);
    }

    function test_beacon_lives_at_slot_one() public {
        vm.store(address(cs), bytes32(uint256(1)), bytes32(uint256(0xABCD)));
        assertEq(cs.healthBeacon(), 0xABCD);
    }
}
