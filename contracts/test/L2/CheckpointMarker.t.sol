// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {CheckpointMarker} from "../../src/L2/CheckpointMarker.sol";
import {Inbox} from "../../src/L2/Inbox.sol";
import {Outbox} from "../../src/L2/Outbox.sol";
import {XChain} from "../../src/L2/XChain.sol";

/// The marker rule, seen from one chain (SELF) with two peers (A and B).
/// All three predeploys are etched at their canonical addresses, exactly as
/// genesis seeds them.
contract CheckpointMarkerTest is Test {
    CheckpointMarker marker;
    Inbox inbox;
    Outbox outbox;

    uint64 constant SELF = 412_347;
    uint64 constant PEER_A = 412_346;
    uint64 constant PEER_B = 412_348;
    uint64 constant X = 600_000;

    event RoundOpened(uint64 indexed round, uint64 blockNumber, uint64 indexed trigger);
    event MarkerIgnored(uint64 indexed round, uint64 indexed originChainId);
    event PeerRegistered(uint64 indexed chainId);

    function setUp() public {
        vm.chainId(SELF);
        vm.etch(XChain.INBOX, address(new Inbox()).code);
        vm.etch(XChain.OUTBOX, address(new Outbox()).code);
        vm.etch(XChain.CHECKPOINT_MARKER, address(new CheckpointMarker()).code);
        inbox = Inbox(XChain.INBOX);
        outbox = Outbox(XChain.OUTBOX);
        marker = CheckpointMarker(XChain.CHECKPOINT_MARKER);
        // Round 3 is in progress. Timestamps are milliseconds on this chain.
        vm.warp(3 * X + 1_000);
        vm.roll(100);
    }

    function noCb() internal pure returns (XChain.Callback memory) {
        return XChain.Callback(address(0), 0, bytes32(0));
    }

    /// One ordinary user message from `origin`, so that `Inbox.nextSeq`
    /// proves the lane exists.
    function deliverUserMessage(uint64 origin, uint64 seq) internal {
        vm.prank(XChain.txSender(origin));
        inbox.deliver(origin, seq, address(0xABCD), address(0xDEAD), 0, 100_000, hex"", noCb());
    }

    /// A marker delivery from `origin`, with `originSender` as the claimed
    /// sender on the origin chain.
    function deliverMarker(uint64 origin, uint64 seq, address originSender, uint64 round) internal {
        uint64 gas = marker.MARKER_GAS();
        bytes memory data = abi.encodeCall(CheckpointMarker.onMarker, (round));
        vm.prank(XChain.txSender(origin));
        inbox.deliver(origin, seq, originSender, XChain.CHECKPOINT_MARKER, 0, gas, data, noCb());
    }

    function markerCommitment(uint64 dest, uint64 seq, uint64 round)
        internal
        pure
        returns (bytes32)
    {
        return XChain.hashMessage(
            SELF,
            dest,
            seq,
            XChain.CHECKPOINT_MARKER,
            XChain.CHECKPOINT_MARKER,
            0,
            100_000 + 80_000 * 32,
            keccak256(abi.encodeCall(CheckpointMarker.onMarker, (round))),
            bytes32(0)
        );
    }

    // ---------------------------------------------------------------------
    // Peer registration
    // ---------------------------------------------------------------------

    function test_registerPeer_needsADeliveryFromThatChain() public {
        vm.expectRevert("CheckpointMarker: no delivery from chain");
        marker.registerPeer(PEER_A);

        deliverUserMessage(PEER_A, 0);
        vm.expectEmit(true, false, false, true);
        emit PeerRegistered(PEER_A);
        marker.registerPeer(PEER_A);
        assertTrue(marker.isPeer(PEER_A));
        assertEq(marker.peerCount(), 1);
        assertEq(marker.peerAt(0), PEER_A);

        // Idempotent.
        marker.registerPeer(PEER_A);
        assertEq(marker.peerCount(), 1);
    }

    function test_registerPeer_rejectsSelfAndZero() public {
        vm.expectRevert("CheckpointMarker: bad peer");
        marker.registerPeer(SELF);
        vm.expectRevert("CheckpointMarker: bad peer");
        marker.registerPeer(0);
    }

    // ---------------------------------------------------------------------
    // The own timer
    // ---------------------------------------------------------------------

    function test_startRound_opensRoundAndSendsMarkerOnEveryLane() public {
        deliverUserMessage(PEER_A, 0);
        deliverUserMessage(PEER_B, 0);
        marker.registerPeer(PEER_A);
        marker.registerPeer(PEER_B);

        assertEq(marker.currentRound(), 3);
        vm.expectEmit(true, true, false, true);
        emit RoundOpened(3, 100, 0);
        marker.startRound();

        assertEq(marker.lastRound(), 3);
        assertEq(marker.roundBlock(3), 100);
        // One marker per lane, committed in this chain's Outbox with the
        // predeploy as the sender.
        assertEq(outbox.nonces(PEER_A), 1);
        assertEq(outbox.nonces(PEER_B), 1);
        assertTrue(outbox.sentMessages(markerCommitment(PEER_A, 0, 3)));
        assertTrue(outbox.sentMessages(markerCommitment(PEER_B, 0, 3)));
    }

    function test_startRound_onlyOncePerInterval() public {
        marker.startRound();
        vm.expectRevert("CheckpointMarker: round already open");
        marker.startRound();

        vm.warp(4 * X);
        vm.roll(200);
        marker.startRound();
        assertEq(marker.lastRound(), 4);
        assertEq(marker.roundBlock(4), 200);
        // The old round keeps its block.
        assertEq(marker.roundBlock(3), 100);
    }

    function test_startRound_withNoPeersOpensTheRoundAndSendsNothing() public {
        marker.startRound();
        assertEq(marker.roundBlock(3), 100);
        assertEq(outbox.nonces(PEER_A), 0);
    }

    // ---------------------------------------------------------------------
    // A received marker
    // ---------------------------------------------------------------------

    function test_receivedMarker_opensRoundAndSendsOnEveryLaneIncludingBack() public {
        deliverUserMessage(PEER_B, 0);
        marker.registerPeer(PEER_B);
        assertFalse(marker.isPeer(PEER_A));

        // Peer A's clock is ahead: its marker for round 4 arrives first.
        vm.expectEmit(true, true, false, true);
        emit RoundOpened(4, 100, PEER_A);
        deliverMarker(PEER_A, 0, XChain.CHECKPOINT_MARKER, 4);

        assertEq(inbox.delivered(PEER_A, 0), 1);
        assertEq(marker.lastRound(), 4);
        assertEq(marker.roundBlock(4), 100);
        // The origin is now a peer, and got the marker back.
        assertTrue(marker.isPeer(PEER_A));
        assertEq(outbox.nonces(PEER_B), 1);
        assertEq(outbox.nonces(PEER_A), 1);
        assertTrue(outbox.sentMessages(markerCommitment(PEER_B, 0, 4)));
        assertTrue(outbox.sentMessages(markerCommitment(PEER_A, 0, 4)));

        // The own timer for round 3 is now stale.
        vm.expectRevert("CheckpointMarker: round already open");
        marker.startRound();
    }

    function test_duplicateOrStaleMarkerIsIgnored() public {
        deliverUserMessage(PEER_B, 0);
        marker.registerPeer(PEER_B);
        marker.startRound(); // round 3, one marker to B
        assertEq(outbox.nonces(PEER_B), 1);

        // B's own marker for round 3 arrives: duplicate.
        vm.expectEmit(true, true, false, true);
        emit MarkerIgnored(3, PEER_B);
        deliverMarker(PEER_B, 1, XChain.CHECKPOINT_MARKER, 3);
        assertEq(inbox.delivered(PEER_B, 1), 1);
        assertEq(outbox.nonces(PEER_B), 1); // nothing re-sent

        // A stale marker for round 2 from a new peer: registered, ignored.
        deliverMarker(PEER_A, 0, XChain.CHECKPOINT_MARKER, 2);
        assertTrue(marker.isPeer(PEER_A));
        assertEq(marker.lastRound(), 3);
        assertEq(outbox.nonces(PEER_A), 0);
    }

    function test_farFutureMarkerIsIgnored() public {
        // Round 3 is current. Round 4 is one interval of skew: accepted.
        // Round 5 and beyond: ignored, so a peer with a bad clock cannot
        // park lastRound in the future.
        deliverMarker(PEER_A, 0, XChain.CHECKPOINT_MARKER, 5);
        assertEq(inbox.delivered(PEER_A, 0), 1);
        assertEq(marker.lastRound(), 0);
        assertEq(marker.roundBlock(5), 0);
        assertTrue(marker.isPeer(PEER_A));

        deliverMarker(PEER_A, 1, XChain.CHECKPOINT_MARKER, 4);
        assertEq(marker.lastRound(), 4);
        // The own timer still works once the clock reaches round 5.
        vm.warp(5 * X);
        marker.startRound();
        assertEq(marker.lastRound(), 5);
    }

    // ---------------------------------------------------------------------
    // Forgery
    // ---------------------------------------------------------------------

    function test_userSentMarkerIsRejected() public {
        // A user on the origin chain sends a message shaped like a marker.
        deliverMarker(PEER_A, 0, address(0xBAD), 4);
        // The delivery happened, but the inner call reverted: status 2.
        assertEq(inbox.delivered(PEER_A, 0), 2);
        assertEq(marker.lastRound(), 0);
        assertEq(marker.roundBlock(4), 0);
        assertFalse(marker.isPeer(PEER_A));
    }

    function test_onMarker_rejectsDirectCalls() public {
        vm.expectRevert("CheckpointMarker: not the inbox");
        marker.onMarker(4);
        vm.prank(XChain.INBOX);
        vm.expectRevert("Inbox: no delivery in progress");
        marker.onMarker(4);
    }

    function test_markerGasFitsTheOutboxCap() public view {
        assertLe(marker.MARKER_GAS(), outbox.MAX_MESSAGE_GAS());
    }
}
