// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {Inbox} from "../../src/L2/Inbox.sol";
import {Outbox} from "../../src/L2/Outbox.sol";
import {XChain} from "../../src/L2/XChain.sol";

/// Target that records what it observed during delivery.
contract Receiver {
    uint64 public seenOrigin;
    address public seenSender;
    bytes public seenData;

    function poke(bytes calldata payload) external {
        (seenOrigin, seenSender) = Inbox(msg.sender).xDomainSender();
        seenData = payload;
    }

    function alwaysReverts(bytes calldata) external pure {
        revert("nope");
    }
}

contract InboxTest is Test {
    // Predeploy-addressed instances: Inbox enqueues callbacks through the
    // constant XChain.OUTBOX, so both contracts are etched at their canonical
    // addresses, exactly as genesis seeds them.
    Inbox inbox;
    Outbox outbox;
    Receiver receiver;

    uint64 constant SELF = 412_347;
    uint64 constant ORIGIN = 412_346;

    function setUp() public {
        vm.chainId(SELF);
        vm.etch(XChain.INBOX, address(new Inbox()).code);
        vm.etch(XChain.OUTBOX, address(new Outbox()).code);
        inbox = Inbox(XChain.INBOX);
        outbox = Outbox(XChain.OUTBOX);
        receiver = new Receiver();
    }

    function noCb() internal pure returns (XChain.Callback memory) {
        return XChain.Callback(address(0), 0, bytes32(0));
    }

    function deliverAsOrigin(
        uint64 seq,
        address target,
        bytes memory data,
        XChain.Callback memory cb
    ) internal {
        vm.prank(XChain.txSender(ORIGIN));
        inbox.deliver(ORIGIN, seq, address(0xABCD), target, 0, 500_000, data, cb);
    }

    function test_deliverExposesOriginIdentityToTarget() public {
        bytes memory payload = hex"1234";
        deliverAsOrigin(0, address(receiver), abi.encodeCall(Receiver.poke, (payload)), noCb());
        assertEq(receiver.seenOrigin(), ORIGIN);
        assertEq(receiver.seenSender(), address(0xABCD));
        assertEq(receiver.seenData(), payload);
        assertEq(inbox.delivered(ORIGIN, 0), 1);
    }

    function test_onlyAliasedOriginOutboxMayDeliver() public {
        vm.expectRevert("Inbox: not the origin outbox");
        inbox.deliver(ORIGIN, 0, address(1), address(receiver), 0, 100_000, hex"", noCb());

        // Even the RIGHT aliased sender for the WRONG claimed origin fails.
        vm.prank(XChain.txSender(ORIGIN + 1));
        vm.expectRevert("Inbox: not the origin outbox");
        inbox.deliver(ORIGIN, 0, address(1), address(receiver), 0, 100_000, hex"", noCb());
    }

    function test_doubleDeliveryRejected() public {
        deliverAsOrigin(7, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), noCb());
        vm.prank(XChain.txSender(ORIGIN));
        vm.expectRevert("Inbox: already delivered");
        inbox.deliver(ORIGIN, 7, address(0xABCD), address(receiver), 0, 100_000, hex"", noCb());
    }

    function test_innerRevertRecordsFailure_deliveryStillSucceeds() public {
        deliverAsOrigin(1, address(receiver), abi.encodeCall(Receiver.alwaysReverts, (hex"")), noCb());
        assertEq(inbox.delivered(ORIGIN, 1), 2);
    }

    function test_xDomainSenderRevertsOutsideDelivery() public {
        vm.expectRevert("Inbox: no delivery in progress");
        inbox.xDomainSender();
    }

    function test_callbackEnqueuedThroughOwnOutbox_withoutNestedCallback() public {
        XChain.Callback memory cb =
            XChain.Callback(address(0xCB), 300_000, bytes32(uint256(42)));
        deliverAsOrigin(2, address(receiver), abi.encodeCall(Receiver.poke, (hex"55")), cb);

        // The response consumed seq 0 of this chain's outbox lane back to the
        // origin, committed with a ZERO callback hash — depth capped at one.
        assertEq(outbox.nonces(ORIGIN), 1);
        bytes memory responseData = abi.encodeWithSelector(
            inbox.RESPONSE_SELECTOR(), true, keccak256(hex""), bytes32(uint256(42))
        );
        bytes32 expected = XChain.hashMessage(
            SELF, ORIGIN, 0, XChain.INBOX, address(0xCB), 0, 300_000, keccak256(responseData), bytes32(0)
        );
        assertTrue(outbox.sentMessages(expected));
    }

    function test_failureAlsoTriggersCallback() public {
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(7)));
        deliverAsOrigin(3, address(receiver), abi.encodeCall(Receiver.alwaysReverts, (hex"")), cb);
        assertEq(inbox.delivered(ORIGIN, 3), 2);
        // A response was still enqueued — origin apps never wait forever.
        assertEq(outbox.nonces(ORIGIN), 1);
    }
}
