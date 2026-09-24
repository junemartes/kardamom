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

    function echo(bytes calldata payload) external pure returns (bytes memory) {
        return payload;
    }
}

/// Returns `size` bytes (a return-data bomb).
contract Bomb {
    function boom(uint256 size) external pure returns (bytes memory out) {
        out = new bytes(size);
        for (uint256 i = 0; i < size && i < 300; i++) {
            out[i] = bytes1(uint8(i + 1));
        }
    }
}

/// Records the gas it received, burns almost all of it, and returns more
/// bytes than the Inbox copies.
contract GasProbe {
    uint256 public entryGas;

    fallback() external payable {
        _probe();
    }

    function _probe() internal {
        entryGas = gasleft();
        while (gasleft() > 20_000) {}
        assembly {
            // Untouched memory: the returned bytes are all zero.
            return(0x1000, 1024)
        }
    }
}

/// Burns every unit of gas it is given.
contract Burner {
    fallback() external payable {
        while (true) {}
    }
}

/// Sends a message from inside a delivery, with the hop count it is told.
contract Forwarder {
    function forward(uint64 dest, uint8 hops) external {
        Outbox(XChain.OUTBOX)
            .sendMessage(
                dest, address(1), 100_000, hops, hex"", XChain.Callback(address(0), 0, bytes32(0))
            );
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
    address constant ORIGIN_SENDER = address(0xABCD);

    event MessageDelivered(uint64 indexed originChainId, uint64 indexed seq, bool success);
    event DeliveryUnderBudget(
        uint64 indexed originChainId, uint64 indexed seq, uint256 gasLeft, uint256 needed
    );
    event CallbackDropped(uint64 indexed originChainId, uint64 indexed seq, bytes reason);

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
        inbox.deliver(ORIGIN, seq, ORIGIN_SENDER, target, 0, 500_000, 2, data, cb);
    }

    function deliverWithGas(
        uint256 gas,
        uint64 seq,
        address target,
        uint64 gasLimit,
        bytes memory data,
        XChain.Callback memory cb
    ) internal returns (bool reverted) {
        vm.prank(XChain.txSender(ORIGIN));
        // solhint-disable-next-line no-empty-blocks
        try inbox.deliver{gas: gas}(ORIGIN, seq, ORIGIN_SENDER, target, 0, gasLimit, 2, data, cb) {}
        catch {
            reverted = true;
        }
    }

    /// The response leaf the Inbox must have committed for `(seq, ok, retHash,
    /// truncated)`.
    function responseLeaf(
        uint64 seq,
        bool ok,
        bytes32 retHash,
        bool truncated,
        XChain.Callback memory cb,
        uint64 responseSeq
    ) internal view returns (bytes32) {
        bytes memory responseData = abi.encodeWithSelector(
            inbox.RESPONSE_SELECTOR(), ORIGIN_SENDER, seq, ok, retHash, truncated, cb.context
        );
        return XChain.hashMessage(
            SELF,
            ORIGIN,
            responseSeq,
            XChain.INBOX,
            cb.target,
            0,
            cb.gasLimit,
            0, // responses carry a zero hop budget
            keccak256(responseData),
            bytes32(0) // responses never carry a callback
        );
    }

    function test_responseSelector_isPinned() public view {
        assertEq(
            inbox.RESPONSE_SELECTOR(),
            bytes4(keccak256("onXChainResult(address,uint64,bool,bytes32,bool,bytes32)"))
        );
    }

    function test_deliverExposesOriginIdentityToTarget() public {
        bytes memory payload = hex"1234";
        deliverAsOrigin(0, address(receiver), abi.encodeCall(Receiver.poke, (payload)), noCb());
        assertEq(receiver.seenOrigin(), ORIGIN);
        assertEq(receiver.seenSender(), ORIGIN_SENDER);
        assertEq(receiver.seenData(), payload);
        assertEq(inbox.delivered(ORIGIN, 0), 1);
        assertFalse(inbox.inDelivery());
        assertEq(inbox.currentHops(), 0);
    }

    function test_onlyAliasedOriginOutboxMayDeliver() public {
        vm.expectRevert("Inbox: not the origin outbox");
        inbox.deliver(ORIGIN, 0, address(1), address(receiver), 0, 100_000, 0, hex"", noCb());

        // Even the RIGHT aliased sender for the WRONG claimed origin fails.
        vm.prank(XChain.txSender(ORIGIN + 1));
        vm.expectRevert("Inbox: not the origin outbox");
        inbox.deliver(ORIGIN, 0, address(1), address(receiver), 0, 100_000, 0, hex"", noCb());
    }

    function test_doubleDeliveryRejected() public {
        deliverAsOrigin(7, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), noCb());
        vm.prank(XChain.txSender(ORIGIN));
        vm.expectRevert("Inbox: already delivered");
        inbox.deliver(ORIGIN, 7, ORIGIN_SENDER, address(receiver), 0, 100_000, 0, hex"", noCb());
    }

    function test_innerRevertRecordsFailure_deliveryStillSucceeds() public {
        deliverAsOrigin(
            1, address(receiver), abi.encodeCall(Receiver.alwaysReverts, (hex"")), noCb()
        );
        assertEq(inbox.delivered(ORIGIN, 1), 2);
    }

    function test_xDomainSenderRevertsOutsideDelivery() public {
        vm.expectRevert("Inbox: no delivery in progress");
        inbox.xDomainSender();
    }

    /// Audit H5: the response names the requester and the request seq.
    function test_callbackEnqueuedThroughOwnOutbox_identifiesRequesterAndSeq() public {
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(42)));
        deliverAsOrigin(2, address(receiver), abi.encodeCall(Receiver.poke, (hex"55")), cb);

        // The response consumed seq 0 of this chain's outbox lane back to the
        // origin, committed with a ZERO callback hash and ZERO hops.
        assertEq(outbox.nonces(ORIGIN), 1);
        assertTrue(outbox.sentMessages(responseLeaf(2, true, keccak256(hex""), false, cb, 0)));
        // A response for another seq, or with the other status, is a
        // different commitment.
        assertFalse(outbox.sentMessages(responseLeaf(3, true, keccak256(hex""), false, cb, 0)));
        assertFalse(outbox.sentMessages(responseLeaf(2, false, keccak256(hex""), false, cb, 0)));
    }

    /// The return data hash covers what the target returned.
    function test_responseCarriesReturnDataHash() public {
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(1)));
        bytes memory payload = hex"C0FFEE";
        deliverAsOrigin(0, address(receiver), abi.encodeCall(Receiver.echo, (payload)), cb);
        bytes32 retHash = keccak256(abi.encode(payload));
        assertTrue(outbox.sentMessages(responseLeaf(0, true, retHash, false, cb, 0)));
    }

    /// Audit H4 (b): a return-data bomb is hashed through a bounded copy.
    function test_returnDataBomb_isBoundedAndFlagged() public {
        Bomb bomb = new Bomb();
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(9)));
        uint256 size = 196 * 1024;
        vm.prank(XChain.txSender(ORIGIN));
        inbox.deliver(
            ORIGIN,
            0,
            ORIGIN_SENDER,
            address(bomb),
            0,
            2_000_000,
            0,
            abi.encodeCall(Bomb.boom, (size)),
            cb
        );
        assertEq(inbox.delivered(ORIGIN, 0), 1, "the bomb does not fail the delivery");

        bytes memory full = abi.encode(bomb.boom(size));
        bytes memory prefix = new bytes(inbox.MAX_RETURN_BYTES());
        for (uint256 i = 0; i < prefix.length; i++) {
            prefix[i] = full[i];
        }
        assertTrue(outbox.sentMessages(responseLeaf(0, true, keccak256(prefix), true, cb, 0)));
    }

    /// Return data exactly at the bound is not truncated.
    function test_returnDataAtBound_isNotTruncated() public {
        Bomb bomb = new Bomb();
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(9)));
        // abi.encode(bytes) = offset word + length word + padded data.
        uint256 size = inbox.MAX_RETURN_BYTES() - 64;
        deliverAsOrigin(0, address(bomb), abi.encodeCall(Bomb.boom, (size)), cb);
        bytes memory full = abi.encode(bomb.boom(size));
        assertEq(full.length, inbox.MAX_RETURN_BYTES());
        assertTrue(outbox.sentMessages(responseLeaf(0, true, keccak256(full), false, cb, 0)));
    }

    // ── nextSeq (audit L9)

    function test_nextSeqIsSeqPlusOne_evenMidHistory() public {
        assertEq(inbox.nextSeq(ORIGIN), 0);
        deliverAsOrigin(5, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), noCb());
        assertEq(inbox.nextSeq(ORIGIN), 6, "a lane that starts mid-history reports seq + 1");

        // A second origin's lane counts alone.
        uint64 other = ORIGIN + 1;
        vm.prank(XChain.txSender(other));
        inbox.deliver(other, 0, ORIGIN_SENDER, address(receiver), 0, 500_000, 0, hex"", noCb());
        assertEq(inbox.nextSeq(other), 1);
        assertEq(inbox.nextSeq(ORIGIN), 6, "another origin's delivery must not move this lane");

        deliverAsOrigin(6, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), noCb());
        assertEq(inbox.nextSeq(ORIGIN), 7);
    }

    function test_nextSeqCountsInnerRevertsToo() public {
        // Status 2 is still a DELIVERY — the message was consumed from the
        // lane, so the cursor view must advance past it.
        deliverAsOrigin(
            0, address(receiver), abi.encodeCall(Receiver.alwaysReverts, (hex"")), noCb()
        );
        assertEq(inbox.delivered(ORIGIN, 0), 2);
        assertEq(inbox.nextSeq(ORIGIN), 1);
    }

    function test_nextSeqIsTheFirstUndeliveredSeq() public {
        for (uint64 seq = 0; seq < 3; seq++) {
            deliverAsOrigin(seq, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), noCb());
            assertEq(inbox.nextSeq(ORIGIN), seq + 1);
        }
        // A rejected double delivery must NOT count.
        vm.prank(XChain.txSender(ORIGIN));
        vm.expectRevert("Inbox: already delivered");
        inbox.deliver(ORIGIN, 2, ORIGIN_SENDER, address(receiver), 0, 100_000, 0, hex"", noCb());
        assertEq(inbox.nextSeq(ORIGIN), 3);
    }

    function test_failureAlsoTriggersCallback() public {
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(7)));
        deliverAsOrigin(3, address(receiver), abi.encodeCall(Receiver.alwaysReverts, (hex"")), cb);
        assertEq(inbox.delivered(ORIGIN, 3), 2);
        // A response was still enqueued — origin apps never wait forever.
        assertEq(outbox.nonces(ORIGIN), 1);
        bytes memory reason = abi.encodeWithSignature("Error(string)", "nope");
        assertTrue(outbox.sentMessages(responseLeaf(3, false, keccak256(reason), false, cb, 0)));
    }

    // ── storage layout pins (audit L5)

    function test_storageLayout_isPinned() public {
        deliverAsOrigin(4, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), noCb());
        bytes32 inner = keccak256(abi.encode(uint256(ORIGIN), uint256(0)));
        bytes32 deliveredSlot = keccak256(abi.encode(uint256(4), inner));
        assertEq(uint256(vm.load(address(inbox), deliveredSlot)), 1, "delivered at slot 0");
        bytes32 nextSeqSlot = keccak256(abi.encode(uint256(ORIGIN), uint256(1)));
        assertEq(uint256(vm.load(address(inbox), nextSeqSlot)), 5, "nextSeq at slot 1");
        assertEq(uint256(vm.load(address(inbox), bytes32(uint256(2)))), 0, "slot 2 cleared");
    }

    // ── hop rule (audit H6)

    function test_hopsInsideDelivery_mustBeOneLess() public {
        Forwarder fwd = new Forwarder();
        uint64 dest = 777;

        // Delivery with hops 2: a send with hops 1 passes.
        deliverAsOrigin(0, address(fwd), abi.encodeCall(Forwarder.forward, (dest, 1)), noCb());
        assertEq(inbox.delivered(ORIGIN, 0), 1);
        assertEq(outbox.nonces(dest), 1);

        // hops 2 (same) and hops 0 (skipping a level) fail the inner call.
        deliverAsOrigin(1, address(fwd), abi.encodeCall(Forwarder.forward, (dest, 2)), noCb());
        assertEq(inbox.delivered(ORIGIN, 1), 2);
        deliverAsOrigin(2, address(fwd), abi.encodeCall(Forwarder.forward, (dest, 0)), noCb());
        assertEq(inbox.delivered(ORIGIN, 2), 2);
        assertEq(outbox.nonces(dest), 1);
    }

    function test_hopsZeroDelivery_cannotSend() public {
        Forwarder fwd = new Forwarder();
        vm.prank(XChain.txSender(ORIGIN));
        inbox.deliver(
            ORIGIN,
            0,
            ORIGIN_SENDER,
            address(fwd),
            0,
            500_000,
            0,
            abi.encodeCall(Forwarder.forward, (777, 0)),
            noCb()
        );
        assertEq(inbox.delivered(ORIGIN, 0), 2);
        assertEq(outbox.nonces(777), 0);
    }

    /// The Inbox's own response is sent after the delivery state is
    /// cleared, so it passes the outside-delivery rule with hops 0 even for
    /// a hops-0 delivery.
    function test_responseIsSentWithZeroHops_afterHopsZeroDelivery() public {
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(5)));
        vm.prank(XChain.txSender(ORIGIN));
        inbox.deliver(
            ORIGIN,
            0,
            ORIGIN_SENDER,
            address(receiver),
            0,
            500_000,
            0,
            abi.encodeCall(Receiver.poke, (hex"")),
            cb
        );
        assertEq(outbox.nonces(ORIGIN), 1);
        assertTrue(outbox.sentMessages(responseLeaf(0, true, keccak256(hex""), false, cb, 0)));
    }

    // ── audit H4 (a): under-budget deliveries never revert

    function test_underBudget_recordsStatus2AndSendsCallback() public {
        Burner burner = new Burner();
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(3)));
        uint64 gasLimit = 1_000_000;
        // Well below gasLimit + gasLimit / 63 + RESERVE, but enough for the
        // bookkeeping.
        uint256 gas = inbox.RESERVE() + 300_000;
        uint256 needed = uint256(gasLimit) + uint256(gasLimit) / 63 + inbox.RESERVE();

        vm.expectEmit(true, true, false, false);
        emit DeliveryUnderBudget(ORIGIN, 0, 0, needed);
        vm.expectEmit(true, true, true, true);
        emit MessageDelivered(ORIGIN, 0, false);
        bool reverted = deliverWithGas(gas, 0, address(burner), gasLimit, hex"", cb);
        assertFalse(reverted, "an under-budget delivery must not revert");
        assertEq(inbox.delivered(ORIGIN, 0), 2);
        assertEq(inbox.nextSeq(ORIGIN), 1);
        assertEq(outbox.nonces(ORIGIN), 1, "the callback still goes out");
        assertTrue(outbox.sentMessages(responseLeaf(0, false, keccak256(hex""), false, cb, 0)));
    }

    // ── audit H4 (c): a rejected callback never drops the delivery record

    function fillReturnLaneGasBudget() internal {
        uint64 big = outbox.MAX_MESSAGE_GAS();
        uint256 charge = outbox.deliveryGasCharge(big, hex"", 0);
        uint256 used;
        while (used + charge <= outbox.MAX_BLOCK_DEST_GAS()) {
            outbox.sendMessage(ORIGIN, address(1), big, 0, hex"", noCb());
            used += charge;
        }
        uint256 room = outbox.MAX_BLOCK_DEST_GAS() - used;
        uint256 base = outbox.deliveryGasCharge(0, hex"", 0);
        if (room >= base) {
            outbox.sendMessage(ORIGIN, address(1), uint64(room - base), 0, hex"", noCb());
        }
    }

    function test_callbackRejectedByOutbox_isDroppedWithEvent_recordStands() public {
        fillReturnLaneGasBudget();
        uint64 before = outbox.nonces(ORIGIN);
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(8)));

        vm.expectEmit(true, true, false, false);
        emit CallbackDropped(ORIGIN, 0, hex"");
        deliverAsOrigin(0, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), cb);
        assertEq(inbox.delivered(ORIGIN, 0), 1);
        assertEq(inbox.nextSeq(ORIGIN), 1);
        assertEq(outbox.nonces(ORIGIN), before, "no response was enqueued");

        // Next block: the same callback would go through.
        vm.roll(block.number + 1);
        deliverAsOrigin(1, address(receiver), abi.encodeCall(Receiver.poke, (hex"")), cb);
        assertEq(outbox.nonces(ORIGIN), before + 1);
    }

    // ── gas reserve and delivery overhead (audit H4, pinned)

    function worstCaseData() internal pure returns (bytes memory data) {
        data = new bytes(65_536);
        for (uint256 i = 0; i < data.length; i++) {
            data[i] = 0xEE;
        }
    }

    /// Runs `deliver` with exactly `gas` from a snapshot. Returns (reverted,
    /// status, callbackEnqueued, underBudget).
    function probeAt(uint256 gas, address target, uint64 gasLimit, bytes memory data)
        internal
        returns (bool reverted, uint8 status, bool enqueued, bool underBudget)
    {
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(1)));
        uint256 snap = vm.snapshotState();
        vm.recordLogs();
        reverted = deliverWithGas(gas, 0, target, gasLimit, data, cb);
        status = inbox.delivered(ORIGIN, 0);
        enqueued = outbox.nonces(ORIGIN) == 1;
        Vm.Log[] memory logs = vm.getRecordedLogs();
        for (uint256 i = 0; i < logs.length; i++) {
            if (logs[i].topics[0] == DeliveryUnderBudget.selector) underBudget = true;
        }
        vm.revertToState(snap);
    }

    /// Smallest gas at which a full-budget delivery (status 1 or an inner
    /// out-of-gas with the full `gasLimit` handed over, callback enqueued,
    /// no under-budget path) succeeds.
    function minFullDeliveryGas(address target, uint64 gasLimit, bytes memory data)
        internal
        returns (uint256)
    {
        uint256 lo = gasLimit;
        uint256 hi = uint256(gasLimit) + 2_000_000;
        (bool r, uint8 s, bool e, bool u) = probeAt(hi, target, gasLimit, data);
        assertTrue(!r && s != 0 && e && !u, "hi must be a full delivery");
        while (hi - lo > 1) {
            uint256 mid = (lo + hi) / 2;
            (r, s, e, u) = probeAt(mid, target, gasLimit, data);
            if (!r && s != 0 && e && !u) hi = mid;
            else lo = mid;
        }
        return hi;
    }

    /// Smallest gas at which the delivery is recorded and the callback is
    /// enqueued, through any path.
    function minRecordedDeliveryGas(address target, uint64 gasLimit, bytes memory data)
        internal
        returns (uint256)
    {
        uint256 lo = 0;
        uint256 hi = uint256(gasLimit) + 2_000_000;
        while (hi - lo > 1) {
            uint256 mid = (lo + hi) / 2;
            (bool r, uint8 s, bool e,) = probeAt(mid, target, gasLimit, data);
            if (!r && s != 0 && e) hi = mid;
            else lo = mid;
        }
        return hi;
    }

    /// DELIVERY_OVERHEAD covers the worst case with a 20% margin: cold lane,
    /// callback, 64 KiB data, the full MAX_MESSAGE_GAS forwarded and burned,
    /// return data above the copy bound.
    function test_deliveryOverhead_coversWorstCase_withMargin() public {
        GasProbe probe = new GasProbe();
        uint64 gasLimit = outbox.MAX_MESSAGE_GAS();
        bytes memory data = worstCaseData();
        uint256 minGas = minFullDeliveryGas(address(probe), gasLimit, data);
        uint256 needed = minGas - gasLimit;
        emit log_named_uint("worst-case overhead needed", needed);
        uint256 overhead = outbox.DELIVERY_OVERHEAD();
        assertLe(needed * 12 / 10, overhead, "DELIVERY_OVERHEAD must hold a 20% margin");
        assertGe(needed * 13 / 10, overhead, "DELIVERY_OVERHEAD is more than 30% above the need");
    }

    /// At exactly gasLimit + DELIVERY_OVERHEAD (the executor's budget minus
    /// the calldata intrinsic gas, which forge's internal call does not
    /// charge) the target receives its full gasLimit.
    function test_deliveryAtExecutorBudget_handsOverFullGasLimit() public {
        GasProbe probe = new GasProbe();
        uint64 gasLimit = outbox.MAX_MESSAGE_GAS();
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(1)));
        uint256 gas = uint256(gasLimit) + outbox.DELIVERY_OVERHEAD();
        bool reverted = deliverWithGas(gas, 0, address(probe), gasLimit, worstCaseData(), cb);
        assertFalse(reverted);
        assertEq(inbox.delivered(ORIGIN, 0), 1);
        assertGe(probe.entryGas(), uint256(gasLimit) - 200, "the target got its full budget");
        assertEq(outbox.nonces(ORIGIN), 1, "the callback was enqueued");
        assertTrue(
            outbox.sentMessages(responseLeaf(0, true, keccak256(new bytes(256)), true, cb, 0))
        );
    }

    /// Same budget, a target that burns everything: status 2, callback out.
    function test_deliveryAtExecutorBudget_burnerYieldsStatus2WithCallback() public {
        Burner burner = new Burner();
        uint64 gasLimit = outbox.MAX_MESSAGE_GAS();
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 300_000, bytes32(uint256(1)));
        uint256 gas = uint256(gasLimit) + outbox.DELIVERY_OVERHEAD();
        vm.recordLogs();
        bool reverted = deliverWithGas(gas, 0, address(burner), gasLimit, worstCaseData(), cb);
        assertFalse(reverted);
        assertEq(inbox.delivered(ORIGIN, 0), 2);
        assertEq(outbox.nonces(ORIGIN), 1);
        Vm.Log[] memory logs = vm.getRecordedLogs();
        for (uint256 i = 0; i < logs.length; i++) {
            assertTrue(logs[i].topics[0] != DeliveryUnderBudget.selector, "not under budget");
        }
    }

    /// RESERVE covers the post-call work with a margin: at gasLimit 0 the
    /// check reduces to `gasleft() >= RESERVE`, and the callback must still
    /// be enqueued at that boundary.
    function test_reserve_coversPostCallWork_withMargin() public {
        Burner burner = new Burner();
        bytes memory data = worstCaseData();
        uint256 full = minFullDeliveryGas(address(burner), 0, data);
        uint256 recorded = minRecordedDeliveryGas(address(burner), 0, data);
        // `full - recorded` is the reserve's margin over the work after the
        // check (see the Inbox docs).
        emit log_named_uint("gas for a recorded delivery at gasLimit 0", recorded);
        emit log_named_uint("gas for a full delivery at gasLimit 0", full);
        assertGe(full, recorded);
        assertGe(full - recorded, inbox.RESERVE() / 6, "RESERVE margin below ~16%");
        // One unit below the full boundary takes the under-budget path and
        // still records the delivery with its callback.
        (bool r, uint8 s, bool e, bool u) = probeAt(full - 1, address(burner), 0, data);
        assertTrue(!r && s == 2 && e && u, "under-budget path at full - 1");
    }
}
