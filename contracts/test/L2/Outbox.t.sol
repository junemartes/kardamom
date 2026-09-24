// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {CalldataGas} from "../../src/common/CalldataGas.sol";
import {Inbox} from "../../src/L2/Inbox.sol";
import {Outbox} from "../../src/L2/Outbox.sol";
import {XChain} from "../../src/L2/XChain.sol";

contract XChainHarness {
    function hashMessage(
        uint64 o,
        uint64 d,
        uint64 s,
        address sender,
        address target,
        uint256 v,
        uint64 g,
        uint8 hops,
        bytes32 dh,
        bytes32 ch
    ) external pure returns (bytes32) {
        return XChain.hashMessage(o, d, s, sender, target, v, g, hops, dh, ch);
    }

    function hashCallback(XChain.Callback calldata cb) external pure returns (bytes32) {
        return XChain.hashCallback(cb);
    }

    function aliasRemote(uint64 o, address a) external pure returns (address) {
        return XChain.aliasRemote(o, a);
    }

    function txSender(uint64 o) external pure returns (address) {
        return XChain.txSender(o);
    }

    function countNonZero(bytes calldata data) external pure returns (uint256) {
        return CalldataGas.countNonZero(data);
    }

    /// `trailing` sits after `data` in calldata; it must never count.
    function countNonZeroWithTrailing(bytes calldata data, bytes calldata trailing)
        external
        pure
        returns (uint256)
    {
        require(trailing.length > 0, "harness: trailing");
        return CalldataGas.countNonZero(data);
    }

    function intrinsicGas(bytes calldata data, uint256 extra) external pure returns (uint256) {
        return CalldataGas.intrinsicGas(data, extra);
    }
}

contract OutboxTest is Test {
    // Predeploy-addressed instances: the Outbox reads the Inbox's delivery
    // state through the constant XChain.INBOX, so both contracts are etched
    // at their canonical addresses, exactly as genesis seeds them.
    Outbox outbox;
    Inbox inbox;
    XChainHarness h;

    uint64 constant SELF = 412_346;
    uint64 constant DEST = 412_347;

    event MessageSent(
        uint64 indexed destChainId,
        uint64 indexed seq,
        address indexed sender,
        address target,
        uint256 value,
        uint64 gasLimit,
        uint8 hops,
        bytes data,
        bytes32 msgHash,
        XChain.Callback callback
    );

    function setUp() public {
        vm.chainId(SELF);
        vm.etch(XChain.OUTBOX, address(new Outbox()).code);
        vm.etch(XChain.INBOX, address(new Inbox()).code);
        outbox = Outbox(XChain.OUTBOX);
        inbox = Inbox(XChain.INBOX);
        h = new XChainHarness();
    }

    function noCb() internal pure returns (XChain.Callback memory) {
        return XChain.Callback(address(0), 0, bytes32(0));
    }

    function send(uint64 gasLimit, bytes memory data) internal returns (uint64) {
        return outbox.sendMessage(DEST, address(1), gasLimit, 0, data, noCb());
    }

    // ── cross-language vectors: values computed by kardamom-types xchain.rs.
    // A failure here means Rust and Solidity disagree on a byte layout —
    // a chain-splitting bug, never "just update the constant on one side".

    function test_leafVector_matchesRust() public view {
        bytes32 leaf = h.hashMessage(
            1,
            2,
            3,
            address(0x0404040404040404040404040404040404040404),
            address(0x0505050505050505050505050505050505050505),
            6,
            7,
            9,
            keccak256(hex"08"),
            bytes32(0)
        );
        assertEq(leaf, bytes32(0x3bffc28cda803d8ff97702a187a7218fd692b9f36b265256736d6c159f89696f));
    }

    function test_leafDomain_isV1() public pure {
        assertEq(XChain.LEAF_DOMAIN, keccak256("KARDAMOM_XCHAIN_MESSAGE_V1"));
    }

    function test_callbackVector_matchesRust() public view {
        bytes32 ch = h.hashCallback(
            XChain.Callback(
                address(0x0101010101010101010101010101010101010101),
                100_000,
                bytes32(0x0202020202020202020202020202020202020202020202020202020202020202)
            )
        );
        assertEq(ch, bytes32(0x3cc4851e518423fb0983f20dc6198ffd6ef901107d7b7911ffc4e8f942442b05));
    }

    function test_aliasVectors_matchRust() public view {
        assertEq(
            h.aliasRemote(SELF, address(0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa)),
            address(0xaa1fBDC71f2E2531F6704eDfF74f45bFf135dB61)
        );
        assertEq(h.txSender(SELF), address(0x32122ab04da66c349463091CfDA2773e379f678b));
    }

    /// The zero struct hashes to ZERO, on both sides (audit M5). A nonzero
    /// struct never does.
    function test_zeroCallback_hashesToZero_likeRustNone() public view {
        assertEq(h.hashCallback(noCb()), bytes32(0));
        assertTrue(h.hashCallback(XChain.Callback(address(0), 1, bytes32(0))) != bytes32(0));
    }

    // ── storage layout pins (audit L5). The validator's BAL cross-check and
    // the e2e scenarios compute these slots; a field inserted above one of
    // them halts every validator on the first send.

    function test_storageLayout_isPinned() public {
        send(100_000, hex"01");
        bytes32 nonceSlot = keccak256(abi.encode(uint256(DEST), uint256(0)));
        assertEq(uint256(vm.load(address(outbox), nonceSlot)), 1, "nonces at slot 0");

        bytes32 leaf = XChain.hashMessage(
            SELF, DEST, 0, address(this), address(1), 0, 100_000, 0, keccak256(hex"01"), 0
        );
        bytes32 sentSlot = keccak256(abi.encode(leaf, uint256(1)));
        assertEq(uint256(vm.load(address(outbox), sentSlot)), 1, "sentMessages at slot 1");

        bytes32 gasInner = keccak256(abi.encode(uint256(DEST), uint256(2)));
        bytes32 gasSlot = keccak256(abi.encode(block.number, gasInner));
        assertEq(
            uint256(vm.load(address(outbox), gasSlot)),
            outbox.deliveryGasCharge(100_000, hex"01", 0),
            "blockGas at slot 2"
        );

        bytes32 bytesInner = keccak256(abi.encode(uint256(DEST), uint256(3)));
        bytes32 bytesSlot = keccak256(abi.encode(block.number, bytesInner));
        assertEq(
            uint256(vm.load(address(outbox), bytesSlot)),
            outbox.MESSAGE_WIRE_OVERHEAD() + 1,
            "blockBytes at slot 3"
        );
    }

    // ── behavior

    function test_seqIsDensePerDestination() public {
        assertEq(send(100_000, hex"01"), 0);
        assertEq(send(100_000, hex"02"), 1);
        assertEq(outbox.sendMessage(DEST + 1, address(1), 100_000, 0, hex"03", noCb()), 0);
        assertEq(outbox.nonces(DEST), 2);
        assertEq(outbox.nonces(DEST + 1), 1);
    }

    function test_sendRecordsCommitmentAndEmits() public {
        bytes memory data = hex"CAFE";
        bytes32 expected = h.hashMessage(
            SELF,
            DEST,
            0,
            address(this),
            address(0xBEEF),
            0,
            200_000,
            3,
            keccak256(data),
            bytes32(0)
        );
        vm.expectEmit(true, true, true, true);
        emit MessageSent(
            DEST, 0, address(this), address(0xBEEF), 0, 200_000, 3, data, expected, noCb()
        );
        outbox.sendMessage(DEST, address(0xBEEF), 200_000, 3, data, noCb());
        assertTrue(outbox.sentMessages(expected));
    }

    function test_rejectsSelfAndZeroDestination() public {
        vm.expectRevert("Outbox: bad destination");
        outbox.sendMessage(SELF, address(1), 100_000, 0, hex"", noCb());
        vm.expectRevert("Outbox: bad destination");
        outbox.sendMessage(0, address(1), 100_000, 0, hex"", noCb());
    }

    function test_rejectsGasAboveCap() public {
        uint64 overCap = outbox.MAX_MESSAGE_GAS() + 1;
        vm.expectRevert("Outbox: gas limit above cap");
        outbox.sendMessage(DEST, address(1), overCap, 0, hex"", noCb());
    }

    /// Audit H4: the callback gas is validated on the paid side, so the
    /// Inbox's response send can never trip the destination Outbox's cap.
    function test_rejectsCallbackGasAboveCap() public {
        uint64 overCap = outbox.MAX_MESSAGE_GAS() + 1;
        XChain.Callback memory cb = XChain.Callback(address(0xCB), overCap, bytes32(uint256(1)));
        vm.expectRevert("Outbox: callback gas limit above cap");
        outbox.sendMessage(DEST, address(1), 100_000, 0, hex"", cb);

        cb.gasLimit = outbox.MAX_MESSAGE_GAS();
        outbox.sendMessage(DEST, address(1), 100_000, 0, hex"", cb);
    }

    function test_rejectsValueUntilBurnMintShips() public {
        vm.deal(address(this), 1 ether);
        vm.expectRevert("Outbox: value transfer not enabled");
        outbox.sendMessage{value: 1}(DEST, address(1), 100_000, 0, hex"", noCb());
    }

    /// Audit H6: outside a delivery, a send may carry at most MAX_HOPS.
    function test_hopsOutsideDelivery_cappedAtMaxHops() public {
        uint8 max = outbox.MAX_HOPS();
        assertEq(max, 4);
        outbox.sendMessage(DEST, address(1), 100_000, max, hex"", noCb());
        vm.expectRevert("Outbox: hops above cap");
        outbox.sendMessage(DEST, address(1), 100_000, max + 1, hex"", noCb());
    }

    // ── per-block budgets (audit C3)

    function test_gasBudget_boundary() public {
        uint256 cap = outbox.MAX_BLOCK_DEST_GAS();
        assertEq(cap, 30_000_000);
        uint256 charge = outbox.deliveryGasCharge(outbox.MAX_MESSAGE_GAS(), hex"", 0);
        // Fill the block with full-size sends until one more would overflow.
        uint256 used;
        while (used + charge <= cap) {
            send(outbox.MAX_MESSAGE_GAS(), hex"");
            used += charge;
        }
        assertEq(outbox.blockGas(DEST, block.number), used);

        // Exactly the remaining budget passes.
        uint256 room = cap - used;
        uint256 base = outbox.deliveryGasCharge(0, hex"", 0);
        assertGt(room, base, "test setup: room must exceed the fixed charge");
        uint64 fit = uint64(room - base);
        send(fit, hex"");
        assertEq(outbox.blockGas(DEST, block.number), cap);

        // One more gas unit fails; a different destination and the next
        // block are unaffected.
        vm.expectRevert("Outbox: destination block gas budget");
        send(0, hex"");
        outbox.sendMessage(DEST + 1, address(1), 0, 0, hex"", noCb());
        vm.roll(block.number + 1);
        send(0, hex"");
    }

    /// The callback gas counts against the same budget.
    function test_gasBudget_countsCallbackGas() public {
        XChain.Callback memory cb = XChain.Callback(address(0xCB), 1_000_000, bytes32(uint256(1)));
        outbox.sendMessage(DEST, address(1), 2_000_000, 0, hex"", cb);
        assertEq(
            outbox.blockGas(DEST, block.number),
            outbox.deliveryGasCharge(2_000_000, hex"", 1_000_000)
        );
        assertEq(
            outbox.deliveryGasCharge(2_000_000, hex"", 1_000_000),
            outbox.deliveryGasCharge(2_000_000, hex"", 0) + 1_000_000
        );
    }

    /// The gas charge is what the executor allots: the inner-call budget,
    /// the fixed overhead, and the intrinsic gas of the delivery calldata
    /// with its head counted as nonzero bytes.
    function test_gasCharge_isTheExecutorBudget() public view {
        // Must equal `kardamom_exec_core::XCHAIN_DELIVERY_OVERHEAD`.
        assertEq(outbox.DELIVERY_OVERHEAD(), 462_000);
        bytes memory data = hex"00ff";
        uint256 head = outbox.DELIVER_HEAD_BYTES();
        assertEq(head, 388);
        uint256 nonZero = 1 + head;
        uint256 zero = 1;
        uint256 standard = 21_000 + 16 * nonZero + 4 * zero;
        uint256 floor = 21_000 + 10 * (zero + 4 * nonZero);
        uint256 intrinsic = standard > floor ? standard : floor;
        assertEq(
            outbox.deliveryGasCharge(5, data, 7), 5 + outbox.DELIVERY_OVERHEAD() + intrinsic + 7
        );
    }

    function test_byteBudget_boundary() public {
        uint256 cap = outbox.MAX_BLOCK_DEST_BYTES();
        uint256 per = outbox.MESSAGE_WIRE_OVERHEAD();
        assertEq(cap, 600_000);
        assertEq(per, 170);
        // 600_060 (one record at the cap) stays under the DA record cap.
        assertLe(cap + 60, 630_784);

        uint256 maxData = outbox.MAX_DATA_BYTES();
        bytes memory big = new bytes(maxData);
        uint256 used;
        while (used + per + maxData <= cap) {
            send(0, big);
            used += per + maxData;
        }
        assertEq(outbox.blockBytes(DEST, block.number), used);

        uint256 room = cap - used - per;
        send(0, new bytes(room));
        assertEq(outbox.blockBytes(DEST, block.number), cap);

        vm.expectRevert("Outbox: destination block byte budget");
        send(0, hex"");
        outbox.sendMessage(DEST + 1, address(1), 0, 0, hex"", noCb());
        vm.roll(block.number + 1);
        send(0, hex"");
    }

    // ── CalldataGas

    function naiveNonZero(bytes memory data) internal pure returns (uint256 n) {
        for (uint256 i = 0; i < data.length; i++) {
            if (data[i] != 0) n++;
        }
    }

    function test_countNonZero_matchesNaive() public view {
        uint256[6] memory lens = [uint256(0), 1, 31, 32, 33, 100];
        for (uint256 k = 0; k < lens.length; k++) {
            bytes memory data = new bytes(lens[k]);
            for (uint256 i = 0; i < data.length; i++) {
                // A mix of zero and nonzero bytes, including 0x80 and 0xff.
                data[i] = bytes1(uint8((i * 37) % 5 == 0 ? 0 : (i * 91 + 128) % 256));
            }
            assertEq(h.countNonZero(data), naiveNonZero(data), "length case");
        }
        assertEq(
            h.countNonZero(hex"0000000000000000000000000000000000000000000000000000000000000000"), 0
        );
        assertEq(
            h.countNonZero(hex"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            32
        );
    }

    function test_countNonZero_ignoresBytesAfterData() public view {
        assertEq(h.countNonZeroWithTrailing(hex"0001", hex"ffffffffff"), 1);
        assertEq(h.countNonZeroWithTrailing(hex"", hex"ff"), 0);
    }

    function test_intrinsicGas_isMaxOfStandardAndFloor() public view {
        // Empty calldata: both rules give the base.
        assertEq(h.intrinsicGas(hex"", 0), 21_000);
        // Ten nonzero bytes: standard 21_160, floor 21_400.
        assertEq(h.intrinsicGas(hex"01020304050607080910", 0), 21_400);
        // Ten zero bytes: standard 21_040, floor 21_100.
        assertEq(h.intrinsicGas(hex"00000000000000000000", 0), 21_100);
        // Extra nonzero bytes are added to the count.
        assertEq(h.intrinsicGas(hex"", 388), 21_000 + 10 * 4 * 388);
    }
}
