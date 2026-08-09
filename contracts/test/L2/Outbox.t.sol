// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
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
        bytes32 dh,
        bytes32 ch
    ) external pure returns (bytes32) {
        return XChain.hashMessage(o, d, s, sender, target, v, g, dh, ch);
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
}

contract OutboxTest is Test {
    Outbox outbox;
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
        bytes data,
        bytes32 msgHash,
        XChain.Callback callback
    );

    function setUp() public {
        vm.chainId(SELF);
        outbox = new Outbox();
        h = new XChainHarness();
    }

    function noCb() internal pure returns (XChain.Callback memory) {
        return XChain.Callback(address(0), 0, bytes32(0));
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
            keccak256(hex"08"),
            bytes32(0)
        );
        assertEq(leaf, bytes32(0x0df14340efd8c8b32f4c333c3dca8470b0bae319a3dfe32adb213df2b8834d3c));
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

    function test_zeroCallback_hashesToZero_likeRustNone() public view {
        assertEq(h.hashCallback(noCb()), bytes32(0));
    }

    // ── behavior

    function test_seqIsDensePerDestination() public {
        assertEq(outbox.sendMessage(DEST, address(1), 100_000, hex"01", noCb()), 0);
        assertEq(outbox.sendMessage(DEST, address(1), 100_000, hex"02", noCb()), 1);
        assertEq(outbox.sendMessage(DEST + 1, address(1), 100_000, hex"03", noCb()), 0);
        assertEq(outbox.nonces(DEST), 2);
        assertEq(outbox.nonces(DEST + 1), 1);
    }

    function test_sendRecordsCommitmentAndEmits() public {
        bytes memory data = hex"CAFE";
        bytes32 expected = h.hashMessage(
            SELF, DEST, 0, address(this), address(0xBEEF), 0, 200_000, keccak256(data), bytes32(0)
        );
        vm.expectEmit(true, true, true, true);
        emit MessageSent(DEST, 0, address(this), address(0xBEEF), 0, 200_000, data, expected, noCb());
        outbox.sendMessage(DEST, address(0xBEEF), 200_000, data, noCb());
        assertTrue(outbox.sentMessages(expected));
    }

    function test_rejectsSelfAndZeroDestination() public {
        vm.expectRevert("Outbox: bad destination");
        outbox.sendMessage(SELF, address(1), 100_000, hex"", noCb());
        vm.expectRevert("Outbox: bad destination");
        outbox.sendMessage(0, address(1), 100_000, hex"", noCb());
    }

    function test_rejectsGasAboveCap() public {
        uint64 overCap = outbox.MAX_MESSAGE_GAS() + 1;
        vm.expectRevert("Outbox: gas limit above cap");
        outbox.sendMessage(DEST, address(1), overCap, hex"", noCb());
    }

    function test_rejectsValueUntilBurnMintShips() public {
        vm.deal(address(this), 1 ether);
        vm.expectRevert("Outbox: value transfer not enabled");
        outbox.sendMessage{value: 1}(DEST, address(1), 100_000, hex"", noCb());
    }
}
