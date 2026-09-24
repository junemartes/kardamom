// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {CalldataGas} from "../common/CalldataGas.sol";
import {XChain} from "./XChain.sol";

/// @title Outbox
/// @notice Predeploy that initiates cross-chain messages to other Kardamom
///         chains (`docs/specs/interop-outbox-messaging-spec.md`). Storage
///         holds COMMITMENTS and BUDGETS only — per-destination dense
///         sequence counters, `sentMessages[msgHash]`, and the per-block
///         budgets — laid out append-only so the executor's BAL claims for
///         this address map 1:1 onto sends; the payload itself travels in
///         the `MessageSent` event / calldata and is re-hashed and
///         cross-checked by the validator's extractor (never trusted from
///         the event).
/// @dev    Genesis predeploy at `XChain.OUTBOX`: runtime bytecode seeded at a
///         fixed address, not upgradeable, no constructor-time state.
///
///         Storage layout (append-only; pinned by `vm.load` tests):
///           slot 0  `nonces`
///           slot 1  `sentMessages`   (the validator's BAL cross-check key)
///           slot 2  `blockGas`
///           slot 3  `blockBytes`
// `sendMessage` is payable ONLY for ABI stability with the value-transfer
// phase (spec §13); until that ships it requires `msg.value == 0`, so no ether
// can ever accumulate here and a withdraw function would be dead code. The
// L2ToL1MessagePasser carries the same suppression for the same reason.
// slither-disable-next-line locked-ether
contract Outbox {
    /// @notice Next sequence number per destination chain. Dense per pair —
    ///         the destination enforces its no-skip rule on exactly this
    ///         counter.
    mapping(uint64 => uint64) public nonces;

    /// @notice Sent message commitments. The BAL-extraction anchor: one new
    ///         `true` slot per send, never overwritten.
    mapping(bytes32 => bool) public sentMessages;

    /// @notice Destination gas charged per (destination, origin block). See
    ///         `MAX_BLOCK_DEST_GAS`.
    mapping(uint64 => mapping(uint256 => uint64)) public blockGas;

    /// @notice Wire bytes charged per (destination, origin block). See
    ///         `MAX_BLOCK_DEST_BYTES`.
    mapping(uint64 => mapping(uint256 => uint64)) public blockBytes;

    /// @notice Per-message destination gas ceiling, enforced here on the paid
    ///         side so destination quotas reject nothing that was accepted
    ///         for a fee. Applies to the inner call AND to the callback
    ///         response (audit H4): the response is a message on the return
    ///         lane, and the Inbox must be able to enqueue it.
    uint64 public constant MAX_MESSAGE_GAS = 10_000_000;

    /// @notice Payload size ceiling (spam bound; revisit with blob transport).
    uint256 public constant MAX_DATA_BYTES = 65_536;

    /// @notice Hop budget of a user-initiated send (audit H6). A send made
    ///         inside a delivery must carry exactly one hop less than the
    ///         delivery it runs in, so one paid send starts at most
    ///         `MAX_HOPS` derived sends along any chain of deliveries. The
    ///         paid send pays for that whole chain. The fan-out at each level
    ///         is bounded only by the per-block budgets below; the prepaid
    ///         callback leg (spec §12) is still pending.
    uint8 public constant MAX_HOPS = 4;

    /// @notice Gas the destination adds to `gasLimit` for one delivery, on
    ///         top of the calldata intrinsic gas. Must equal
    ///         `kardamom_exec_core::XCHAIN_DELIVERY_OVERHEAD`. Measured in
    ///         `test/L2/Inbox.t.sol` (worst case: cold lane, callback, 64 KiB
    ///         data, the full `MAX_MESSAGE_GAS` forwarded, return data above
    ///         the copy bound): 384_484 gas, of which 158_730 is the EIP-150
    ///         63/64 share of `MAX_MESSAGE_GAS`, 200_000 is `Inbox.RESERVE`,
    ///         and the rest is the work before the inner call. Pinned at
    ///         `384_484 * 1.2`, rounded up.
    uint64 public constant DELIVERY_OVERHEAD = 462_000;

    /// @notice Calldata bytes of `Inbox.deliver` that are not `data`: the
    ///         4-byte selector, 11 head words, and the length word of `data`.
    ///         The budget counts them as nonzero, the worst case.
    uint256 public constant DELIVER_HEAD_BYTES = 4 + 12 * 32;

    /// @notice Destination gas budget per (destination, origin block): one
    ///         destination block of work (`BLOCK_GAS_LIMIT` in
    ///         `crates/exec-core/src/block_env.rs`) per origin block (audit
    ///         C3). The destination derives one record per origin block and
    ///         executes it as one contiguous slot range, so the record must
    ///         fit in one block. Every send charges what the executor really
    ///         allots to its delivery: `gasLimit + DELIVERY_OVERHEAD +
    ///         intrinsic(data)`, plus `cb.gasLimit` for the response leg.
    uint64 public constant MAX_BLOCK_DEST_GAS = 30_000_000;

    /// @notice Wire bytes one message adds to the KAR1 DA frame on top of its
    ///         `data`: `source_hash` (32) + `seq` (8) + `origin_sender` (20) +
    ///         `target` (20) + `value` (16) + `gas_limit` (8) + `hops` (1) +
    ///         `input_len` (4) + callback flag (1) + callback body (60).
    uint64 public constant MESSAGE_WIRE_OVERHEAD = 170;

    /// @notice Wire byte budget per (destination, origin block), counted as
    ///         `Σ (MESSAGE_WIRE_OVERHEAD + data.length)`. One record must
    ///         always fit under `MAX_REMOTE_EPOCH_WIRE_BYTES = 630_784`
    ///         (`kardamom_types::xchain`). A record adds 60 fixed bytes:
    ///         `600_000 + 60 = 600_060 <= 630_784`, with 30_724 to spare.
    uint64 public constant MAX_BLOCK_DEST_BYTES = 600_000;

    /// @notice One send. The extractor decodes this, recomputes `msgHash`
    ///         from the fields, rejects drift, and cross-checks the
    ///         `sentMessages` storage write against the BAL claim.
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

    /// @notice Send a message to `target` on `destChainId`.
    /// @dev    `payable` for ABI stability with the value-transfer phase
    ///         (burn → mint, spec §13); until that ships nonzero value is
    ///         rejected rather than silently locked.
    ///
    ///         Hop rule (audit H6). Outside a delivery, `hops <= MAX_HOPS`.
    ///         Inside a delivery with hop budget `h`, the send must carry
    ///         `hops == h - 1`, and `h` must be nonzero. A delivery with
    ///         `hops == 0` cannot send at all. The Inbox's own response is
    ///         sent after the delivery state is cleared, with `hops == 0`.
    ///
    ///         Budgets (audit C3). The send charges the (destination, origin
    ///         block) gas and byte budgets; a send that would overflow either
    ///         reverts. The response the Inbox sends back is charged on the
    ///         responding chain in the same way.
    ///
    ///         No intrinsic-gas floor applies here: the destination sizes the
    ///         delivery tx from the calldata itself
    ///         (`kardamom_exec_core::xchain_gas_budget`), so `gasLimit` pays
    ///         only for the inner call.
    function sendMessage(
        uint64 destChainId,
        address target,
        uint64 gasLimit,
        uint8 hops,
        bytes calldata data,
        XChain.Callback calldata cb
    ) external payable returns (uint64 seq) {
        require(block.chainid <= type(uint64).max, "Outbox: chain id exceeds u64");
        uint64 originChainId = uint64(block.chainid);
        require(destChainId != 0 && destChainId != originChainId, "Outbox: bad destination");
        require(gasLimit <= MAX_MESSAGE_GAS, "Outbox: gas limit above cap");
        require(cb.gasLimit <= MAX_MESSAGE_GAS, "Outbox: callback gas limit above cap");
        require(data.length <= MAX_DATA_BYTES, "Outbox: data above cap");
        require(msg.value == 0, "Outbox: value transfer not enabled");

        _checkHops(hops);
        _chargeBudgets(destChainId, gasLimit, data, cb.gasLimit);

        seq = nonces[destChainId];
        nonces[destChainId] = seq + 1;

        bytes32 msgHash = XChain.hashMessage(
            originChainId,
            destChainId,
            seq,
            msg.sender,
            target,
            msg.value,
            gasLimit,
            hops,
            keccak256(data),
            XChain.hashCallback(cb)
        );
        sentMessages[msgHash] = true;
        emit MessageSent(
            destChainId, seq, msg.sender, target, msg.value, gasLimit, hops, data, msgHash, cb
        );
    }

    /// @notice What one send charges against the destination's per-block gas
    ///         budget. Exposed so apps can size a batch before sending.
    function deliveryGasCharge(uint64 gasLimit, bytes calldata data, uint64 cbGasLimit)
        public
        pure
        returns (uint256)
    {
        return uint256(gasLimit) + DELIVERY_OVERHEAD
            + CalldataGas.intrinsicGas(data, DELIVER_HEAD_BYTES) + cbGasLimit;
    }

    function _checkHops(uint8 hops) private view {
        if (XChain.inbox().inDelivery()) {
            uint8 current = XChain.inbox().currentHops();
            require(current > 0, "Outbox: hop budget exhausted");
            require(uint256(hops) + 1 == current, "Outbox: hops must be one less than current");
        } else {
            require(hops <= MAX_HOPS, "Outbox: hops above cap");
        }
    }

    function _chargeBudgets(
        uint64 destChainId,
        uint64 gasLimit,
        bytes calldata data,
        uint64 cbGasLimit
    ) private {
        uint256 gasUsed = blockGas[destChainId][block.number];
        uint256 gasCharge = deliveryGasCharge(gasLimit, data, cbGasLimit);
        require(gasUsed + gasCharge <= MAX_BLOCK_DEST_GAS, "Outbox: destination block gas budget");
        blockGas[destChainId][block.number] = uint64(gasUsed + gasCharge);

        uint256 bytesUsed = blockBytes[destChainId][block.number];
        uint256 byteCharge = MESSAGE_WIRE_OVERHEAD + data.length;
        require(
            bytesUsed + byteCharge <= MAX_BLOCK_DEST_BYTES, "Outbox: destination block byte budget"
        );
        blockBytes[destChainId][block.number] = uint64(bytesUsed + byteCharge);
    }
}
