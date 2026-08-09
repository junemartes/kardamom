// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {XChain} from "./XChain.sol";

/// @title Outbox
/// @notice Predeploy that initiates cross-chain messages to other Kardamom
///         chains (`docs/specs/interop-outbox-messaging-spec.md`). Storage
///         holds COMMITMENTS only — per-destination dense sequence counters
///         and `sentMessages[msgHash]` — laid out append-only so the
///         executor's BAL claims for this address map 1:1 onto sends; the
///         payload itself travels in the `MessageSent` event / calldata and
///         is re-hashed and cross-checked by the validator's extractor
///         (never trusted from the event).
/// @dev    Genesis predeploy at `XChain.OUTBOX`: runtime bytecode seeded at a
///         fixed address, not upgradeable, no constructor-time state.
contract Outbox {
    /// @notice Next sequence number per destination chain. Dense per pair —
    ///         the destination enforces its no-skip rule on exactly this
    ///         counter.
    mapping(uint64 => uint64) public nonces;

    /// @notice Sent message commitments. The BAL-extraction anchor: one new
    ///         `true` slot per send, never overwritten.
    mapping(bytes32 => bool) public sentMessages;

    /// @notice Per-message destination gas ceiling, enforced here on the paid
    ///         side so destination quotas reject nothing that was accepted
    ///         for a fee.
    uint64 public constant MAX_MESSAGE_GAS = 10_000_000;

    /// @notice Payload size ceiling (spam bound; revisit with blob transport).
    uint256 public constant MAX_DATA_BYTES = 65_536;

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
        bytes data,
        bytes32 msgHash,
        XChain.Callback callback
    );

    /// @notice Send a message to `target` on `destChainId`.
    /// @dev    `payable` for ABI stability with the value-transfer phase
    ///         (burn → mint, spec §13); until that ships nonzero value is
    ///         rejected rather than silently locked.
    function sendMessage(
        uint64 destChainId,
        address target,
        uint64 gasLimit,
        bytes calldata data,
        XChain.Callback calldata cb
    ) external payable returns (uint64 seq) {
        require(block.chainid <= type(uint64).max, "Outbox: chain id exceeds u64");
        uint64 originChainId = uint64(block.chainid);
        require(destChainId != 0 && destChainId != originChainId, "Outbox: bad destination");
        require(gasLimit <= MAX_MESSAGE_GAS, "Outbox: gas limit above cap");
        require(data.length <= MAX_DATA_BYTES, "Outbox: data above cap");
        require(msg.value == 0, "Outbox: value transfer not enabled");

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
            keccak256(data),
            XChain.hashCallback(cb)
        );
        sentMessages[msgHash] = true;
        emit MessageSent(destChainId, seq, msg.sender, target, msg.value, gasLimit, data, msgHash, cb);
    }
}
