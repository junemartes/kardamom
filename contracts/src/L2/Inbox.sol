// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {XChain} from "./XChain.sol";
import {Outbox} from "./Outbox.sol";

/// @title Inbox
/// @notice Predeploy through which every derived cross-chain (0x7D) tx
///         delivers. Records delivery status (app-queryable bookkeeping —
///         exactly-once itself is guaranteed upstream by deterministic
///         derivation + dedup + fail-stop verification), exposes the origin
///         sender to the inner call, and atomically enqueues the requested
///         callback through this chain's own Outbox.
/// @dev    Genesis predeploy at `XChain.INBOX`. The derived tx's EVM sender
///         is the ORIGIN chain's Outbox aliased into this chain's address
///         space (`XChain.txSender(originChainId)`) — an address with no key,
///         so `deliver` cannot be forged by a user transaction.
contract Inbox {
    /// @notice Delivery status per (origin chain, seq): 0 = none,
    ///         1 = delivered (inner call succeeded), 2 = delivered (inner
    ///         call reverted).
    mapping(uint64 => mapping(uint64 => uint8)) public delivered;

    /// @notice Count of messages delivered per origin lane — equivalently,
    ///         the first seq not yet delivered. Because delivery is dense and
    ///         in-order per lane by construction (the derivation pipeline's
    ///         no-skip rule), a bare counter is exact: after delivering seq N
    ///         it always equals N + 1. Status 2 (inner call reverted) counts —
    ///         the message WAS delivered. This is the app-facing "how far has
    ///         this lane delivered" view and the future authoritative source
    ///         for watcher cursor reconciliation.
    mapping(uint64 => uint64) public nextSeq;

    uint64 private _xdOrigin;
    address private _xdSender;

    event MessageDelivered(uint64 indexed originChainId, uint64 indexed seq, bool success);

    /// @notice Selector the auto-enqueued response carries back to the
    ///         origin-side callback target:
    ///         `onXChainResult(bool success, bytes32 returnDataHash, bytes32 context)`.
    bytes4 public constant RESPONSE_SELECTOR =
        bytes4(keccak256("onXChainResult(bool,bytes32,bytes32)"));

    /// @notice Deliver one derived cross-chain message. Callable only by the
    ///         aliased origin Outbox (see contract docs).
    function deliver(
        uint64 originChainId,
        uint64 seq,
        address originSender,
        address target,
        uint256 value,
        uint64 gasLimit,
        bytes calldata data,
        XChain.Callback calldata cb
    ) external {
        require(msg.sender == XChain.txSender(originChainId), "Inbox: not the origin outbox");
        require(delivered[originChainId][seq] == 0, "Inbox: already delivered");
        require(value == 0, "Inbox: value transfer not enabled");

        // Expose the origin identity to the inner call, OP-messenger style:
        // `target` sees `msg.sender == INBOX` and reads the real origin via
        // `xDomainSender()`. Cleared after — a call outside a delivery
        // reverts rather than reading stale identity.
        _xdOrigin = originChainId;
        _xdSender = originSender;
        // A low-level call is the POINT here: the inner call's revert must be
        // recorded as a failed delivery (and trigger the callback), never
        // bubble up and fail the delivery tx itself — the deposit posture.
        // solhint-disable-next-line avoid-low-level-calls
        (bool ok, bytes memory ret) = target.call{gas: gasLimit}(data);
        _xdOrigin = 0;
        _xdSender = address(0);

        // Status is recorded whether the inner call succeeded or reverted —
        // the delivery tx itself succeeds either way (the deposit posture),
        // and FAILURE also triggers the callback: an origin app must never
        // wait forever on a revert.
        delivered[originChainId][seq] = ok ? 1 : 2;
        unchecked {
            // Exact under the density invariant (see nextSeq docs): the lane
            // delivers 0, 1, 2, … with no skips, so counting deliveries and
            // tracking seq + 1 are the same number.
            nextSeq[originChainId]++;
        }
        emit MessageDelivered(originChainId, seq, ok);

        if (!XChain.isNone(cb)) {
            // The response is an ordinary message on this chain's own Outbox,
            // sent WITHOUT a callback — depth is capped at one by
            // construction, so auto-responses can never ping-pong. The
            // returned seq is deliberately dropped: the response's identity
            // travels in the Outbox's own MessageSent event / commitment, and
            // the Inbox records nothing per-response.
            // slither-disable-next-line unused-return
            Outbox(XChain.OUTBOX)
                .sendMessage(
                    originChainId,
                    cb.target,
                    cb.gasLimit,
                    abi.encodeWithSelector(RESPONSE_SELECTOR, ok, keccak256(ret), cb.context),
                    XChain.Callback(address(0), 0, bytes32(0))
                );
        }
    }

    /// @notice Origin identity of the delivery currently in progress.
    ///         Reverts outside a delivery.
    /// @dev    A receiver must check BOTH `msg.sender == XChain.INBOX` and
    ///         the pair this returns. The pair alone is not enough. It stays
    ///         set for the whole inner call, so every contract the delivery
    ///         target calls during a delivery sees the same value. A
    ///         contract that checks only `xDomainSender()` trusts a call the
    ///         target made on its own behalf (a confused deputy). See the
    ///         interop spec, section 8.
    function xDomainSender() external view returns (uint64 originChainId, address sender) {
        require(_xdSender != address(0), "Inbox: no delivery in progress");
        return (_xdOrigin, _xdSender);
    }
}
