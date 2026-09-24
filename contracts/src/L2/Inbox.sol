// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {XChain} from "./XChain.sol";
import {Outbox} from "./Outbox.sol";

/// @title Inbox
/// @notice Predeploy through which every derived cross-chain (0x7D) tx
///         delivers. Records delivery status (app-queryable bookkeeping —
///         exactly-once itself is guaranteed upstream by deterministic
///         derivation + dedup + fail-stop verification), exposes the origin
///         sender to the inner call, and enqueues the requested callback
///         through this chain's own Outbox.
/// @dev    Genesis predeploy at `XChain.INBOX`. The derived tx's EVM sender
///         is the ORIGIN chain's Outbox aliased into this chain's address
///         space (`XChain.txSender(originChainId)`) — an address with no key,
///         so `deliver` cannot be forged by a user transaction.
///
///         Storage layout (append-only; pinned by `vm.load` tests):
///           slot 0  `delivered`
///           slot 1  `nextSeq`
///           slot 2  the delivery-in-progress record (packed)
///
///         `deliver` never reverts once the sender, replay, and value checks
///         pass (audit H4). The inner call runs only when the tx carries the
///         gas it needs; otherwise the message is recorded as status 2 and
///         the callback still goes out. The return data is hashed through a
///         bounded copy. The callback send is wrapped in `try`, so a revert
///         in the Outbox drops the callback (with an event) instead of the
///         delivery record.
contract Inbox {
    /// @notice Delivery status per (origin chain, seq): 0 = none,
    ///         1 = delivered (inner call succeeded), 2 = delivered (inner
    ///         call reverted, or the delivery tx carried too little gas to
    ///         run it — see `DeliveryUnderBudget`).
    mapping(uint64 => mapping(uint64 => uint8)) public delivered;

    /// @notice First seq not yet delivered per origin lane: `seq + 1` of the
    ///         last delivery (audit L9), so a lane that starts mid-history
    ///         reports the right cursor. Status 2 counts — the message WAS
    ///         delivered. This is the app-facing "how far has this lane
    ///         delivered" view and the authoritative source for watcher
    ///         cursor reconciliation.
    mapping(uint64 => uint64) public nextSeq;

    uint64 private _xdOrigin;
    address private _xdSender;
    uint8 private _xdHops;
    bool private _inDelivery;

    event MessageDelivered(uint64 indexed originChainId, uint64 indexed seq, bool success);

    /// @notice The delivery tx carried less gas than the inner call needs
    ///         (`gasLimit + gasLimit / 63 + RESERVE`). The inner call did
    ///         not run; the message is recorded as status 2.
    event DeliveryUnderBudget(
        uint64 indexed originChainId, uint64 indexed seq, uint256 gasLeft, uint256 needed
    );

    /// @notice The Outbox rejected the callback response (for example, the
    ///         return lane's per-block budget is full). The delivery record
    ///         stands; the origin app receives no response for this message.
    event CallbackDropped(uint64 indexed originChainId, uint64 indexed seq, bytes reason);

    /// @notice Gas kept back from the inner call for the Inbox's own work
    ///         after it: the bounded return copy, the status and cursor
    ///         writes, the events, and the callback send through the Outbox
    ///         (cold lane, four cold slots). Measured in `test/L2/Inbox.t.sol`
    ///         and pinned there with a margin.
    uint256 public constant RESERVE = 200_000;

    /// @notice Bound on the return data the Inbox copies and hashes (audit
    ///         H4). A target can return far more than this within its gas;
    ///         the response then carries the hash of the first
    ///         `MAX_RETURN_BYTES` bytes and `truncated = true`.
    uint256 public constant MAX_RETURN_BYTES = 256;

    /// @notice Hop budget of the auto-enqueued response. Zero: the response
    ///         target cannot send further derived messages (audit H6).
    uint8 public constant RESPONSE_HOPS = 0;

    /// @notice Selector the auto-enqueued response carries back to the
    ///         origin-side callback target:
    ///         `onXChainResult(address requester, uint64 requestSeq, bool ok,
    ///         bytes32 retHash, bool truncated, bytes32 context)`.
    ///         `requester` is the origin sender of the request and
    ///         `requestSeq` its seq on the (origin, destination) lane, so a
    ///         forged response cannot pose as the real one (audit H5).
    ///
    ///         A target MUST check all of:
    ///           - `requester == address(this)`,
    ///           - `msg.sender == XChain.INBOX`,
    ///           - `Inbox(XChain.INBOX).xDomainSender() ==
    ///             (peerChainId, XChain.INBOX)`.
    ///         `msg.sender == INBOX` alone says only "some delivery"; the
    ///         `xDomainSender` check ties it to the peer's Inbox; the
    ///         `requester` check ties it to this contract's own request.
    bytes4 public constant RESPONSE_SELECTOR =
        bytes4(keccak256("onXChainResult(address,uint64,bool,bytes32,bool,bytes32)"));

    /// @notice Deliver one derived cross-chain message. Callable only by the
    ///         aliased origin Outbox (see contract docs).
    function deliver(
        uint64 originChainId,
        uint64 seq,
        address originSender,
        address target,
        uint256 value,
        uint64 gasLimit,
        uint8 hops,
        bytes calldata data,
        XChain.Callback calldata cb
    ) external {
        require(msg.sender == XChain.txSender(originChainId), "Inbox: not the origin outbox");
        require(delivered[originChainId][seq] == 0, "Inbox: already delivered");
        require(value == 0, "Inbox: value transfer not enabled");

        // Expose the origin identity to the inner call, OP-messenger style:
        // `target` sees `msg.sender == INBOX` and reads the real origin via
        // `xDomainSender()`. The hop budget lets the Outbox apply its rule
        // to sends made inside this delivery. Cleared after — a call
        // outside a delivery reverts rather than reading stale identity.
        _xdOrigin = originChainId;
        _xdSender = originSender;
        _xdHops = hops;
        _inDelivery = true;

        bool ok = false;
        bytes32 retHash;
        bool truncated = false;
        uint256 needed = uint256(gasLimit) + uint256(gasLimit) / 63 + RESERVE;
        uint256 left = gasleft();
        if (left < needed) {
            // The 63/64 rule would hand the target less than `gasLimit`, or
            // the work after the call could run out of gas. Do not start a
            // call that cannot get its budget: record the failure, keep the
            // callback. Deterministic on every replica — the executor sizes
            // the delivery tx the same way everywhere.
            emit DeliveryUnderBudget(originChainId, seq, left, needed);
            retHash = keccak256("");
        } else {
            (ok, retHash, truncated) = _boundedCall(target, gasLimit, data);
        }
        _xdOrigin = 0;
        _xdSender = address(0);
        _xdHops = 0;
        _inDelivery = false;

        // Status is recorded whether the inner call succeeded or reverted —
        // the delivery tx itself succeeds either way (the deposit posture),
        // and FAILURE also triggers the callback: an origin app must never
        // wait forever on a revert.
        delivered[originChainId][seq] = ok ? 1 : 2;
        nextSeq[originChainId] = seq + 1;
        emit MessageDelivered(originChainId, seq, ok);

        if (!XChain.isNone(cb)) {
            _sendResponse(originChainId, seq, originSender, ok, retHash, truncated, cb);
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
        require(_inDelivery, "Inbox: no delivery in progress");
        return (_xdOrigin, _xdSender);
    }

    /// @notice True while `deliver` runs the inner call.
    function inDelivery() external view returns (bool) {
        return _inDelivery;
    }

    /// @notice Hop budget of the delivery in progress. Zero outside one.
    function currentHops() external view returns (uint8) {
        return _xdHops;
    }

    /// @dev A low-level call is the POINT here: the inner call's revert must
    ///      be recorded as a failed delivery (and trigger the callback), never
    ///      bubble up and fail the delivery tx itself — the deposit posture.
    ///      Assembly so the return data copy is bounded: a target can return
    ///      hundreds of kilobytes within its gas, and a full copy would run
    ///      the Inbox out of gas after the call (audit H4).
    function _boundedCall(address target, uint64 gasLimit, bytes calldata data)
        private
        returns (bool ok, bytes32 retHash, bool truncated)
    {
        uint256 cap = MAX_RETURN_BYTES;
        uint256 size = 0;
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            calldatacopy(ptr, data.offset, data.length)
            ok := call(gasLimit, target, 0, ptr, data.length, 0, 0)
            size := returndatasize()
            let n := size
            if gt(n, cap) { n := cap }
            returndatacopy(ptr, 0, n)
            retHash := keccak256(ptr, n)
        }
        truncated = size > cap;
    }

    /// @dev The response is an ordinary message on this chain's own Outbox,
    ///      sent WITHOUT a callback and with a zero hop budget, so
    ///      auto-responses can never ping-pong or seed further derived sends.
    ///      The returned seq is deliberately dropped: the response's identity
    ///      travels in the Outbox's own MessageSent event / commitment, and
    ///      the Inbox records nothing per-response. The Outbox may reject the
    ///      send (its per-block budgets apply to the return lane); the
    ///      delivery record must survive that, so the call is wrapped.
    function _sendResponse(
        uint64 originChainId,
        uint64 seq,
        address originSender,
        bool ok,
        bytes32 retHash,
        bool truncated,
        XChain.Callback calldata cb
    ) private {
        bytes memory payload = abi.encodeWithSelector(
            RESPONSE_SELECTOR, originSender, seq, ok, retHash, truncated, cb.context
        );
        // slither-disable-next-line unused-return
        try Outbox(XChain.OUTBOX)
            .sendMessage(
                originChainId,
                cb.target,
                cb.gasLimit,
                RESPONSE_HOPS,
                payload,
                XChain.Callback(address(0), 0, bytes32(0))
            ) returns (
            uint64
        ) {}
        catch (bytes memory reason) {
            emit CallbackDropped(originChainId, seq, reason);
        }
    }
}
