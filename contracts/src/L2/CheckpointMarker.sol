// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {XChain} from "./XChain.sol";
import {Outbox} from "./Outbox.sol";
import {Inbox} from "./Inbox.sol";

/// @title CheckpointMarker
/// @notice Predeploy that emits the checkpoint markers for coordinated
///         recovery lines. A round is a consistent cut across the chains
///         that exchange messages. The rule is the Chandy-Lamport marker
///         rule:
///
///         1. A chain opens a round on the FIRST marker it sees for that
///            round. The marker is its own timer (`startRound`) or a marker
///            received on any inbound lane (`onMarker`).
///         2. In that same transaction the chain sends the round's marker on
///            EVERY registered outbound lane.
///
///         The snapshot for a round is the state at `roundBlock[round]`.
///         Lanes are dense FIFO, so the in-flight set of a lane at the cut
///         is `sent - delivered`, and no channel state needs recording.
///
///         A marker is an ordinary lane message. Its sender and its target
///         are this predeploy. The receiving side accepts a marker only when
///         the Inbox reports the origin sender as this address. No key exists
///         for a predeploy address, so a user cannot forge a marker.
/// @dev    Genesis predeploy at `XChain.CHECKPOINT_MARKER`. Not upgradeable,
///         no constructor-time state. `ROUND_INTERVAL_MS` is a protocol
///         constant. A change to it needs a coordinated genesis change on
///         every member chain, the same as `KardamomChainState`.
///
///         `block.timestamp` on this chain is epoch milliseconds. Every time
///         value here is in milliseconds.
contract CheckpointMarker {
    /// @notice Round cadence (X). The round id is `block.timestamp / ROUND_INTERVAL_MS`.
    uint64 public constant ROUND_INTERVAL_MS = 600_000;

    /// @notice Upper bound on registered peers. It keeps `MARKER_GAS` under
    ///         the Outbox message gas cap.
    uint256 public constant MAX_PEERS = 32;

    /// @notice Gas budget of a marker delivery on the destination. The
    ///         destination's `onMarker` sends one marker per registered peer,
    ///         so the budget covers `MAX_PEERS` sends plus bookkeeping.
    uint64 public constant MARKER_GAS = 100_000 + 80_000 * uint64(MAX_PEERS);

    /// @notice The last round this chain opened. 0 means none.
    uint64 public lastRound;

    /// @notice The block that opened a round. 0 means the round is not open.
    mapping(uint64 => uint64) public roundBlock;

    /// @notice Whether a chain id is a registered outbound lane.
    mapping(uint64 => bool) public isPeer;

    uint64[] internal _peers;

    /// @notice Emitted once per registered peer.
    event PeerRegistered(uint64 indexed chainId);

    /// @notice Emitted when a round opens. `trigger` is 0 for the own
    ///         timer, else the origin chain whose marker opened the round.
    event RoundOpened(uint64 indexed round, uint64 blockNumber, uint64 indexed trigger);

    /// @notice Emitted for every marker sent on an outbound lane.
    event MarkerSent(uint64 indexed round, uint64 indexed destChainId, uint64 seq);

    /// @notice Emitted when a received marker is stale or a duplicate. The
    ///         delivery still succeeds. Nothing else happens.
    event MarkerIgnored(uint64 indexed round, uint64 indexed originChainId);

    /// @notice The round id for the current block time.
    function currentRound() public view returns (uint64) {
        require(block.timestamp <= type(uint64).max, "CheckpointMarker: timestamp exceeds u64");
        return uint64(block.timestamp) / ROUND_INTERVAL_MS;
    }

    /// @notice The number of registered peers.
    function peerCount() external view returns (uint256) {
        return _peers.length;
    }

    /// @notice The registered peer at `index`.
    function peerAt(uint256 index) external view returns (uint64) {
        return _peers[index];
    }

    /// @notice Register an outbound lane. Anyone can call this. The call
    ///         succeeds only for a chain that has delivered at least one
    ///         message to this chain. Only the derivation pipeline can
    ///         deliver, and it delivers only from admitted peers, so the
    ///         peer list is the on-chain view of the peer registry.
    ///         Registration is idempotent.
    function registerPeer(uint64 chainId) external {
        require(chainId != 0 && chainId != block.chainid, "CheckpointMarker: bad peer");
        if (isPeer[chainId]) return;
        require(
            Inbox(XChain.INBOX).nextSeq(chainId) > 0, "CheckpointMarker: no delivery from chain"
        );
        _register(chainId);
    }

    /// @notice The timer. Anyone can call this once per interval. It opens
    ///         the round for the current block time and sends the marker on
    ///         every registered lane. A caller cannot pick the round id, so
    ///         a call is the same as the timer firing.
    function startRound() external {
        uint64 round = currentRound();
        require(round > lastRound, "CheckpointMarker: round already open");
        _open(round, 0);
    }

    /// @notice Receive a marker from a peer. Only the Inbox can call this,
    ///         and only while it delivers a message whose origin sender is
    ///         this predeploy. A stale or duplicate round is ignored. A
    ///         round more than one interval ahead of this chain's clock is
    ///         ignored too: a peer with a bad clock must not park
    ///         `lastRound` in the future and stop this chain's own timer.
    function onMarker(uint64 round) external {
        require(msg.sender == XChain.INBOX, "CheckpointMarker: not the inbox");
        // The Inbox exposes the origin sender as recorded on the origin
        // chain, not aliased. The derived delivery passes it through
        // unchanged.
        (uint64 origin, address sender) = Inbox(XChain.INBOX).xDomainSender();
        require(sender == XChain.CHECKPOINT_MARKER, "CheckpointMarker: not a system marker");
        // The Inbox proved the delivery, so the origin is an admitted peer.
        // `nextSeq` is not yet incremented for this delivery, so do not use
        // `registerPeer` here.
        if (!isPeer[origin]) _register(origin);
        if (round <= lastRound || round > currentRound() + 1) {
            emit MarkerIgnored(round, origin);
            return;
        }
        _open(round, origin);
    }

    function _register(uint64 chainId) internal {
        require(_peers.length < MAX_PEERS, "CheckpointMarker: too many peers");
        isPeer[chainId] = true;
        _peers.push(chainId);
        emit PeerRegistered(chainId);
    }

    /// @dev Opens `round` at this block and sends its marker on every lane.
    function _open(uint64 round, uint64 trigger) internal {
        lastRound = round;
        roundBlock[round] = uint64(block.number);
        emit RoundOpened(round, uint64(block.number), trigger);

        bytes memory data = abi.encodeCall(this.onMarker, (round));
        XChain.Callback memory noCb = XChain.Callback(address(0), 0, bytes32(0));
        uint256 n = _peers.length;
        for (uint256 i = 0; i < n; i++) {
            uint64 dest = _peers[i];
            uint64 seq = Outbox(XChain.OUTBOX)
                .sendMessage(dest, XChain.CHECKPOINT_MARKER, MARKER_GAS, data, noCb);
            emit MarkerSent(round, dest, seq);
        }
    }
}
