// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @title WithdrawalOutputOracle
/// @notice The L1 registry of attested L2 output roots. A permissioned
///         `attester` (the validator's attestation key) appends one output
///         per L2 block range. A permissioned `challenger` can delete an
///         output while it is still inside its finalization window. The
///         withdrawal bridge reads finalized, non-deleted outputs to
///         authorize payouts. A permissioned `recovery` account can pause
///         finalization and roll back the unsettled suffix of outputs
///         after a coordinated chain revert.
/// @dev    This milestone is optimistic with a permissioned challenge. The
///         `challenger` stands in for a trustless ZK fault proof
///         (`challenge(zkProof)` that recomputes the output root and finds
///         it differs). To make challenges trustless, swap the gate on
///         `deleteOutput` for a SNARK verifier. The bridge, attester, and
///         output format do not need to change.
contract WithdrawalOutputOracle is KardamomUUPSBase {
    /// @notice The version byte committed into every `outputRoot`:
    ///         `keccak256(abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot))`.
    uint8 public constant OUTPUT_VERSION = 0;

    struct Output {
        bytes32 outputRoot; // keccak(VERSION ++ stateRoot ++ withdrawalsRoot)
        uint64 l2BlockNumber; // the last L2 block covered by this output
        uint64 timestamp; // the L1 time when the output was proposed
        bool deleted; // set to true by a successful challenge
    }

    /// @notice The authorized output proposer (the validator's attester key).
    address public attester;
    /// @notice The authorized challenger (this milestone's permissioned stand-in for ZK).
    address public challenger;
    /// @notice Seconds an output must age before a withdrawal can finalize.
    uint64 public finalizationWindow;

    Output[] internal _outputs;

    /// @notice The recovery account. It can pause finalization and roll
    ///         back unsettled outputs. Zero disables both paths.
    /// @dev    Appended after `_outputs`. The slots above must not move on
    ///         a live proxy.
    address public recovery;
    /// @notice True while finalization is paused.
    bool public paused;
    /// @notice The L1 time of the current pause. Only valid while `paused`.
    uint64 public pausedAt;

    event OutputProposed(
        uint256 indexed index,
        bytes32 indexed outputRoot,
        uint64 indexed l2BlockNumber,
        uint64 timestamp
    );
    event OutputDeleted(uint256 indexed index, bytes32 outputRoot);
    event AttesterUpdated(address indexed previous, address indexed current);
    event ChallengerUpdated(address indexed previous, address indexed current);
    event FinalizationWindowUpdated(uint64 previous, uint64 current);
    event RecoveryUpdated(address indexed previous, address indexed current);
    event OutputsRolledBack(uint256 indexed fromIndex, uint256 count);
    event FinalizationPaused(uint64 timestamp);
    event FinalizationResumed(uint64 timestamp, uint256 restarted);

    error NotAttester();
    error NotChallenger();
    error NonMonotonicBlock();
    error UnknownOutput();
    error AlreadyDeleted();
    error WindowElapsed();
    error ZeroAddress();
    error ZeroWindow();
    error NotRecovery();
    error BelowSettlementFloor(uint256 index);
    error AlreadyPaused();
    error NotPaused();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address _attester, address _challenger, uint64 _finalizationWindow)
        external
        initializer
    {
        // A zero attester blocks all proposals. A zero challenger removes
        // this milestone's only fraud backstop. A zero window makes every
        // output finalize at once, so no challenge is possible. None of
        // these values is ever valid.
        if (_attester == address(0) || _challenger == address(0)) revert ZeroAddress();
        if (_finalizationWindow == 0) revert ZeroWindow();
        attester = _attester;
        challenger = _challenger;
        finalizationWindow = _finalizationWindow;
    }

    // -------------------------------------------------------------------------
    // Key rotation. The factory gates this: the same authority that can
    // upgrade the implementation. This gives a recovery path for a leaked
    // attester or challenger key without a full UUPS upgrade.
    // -------------------------------------------------------------------------

    /// @notice Rotate the attester key. Only the Kardamom factory can call this.
    function setAttester(address _attester) external {
        if (msg.sender != FACTORY) revert NotFactory();
        if (_attester == address(0)) revert ZeroAddress();
        emit AttesterUpdated(attester, _attester);
        attester = _attester;
    }

    /// @notice Rotate the challenger key. Only the Kardamom factory can call this.
    function setChallenger(address _challenger) external {
        if (msg.sender != FACTORY) revert NotFactory();
        if (_challenger == address(0)) revert ZeroAddress();
        emit ChallengerUpdated(challenger, _challenger);
        challenger = _challenger;
    }

    /// @notice Adjust the finalization window. Only the Kardamom factory
    ///         can call this. The new value applies to outputs proposed
    ///         after the change, and also to pending ones, because
    ///         `isFinalizable` and `deleteOutput` read the live value.
    function setFinalizationWindow(uint64 _finalizationWindow) external {
        if (msg.sender != FACTORY) revert NotFactory();
        if (_finalizationWindow == 0) revert ZeroWindow();
        emit FinalizationWindowUpdated(finalizationWindow, _finalizationWindow);
        finalizationWindow = _finalizationWindow;
    }

    /// @notice Set the recovery account. Only the Kardamom factory can call
    ///         this. A zero address disables the pause and rollback paths.
    ///         The recovery account belongs to the chain's dedicated recovery
    ///         principal, not to the attester or challenger key holders.
    function setRecovery(address _recovery) external {
        if (msg.sender != FACTORY) revert NotFactory();
        emit RecoveryUpdated(recovery, _recovery);
        recovery = _recovery;
    }

    /// @notice The number of outputs ever proposed. Deleted outputs still
    ///         count, so indices stay stable.
    function outputCount() external view returns (uint256) {
        return _outputs.length;
    }

    /// @notice Read a full output record by index.
    function getOutput(uint256 index) external view returns (Output memory) {
        if (index >= _outputs.length) revert UnknownOutput();
        return _outputs[index];
    }

    /// @notice The committed output root at `index`.
    function outputRootAt(uint256 index) external view returns (bytes32) {
        if (index >= _outputs.length) revert UnknownOutput();
        return _outputs[index].outputRoot;
    }

    /// @notice Append an attested output. Only the attester can call this.
    ///         The covered L2 block must strictly advance past the latest
    ///         non-deleted output. Deleted (successfully challenged) outputs
    ///         do not count toward this floor, so the attester can
    ///         re-propose a corrected output for the challenged range (same
    ///         `l2BlockNumber`). Otherwise, every honest withdrawal in that
    ///         range would stay stranded forever.
    function proposeOutput(bytes32 outputRoot, uint64 l2BlockNumber)
        external
        returns (uint256 index)
    {
        if (msg.sender != attester) revert NotAttester();
        uint256 n = _outputs.length;
        // Compare against the latest non-deleted output. The backward scan
        // stays short: the trailing run of deleted outputs is small,
        // because deletions are permissioned, rare, and window-limited.
        for (uint256 i = n; i > 0; i--) {
            Output storage prev = _outputs[i - 1];
            if (prev.deleted) continue;
            if (l2BlockNumber <= prev.l2BlockNumber) revert NonMonotonicBlock();
            break;
        }
        index = n;
        _outputs.push(
            Output({
                outputRoot: outputRoot,
                l2BlockNumber: l2BlockNumber,
                timestamp: uint64(block.timestamp),
                deleted: false
            })
        );
        emit OutputProposed(index, outputRoot, l2BlockNumber, uint64(block.timestamp));
    }

    /// @notice Delete an output that is still inside its window. Only the
    ///         challenger can call this. This milestone stands in for "a
    ///         valid ZK proof showed a different state transition for this
    ///         range." A deleted output can never finalize a withdrawal.
    /// @dev    This milestone deletes a single output. A production
    ///         challenge would roll back this output and every later one,
    ///         because they build on rejected state. That change is a
    ///         follow-up, once outputs chain explicitly. The attester can
    ///         re-propose a corrected output for the deleted range, because
    ///         deleted outputs do not count toward the monotonicity floor
    ///         in `proposeOutput`.
    function deleteOutput(uint256 index) external {
        if (msg.sender != challenger) revert NotChallenger();
        if (index >= _outputs.length) revert UnknownOutput();
        Output storage o = _outputs[index];
        if (o.deleted) revert AlreadyDeleted();
        if (_settled(o)) revert WindowElapsed();
        o.deleted = true;
        emit OutputDeleted(index, o.outputRoot);
    }

    // -------------------------------------------------------------------------
    // Recovery. A coordinated chain revert discards a suffix of L2 blocks.
    // The outputs posted for that suffix must not finalize a withdrawal,
    // because the restored chain has no record of them. The recovery
    // account rolls those outputs back. It can only roll back outputs that
    // are still inside their window. An output whose window has ended is
    // the settlement floor: withdrawals under it may already be paid on
    // L1, and no revert may go below it. That rule is enforced here, not
    // only in the operator's ceremony.
    //
    // The finalization window must be at least the revert window of the
    // trust set (`W`). Otherwise a withdrawal can finalize before the
    // operator can declare the incident that reverts it. This is an
    // operator invariant on `finalizationWindow`.
    // -------------------------------------------------------------------------

    /// @notice Pause finalization. Only the recovery account can call this.
    ///         While paused, no output is finalizable and the finalization
    ///         clock stops. `unpause` restarts the clock of every output
    ///         that was still inside its window.
    function pause() external {
        if (msg.sender != recovery) revert NotRecovery();
        if (paused) revert AlreadyPaused();
        paused = true;
        pausedAt = uint64(block.timestamp);
        emit FinalizationPaused(uint64(block.timestamp));
    }

    /// @notice Resume finalization. Only the recovery account can call
    ///         this. Every output that was not yet settled when the pause
    ///         began gets a fresh timestamp, so it waits a full window
    ///         again. A boundary effect never completes across an incident.
    /// @dev    The scan runs backward from the newest output and stops at
    ///         the first settled one. Outputs are proposed in time order,
    ///         so every older output is settled too. The unsettled suffix
    ///         is bounded by the window and the proposal cadence.
    function unpause() external {
        if (msg.sender != recovery) revert NotRecovery();
        if (!paused) revert NotPaused();
        uint256 restarted = 0;
        for (uint256 i = _outputs.length; i > 0; i--) {
            Output storage o = _outputs[i - 1];
            if (_settled(o)) break;
            if (o.deleted) continue;
            o.timestamp = uint64(block.timestamp);
            restarted++;
        }
        paused = false;
        pausedAt = 0;
        emit FinalizationResumed(uint64(block.timestamp), restarted);
    }

    /// @notice Roll back every output from `fromIndex` to the newest one.
    ///         Only the recovery account can call this. Each output in the
    ///         range is marked deleted, the same as a successful challenge.
    ///         The call reverts if any output in the range is already
    ///         settled: that output is the settlement floor, and a rollback
    ///         below it is never valid. Already deleted outputs are skipped.
    ///         The attester then re-proposes outputs for the restored chain.
    ///         Deleted outputs do not count toward the monotonicity floor,
    ///         so the restored outputs can cover lower L2 blocks.
    function rollbackOutputs(uint256 fromIndex) external {
        if (msg.sender != recovery) revert NotRecovery();
        uint256 n = _outputs.length;
        if (fromIndex >= n) revert UnknownOutput();
        uint256 count = 0;
        for (uint256 i = fromIndex; i < n; i++) {
            Output storage o = _outputs[i];
            if (o.deleted) continue;
            if (_settled(o)) revert BelowSettlementFloor(i);
            o.deleted = true;
            emit OutputDeleted(i, o.outputRoot);
            count++;
        }
        emit OutputsRolledBack(fromIndex, count);
    }

    /// @notice True once `index` exists, is not deleted, its window has
    ///         ended, and finalization is not paused.
    function isFinalizable(uint256 index) external view returns (bool) {
        if (paused) return false;
        if (index >= _outputs.length) return false;
        Output storage o = _outputs[index];
        return !o.deleted && _settled(o);
    }

    /// @dev True when the output's window has ended on the settlement
    ///      clock. The clock is the block time, or the pause time while
    ///      paused. A window that ends during a pause does not settle.
    function _settled(Output storage o) internal view returns (bool) {
        uint256 clock = paused ? uint256(pausedAt) : block.timestamp;
        return clock >= uint256(o.timestamp) + finalizationWindow;
    }
}
