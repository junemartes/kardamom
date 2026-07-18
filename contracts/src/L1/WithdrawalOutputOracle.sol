// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @title WithdrawalOutputOracle
/// @notice L1 registry of attested L2 output roots. A permissioned `attester`
///         (the validator's attestation key) appends one output per L2 block
///         range; a permissioned `challenger` may delete an output while it is
///         still inside its finalization window. The withdrawal bridge reads
///         finalized, non-deleted outputs to authorize payouts.
/// @dev    Milestone 1 is **optimistic with a permissioned challenge**: the
///         `challenger` is the stand-in for a trustless ZK fault proof
///         (`challenge(zkProof)` that recomputes the output root and finds it
///         differs). Swapping the gate on `deleteOutput` for a SNARK verifier is
///         the only change needed to make challenges trustless — the bridge,
///         attester, and output format are unchanged.
contract WithdrawalOutputOracle is KardamomUUPSBase {
    /// @notice Version byte committed into every `outputRoot`:
    ///         `keccak256(abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot))`.
    uint8 public constant OUTPUT_VERSION = 0;

    struct Output {
        bytes32 outputRoot; // keccak(VERSION ++ stateRoot ++ withdrawalsRoot)
        uint64 l2BlockNumber; // last L2 block covered by this output
        uint64 timestamp; // L1 time the output was proposed
        bool deleted; // set by a successful challenge
    }

    /// @notice Authorized output proposer (the validator's attester key).
    address public attester;
    /// @notice Authorized challenger (milestone-1 permissioned stand-in for ZK).
    address public challenger;
    /// @notice Seconds an output must age before a withdrawal can finalize.
    uint64 public finalizationWindow;

    Output[] internal _outputs;

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

    error NotAttester();
    error NotChallenger();
    error NonMonotonicBlock();
    error UnknownOutput();
    error AlreadyDeleted();
    error WindowElapsed();
    error ZeroAddress();
    error ZeroWindow();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address _attester, address _challenger, uint64 _finalizationWindow)
        external
        initializer
    {
        // A zero attester bricks proposals, a zero challenger removes the only
        // milestone-1 fraud backstop, and a zero window makes every output
        // instantly finalizable (challenge impossible). None is ever valid.
        if (_attester == address(0) || _challenger == address(0)) revert ZeroAddress();
        if (_finalizationWindow == 0) revert ZeroWindow();
        attester = _attester;
        challenger = _challenger;
        finalizationWindow = _finalizationWindow;
    }

    // -------------------------------------------------------------------------
    // Key rotation (factory-gated: the same authority that can upgrade the
    // implementation — recovery path for a leaked attester/challenger key
    // without a full UUPS upgrade).
    // -------------------------------------------------------------------------

    /// @notice Rotate the attester key. Only the kardamom factory.
    function setAttester(address _attester) external {
        if (msg.sender != FACTORY) revert NotFactory();
        if (_attester == address(0)) revert ZeroAddress();
        emit AttesterUpdated(attester, _attester);
        attester = _attester;
    }

    /// @notice Rotate the challenger key. Only the kardamom factory.
    function setChallenger(address _challenger) external {
        if (msg.sender != FACTORY) revert NotFactory();
        if (_challenger == address(0)) revert ZeroAddress();
        emit ChallengerUpdated(challenger, _challenger);
        challenger = _challenger;
    }

    /// @notice Adjust the finalization window. Only the kardamom factory.
    ///         Applies to outputs proposed after the change AND retroactively to
    ///         pending ones (`isFinalizable`/`deleteOutput` read the live value).
    function setFinalizationWindow(uint64 _finalizationWindow) external {
        if (msg.sender != FACTORY) revert NotFactory();
        if (_finalizationWindow == 0) revert ZeroWindow();
        emit FinalizationWindowUpdated(finalizationWindow, _finalizationWindow);
        finalizationWindow = _finalizationWindow;
    }

    /// @notice Number of outputs ever proposed (deleted ones still count, so
    ///         indices are stable).
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

    /// @notice Append an attested output. Only the attester; the covered L2 block
    ///         must strictly advance past the latest **non-deleted** output.
    ///         Deleted (successfully challenged) outputs are excluded from the
    ///         monotonicity floor so a corrected output for the challenged range
    ///         (same `l2BlockNumber`) can be re-proposed — otherwise every honest
    ///         withdrawal in that range would be permanently stranded.
    function proposeOutput(bytes32 outputRoot, uint64 l2BlockNumber)
        external
        returns (uint256 index)
    {
        if (msg.sender != attester) revert NotAttester();
        uint256 n = _outputs.length;
        // Compare against the latest non-deleted output. The backward scan is
        // bounded by the trailing run of deleted outputs — deletions are
        // permissioned, rare, and window-limited.
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
    ///         challenger. Milestone-1 stand-in for "a valid ZK proof showed a
    ///         different state transition for this range". A deleted output can
    ///         never finalize a withdrawal.
    /// @dev    Milestone 1 deletes a single output. A production challenge rolls
    ///         back this output and every later one (they build on rejected
    ///         state); that is a follow-up once outputs chain explicitly. The
    ///         attester can re-propose a corrected output for the deleted range
    ///         (deleted outputs don't count toward the monotonicity floor in
    ///         `proposeOutput`).
    function deleteOutput(uint256 index) external {
        if (msg.sender != challenger) revert NotChallenger();
        if (index >= _outputs.length) revert UnknownOutput();
        Output storage o = _outputs[index];
        if (o.deleted) revert AlreadyDeleted();
        if (block.timestamp >= uint256(o.timestamp) + finalizationWindow) {
            revert WindowElapsed();
        }
        o.deleted = true;
        emit OutputDeleted(index, o.outputRoot);
    }

    /// @notice True once `index` exists, is not deleted, and its window elapsed.
    function isFinalizable(uint256 index) external view returns (bool) {
        if (index >= _outputs.length) return false;
        Output storage o = _outputs[index];
        return !o.deleted && block.timestamp >= uint256(o.timestamp) + finalizationWindow;
    }
}
