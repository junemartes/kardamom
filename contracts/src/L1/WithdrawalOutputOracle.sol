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

    error NotAttester();
    error NotChallenger();
    error NonMonotonicBlock();
    error UnknownOutput();
    error AlreadyDeleted();
    error WindowElapsed();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address _attester, address _challenger, uint64 _finalizationWindow)
        external
        initializer
    {
        attester = _attester;
        challenger = _challenger;
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
    ///         must strictly advance.
    function proposeOutput(bytes32 outputRoot, uint64 l2BlockNumber)
        external
        returns (uint256 index)
    {
        if (msg.sender != attester) revert NotAttester();
        uint256 n = _outputs.length;
        if (n > 0 && l2BlockNumber <= _outputs[n - 1].l2BlockNumber) {
            revert NonMonotonicBlock();
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
    ///         state); that is a follow-up once outputs chain explicitly.
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
