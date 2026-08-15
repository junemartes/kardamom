// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @notice Succinct's standard SP1 verifier gateway interface.
interface ISP1Verifier {
    /// @dev Reverts when the proof is invalid.
    function verifyProof(
        bytes32 programVKey,
        bytes calldata publicValues,
        bytes calldata proofBytes
    ) external view;
}

/// @notice The settlement surface the oracle cross-reads (spec PR 4).
interface IKardamomL2Settlement {
    function batches(uint64 index)
        external
        view
        returns (uint64 l2BlockStart, uint64 l2BlockEnd, bytes32 recordsCommitment);

    function lastBatchIndex() external view returns (uint64);
}

/// @title KardamomProofOracle
/// @notice The zk root chain (spec: no-std-exec-core, PR 4). Holds the L2
///         running state root and advances it one POSTED BATCH at a time on
///         a verified validity proof whose public values match the batch the
///         settlement contract stored — proofs attest the posted data, and
///         the root chain is inductive from the genesis root.
/// @dev    Submission is PERMISSIONLESS: the proof is the authorization.
///         Public values layout (160 bytes, 5x32, exactly what the batch
///         guest commits): preStateRoot || postStateRoot ||
///         firstBlock(u256) || lastBlock(u256) || recordsCommitment.
contract KardamomProofOracle is KardamomUUPSBase {
    IKardamomL2Settlement public settlement;
    ISP1Verifier public verifier;
    /// @notice The batch guest program's verifying key.
    bytes32 public programVKey;
    /// @notice The proven running L2 state root.
    bytes32 public stateRoot;
    /// @notice Index of the last proven batch (proofs are strictly
    ///         sequential; 0 = nothing proven).
    uint64 public lastProvenBatch;

    event BatchProven(
        uint64 indexed batchIndex,
        bytes32 preStateRoot,
        bytes32 postStateRoot,
        bytes32 recordsCommitment
    );

    error BadPublicValuesLength();
    error NonSequentialBatch();
    error UnknownBatch();
    error RangeMismatch();
    error RecordsCommitmentMismatch();
    error PreRootMismatch();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(
        address _settlement,
        address _verifier,
        bytes32 _programVKey,
        bytes32 _genesisRoot
    ) external initializer {
        settlement = IKardamomL2Settlement(_settlement);
        verifier = ISP1Verifier(_verifier);
        programVKey = _programVKey;
        stateRoot = _genesisRoot;
    }

    /// @notice Verify one batch's validity proof and advance the root.
    function submitBatchProof(uint64 batchIndex, bytes calldata publicValues, bytes calldata proof)
        external
    {
        if (publicValues.length != 160) revert BadPublicValuesLength();
        if (batchIndex != lastProvenBatch + 1) revert NonSequentialBatch();

        (
            bytes32 preRoot,
            bytes32 postRoot,
            uint256 firstBlock,
            uint256 lastBlock,
            bytes32 recordsCommitment
        ) = abi.decode(publicValues, (bytes32, bytes32, uint256, uint256, bytes32));

        (uint64 postedStart, uint64 postedEnd, bytes32 postedCommitment) =
            settlement.batches(batchIndex);
        // recordsCommitment is keccak over a nonempty domain-tagged preimage
        // — zero means "no such batch entry".
        if (postedCommitment == bytes32(0)) revert UnknownBatch();
        if (firstBlock != postedStart || lastBlock != postedEnd) revert RangeMismatch();
        if (recordsCommitment != postedCommitment) revert RecordsCommitmentMismatch();
        if (preRoot != stateRoot) revert PreRootMismatch();

        verifier.verifyProof(programVKey, publicValues, proof);

        stateRoot = postRoot;
        lastProvenBatch = batchIndex;
        emit BatchProven(batchIndex, preRoot, postRoot, recordsCommitment);
    }
}
