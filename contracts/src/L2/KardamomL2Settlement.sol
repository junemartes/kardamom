// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @title KardamomL2Settlement
/// @notice A pure data-availability sink. It records `(prevBatchIndex,
///         blobHashes, l2BlockStart, l2BlockEnd)` and emits `BatchPosted`.
///         A compare-and-swap check on `prevBatchIndex` guards against
///         replay. This contract stores no state root; state-root
///         attestation is a deferred validator concern.
/// @dev    Only the Kardamom factory can upgrade this contract, through
///         `KardamomUUPSBase`.
contract KardamomL2Settlement is KardamomUUPSBase {
    /// @notice The authorized L1 batcher account. Only this address can call `postBatch`.
    address public l1Batcher;
    /// @notice Index of the last successfully posted batch. Starts at 0
    ///         and only increases.
    uint64 public lastBatchIndex;

    /// @notice One posted batch's on-chain record. `recordsCommitment`
    ///         binds the batch's canonical record identities, so a
    ///         validity proof attests to the posted data. The proof
    ///         oracle reads this entry.
    struct BatchEntry {
        uint64 l2BlockStart;
        uint64 l2BlockEnd;
        bytes32 recordsCommitment;
    }

    /// @notice The posted batches, by index. Index 0 is never used.
    mapping(uint64 => BatchEntry) public batches;

    /// @notice Emitted on every successful `postBatch` call.
    event BatchPosted(
        uint64 indexed batchIndex,
        bytes32[] blobHashes,
        uint64 l2BlockStart,
        uint64 l2BlockEnd,
        bytes32 recordsCommitment
    );

    error NotBatcher();
    error StaleBatchIndex();
    error EmptyBlobs();
    error BadBlockRange();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    /// @notice Initialize the proxy. The factory calls this at deploy time.
    function initialize(address _l1Batcher) external initializer {
        l1Batcher = _l1Batcher;
    }

    /// @notice Record a posted batch on L1.
    /// @dev    Reverts unless `msg.sender == l1Batcher` and
    ///         `prevBatchIndex == lastBatchIndex` (the replay-protection
    ///         check). Blob bytes travel in the 4844 sidecar and are not
    ///         stored on chain; only their versioned hashes stay here.
    function postBatch(
        uint64 prevBatchIndex,
        bytes32[] calldata blobVersionedHashes,
        uint64 l2BlockStart,
        uint64 l2BlockEnd,
        bytes32 recordsCommitment
    ) external {
        if (msg.sender != l1Batcher) revert NotBatcher();
        if (prevBatchIndex != lastBatchIndex) revert StaleBatchIndex();
        if (blobVersionedHashes.length == 0) revert EmptyBlobs();
        if (l2BlockEnd < l2BlockStart) revert BadBlockRange();

        uint64 next = prevBatchIndex + 1;
        lastBatchIndex = next;
        batches[next] = BatchEntry({
            l2BlockStart: l2BlockStart, l2BlockEnd: l2BlockEnd, recordsCommitment: recordsCommitment
        });
        emit BatchPosted(next, blobVersionedHashes, l2BlockStart, l2BlockEnd, recordsCommitment);
    }
}
