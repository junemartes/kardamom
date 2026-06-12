// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @title KardamomL2Settlement
/// @notice Pure data-availability sink. Records `(prevBatchIndex, blobHashes,
///         l2BlockStart, l2BlockEnd)` and emits `BatchPosted`. Replay protection
///         via CAS on `prevBatchIndex`. **No state root is stored** — per S0
///         D-Sh11, state-root attestation is a deferred validator concern.
/// @dev    Upgrades are gated to the kardamom factory via `KardamomUUPSBase`.
contract KardamomL2Settlement is KardamomUUPSBase {
    /// @notice Authorized L1 batcher EOA. Only this address may `postBatch`.
    address public l1Batcher;
    /// @notice Index of the last successfully posted batch. Starts at 0;
    ///         monotonically increasing.
    uint64 public lastBatchIndex;

    /// @notice Emitted on every successful `postBatch` call.
    event BatchPosted(
        uint64 indexed batchIndex, bytes32[] blobHashes, uint64 l2BlockStart, uint64 l2BlockEnd
    );

    error NotBatcher();
    error StaleBatchIndex();
    error EmptyBlobs();
    error BadBlockRange();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    /// @notice Initialize the proxy. Called by the factory at deploy time.
    function initialize(address _l1Batcher) external initializer {
        l1Batcher = _l1Batcher;
    }

    /// @notice Record a posted batch on L1.
    /// @dev    Reverts unless `msg.sender == l1Batcher` and `prevBatchIndex ==
    ///         lastBatchIndex` (CAS replay protection). Blob bytes themselves
    ///         travel in the 4844 sidecar and are not stored on chain; only
    ///         their versioned hashes are kept here.
    function postBatch(
        uint64 prevBatchIndex,
        bytes32[] calldata blobVersionedHashes,
        uint64 l2BlockStart,
        uint64 l2BlockEnd
    ) external {
        if (msg.sender != l1Batcher) revert NotBatcher();
        if (prevBatchIndex != lastBatchIndex) revert StaleBatchIndex();
        if (blobVersionedHashes.length == 0) revert EmptyBlobs();
        if (l2BlockEnd < l2BlockStart) revert BadBlockRange();

        uint64 newIndex = prevBatchIndex + 1;
        lastBatchIndex = newIndex;
        emit BatchPosted(newIndex, blobVersionedHashes, l2BlockStart, l2BlockEnd);
    }
}
