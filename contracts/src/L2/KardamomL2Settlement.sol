// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @title KardamomL2Settlement
/// @notice A pure data-availability sink. It records `(prevBatchIndex,
///         daCert, l2BlockStart, l2BlockEnd)` and emits `BatchPosted`.
///         The batch bytes live in EigenDA; `daCert` is the certificate
///         EigenDA's disperser returned for them, opaque to this contract.
///         A reader retrieves the bytes by the certificate through the
///         EigenDA proxy, which checks the certificate and the bytes
///         against it. A compare-and-swap check on `prevBatchIndex`
///         guards against replay. This contract stores no state root;
///         state-root attestation is a deferred validator concern.
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

    /// @notice Emitted on every successful `postBatch` call. `daCert` is
    ///         the only place the certificate is kept: a reader takes it
    ///         from the log, as the blob hashes of a 4844 post would be.
    event BatchPosted(
        uint64 indexed batchIndex,
        bytes daCert,
        uint64 l2BlockStart,
        uint64 l2BlockEnd,
        bytes32 recordsCommitment
    );

    error NotBatcher();
    error StaleBatchIndex();
    error EmptyCert();
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
    ///         check). The batch bytes are not on chain: the certificate
    ///         names them in EigenDA.
    function postBatch(
        uint64 prevBatchIndex,
        bytes calldata daCert,
        uint64 l2BlockStart,
        uint64 l2BlockEnd,
        bytes32 recordsCommitment
    ) external {
        if (msg.sender != l1Batcher) revert NotBatcher();
        if (prevBatchIndex != lastBatchIndex) revert StaleBatchIndex();
        if (daCert.length == 0) revert EmptyCert();
        if (l2BlockEnd < l2BlockStart) revert BadBlockRange();

        uint64 next = prevBatchIndex + 1;
        lastBatchIndex = next;
        batches[next] = BatchEntry({
            l2BlockStart: l2BlockStart, l2BlockEnd: l2BlockEnd, recordsCommitment: recordsCommitment
        });
        emit BatchPosted(next, daCert, l2BlockStart, l2BlockEnd, recordsCommitment);
    }
}
