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

/// @notice The settlement surface the oracle cross-reads (spec PR 4/PR 5).
interface IKardamomL2Settlement {
    function batches(uint64 index)
        external
        view
        returns (uint64 l2BlockStart, uint64 l2BlockEnd, bytes32 recordsCommitment);

    function lastBatchIndex() external view returns (uint64);
}

/// @title KardamomProofOracle
/// @notice The zk root chain, v2 (spec: no-std-exec-core, PR 5): one root
///         chain, two ways to advance it.
///
///         VALIDITY mode (`submitBatchProof`): a batch validity proof
///         advances the root immediately — PR 4 semantics, kept for
///         operators that want instant finality.
///
///         OPTIMISTIC mode: a bonded `claimBatch` carries PER-BLOCK
///         attestations — `(post_root, records_digest)` per block, pinned
///         to the settlement's stored records commitment by a claim-time
///         fold check — and finalizes after an unchallenged window.
///         `challengeBlock` targets the FIRST divergent block with a
///         SINGLE-BLOCK proof: at the first divergence the claimed parent
///         root is still honest, so the refuting proof always exists (no
///         bisection). A won challenge slashes the bond to the challenger,
///         cascade-cancels dependent pending claims (refunds — their fault
///         was a wrong base), and REWINDS: the root chain stays at the last
///         finalized root and the batch reopens for an honest claim.
///
///         Bond refunds and slash rewards are PULL payments (`withdraw`).
/// @dev    Single-block public values (160 bytes, 5x32): preStateRoot ||
///         postStateRoot || blockNumber(u256) || recordsDigest ||
///         balCommitment. Batch public values as in PR 4.
contract KardamomProofOracle is KardamomUUPSBase {
    struct Claim {
        address claimer;
        uint96 bond;
        uint64 claimedAt;
        /// @dev The root this claim chains from (parent claim's final root,
        ///      or `stateRoot` for the first pending claim).
        bytes32 preRoot;
        /// @dev `blockRoots[last]` — what `stateRoot` becomes on finalize.
        bytes32 finalRoot;
        /// @dev keccak(abi.encode(blockRoots, blockDigests)) — the
        ///      challenge re-derives it from calldata arrays.
        bytes32 seqHash;
    }

    IKardamomL2Settlement public settlement;
    ISP1Verifier public verifier;
    /// @notice The BATCH guest's verifying key (validity mode).
    bytes32 public batchVKey;
    /// @notice The SINGLE-BLOCK guest's verifying key (dispute mode).
    bytes32 public blockVKey;
    /// @notice The proven/finalized running L2 state root.
    bytes32 public stateRoot;
    /// @notice Index of the last FINALIZED batch (by proof or by window).
    uint64 public lastFinalizedBatch;
    /// @notice Index of the highest pending claim (== lastFinalizedBatch
    ///         when nothing is pending).
    uint64 public highestClaimedBatch;
    /// @notice Challenge window in seconds.
    uint64 public challengeWindow;
    /// @notice Minimum claim bond in wei.
    uint96 public minBond;

    mapping(uint64 => Claim) public claims;
    /// @notice Pull-payment balances (bond refunds, slash rewards).
    mapping(address => uint256) public withdrawable;

    event BatchClaimed(
        uint64 indexed batchIndex,
        address indexed claimer,
        bytes32 preRoot,
        bytes32 finalRoot,
        bytes32 seqHash
    );
    event BatchFinalized(uint64 indexed batchIndex, bytes32 postStateRoot, bool byProof);
    event ClaimChallenged(
        uint64 indexed batchIndex,
        uint64 blockOffset,
        address indexed challenger,
        bytes32 claimedRoot,
        bytes32 provenRoot
    );
    event ClaimCancelled(uint64 indexed batchIndex, address indexed claimer, bool slashed);
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
    error PendingClaimsExist();
    error BondTooSmall();
    error LengthMismatch();
    error DigestFoldMismatch();
    error NoSuchClaim();
    error SequenceMismatch();
    error WindowNotElapsed();
    error BadOffset();
    error BlockNumberMismatch();
    error BlockDigestMismatch();
    error ProofAgreesWithClaim();
    error NothingToWithdraw();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(
        address _settlement,
        address _verifier,
        bytes32 _batchVKey,
        bytes32 _blockVKey,
        bytes32 _genesisRoot,
        uint64 _challengeWindow,
        uint96 _minBond
    ) external initializer {
        settlement = IKardamomL2Settlement(_settlement);
        verifier = ISP1Verifier(_verifier);
        batchVKey = _batchVKey;
        blockVKey = _blockVKey;
        stateRoot = _genesisRoot;
        challengeWindow = _challengeWindow;
        minBond = _minBond;
    }

    // ------------------------------------------------------------------
    // Optimistic mode
    // ------------------------------------------------------------------

    /// @notice Claim a posted batch with per-block attestations. Bonded;
    ///         permissionless; claims chain ahead of finalization.
    function claimBatch(
        uint64 batchIndex,
        bytes32[] calldata blockRoots,
        bytes32[] calldata blockDigests
    ) external payable {
        if (batchIndex != highestClaimedBatch + 1) {
            revert NonSequentialBatch();
        }
        if (msg.value < minBond || msg.value > type(uint96).max) revert BondTooSmall();

        (uint64 postedStart, uint64 postedEnd, bytes32 postedCommitment) =
            settlement.batches(batchIndex);
        if (postedCommitment == bytes32(0)) revert UnknownBatch();
        uint256 blocksInBatch = uint256(postedEnd) - uint256(postedStart) + 1;
        if (blockRoots.length != blocksInBatch || blockDigests.length != blocksInBatch) {
            revert LengthMismatch();
        }
        // The anti-smuggling check: the claimed per-block record partition
        // must fold to EXACTLY the commitment the batcher posted.
        if (_foldDigests(blockDigests) != postedCommitment) revert DigestFoldMismatch();

        bytes32 preRoot =
            batchIndex == lastFinalizedBatch + 1 ? stateRoot : claims[batchIndex - 1].finalRoot;
        bytes32 seqHash = keccak256(abi.encode(blockRoots, blockDigests));
        claims[batchIndex] = Claim({
            claimer: msg.sender,
            bond: uint96(msg.value),
            claimedAt: uint64(block.timestamp),
            preRoot: preRoot,
            finalRoot: blockRoots[blockRoots.length - 1],
            seqHash: seqHash
        });
        highestClaimedBatch = batchIndex;
        emit BatchClaimed(
            batchIndex, msg.sender, preRoot, blockRoots[blockRoots.length - 1], seqHash
        );
    }

    /// @notice Finalize the next claim once its window elapsed unchallenged.
    function finalizeBatch(uint64 batchIndex) external {
        if (batchIndex != lastFinalizedBatch + 1) revert NonSequentialBatch();
        Claim memory c = claims[batchIndex];
        if (c.claimer == address(0)) revert NoSuchClaim();
        if (block.timestamp < uint256(c.claimedAt) + challengeWindow) revert WindowNotElapsed();

        stateRoot = c.finalRoot;
        lastFinalizedBatch = batchIndex;
        withdrawable[c.claimer] += c.bond;
        delete claims[batchIndex];
        emit BatchFinalized(batchIndex, stateRoot, false);
    }

    /// @notice Refute a pending claim at its FIRST divergent block with a
    ///         single-block validity proof. Slashes the claim's bond to the
    ///         challenger, cascade-cancels dependent claims (refunded), and
    ///         REWINDS — `stateRoot` does not move; the batch reopens.
    function challengeBlock(
        uint64 batchIndex,
        uint64 blockOffset,
        bytes32[] calldata blockRoots,
        bytes32[] calldata blockDigests,
        bytes calldata publicValues,
        bytes calldata proof
    ) external {
        if (publicValues.length != 160) revert BadPublicValuesLength();
        Claim memory c = claims[batchIndex];
        if (c.claimer == address(0)) revert NoSuchClaim();
        if (keccak256(abi.encode(blockRoots, blockDigests)) != c.seqHash) {
            revert SequenceMismatch();
        }
        if (blockOffset >= blockRoots.length) revert BadOffset();

        bytes32 provenRoot = _checkRefutation(
            c.preRoot, batchIndex, blockOffset, blockRoots, blockDigests, publicValues
        );
        verifier.verifyProof(blockVKey, publicValues, proof);
        _resolveChallenge(batchIndex, blockOffset, blockRoots[blockOffset], provenRoot, c);
    }

    /// @dev The won-challenge effects: slash to the challenger, cascade-
    ///      cancel dependents with refunds, REWIND (stateRoot untouched).
    function _resolveChallenge(
        uint64 batchIndex,
        uint64 blockOffset,
        bytes32 claimedRoot,
        bytes32 provenRoot,
        Claim memory c
    ) internal {
        emit ClaimChallenged(batchIndex, blockOffset, msg.sender, claimedRoot, provenRoot);
        withdrawable[msg.sender] += c.bond;
        emit ClaimCancelled(batchIndex, c.claimer, true);
        delete claims[batchIndex];
        for (uint64 i = batchIndex + 1; i <= highestClaimedBatch; i++) {
            Claim memory dep = claims[i];
            if (dep.claimer != address(0)) {
                withdrawable[dep.claimer] += dep.bond;
                emit ClaimCancelled(i, dep.claimer, false);
                delete claims[i];
            }
        }
        highestClaimedBatch = batchIndex - 1;
    }

    /// @notice Pull bond refunds / slash rewards.
    function withdraw() external {
        uint256 amount = withdrawable[msg.sender];
        if (amount == 0) revert NothingToWithdraw();
        withdrawable[msg.sender] = 0;
        (bool ok,) = msg.sender.call{value: amount}("");
        require(ok, "withdraw transfer failed");
    }

    // ------------------------------------------------------------------
    // Validity mode (PR 4, kept)
    // ------------------------------------------------------------------

    /// @notice Verify one batch's validity proof and advance the root
    ///         immediately. Only when no claims are pending — the two modes
    ///         do not interleave mid-chain.
    function submitBatchProof(uint64 batchIndex, bytes calldata publicValues, bytes calldata proof)
        external
    {
        if (publicValues.length != 160) revert BadPublicValuesLength();
        if (batchIndex != lastFinalizedBatch + 1) revert NonSequentialBatch();
        if (highestClaimedBatch != lastFinalizedBatch) revert PendingClaimsExist();

        (
            bytes32 preRoot,
            bytes32 postRoot,
            uint256 firstBlock,
            uint256 lastBlock,
            bytes32 recordsCommitment
        ) = abi.decode(publicValues, (bytes32, bytes32, uint256, uint256, bytes32));

        (uint64 postedStart, uint64 postedEnd, bytes32 postedCommitment) =
            settlement.batches(batchIndex);
        if (postedCommitment == bytes32(0)) revert UnknownBatch();
        if (firstBlock != postedStart || lastBlock != postedEnd) revert RangeMismatch();
        if (recordsCommitment != postedCommitment) revert RecordsCommitmentMismatch();
        if (preRoot != stateRoot) revert PreRootMismatch();

        verifier.verifyProof(batchVKey, publicValues, proof);

        stateRoot = postRoot;
        lastFinalizedBatch = batchIndex;
        highestClaimedBatch = batchIndex;
        emit BatchProven(batchIndex, preRoot, postRoot, recordsCommitment);
        emit BatchFinalized(batchIndex, postRoot, true);
    }

    /// @dev The refutation preconditions, isolated so `challengeBlock`'s
    ///      frame stays shallow: decode the 160-byte public values, anchor
    ///      the pre-root into the claimed sequence (first-divergence rule),
    ///      pin block number and records digest, and require disagreement.
    ///      Returns the proven post root for the event.
    function _checkRefutation(
        bytes32 claimPreRoot,
        uint64 batchIndex,
        uint64 blockOffset,
        bytes32[] calldata blockRoots,
        bytes32[] calldata blockDigests,
        bytes calldata publicValues
    ) internal view returns (bytes32) {
        (bytes32 pre, bytes32 post, uint256 blockNumber, bytes32 recordsDigest,) =
            abi.decode(publicValues, (bytes32, bytes32, uint256, bytes32, bytes32));
        (uint64 postedStart,,) = settlement.batches(batchIndex);
        bytes32 expectedPre = blockOffset == 0 ? claimPreRoot : blockRoots[blockOffset - 1];
        if (pre != expectedPre) revert PreRootMismatch();
        if (blockNumber != uint256(postedStart) + blockOffset) revert BlockNumberMismatch();
        if (recordsDigest != blockDigests[blockOffset]) revert BlockDigestMismatch();
        if (post == blockRoots[blockOffset]) revert ProofAgreesWithClaim();
        return post;
    }

    /// @dev keccak("KBAT" || d_0 || .. || d_n-1) — byte-identical to
    ///      `kardamom_types::batch_records_commitment`.
    function _foldDigests(bytes32[] calldata digests) internal pure returns (bytes32) {
        bytes memory buf = new bytes(4 + digests.length * 32);
        buf[0] = "K";
        buf[1] = "B";
        buf[2] = "A";
        buf[3] = "T";
        for (uint256 i = 0; i < digests.length; i++) {
            bytes32 d = digests[i];
            assembly {
                mstore(add(add(buf, 36), mul(i, 32)), d)
            }
        }
        return keccak256(buf);
    }
}
