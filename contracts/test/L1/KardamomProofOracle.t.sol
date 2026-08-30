// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {KardamomL2Settlement} from "../../src/L2/KardamomL2Settlement.sol";
import {ISP1Verifier, KardamomProofOracle} from "../../src/L1/KardamomProofOracle.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

/// Accepts every proof — the contract-logic tests' stand-in for the real
/// SP1 gateway (a deploy parameter in production).
contract AcceptingVerifier is ISP1Verifier {
    function verifyProof(bytes32, bytes calldata, bytes calldata) external pure {}
}

/// Rejects every proof — the verifier-is-actually-consulted test.
contract RejectingVerifier is ISP1Verifier {
    error BadProof();

    function verifyProof(bytes32, bytes calldata, bytes calldata) external pure {
        revert BadProof();
    }
}

contract KardamomProofOracleTest is Test {
    KardamomL2Settlement settlement;
    KardamomProofOracle oracle;
    AcceptingVerifier accepting;

    address constant BATCHER = address(0xBA7);
    address constant CLAIMER = address(0xC1A);
    address constant CLAIMER2 = address(0xC1B);
    address constant CHALLENGER = address(0xCA11E);
    bytes32 constant BATCH_VKEY = keccak256("batch-vkey");
    bytes32 constant BLOCK_VKEY = keccak256("block-vkey");
    bytes32 constant GENESIS_ROOT = keccak256("genesis");
    uint64 constant WINDOW = 1 days;
    uint96 constant BOND = 1 ether;

    // Batch 1: blocks 100..101.
    bytes32 D0 = keccak256("digest-100");
    bytes32 D1 = keccak256("digest-101");
    bytes32 R0 = keccak256("root-100");
    bytes32 R1 = keccak256("root-101");
    // The HONEST root for block 100 (differs from the claimed R0 in the
    // lying-claim tests).
    bytes32 H0 = keccak256("honest-100");

    function setUp() public {
        KardamomL2Settlement sImpl = new KardamomL2Settlement();
        settlement = KardamomL2Settlement(
            address(
                new ERC1967Proxy(
                    address(sImpl),
                    abi.encodeWithSelector(KardamomL2Settlement.initialize.selector, BATCHER)
                )
            )
        );
        accepting = new AcceptingVerifier();
        oracle = _deployOracle(address(accepting));
        vm.deal(CLAIMER, 10 ether);
        vm.deal(CLAIMER2, 10 ether);
    }

    function _deployOracle(address verifier) internal returns (KardamomProofOracle) {
        KardamomProofOracle oImpl = new KardamomProofOracle();
        return KardamomProofOracle(
            address(
                new ERC1967Proxy(
                    address(oImpl),
                    abi.encodeWithSelector(
                        KardamomProofOracle.initialize.selector,
                        address(settlement),
                        verifier,
                        BATCH_VKEY,
                        BLOCK_VKEY,
                        GENESIS_ROOT,
                        WINDOW,
                        BOND
                    )
                )
            )
        );
    }

    function _fold(bytes32[] memory digests) internal pure returns (bytes32) {
        bytes memory buf = abi.encodePacked("KBAT");
        for (uint256 i = 0; i < digests.length; i++) {
            buf = abi.encodePacked(buf, digests[i]);
        }
        return keccak256(buf);
    }

    function _digests() internal view returns (bytes32[] memory d) {
        d = new bytes32[](2);
        d[0] = D0;
        d[1] = D1;
    }

    function _roots() internal view returns (bytes32[] memory r) {
        r = new bytes32[](2);
        r[0] = R0;
        r[1] = R1;
    }

    /// Post batch `index` covering blocks 100..101 with the digests' fold.
    function _postBatch(uint64 prev) internal {
        bytes32[] memory h = new bytes32[](1);
        h[0] = bytes32(uint256(0xC0FFEE));
        vm.prank(BATCHER);
        settlement.postBatch(prev, h, 100, 101, _fold(_digests()));
    }

    function _claim() internal {
        vm.prank(CLAIMER);
        oracle.claimBatch{value: BOND}(1, _roots(), _digests());
    }

    /// Single-block public values (160 bytes, 5x32).
    function _blockPv(bytes32 pre, bytes32 post, uint256 blockNumber, bytes32 digest)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encode(pre, post, blockNumber, digest, bytes32(uint256(0xBA1)));
    }

    // ------------------------------------------------------------------
    // Optimistic happy path
    // ------------------------------------------------------------------

    function test_claim_finalize_advances_root_and_refunds() public {
        _postBatch(0);
        _claim();
        assertEq(oracle.highestClaimedBatch(), 1);
        assertEq(oracle.stateRoot(), GENESIS_ROOT, "no advance before finalize");

        vm.warp(block.timestamp + WINDOW);
        oracle.finalizeBatch(1);
        assertEq(oracle.stateRoot(), R1);
        assertEq(oracle.lastFinalizedBatch(), 1);
        assertEq(oracle.withdrawable(CLAIMER), BOND);

        uint256 before = CLAIMER.balance;
        vm.prank(CLAIMER);
        oracle.withdraw();
        assertEq(CLAIMER.balance, before + BOND);
    }

    function test_finalize_before_window_reverts() public {
        _postBatch(0);
        _claim();
        vm.warp(block.timestamp + WINDOW - 1);
        vm.expectRevert(KardamomProofOracle.WindowNotElapsed.selector);
        oracle.finalizeBatch(1);
    }

    function test_claims_chain_ahead_of_finalization() public {
        _postBatch(0);
        _postBatch(1); // batch 2, same range shape for simplicity
        _claim();
        vm.prank(CLAIMER2);
        oracle.claimBatch{value: BOND}(2, _roots(), _digests());
        assertEq(oracle.highestClaimedBatch(), 2);
        // Claim 2's preRoot chained off claim 1's final root.
        (,,, bytes32 pre2,,) = oracle.claims(2);
        assertEq(pre2, R1);
    }

    // ------------------------------------------------------------------
    // Claim rejections
    // ------------------------------------------------------------------

    function test_claim_rejects_bad_fold() public {
        _postBatch(0);
        bytes32[] memory wrong = _digests();
        wrong[1] = keccak256("smuggled-partition");
        vm.prank(CLAIMER);
        vm.expectRevert(KardamomProofOracle.DigestFoldMismatch.selector);
        oracle.claimBatch{value: BOND}(1, _roots(), wrong);
    }

    function test_claim_rejects_wrong_lengths() public {
        _postBatch(0);
        bytes32[] memory short_ = new bytes32[](1);
        short_[0] = R0;
        vm.prank(CLAIMER);
        vm.expectRevert(KardamomProofOracle.LengthMismatch.selector);
        oracle.claimBatch{value: BOND}(1, short_, _digests());
    }

    function test_claim_rejects_small_bond() public {
        _postBatch(0);
        vm.prank(CLAIMER);
        vm.expectRevert(KardamomProofOracle.BondTooSmall.selector);
        oracle.claimBatch{value: BOND - 1}(1, _roots(), _digests());
    }

    function test_claim_rejects_unposted_batch() public {
        vm.prank(CLAIMER);
        vm.expectRevert(KardamomProofOracle.UnknownBatch.selector);
        oracle.claimBatch{value: BOND}(1, _roots(), _digests());
    }

    function test_claim_rejects_non_sequential() public {
        _postBatch(0);
        vm.prank(CLAIMER);
        vm.expectRevert(KardamomProofOracle.NonSequentialBatch.selector);
        oracle.claimBatch{value: BOND}(2, _roots(), _digests());
    }

    // ------------------------------------------------------------------
    // Challenges
    // ------------------------------------------------------------------

    function test_challenge_at_offset_zero_slashes_cascades_and_rewinds() public {
        _postBatch(0);
        _postBatch(1);
        _claim(); // batch 1 (the lie: R0 != honest H0)
        vm.prank(CLAIMER2);
        oracle.claimBatch{value: BOND}(2, _roots(), _digests()); // dependent

        // The refuting proof: pre = claim's preRoot (genesis), proven post
        // H0 != claimed R0, block number 100, digest D0.
        vm.prank(CHALLENGER);
        oracle.challengeBlock(
            1, 0, _roots(), _digests(), _blockPv(GENESIS_ROOT, H0, 100, D0), hex""
        );

        // Slash to challenger; dependent refunded; rewind.
        assertEq(oracle.withdrawable(CHALLENGER), BOND);
        assertEq(oracle.withdrawable(CLAIMER2), BOND);
        assertEq(oracle.withdrawable(CLAIMER), 0, "liar's bond is gone");
        assertEq(oracle.stateRoot(), GENESIS_ROOT, "rewind: root untouched");
        assertEq(oracle.highestClaimedBatch(), 0);
        assertEq(oracle.lastFinalizedBatch(), 0);

        // The batch reopens for an honest claim.
        vm.prank(CLAIMER2);
        oracle.claimBatch{value: BOND}(1, _roots(), _digests());
        assertEq(oracle.highestClaimedBatch(), 1);
    }

    function test_challenge_mid_offset_uses_previous_claimed_root() public {
        _postBatch(0);
        _claim();
        // Lie at offset 1: pre = claimed R0, proven post != R1.
        vm.prank(CHALLENGER);
        oracle.challengeBlock(1, 1, _roots(), _digests(), _blockPv(R0, H0, 101, D1), hex"");
        assertEq(oracle.withdrawable(CHALLENGER), BOND);
    }

    function test_agreeing_proof_reverts_and_claim_survives() public {
        _postBatch(0);
        _claim();
        vm.prank(CHALLENGER);
        vm.expectRevert(KardamomProofOracle.ProofAgreesWithClaim.selector);
        oracle.challengeBlock(
            1, 0, _roots(), _digests(), _blockPv(GENESIS_ROOT, R0, 100, D0), hex""
        );
        // Strict no-op: claim intact, window position unchanged.
        (address claimer,,,,,) = oracle.claims(1);
        assertEq(claimer, CLAIMER);
    }

    function test_challenge_rejects_wrong_sequence_arrays() public {
        _postBatch(0);
        _claim();
        bytes32[] memory forged = _roots();
        forged[0] = keccak256("forged");
        vm.expectRevert(KardamomProofOracle.SequenceMismatch.selector);
        oracle.challengeBlock(1, 0, forged, _digests(), _blockPv(GENESIS_ROOT, H0, 100, D0), hex"");
    }

    function test_challenge_rejects_digest_mismatch() public {
        _postBatch(0);
        _claim();
        vm.expectRevert(KardamomProofOracle.BlockDigestMismatch.selector);
        oracle.challengeBlock(
            1,
            0,
            _roots(),
            _digests(),
            _blockPv(GENESIS_ROOT, H0, 100, keccak256("other-digest")),
            hex""
        );
    }

    function test_challenge_rejects_wrong_pre_root() public {
        _postBatch(0);
        _claim();
        vm.expectRevert(KardamomProofOracle.PreRootMismatch.selector);
        oracle.challengeBlock(
            1, 0, _roots(), _digests(), _blockPv(keccak256("bad-pre"), H0, 100, D0), hex""
        );
    }

    function test_challenge_rejects_wrong_block_number() public {
        _postBatch(0);
        _claim();
        vm.expectRevert(KardamomProofOracle.BlockNumberMismatch.selector);
        oracle.challengeBlock(
            1, 0, _roots(), _digests(), _blockPv(GENESIS_ROOT, H0, 101, D0), hex""
        );
    }

    function test_challenge_verifier_rejection_propagates() public {
        RejectingVerifier rejecting = new RejectingVerifier();
        KardamomProofOracle strict = _deployOracle(address(rejecting));
        _postBatch(0);
        vm.prank(CLAIMER);
        strict.claimBatch{value: BOND}(1, _roots(), _digests());
        vm.expectRevert(RejectingVerifier.BadProof.selector);
        strict.challengeBlock(
            1, 0, _roots(), _digests(), _blockPv(GENESIS_ROOT, H0, 100, D0), hex""
        );
    }

    // ------------------------------------------------------------------
    // Validity mode + interleaving
    // ------------------------------------------------------------------

    function _batchPv(bytes32 pre, bytes32 post) internal view returns (bytes memory) {
        return abi.encode(pre, post, uint256(100), uint256(101), _fold(_digests()));
    }

    function test_validity_mode_still_advances() public {
        _postBatch(0);
        oracle.submitBatchProof(1, _batchPv(GENESIS_ROOT, R1), hex"");
        assertEq(oracle.stateRoot(), R1);
        assertEq(oracle.lastFinalizedBatch(), 1);
        assertEq(oracle.highestClaimedBatch(), 1);
    }

    function test_validity_mode_blocked_by_pending_claims() public {
        _postBatch(0);
        _claim();
        vm.expectRevert(KardamomProofOracle.PendingClaimsExist.selector);
        oracle.submitBatchProof(1, _batchPv(GENESIS_ROOT, R1), hex"");
    }

    function test_withdraw_nothing_reverts() public {
        vm.expectRevert(KardamomProofOracle.NothingToWithdraw.selector);
        oracle.withdraw();
    }

    function test_unauthorized_upgrade_reverts() public {
        KardamomProofOracle newImpl = new KardamomProofOracle();
        (bool ok,) = address(oracle)
            .call(abi.encodeWithSignature("upgradeToAndCall(address,bytes)", address(newImpl), ""));
        assertFalse(ok, "non-factory upgrade must revert");
    }
}
