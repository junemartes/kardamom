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
    bytes32 constant VKEY = keccak256("kardamom-batch-guest-vkey");
    bytes32 constant GENESIS_ROOT = keccak256("genesis");
    bytes32 constant ROOT_A = keccak256("root-a");
    bytes32 constant ROOT_B = keccak256("root-b");
    bytes32 constant RC1 = keccak256("records-1");
    bytes32 constant RC2 = keccak256("records-2");

    event BatchProven(
        uint64 indexed batchIndex,
        bytes32 preStateRoot,
        bytes32 postStateRoot,
        bytes32 recordsCommitment
    );

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
        KardamomProofOracle oImpl = new KardamomProofOracle();
        oracle = KardamomProofOracle(
            address(
                new ERC1967Proxy(
                    address(oImpl),
                    abi.encodeWithSelector(
                        KardamomProofOracle.initialize.selector,
                        address(settlement),
                        address(accepting),
                        VKEY,
                        GENESIS_ROOT
                    )
                )
            )
        );
    }

    function _post(uint64 prev, uint64 startBlock, uint64 endBlock, bytes32 rc) internal {
        bytes32[] memory h = new bytes32[](1);
        h[0] = bytes32(uint256(0xC0FFEE));
        vm.prank(BATCHER);
        settlement.postBatch(prev, h, startBlock, endBlock, rc);
    }

    function _pv(bytes32 pre, bytes32 post, uint256 first, uint256 last, bytes32 rc)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encode(pre, post, first, last, rc);
    }

    function test_initial_state() public view {
        assertEq(oracle.stateRoot(), GENESIS_ROOT);
        assertEq(oracle.lastProvenBatch(), 0);
    }

    function test_proves_batch_and_advances_root() public {
        _post(0, 1, 5, RC1);
        vm.expectEmit(true, false, false, true);
        emit BatchProven(1, GENESIS_ROOT, ROOT_A, RC1);
        oracle.submitBatchProof(1, _pv(GENESIS_ROOT, ROOT_A, 1, 5, RC1), hex"");
        assertEq(oracle.stateRoot(), ROOT_A);
        assertEq(oracle.lastProvenBatch(), 1);
    }

    function test_root_chain_across_two_batches() public {
        _post(0, 1, 5, RC1);
        _post(1, 6, 9, RC2);
        oracle.submitBatchProof(1, _pv(GENESIS_ROOT, ROOT_A, 1, 5, RC1), hex"");
        oracle.submitBatchProof(2, _pv(ROOT_A, ROOT_B, 6, 9, RC2), hex"");
        assertEq(oracle.stateRoot(), ROOT_B);
        assertEq(oracle.lastProvenBatch(), 2);
    }

    function test_rejects_non_sequential_batch() public {
        _post(0, 1, 5, RC1);
        _post(1, 6, 9, RC2);
        vm.expectRevert(KardamomProofOracle.NonSequentialBatch.selector);
        oracle.submitBatchProof(2, _pv(GENESIS_ROOT, ROOT_A, 6, 9, RC2), hex"");
    }

    function test_rejects_unposted_batch() public {
        vm.expectRevert(KardamomProofOracle.UnknownBatch.selector);
        oracle.submitBatchProof(1, _pv(GENESIS_ROOT, ROOT_A, 1, 5, RC1), hex"");
    }

    function test_rejects_range_mismatch() public {
        _post(0, 1, 5, RC1);
        vm.expectRevert(KardamomProofOracle.RangeMismatch.selector);
        oracle.submitBatchProof(1, _pv(GENESIS_ROOT, ROOT_A, 1, 4, RC1), hex"");
    }

    function test_rejects_records_commitment_mismatch() public {
        _post(0, 1, 5, RC1);
        vm.expectRevert(KardamomProofOracle.RecordsCommitmentMismatch.selector);
        oracle.submitBatchProof(1, _pv(GENESIS_ROOT, ROOT_A, 1, 5, RC2), hex"");
    }

    function test_rejects_wrong_pre_root() public {
        _post(0, 1, 5, RC1);
        vm.expectRevert(KardamomProofOracle.PreRootMismatch.selector);
        oracle.submitBatchProof(1, _pv(ROOT_A, ROOT_B, 1, 5, RC1), hex"");
    }

    function test_rejects_bad_public_values_length() public {
        _post(0, 1, 5, RC1);
        vm.expectRevert(KardamomProofOracle.BadPublicValuesLength.selector);
        oracle.submitBatchProof(1, hex"deadbeef", hex"");
    }

    function test_verifier_rejection_propagates() public {
        RejectingVerifier rejecting = new RejectingVerifier();
        KardamomProofOracle oImpl = new KardamomProofOracle();
        KardamomProofOracle strict = KardamomProofOracle(
            address(
                new ERC1967Proxy(
                    address(oImpl),
                    abi.encodeWithSelector(
                        KardamomProofOracle.initialize.selector,
                        address(settlement),
                        address(rejecting),
                        VKEY,
                        GENESIS_ROOT
                    )
                )
            )
        );
        _post(0, 1, 5, RC1);
        vm.expectRevert(RejectingVerifier.BadProof.selector);
        strict.submitBatchProof(1, _pv(GENESIS_ROOT, ROOT_A, 1, 5, RC1), hex"");
        // And the root did NOT advance.
        assertEq(strict.stateRoot(), GENESIS_ROOT);
    }

    function test_unauthorized_upgrade_reverts() public {
        KardamomProofOracle newImpl = new KardamomProofOracle();
        (bool ok,) = address(oracle)
            .call(abi.encodeWithSignature("upgradeToAndCall(address,bytes)", address(newImpl), ""));
        assertFalse(ok, "non-factory upgrade must revert");
    }
}
