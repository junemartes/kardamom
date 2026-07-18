// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {ETHLockbox} from "../../src/L1/ETHLockbox.sol";
import {WithdrawalOutputOracle} from "../../src/L1/WithdrawalOutputOracle.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

/// End-to-end L1 withdrawal protocol test against the REAL oracle (no mocks):
/// deposit funds the lockbox, the attester proposes an output committing a
/// withdrawals root, and a withdrawal finalizes only after the 1-day window —
/// while the permissioned challenge (deleteOutput) blocks a bad output.
contract WithdrawalFlowTest is Test {
    ETHLockbox lockbox;
    WithdrawalOutputOracle oracle;

    address constant ATTESTER = address(0xA77E);
    address constant CHALLENGER = address(0xC4A1);
    address constant L2_MINTER = address(0xBEEF);
    uint64 constant WINDOW = 1 days;

    uint8 constant OUTPUT_VERSION = 0;
    address constant ALICE_L2 = address(0xA11CE);
    address constant ALICE_L1 = address(0xA11CE);
    address constant BOB_L2 = address(0xB0B);
    address constant BOB_L1 = address(0xB0B);

    function setUp() public {
        // Oracle proxy.
        WithdrawalOutputOracle oImpl = new WithdrawalOutputOracle();
        oracle = WithdrawalOutputOracle(
            address(
                new ERC1967Proxy(
                    address(oImpl),
                    abi.encodeWithSelector(
                        WithdrawalOutputOracle.initialize.selector, ATTESTER, CHALLENGER, WINDOW
                    )
                )
            )
        );
        // Lockbox proxy, pointed at the oracle.
        ETHLockbox lImpl = new ETHLockbox();
        lockbox = ETHLockbox(
            payable(address(
                    new ERC1967Proxy(
                        address(lImpl),
                        abi.encodeWithSelector(
                            ETHLockbox.initialize.selector, L2_MINTER, address(oracle)
                        )
                    )
                ))
        );
        vm.warp(1_700_000_000);
    }

    function _leaf(uint256 nonce, address l2sender, address l1target, uint256 value)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(abi.encode(nonce, l2sender, l1target, value));
    }

    function _outputRoot(bytes32 stateRoot, bytes32 withdrawalsRoot)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot));
    }

    /// Domain-separated tree hashing — mirrors ETHLockbox LEAF_DOMAIN/NODE_DOMAIN
    /// and kardamom_types::withdrawals.
    function _hashLeaf(bytes32 wh) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(bytes1(0x00), wh));
    }

    function _hashNode(bytes32 l, bytes32 r) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(bytes1(0x01), l, r));
    }

    /// Full happy path: deposit -> attest output with a 2-withdrawal tree ->
    /// wait the window -> finalize both withdrawals -> recipients paid.
    function test_full_flow_deposit_attest_finalize() public {
        // 1. Deposits fund the lockbox (the on-ramp already works).
        vm.deal(ALICE_L1, 10 ether);
        vm.prank(ALICE_L1);
        lockbox.depositETH{value: 6 ether}(ALICE_L2, 0, hex"");
        assertEq(address(lockbox).balance, 6 ether);

        // 2. Two withdrawals were initiated on L2 in this output's block range.
        bytes32 wlAlice = _leaf(0, ALICE_L2, ALICE_L1, 1 ether);
        bytes32 wlBob = _leaf(1, BOB_L2, BOB_L1, 2 ether);
        bytes32 withdrawalsRoot = _hashNode(_hashLeaf(wlAlice), _hashLeaf(wlBob));
        bytes32 stateRoot = keccak256("l2-state-root@block-100");

        // 3. Attester proposes the output for L2 block 100.
        vm.prank(ATTESTER);
        uint256 outIdx = oracle.proposeOutput(_outputRoot(stateRoot, withdrawalsRoot), 100);

        // 4. Finalizing before the window must fail.
        ETHLockbox.WithdrawalTransaction memory aTx =
            ETHLockbox.WithdrawalTransaction(0, ALICE_L2, ALICE_L1, 1 ether);
        bytes32[] memory aProof = new bytes32[](1);
        aProof[0] = _hashLeaf(wlBob); // alice is index 0, sibling node is bob's leaf hash
        vm.expectRevert(ETHLockbox.NotFinalizable.selector);
        lockbox.finalizeWithdrawal(aTx, outIdx, stateRoot, withdrawalsRoot, 0, aProof);

        // 5. Warp past the 1-day window, then finalize Alice.
        vm.warp(block.timestamp + WINDOW + 1);
        lockbox.finalizeWithdrawal(aTx, outIdx, stateRoot, withdrawalsRoot, 0, aProof);
        assertEq(ALICE_L1.balance, 4 ether + 1 ether); // 10 - 6 deposited + 1 withdrawn

        // 6. Finalize Bob (index 1, sibling is alice).
        ETHLockbox.WithdrawalTransaction memory bTx =
            ETHLockbox.WithdrawalTransaction(1, BOB_L2, BOB_L1, 2 ether);
        bytes32[] memory bProof = new bytes32[](1);
        bProof[0] = _hashLeaf(wlAlice);
        lockbox.finalizeWithdrawal(bTx, outIdx, stateRoot, withdrawalsRoot, 1, bProof);
        assertEq(BOB_L1.balance, 2 ether);

        assertEq(address(lockbox).balance, 3 ether); // 6 - 1 - 2
    }

    /// Challenge path: a bad output is deleted within the window; withdrawals
    /// against it can never finalize even after the window passes.
    function test_challenge_blocks_finalization() public {
        vm.deal(ALICE_L1, 10 ether);
        vm.prank(ALICE_L1);
        lockbox.depositETH{value: 5 ether}(ALICE_L2, 0, hex"");

        bytes32 wlAlice = _leaf(0, ALICE_L2, ALICE_L1, 1 ether);
        bytes32 withdrawalsRoot = _hashLeaf(wlAlice); // single-leaf tree
        bytes32 stateRoot = keccak256("fraudulent-state-root");

        vm.prank(ATTESTER);
        uint256 outIdx = oracle.proposeOutput(_outputRoot(stateRoot, withdrawalsRoot), 100);

        // Challenger (stand-in for a ZK fault proof) deletes the bad output.
        vm.prank(CHALLENGER);
        oracle.deleteOutput(outIdx);

        // Even after the window, the deleted output cannot finalize anything.
        vm.warp(block.timestamp + WINDOW + 1);
        ETHLockbox.WithdrawalTransaction memory aTx =
            ETHLockbox.WithdrawalTransaction(0, ALICE_L2, ALICE_L1, 1 ether);
        bytes32[] memory empty = new bytes32[](0);
        vm.expectRevert(ETHLockbox.NotFinalizable.selector);
        lockbox.finalizeWithdrawal(aTx, outIdx, stateRoot, withdrawalsRoot, 0, empty);
        assertEq(ALICE_L1.balance, 5 ether); // only the un-deposited remainder
    }

    /// Regression for the stranded-range deadlock: after a successful challenge
    /// deletes a bad output, the attester re-proposes a CORRECTED output for the
    /// same L2 block range and the honest withdrawal in that range finalizes.
    function test_challenged_range_reattested_and_finalized() public {
        vm.deal(ALICE_L1, 10 ether);
        vm.prank(ALICE_L1);
        lockbox.depositETH{value: 5 ether}(ALICE_L2, 0, hex"");

        // Bad output for range ending at block 100 gets challenged away.
        vm.prank(ATTESTER);
        uint256 badIdx =
            oracle.proposeOutput(_outputRoot(keccak256("fraud"), keccak256("junk")), 100);
        vm.prank(CHALLENGER);
        oracle.deleteOutput(badIdx);

        // Corrected output for the SAME range (block 100), honest withdrawal.
        bytes32 wlAlice = _leaf(0, ALICE_L2, ALICE_L1, 1 ether);
        bytes32 withdrawalsRoot = _hashLeaf(wlAlice);
        bytes32 stateRoot = keccak256("honest-state-root@block-100");
        vm.prank(ATTESTER);
        uint256 goodIdx = oracle.proposeOutput(_outputRoot(stateRoot, withdrawalsRoot), 100);

        vm.warp(block.timestamp + WINDOW + 1);
        ETHLockbox.WithdrawalTransaction memory aTx =
            ETHLockbox.WithdrawalTransaction(0, ALICE_L2, ALICE_L1, 1 ether);
        lockbox.finalizeWithdrawal(aTx, goodIdx, stateRoot, withdrawalsRoot, 0, new bytes32[](0));
        assertEq(ALICE_L1.balance, 6 ether); // 5 remaining + 1 withdrawn
    }
}
