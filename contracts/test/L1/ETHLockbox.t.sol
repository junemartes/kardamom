// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import {ETHLockbox} from "../../src/L1/ETHLockbox.sol";
import {IWithdrawalOutputOracle} from "../../src/L1/ETHLockbox.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

/// Minimal oracle double: one settable output slot.
contract MockOracle is IWithdrawalOutputOracle {
    mapping(uint256 => bytes32) public roots;
    mapping(uint256 => bool) public finalizable;

    function set(uint256 index, bytes32 root, bool isFinal) external {
        roots[index] = root;
        finalizable[index] = isFinal;
    }

    function outputRootAt(uint256 index) external view returns (bytes32) {
        return roots[index];
    }

    function isFinalizable(uint256 index) external view returns (bool) {
        return finalizable[index];
    }
}

contract ETHLockboxTest is Test {
    ETHLockbox lockbox;
    MockOracle oracle;
    address constant L2_MINTER = address(0xBEEF);

    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );
    event WithdrawalFinalized(
        bytes32 indexed withdrawalHash, address indexed target, uint256 value
    );

    function setUp() public {
        oracle = new MockOracle();
        ETHLockbox impl = new ETHLockbox();
        bytes memory initData =
            abi.encodeWithSelector(ETHLockbox.initialize.selector, L2_MINTER, address(oracle));
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        lockbox = ETHLockbox(payable(address(proxy)));
    }

    function test_initializer_stores_config() public view {
        assertEq(lockbox.l2Minter(), L2_MINTER);
        assertEq(lockbox.outputOracle(), address(oracle));
    }

    function test_impl_cannot_be_initialized_directly() public {
        ETHLockbox impl = new ETHLockbox();
        vm.expectRevert();
        impl.initialize(L2_MINTER, address(oracle));
    }

    function test_depositETH_increments_balance_and_emits() public {
        address alice = address(0xA11CE);
        vm.deal(alice, 10 ether);
        address to = address(0xB0B);

        vm.expectEmit(true, true, true, true);
        emit DepositInitiated(1, alice, to, 1 ether, 100_000, hex"");

        vm.prank(alice);
        lockbox.depositETH{value: 1 ether}(to, 100_000, hex"");

        assertEq(address(lockbox).balance, 1 ether);
        assertEq(lockbox.depositNonce(), 1);
    }

    function test_receive_reverts() public {
        (bool ok,) = address(lockbox).call{value: 1 ether}("");
        assertFalse(ok);
        assertEq(address(lockbox).balance, 0);
    }

    function test_zero_value_reverts() public {
        vm.expectRevert(ETHLockbox.ZeroDeposit.selector);
        lockbox.depositETH{value: 0}(address(0xB0B), 0, hex"");
    }

    // ---------------------------------------------------------------------
    // Withdrawal off-ramp
    // ---------------------------------------------------------------------

    uint8 constant OUTPUT_VERSION = 0;
    address constant L2_SENDER = address(0x5151);
    address constant L1_TARGET = address(0x7777);

    function _leaf(uint256 nonce, address target, uint256 value) internal pure returns (bytes32) {
        return keccak256(abi.encode(nonce, L2_SENDER, target, value));
    }

    /// Fund the lockbox as if deposits had filled it, and register a 2-leaf
    /// withdrawals tree [leaf0, sibling] whose leaf0 withdraws `value` to target.
    function _setupWithdrawal(uint256 value)
        internal
        returns (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        )
    {
        vm.deal(address(lockbox), 10 ether);
        wtx = ETHLockbox.WithdrawalTransaction({
            nonce: 0,
            sender: L2_SENDER,
            target: L1_TARGET,
            value: value
        });
        bytes32 leaf0 = _leaf(0, L1_TARGET, value);
        bytes32 sibling = _leaf(1, address(0xdead), 3 ether); // some other withdrawal
        // positional tree: root = keccak(leaf0 ++ sibling); leaf0 is index 0.
        withdrawalsRoot = keccak256(abi.encodePacked(leaf0, sibling));
        leafIndex = 0;
        proof = new bytes32[](1);
        proof[0] = sibling;
        stateRoot = keccak256("state-root-block-7");
    }

    function _outputRoot(bytes32 stateRoot, bytes32 withdrawalsRoot)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot));
    }

    function test_finalizeWithdrawal_pays_out() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        bytes32 wh = lockbox.hashWithdrawal(wtx);
        vm.expectEmit(true, true, false, true);
        emit WithdrawalFinalized(wh, L1_TARGET, 1 ether);

        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);

        assertEq(L1_TARGET.balance, 1 ether);
        assertEq(address(lockbox).balance, 9 ether);
        assertTrue(lockbox.finalizedWithdrawals(wh));
    }

    function test_finalizeWithdrawal_replay_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
        vm.expectRevert(ETHLockbox.AlreadyFinalized.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_not_finalizable_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), false); // window not elapsed / deleted

        vm.expectRevert(ETHLockbox.NotFinalizable.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_bad_proof_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        proof[0] = keccak256("wrong-sibling");
        vm.expectRevert(ETHLockbox.BadInclusionProof.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_output_mismatch_reverts() public {
        (
            ETHLockbox.WithdrawalTransaction memory wtx,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] memory proof
        ) = _setupWithdrawal(1 ether);
        // Oracle holds a different output root than the revealed preimage.
        oracle.set(0, keccak256("unrelated-output"), true);

        vm.expectRevert(ETHLockbox.OutputRootMismatch.selector);
        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, leafIndex, proof);
    }

    function test_finalizeWithdrawal_single_leaf_tree() public {
        // A range with exactly one withdrawal: root == leaf, empty proof.
        vm.deal(address(lockbox), 10 ether);
        ETHLockbox.WithdrawalTransaction memory wtx = ETHLockbox.WithdrawalTransaction({
            nonce: 0,
            sender: L2_SENDER,
            target: L1_TARGET,
            value: 2 ether
        });
        bytes32 withdrawalsRoot = _leaf(0, L1_TARGET, 2 ether);
        bytes32 stateRoot = keccak256("sr");
        bytes32[] memory proof = new bytes32[](0);
        oracle.set(0, _outputRoot(stateRoot, withdrawalsRoot), true);

        lockbox.finalizeWithdrawal(wtx, 0, stateRoot, withdrawalsRoot, 0, proof);
        assertEq(L1_TARGET.balance, 2 ether);
    }

    function test_unauthorized_upgrade_reverts() public {
        ETHLockbox newImpl = new ETHLockbox();
        (bool ok,) = address(lockbox).call(
            abi.encodeWithSignature("upgradeToAndCall(address,bytes)", address(newImpl), "")
        );
        assertFalse(ok, "non-factory upgrade must revert");
    }
}
