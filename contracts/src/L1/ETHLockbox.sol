// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @notice Minimal view of the output oracle the lockbox reads when finalizing.
interface IWithdrawalOutputOracle {
    function outputRootAt(uint256 index) external view returns (bytes32);
    function isFinalizable(uint256 index) external view returns (bool);
}

/// @title ETHLockbox
/// @notice The L1 ETH bridge. Holds ETH deposited via `depositETH` (the on-ramp)
///         and releases it via `finalizeWithdrawal` (the off-ramp) once a
///         withdrawal is proven included in an attested, finalized output root.
contract ETHLockbox is KardamomUUPSBase {
    /// @notice An L2->L1 withdrawal, as recorded by the L2 `L2ToL1MessagePasser`.
    ///         The leaf hash is `keccak256(abi.encode(nonce, sender, target, value))`.
    struct WithdrawalTransaction {
        uint256 nonce; // global withdrawal index on L2
        address sender; // L2 initiator
        address target; // L1 recipient of the released ETH
        uint256 value; // wei to release
    }

    /// @notice Output-root version byte; must match `WithdrawalOutputOracle`.
    uint8 internal constant OUTPUT_VERSION = 0;

    uint64 public depositNonce;
    address public l2Minter;

    /// @notice The L1 output oracle this lockbox finalizes withdrawals against.
    address public outputOracle;
    /// @notice Replay guard: withdrawal leaf hash => already paid out.
    mapping(bytes32 => bool) public finalizedWithdrawals;

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

    error ZeroDeposit();
    error AlreadyFinalized();
    error BadInclusionProof();
    error OutputRootMismatch();
    error NotFinalizable();
    error TransferFailed();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address _l2Minter, address _outputOracle) external initializer {
        l2Minter = _l2Minter;
        outputOracle = _outputOracle;
    }

    receive() external payable {
        revert();
    }

    // -------------------------------------------------------------------------
    // On-ramp (deposit)
    // -------------------------------------------------------------------------

    function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable {
        if (msg.value == 0) revert ZeroDeposit();
        unchecked {
            depositNonce += 1;
        }
        emit DepositInitiated(depositNonce, msg.sender, to, msg.value, gasLimit, data);
    }

    // -------------------------------------------------------------------------
    // Off-ramp (withdrawal)
    // -------------------------------------------------------------------------

    /// @notice Finalize a withdrawal and release its ETH to `wtx.target`.
    /// @dev    Verifies, in order: (1) the withdrawal leaf is included in
    ///         `withdrawalsRoot` via a positional Merkle proof; (2) the revealed
    ///         `(stateRoot, withdrawalsRoot)` hash to the output root the oracle
    ///         holds at `outputIndex`; (3) that output is finalizable (challenge
    ///         window elapsed and not deleted). Replay-guarded on the leaf hash.
    ///
    ///         Milestone 1 is single-step (no separate prove call): the oracle's
    ///         window starts at output proposal, so a challenger has the full
    ///         window to delete a bad output before any withdrawal under it can
    ///         finalize.
    function finalizeWithdrawal(
        WithdrawalTransaction calldata wtx,
        uint256 outputIndex,
        bytes32 stateRoot,
        bytes32 withdrawalsRoot,
        uint256 leafIndex,
        bytes32[] calldata proof
    ) external {
        bytes32 wh = hashWithdrawal(wtx);
        if (finalizedWithdrawals[wh]) revert AlreadyFinalized();

        if (_merkleRoot(wh, leafIndex, proof) != withdrawalsRoot) revert BadInclusionProof();

        bytes32 outputRoot =
            keccak256(abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot));
        if (IWithdrawalOutputOracle(outputOracle).outputRootAt(outputIndex) != outputRoot) {
            revert OutputRootMismatch();
        }
        if (!IWithdrawalOutputOracle(outputOracle).isFinalizable(outputIndex)) {
            revert NotFinalizable();
        }

        finalizedWithdrawals[wh] = true;
        (bool ok,) = wtx.target.call{value: wtx.value}("");
        if (!ok) revert TransferFailed();
        emit WithdrawalFinalized(wh, wtx.target, wtx.value);
    }

    /// @notice Canonical withdrawal leaf hash. Mirrors `L2ToL1MessagePasser`.
    function hashWithdrawal(WithdrawalTransaction calldata wtx) public pure returns (bytes32) {
        return keccak256(abi.encode(wtx.nonce, wtx.sender, wtx.target, wtx.value));
    }

    /// @notice Recompute a Merkle root from `leaf` at `index` and its sibling
    ///         `proof`. Positional (index bit selects left/right at each level),
    ///         keccak256 over packed pairs — matches the validator's tree builder.
    function _merkleRoot(bytes32 leaf, uint256 index, bytes32[] calldata proof)
        internal
        pure
        returns (bytes32)
    {
        bytes32 node = leaf;
        for (uint256 i = 0; i < proof.length; i++) {
            if (index & 1 == 0) {
                node = keccak256(abi.encodePacked(node, proof[i]));
            } else {
                node = keccak256(abi.encodePacked(proof[i], node));
            }
            index >>= 1;
        }
        return node;
    }
}
