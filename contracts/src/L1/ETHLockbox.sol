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

    /// @notice Domain tags for the withdrawals tree. Leaves and internal nodes
    ///         are hashed under distinct one-byte prefixes so an internal-node
    ///         preimage can never be replayed as a leaf (classic Merkle
    ///         second-preimage hardening) — even if the withdrawal leaf format
    ///         ever changes shape. Must match
    ///         `kardamom_types::withdrawals::{LEAF_DOMAIN, NODE_DOMAIN}`.
    bytes1 internal constant LEAF_DOMAIN = 0x00;
    bytes1 internal constant NODE_DOMAIN = 0x01;

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

    /// @notice V2 migration entry point: wire (or re-wire) the output oracle on a
    ///         proxy initialized before the withdrawal off-ramp existed (the
    ///         one-arg V1 `initialize`), which would otherwise be deposit-only
    ///         forever (`initializer` blocks re-entry). Factory-gated — intended
    ///         to be called via the factory's `upgradeToAndCall`, whose
    ///         delegatecall preserves the factory as `msg.sender`.
    function initializeV2(address _outputOracle) external reinitializer(2) {
        if (msg.sender != FACTORY) revert NotFactory();
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

        bytes memory packed = abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot);
        bytes32 outputRoot = keccak256(packed);
        if (IWithdrawalOutputOracle(outputOracle).outputRootAt(outputIndex) != outputRoot) {
            revert OutputRootMismatch();
        }
        if (!IWithdrawalOutputOracle(outputOracle).isFinalizable(outputIndex)) {
            revert NotFinalizable();
        }

        finalizedWithdrawals[wh] = true;
        // Paying an arbitrary target is the CONTRACT'S PURPOSE: the recipient +
        // amount are authorized by the Merkle inclusion proof against an
        // attested, challenge-window-finalized output root (checked above), and
        // the replay guard is set before the transfer (CEI).
        // slither-disable-next-line arbitrary-send-eth
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
    ///         domain-separated keccak256 (`LEAF_DOMAIN` for the leaf,
    ///         `NODE_DOMAIN` for internal pairs) — matches the validator's tree
    ///         builder (`kardamom_types::withdrawals`). Reverts if `index` has
    ///         set bits beyond `proof.length`, binding the claimed leaf position
    ///         to the proof depth (otherwise many indices would "verify" the
    ///         same proof).
    function _merkleRoot(bytes32 leaf, uint256 index, bytes32[] calldata proof)
        internal
        pure
        returns (bytes32)
    {
        bytes32 node = keccak256(abi.encodePacked(LEAF_DOMAIN, leaf));
        for (uint256 i = 0; i < proof.length; i++) {
            if (index & 1 == 0) {
                node = keccak256(abi.encodePacked(NODE_DOMAIN, node, proof[i]));
            } else {
                node = keccak256(abi.encodePacked(NODE_DOMAIN, proof[i], node));
            }
            index >>= 1;
        }
        if (index != 0) revert BadInclusionProof();
        return node;
    }
}
