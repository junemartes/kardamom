// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// @title L2ToL1MessagePasser
/// @notice L2 predeploy that initiates withdrawals to L1. Each call locks the
///         sent ETH in this contract (removing it from L2 circulation) and
///         records a withdrawal commitment that the L1 bridge later verifies
///         against an attested output root.
/// @dev    Milestone-1 minimal design: there is **no in-contract Merkle tree**.
///         The validator/attester rebuilds the per-output withdrawals tree
///         off-chain from `MessagePassed` events (ordered by `nonce`) and posts
///         its root inside the output root. `sentMessages` is the L2-state record
///         of what was actually initiated; a future ZK challenge re-derives the
///         same set from the batch, which is what makes the off-chain root sound.
///
///         This is a genesis predeploy (its **runtime** bytecode is seeded at a
///         fixed address), so it is intentionally not upgradeable and has no
///         constructor-time state.
// Locking ETH is the DESIGN: this L2 predeploy escrows the withdrawn value
// permanently on L2 (the burn side of the off-ramp); the corresponding release
// happens on L1 from the ETHLockbox after the output finalizes. There must be
// no L2-side withdrawal function.
// slither-disable-next-line locked-ether
contract L2ToL1MessagePasser {
    /// @notice Monotonic withdrawal counter. Also the withdrawal's global index
    ///         (its position in the validator's leaf ordering).
    uint256 public messageNonce;

    /// @notice Initiated withdrawal hashes. The source of truth for inclusion;
    ///         mirrored into the off-chain withdrawals tree by the attester.
    mapping(bytes32 => bool) public sentMessages;

    /// @notice Emitted on every withdrawal. The attester collects these per L2
    ///         block range to build that output's `withdrawalsRoot`.
    event MessagePassed(
        uint256 indexed nonce,
        address indexed sender,
        address indexed target,
        uint256 value,
        bytes32 withdrawalHash
    );

    error ZeroWithdrawal();

    /// @notice Initiate a withdrawal of `msg.value` to `target` on L1. The ETH is
    ///         locked in this contract; the matching amount is released on L1 from
    ///         the lockbox once the withdrawal is proven against an attested
    ///         output and that output's challenge window has elapsed.
    function initiateWithdrawal(address target) external payable {
        if (msg.value == 0) revert ZeroWithdrawal();
        uint256 nonce = messageNonce;
        bytes32 h = hashWithdrawal(nonce, msg.sender, target, msg.value);
        sentMessages[h] = true;
        emit MessagePassed(nonce, msg.sender, target, msg.value, h);
        unchecked {
            messageNonce = nonce + 1;
        }
    }

    /// @notice Canonical withdrawal leaf hash. Must stay byte-identical with the
    ///         L1 bridge verifier and the validator's off-chain tree builder.
    function hashWithdrawal(uint256 nonce, address sender, address target, uint256 value)
        public
        pure
        returns (bytes32)
    {
        return keccak256(abi.encode(nonce, sender, target, value));
    }
}
