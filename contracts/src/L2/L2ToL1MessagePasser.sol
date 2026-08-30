// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// @title L2ToL1MessagePasser
/// @notice An L2 predeploy that starts withdrawals to L1. Each call locks
///         the sent ETH in this contract, removing it from L2 circulation,
///         and records a withdrawal commitment. The L1 bridge later
///         verifies this commitment against an attested output root.
/// @dev    This milestone uses a minimal design: there is no in-contract
///         Merkle tree. The validator (the attester) rebuilds the
///         per-output withdrawals tree off-chain, from `MessagePassed`
///         events ordered by `nonce`, and posts its root inside the output
///         root. `sentMessages` is the L2-state record of what was
///         actually started. A future ZK challenge re-derives the same set
///         from the batch, which makes the off-chain root sound.
///
///         This is a genesis predeploy: its runtime bytecode is seeded at a
///         fixed address. It is intentionally not upgradeable and has no
///         constructor-time state.
// Locking ETH is the design, not a bug. This L2 predeploy escrows the
// withdrawn value permanently on L2: the burn side of the off-ramp. The
// matching release happens on L1, from the ETHLockbox, after the output
// finalizes. This contract must never have an L2-side withdrawal function.
// slither-disable-next-line locked-ether
contract L2ToL1MessagePasser {
    /// @notice A counter that increases with each withdrawal. This is also
    ///         the withdrawal's global index, its position in the
    ///         validator's leaf ordering.
    uint256 public messageNonce;

    /// @notice The hashes of started withdrawals. This is the source of
    ///         truth for inclusion. The attester mirrors it into the
    ///         off-chain withdrawals tree.
    mapping(bytes32 => bool) public sentMessages;

    /// @notice Emitted on every withdrawal. The attester collects these
    ///         per L2 block range to build that output's `withdrawalsRoot`.
    event MessagePassed(
        uint256 indexed nonce,
        address indexed sender,
        address indexed target,
        uint256 value,
        bytes32 withdrawalHash
    );

    error ZeroWithdrawal();

    /// @notice Start a withdrawal of `msg.value` to `target` on L1. The ETH
    ///         stays locked in this contract. The lockbox releases the
    ///         matching amount on L1 after the withdrawal proves inclusion
    ///         against an attested output, once that output's challenge
    ///         window has ended.
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

    /// @notice The canonical withdrawal leaf hash. It must stay
    ///         byte-identical to the L1 bridge verifier and the validator's
    ///         off-chain tree builder.
    function hashWithdrawal(uint256 nonce, address sender, address target, uint256 value)
        public
        pure
        returns (bytes32)
    {
        return keccak256(abi.encode(nonce, sender, target, value));
    }
}
