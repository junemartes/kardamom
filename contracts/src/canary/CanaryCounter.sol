// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// @title CanaryCounter
/// @notice The counter of the transaction canary's `contract` probe. Each
///         write adds one to the count and takes exactly one wei, so the
///         balance of the contract equals the count. The chain serves no
///         `eth_call`, so the probe reads the count back through
///         `eth_getBalance`.
/// @dev The contract has no `receive` and no `fallback`: a plain transfer
///      reverts, and only a write moves the balance. Only the writers that
///      the constructor names can write, so no other account moves the
///      count between the probe's write and its read. The wei stays in the
///      contract by design: it is the count.
// slither-disable-next-line locked-ether
contract CanaryCounter {
    /// @notice The number of writes.
    uint256 public count;

    /// @notice The accounts that can write.
    mapping(address => bool) public isWriter;

    /// @notice A write set the count to `count`.
    event Incremented(address indexed writer, uint256 count);

    error NotWriter(address caller);
    error WrongValue(uint256 value);

    constructor(address[] memory writers) {
        for (uint256 i = 0; i < writers.length; ++i) {
            isWriter[writers[i]] = true;
        }
    }

    /// @notice Add one to the count. The call must carry exactly one wei.
    /// @return The new count.
    function increment() external payable returns (uint256) {
        if (!isWriter[msg.sender]) revert NotWriter(msg.sender);
        if (msg.value != 1) revert WrongValue(msg.value);
        uint256 next = count + 1;
        count = next;
        emit Incremented(msg.sender, next);
        return next;
    }
}
