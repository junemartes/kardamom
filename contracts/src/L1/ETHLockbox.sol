// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

contract ETHLockbox {
    error ZeroDeposit();

    uint64 public depositNonce;
    address public immutable l2Minter;

    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );

    constructor(address _l2Minter) {
        l2Minter = _l2Minter;
    }

    receive() external payable {
        revert();
    }

    function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable {
        if (msg.value == 0) revert ZeroDeposit();
        unchecked {
            depositNonce += 1;
        }
        emit DepositInitiated(depositNonce, msg.sender, to, msg.value, gasLimit, data);
    }
}
