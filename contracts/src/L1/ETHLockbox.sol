// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

contract ETHLockbox is KardamomUUPSBase {
    error ZeroDeposit();

    uint64 public depositNonce;
    address public l2Minter;

    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address _l2Minter) external initializer {
        __UUPSUpgradeable_init();
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
