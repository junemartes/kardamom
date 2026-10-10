// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/// @title CanaryRwa
/// @notice "Kardamom Canary Asset" (KCA): a made-up test token of the
///         transaction canary. It stands for no real asset and exists only
///         on the chain the canary runs on. It behaves as a real-world-asset
///         token does: the owner mints and burns, and a transfer succeeds
///         only between addresses on the owner's allowlist.
/// @dev Every supply change emits `Supply` with the new total, so the
///      canary compares the total with its own ledger of mints and burns.
contract CanaryRwa is ERC20 {
    /// @notice The account that mints, burns and keeps the allowlist.
    address public owner;

    /// @notice The addresses that can send and receive the token.
    mapping(address => bool) public allowed;

    /// @notice The total supply after a mint or a burn.
    event Supply(uint256 totalSupply);
    /// @notice The owner put `account` on the allowlist or took it off.
    event Allowed(address indexed account, bool allowed);

    error NotOwner(address caller);
    error NotAllowed(address account);

    constructor(address[] memory initial) ERC20("Kardamom Canary Asset", "KCA") {
        owner = msg.sender;
        allowed[msg.sender] = true;
        emit Allowed(msg.sender, true);
        for (uint256 i = 0; i < initial.length; ++i) {
            allowed[initial[i]] = true;
            emit Allowed(initial[i], true);
        }
    }

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner(msg.sender);
        _;
    }

    /// @notice Put `account` on the allowlist, or take it off.
    function setAllowed(address account, bool isAllowed) external onlyOwner {
        allowed[account] = isAllowed;
        emit Allowed(account, isAllowed);
    }

    /// @notice Mint `amount` to `to`, which must be on the allowlist.
    function mint(address to, uint256 amount) external onlyOwner {
        _mint(to, amount);
        emit Supply(totalSupply());
    }

    /// @notice Burn `amount` from `from`: the owner's forced burn.
    function burn(address from, uint256 amount) external onlyOwner {
        _burn(from, amount);
        emit Supply(totalSupply());
    }

    /// @dev A mint goes only to an allowed address; a transfer goes only
    ///      between allowed addresses; a burn takes from any holder.
    function _update(address from, address to, uint256 value) internal override {
        if (from != address(0) && to != address(0) && !allowed[from]) revert NotAllowed(from);
        if (to != address(0) && !allowed[to]) revert NotAllowed(to);
        super._update(from, to, value);
    }
}
