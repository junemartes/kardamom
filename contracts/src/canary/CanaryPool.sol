// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title CanaryPool
/// @notice A constant-product pool of L2 ETH and one token, written for the
///         transaction canary. A swap pays a 0.3% fee into the pool. Every
///         call syncs the reserves to the pool's balances first, writes the
///         reserves after the call before any transfer, and emits the
///         reserves before and after, so the canary checks each result
///         against the formula from the same reserves the EVM used.
contract CanaryPool {
    /// @notice The fee: a swap keeps 997 of each 1000 input units.
    uint256 public constant FEE_KEPT = 997;
    uint256 public constant FEE_BASE = 1000;

    IERC20 public token;

    uint256 public reserveEth;
    uint256 public reserveToken;
    uint256 public totalShares;
    mapping(address => uint256) public shares;
    /// @notice The number of calls that changed the reserves.
    uint256 public seq;

    bool private entered;

    /// @notice One swap. `ethIn` is the direction.
    event Swap(
        address indexed who,
        bool ethIn,
        uint256 amountIn,
        uint256 amountOut,
        uint256 reserveEthBefore,
        uint256 reserveTokenBefore,
        uint256 reserveEthAfter,
        uint256 reserveTokenAfter,
        uint256 seq
    );
    /// @notice One liquidity change. `added` is the direction.
    event Liquidity(
        address indexed who,
        bool added,
        uint256 ethAmount,
        uint256 tokenAmount,
        uint256 shareAmount,
        uint256 reserveEthBefore,
        uint256 reserveTokenBefore,
        uint256 sharesBefore,
        uint256 seq
    );

    error Reentered();
    error ZeroAmount();
    error SlippageExceeded(uint256 amountOut, uint256 minOut);
    error NotEnoughShares(uint256 held, uint256 asked);
    error EthTransferFailed();
    error TokenTransferFailed();

    constructor(IERC20 token_) {
        token = token_;
    }

    modifier nonReentrant() {
        if (entered) revert Reentered();
        entered = true;
        _;
        entered = false;
    }

    /// @notice The output of a swap of `amountIn` against the reserves
    ///         `reserveIn` and `reserveOut`, after the fee.
    function getAmountOut(uint256 amountIn, uint256 reserveIn, uint256 reserveOut)
        public
        pure
        returns (uint256)
    {
        uint256 kept = amountIn * FEE_KEPT;
        return (kept * reserveOut) / (reserveIn * FEE_BASE + kept);
    }

    /// @notice Swap the ETH sent for tokens; at least `minOut` comes back.
    function swapEthForToken(uint256 minOut) external payable nonReentrant returns (uint256 out) {
        if (msg.value == 0) revert ZeroAmount();
        (uint256 eth, uint256 tok) = _balances(msg.value);
        out = getAmountOut(msg.value, eth, tok);
        if (out < minOut) revert SlippageExceeded(out, minOut);
        _settle(eth + msg.value, tok - out);
        emit Swap(msg.sender, true, msg.value, out, eth, tok, reserveEth, reserveToken, seq);
        if (!token.transfer(msg.sender, out)) revert TokenTransferFailed();
    }

    /// @notice Swap `amountIn` tokens for ETH; at least `minOut` comes back.
    ///         The pool takes the tokens with `transferFrom`.
    function swapTokenForEth(uint256 amountIn, uint256 minOut)
        external
        nonReentrant
        returns (uint256 out)
    {
        if (amountIn == 0) revert ZeroAmount();
        (uint256 eth, uint256 tok) = _balances(0);
        out = getAmountOut(amountIn, tok, eth);
        if (out < minOut) revert SlippageExceeded(out, minOut);
        _settle(eth - out, tok + amountIn);
        emit Swap(msg.sender, false, amountIn, out, eth, tok, reserveEth, reserveToken, seq);
        if (!token.transferFrom(msg.sender, address(this), amountIn)) revert TokenTransferFailed();
        _sendEth(msg.sender, out);
    }

    /// @notice Add the ETH sent and `tokenAmount` tokens. The first deposit
    ///         gets one share per wei; a later one gets the smaller of its
    ///         two proportional shares.
    function addLiquidity(uint256 tokenAmount)
        external
        payable
        nonReentrant
        returns (uint256 minted)
    {
        if (msg.value == 0 || tokenAmount == 0) revert ZeroAmount();
        (uint256 eth, uint256 tok) = _balances(msg.value);
        uint256 supply = totalShares;
        minted =
            supply == 0 ? msg.value : _min(msg.value * supply / eth, tokenAmount * supply / tok);
        if (minted < 1) revert ZeroAmount();
        shares[msg.sender] += minted;
        totalShares = supply + minted;
        _settle(eth + msg.value, tok + tokenAmount);
        emit Liquidity(msg.sender, true, msg.value, tokenAmount, minted, eth, tok, supply, seq);
        if (!token.transferFrom(msg.sender, address(this), tokenAmount)) {
            revert TokenTransferFailed();
        }
    }

    /// @notice Burn `amount` shares for their part of each reserve.
    function removeLiquidity(uint256 amount)
        external
        nonReentrant
        returns (uint256 ethOut, uint256 tokenOut)
    {
        uint256 held = shares[msg.sender];
        if (amount == 0) revert ZeroAmount();
        if (amount > held) revert NotEnoughShares(held, amount);
        (uint256 eth, uint256 tok) = _balances(0);
        uint256 supply = totalShares;
        ethOut = amount * eth / supply;
        tokenOut = amount * tok / supply;
        shares[msg.sender] = held - amount;
        totalShares = supply - amount;
        _settle(eth - ethOut, tok - tokenOut);
        emit Liquidity(msg.sender, false, ethOut, tokenOut, amount, eth, tok, supply, seq);
        if (!token.transfer(msg.sender, tokenOut)) revert TokenTransferFailed();
        _sendEth(msg.sender, ethOut);
    }

    /// @dev The reserves synced to the balances at the start of a call:
    ///      the ETH balance less `sent`, the ETH this call carries.
    function _balances(uint256 sent) private view returns (uint256 eth, uint256 tok) {
        eth = address(this).balance - sent;
        tok = token.balanceOf(address(this));
    }

    /// @dev Write the reserves after the call before any transfer runs.
    function _settle(uint256 eth, uint256 tok) private {
        reserveEth = eth;
        reserveToken = tok;
        ++seq;
    }

    function _sendEth(address to, uint256 amount) private {
        (bool ok,) = to.call{value: amount}("");
        if (!ok) revert EthTransferFailed();
    }

    function _min(uint256 a, uint256 b) private pure returns (uint256) {
        return a < b ? a : b;
    }
}
