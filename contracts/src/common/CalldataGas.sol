// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// @title CalldataGas
/// @notice The intrinsic gas of a transaction whose calldata is `data`, under
///         the rules the Kardamom execution layer applies (Osaka):
///         - standard: `21_000 + 16 * nonZero + 4 * zero` (EIP-2028),
///         - floor:    `21_000 + 10 * (zero + 4 * nonZero)` (EIP-7623).
///         A transaction must carry at least `max(standard, floor)` gas, or
///         the execution layer rejects it at validation.
/// @dev    The L1 lockbox uses this to reject a deposit whose `gasLimit`
///         cannot pay for its own calldata (audit C2). The L2 Outbox uses
///         it to charge the destination's per-block gas budget with what
///         the delivery really costs (audit C3).
library CalldataGas {
    uint256 internal constant TX_BASE_GAS = 21_000;
    uint256 internal constant ZERO_BYTE_GAS = 4;
    uint256 internal constant NONZERO_BYTE_GAS = 16;
    /// @notice EIP-7623: one token per zero byte, four per nonzero byte,
    ///         ten gas per token.
    uint256 internal constant FLOOR_TOKEN_GAS = 10;
    uint256 internal constant NONZERO_TOKENS = 4;

    /// @notice Count the nonzero bytes of `data`.
    /// @dev    One word per iteration. The OR-fold moves any set bit of a
    ///         byte into that byte's lowest bit. The mask keeps one flag
    ///         bit per byte. The multiply by `ones` sums the 32 flags into
    ///         the top byte (the sum is at most 32, so no byte overflows).
    ///         The last partial word is masked to `data`'s own bytes, so
    ///         bytes that follow `data` in calldata never count.
    function countNonZero(bytes calldata data) internal pure returns (uint256 nonZero) {
        assembly ("memory-safe") {
            let ones := 0x0101010101010101010101010101010101010101010101010101010101010101
            let fullEnd := add(data.offset, and(data.length, not(31)))
            let p := data.offset
            for {} lt(p, fullEnd) { p := add(p, 32) } {
                let w := calldataload(p)
                w := or(w, shr(1, w))
                w := or(w, shr(2, w))
                w := or(w, shr(4, w))
                nonZero := add(nonZero, shr(248, mul(and(w, ones), ones)))
            }
            let rem := and(data.length, 31)
            if rem {
                // Keep only the top `rem` bytes of the loaded word.
                let w := and(calldataload(p), shl(mul(8, sub(32, rem)), not(0)))
                w := or(w, shr(1, w))
                w := or(w, shr(2, w))
                w := or(w, shr(4, w))
                nonZero := add(nonZero, shr(248, mul(and(w, ones), ones)))
            }
        }
    }

    /// @notice `max(standard, floor)` for a transaction whose calldata is
    ///         `data` plus `extraNonZero` more bytes counted as nonzero
    ///         (the worst case for a fixed head the caller does not hold).
    function intrinsicGas(bytes calldata data, uint256 extraNonZero)
        internal
        pure
        returns (uint256)
    {
        uint256 nonZero = countNonZero(data) + extraNonZero;
        uint256 zero = data.length - (nonZero - extraNonZero);
        uint256 standard = TX_BASE_GAS + NONZERO_BYTE_GAS * nonZero + ZERO_BYTE_GAS * zero;
        uint256 floor = TX_BASE_GAS + FLOOR_TOKEN_GAS * (zero + NONZERO_TOKENS * nonZero);
        return standard > floor ? standard : floor;
    }
}
