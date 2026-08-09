// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// @title XChain
/// @notice Shared constants and hashing for the cross-chain messaging
///         predeploys (`Outbox`, `Inbox`). The byte layouts here are the
///         single Solidity copy of `kardamom-types`' `xchain.rs` and must stay
///         identical to it — the cross-language tie is pinned by vectors in
///         `test/L2/Outbox.t.sol` asserted against Rust-computed values.
library XChain {
    /// @notice First `abi.encode` word of a message leaf. Must equal
    ///         `kardamom_types::xchain::xchain_leaf_domain()`.
    bytes32 internal constant LEAF_DOMAIN = keccak256("KARDAMOM_XCHAIN_MESSAGE_V0");

    /// @notice Canonical predeploy addresses, mirrored in `xchain.rs`.
    address internal constant OUTBOX = 0x42000000000000000000000000000000000000E0;
    address internal constant INBOX = 0x42000000000000000000000000000000000000e1;

    /// @notice Response requested by a message sender. Zeroed = none.
    struct Callback {
        address target; // contract on the ORIGIN chain
        uint64 gasLimit; // response delivery budget on the origin
        bytes32 context; // opaque correlation value, echoed back
    }

    /// @notice Hash-based remote-sender aliasing:
    ///         `address(keccak256(TAG ‖ be64(originChainId) ‖ sender)[12..])`.
    ///         Must equal `xchain.rs::alias_remote_address`. Hash-based (not
    ///         offset-based) so senders from different origins can never
    ///         collide with each other or with local contracts.
    function aliasRemote(uint64 originChainId, address sender) internal pure returns (address) {
        return address(
            uint160(
                uint256(keccak256(abi.encodePacked("KARDAMOM_XCHAIN_ALIAS_V0", originChainId, sender)))
            )
        );
    }

    /// @notice The EVM sender of a derived cross-chain tx from
    ///         `originChainId`: the origin's Outbox, aliased. No key exists
    ///         for this address, which is what makes `Inbox.deliver`
    ///         unforgeable by user transactions. Must equal
    ///         `xchain.rs::xchain_tx_sender`.
    function txSender(uint64 originChainId) internal pure returns (address) {
        return aliasRemote(originChainId, OUTBOX);
    }

    /// @notice `cbHash` of a callback: zero for a zeroed callback (matching
    ///         Rust's `None`), else `keccak256(abi.encode(target, gasLimit,
    ///         context))`. Must equal `xchain.rs::Callback::commitment`.
    function hashCallback(Callback memory cb) internal pure returns (bytes32) {
        if (cb.target == address(0) && cb.gasLimit == 0 && cb.context == bytes32(0)) {
            return bytes32(0);
        }
        return keccak256(abi.encode(cb.target, cb.gasLimit, cb.context));
    }

    /// @notice Whether a callback struct is the "none" sentinel.
    function isNone(Callback memory cb) internal pure returns (bool) {
        return cb.target == address(0) && cb.gasLimit == 0 && cb.context == bytes32(0);
    }

    /// @notice The message leaf: ten static words. Must equal
    ///         `xchain.rs::msg_leaf`. Origin AND destination chain ids inside
    ///         the leaf make replay across pairs impossible.
    function hashMessage(
        uint64 originChainId,
        uint64 destChainId,
        uint64 seq,
        address sender,
        address target,
        uint256 value,
        uint64 gasLimit,
        bytes32 dataHash,
        bytes32 cbHash
    ) internal pure returns (bytes32) {
        return keccak256(
            abi.encode(
                LEAF_DOMAIN, originChainId, destChainId, seq, sender, target, value, gasLimit, dataHash, cbHash
            )
        );
    }
}
