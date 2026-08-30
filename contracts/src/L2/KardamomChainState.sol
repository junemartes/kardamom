// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// @title KardamomChainState
/// @notice An L2 predeploy that holds the chain's feature flags. Each flag
///         is a `featureId => activation timestamp` entry. The protocol
///         reads it at every block boundary and changes behavior once the
///         activation time arrives.
/// @dev    Only the derivation pipeline writes flags. An L1 upgrade
///         transaction (`ETHLockbox.initiateUpgrade`, gated to the L1
///         factory owner) becomes a system deposit from `SYSTEM_UPGRADER`
///         that calls `setFeature` here. That is the whole authorization
///         path. See
///         `docs/specs/2026-08-16-l1-upgrade-feature-flags-design.md` §7.
///
///         This is a genesis predeploy: its runtime bytecode is seeded at a
///         fixed address. It is intentionally not upgradeable and has no
///         constructor-time state. Upgrading it needs a coordinated genesis
///         change, the same as `L2ToL1MessagePasser`.
///
///         All timestamps here use milliseconds. `block.timestamp` on this
///         chain is epoch milliseconds: the sealer stamps its leader clock
///         in milliseconds, and the value passes to the EVM unscaled. So
///         every timestamp here — the argument, the stored activation, the
///         beacon field — is in milliseconds. A seconds value would
///         schedule an activation about 56 years too late.
contract KardamomChainState {
    /// @notice The only sender allowed to write feature flags.
    /// @dev    A synthetic address: the last 20 bytes of
    ///         `keccak256("kardamom.upgrades.system-sender.v1")`. Only the
    ///         derivation rule can produce deposits from it. User deposits
    ///         have their L1 sender aliased (`+0x1111...1111`), so reaching
    ///         this address from L1 would need an inverted keccak hash, and
    ///         no L2 key signs for it. It must match
    ///         `kardamom_types::upgrades::SYSTEM_UPGRADER`.
    address public constant SYSTEM_UPGRADER = 0x454156dAb0518B9244CC7Ff1b0FfFf6c7E031B6D;

    /// @notice The feature id of the health check. This is the first
    ///         feature, and the one that exercises the upgrade path itself.
    ///         It must match
    ///         `kardamom_exec_core::features::FEATURE_HEALTH_CHECK`.
    uint256 public constant FEATURE_HEALTH_CHECK = 1;

    /// @notice Slot 0: `featureId => activation timestamp (ms)`. A value of
    ///         0 means "never scheduled." A flag is active once
    ///         `block.timestamp >= activation`.
    mapping(uint256 => uint256) internal _activation;

    /// @notice Slot 1: the health beacon. The protocol rewrites it once per
    ///         block while the health check is active. It packs three
    ///         values, from low bits to high bits:
    ///
    ///           [0..64)    beat count
    ///           [64..128)  block number
    ///           [128..192) block timestamp (ms)
    ///
    /// @dev    The engine writes this slot directly into the block's state
    ///         delta at block close. No transaction carries it, so no
    ///         Solidity function writes this slot; the EVM can only read
    ///         it. Packing keeps the three values atomic and costs one
    ///         write-set entry per block instead of three. It must match
    ///         `kardamom_exec_core::features::{HEALTH_BEACON_SLOT, pack_beacon}`.
    // Having no Solidity writer is the design, not an oversight. The engine
    // writes this slot into the block's state delta at block close, where
    // no transaction exists to carry it. A setter function would open an
    // unauthorized write path into consensus state, which this contract
    // exists to prevent.
    // slither-disable-next-line uninitialized-state
    uint256 public healthBeacon;

    /// @notice Emitted when a feature is scheduled. `activationTimestamp`
    ///         is the resolved time (ms). For an immediate upgrade, this is
    ///         the activating block's timestamp, not the 0 sent from L1.
    event FeatureScheduled(uint256 indexed featureId, uint256 activationTimestamp);

    error NotSystemUpgrader();

    /// @notice Schedule `featureId` to activate at `activationTimestamp`
    ///         (ms). A value of 0 activates immediately, which resolves to
    ///         the current block's timestamp.
    /// @dev    This function only schedules; it does not evaluate.
    ///         Rescheduling an active feature to a future time suspends
    ///         it. This is allowed, because the authority is trusted and
    ///         determinism is unaffected.
    function setFeature(uint256 featureId, uint64 activationTimestamp) external {
        if (msg.sender != SYSTEM_UPGRADER) revert NotSystemUpgrader();
        uint256 ts = activationTimestamp == 0 ? block.timestamp : uint256(activationTimestamp);
        _activation[featureId] = ts;
        emit FeatureScheduled(featureId, ts);
    }

    /// @notice The activation timestamp (ms) of `featureId`, or 0 if never scheduled.
    function activationOf(uint256 featureId) external view returns (uint256) {
        return _activation[featureId];
    }

    /// @notice Whether `featureId` is active as of the current block.
    function isActive(uint256 featureId) external view returns (bool) {
        uint256 ts = _activation[featureId];
        return ts != 0 && block.timestamp >= ts;
    }

    /// @notice The health beacon, unpacked: how many blocks the health
    ///         check has recorded, and the number and timestamp of the
    ///         most recent one. `(0, 0, 0)` means it has never run.
    function health() external view returns (uint64 count, uint64 blockNumber, uint64 timestampMs) {
        uint256 b = healthBeacon;
        return (uint64(b), uint64(b >> 64), uint64(b >> 128));
    }
}
