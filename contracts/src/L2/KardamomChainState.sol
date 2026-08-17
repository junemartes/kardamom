// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

/// @title KardamomChainState
/// @notice L2 predeploy holding the chain's **feature flags**. Each flag is a
///         `featureId => activation timestamp` entry; the protocol reads it at
///         every block boundary and changes behaviour once the activation time
///         is reached.
/// @dev    Flags are written **only** by the derivation pipeline: an L1 upgrade
///         transaction (`ETHLockbox.initiateUpgrade`, sender-gated to the L1
///         factory owner) becomes a system deposit from `SYSTEM_UPGRADER` that
///         calls `setFeature` here. That is the whole authorization story — see
///         `docs/specs/2026-08-16-l1-upgrade-feature-flags-design.md` §7.
///
///         This is a genesis predeploy (its **runtime** bytecode is seeded at a
///         fixed address), so it is intentionally not upgradeable and has no
///         constructor-time state. Upgrading it means a coordinated genesis
///         change, exactly like `L2ToL1MessagePasser`.
///
///         TIMESTAMPS ARE MILLISECONDS. `block.timestamp` on this chain is
///         epoch-**ms** (the sealer stamps its leader clock in ms and the value
///         is fed to the EVM unscaled), so every timestamp here — the argument,
///         the stored activation, the beacon field — is ms. Passing a seconds
///         value schedules an activation ~56 years out.
contract KardamomChainState {
    /// @notice The only sender allowed to write feature flags.
    /// @dev    Synthetic address, last 20 bytes of
    ///         `keccak256("kardamom.upgrades.system-sender.v1")`. Deposits from
    ///         it can only be produced by the derivation rule: user deposits
    ///         have their L1 sender aliased (`+0x1111...1111`), so reaching this
    ///         address from L1 would require inverting keccak, and no L2 key
    ///         signs for it. Must match
    ///         `kardamom_types::upgrades::SYSTEM_UPGRADER`.
    address public constant SYSTEM_UPGRADER = 0x454156dAb0518B9244CC7Ff1b0FfFf6c7E031B6D;

    /// @notice Feature id of the health check — the first feature, and the one
    ///         that exercises the upgrade path itself. Must match
    ///         `kardamom_exec_core::features::FEATURE_HEALTH_CHECK`.
    uint256 public constant FEATURE_HEALTH_CHECK = 1;

    /// @notice slot 0 — `featureId => activation timestamp (ms)`.
    ///         0 means "never scheduled"; a flag is active once
    ///         `block.timestamp >= activation`.
    mapping(uint256 => uint256) internal _activation;

    /// @notice slot 1 — the health beacon, rewritten once per block by the
    ///         protocol while the health check is active. Packed low-to-high:
    ///
    ///           [0..64)    beat count
    ///           [64..128)  block number
    ///           [128..192) block timestamp (ms)
    ///
    /// @dev    Written by the ENGINE directly into the block's state delta at
    ///         block close — there is no transaction to carry it, so no
    ///         Solidity function writes this slot. Read-only from the EVM.
    ///         Packing keeps the triple atomic and costs one write-set entry
    ///         per block instead of three. Must match
    ///         `kardamom_exec_core::features::{HEALTH_BEACON_SLOT, pack_beacon}`.
    // Having NO Solidity writer is the design, not an oversight: the engine
    // writes this slot into the block's state delta at block close, where no
    // transaction exists to carry it. A setter would be an unauthorized write
    // path into consensus state — exactly what this contract exists to prevent.
    // slither-disable-next-line uninitialized-state
    uint256 public healthBeacon;

    /// @notice Emitted when a feature is scheduled. `activationTimestamp` is the
    ///         RESOLVED time (ms) — for an immediate upgrade this is the
    ///         activating block's timestamp, not the 0 that was sent from L1.
    event FeatureScheduled(uint256 indexed featureId, uint256 activationTimestamp);

    error NotSystemUpgrader();

    /// @notice Schedule `featureId` to activate at `activationTimestamp` (ms).
    ///         `0` means activate immediately, which resolves to the current
    ///         block's timestamp.
    /// @dev    Schedules; it does not evaluate. Re-scheduling an active feature
    ///         to a future time effectively suspends it — allowed, since the
    ///         authority is trusted and determinism is unaffected.
    function setFeature(uint256 featureId, uint64 activationTimestamp) external {
        if (msg.sender != SYSTEM_UPGRADER) revert NotSystemUpgrader();
        uint256 ts = activationTimestamp == 0 ? block.timestamp : uint256(activationTimestamp);
        _activation[featureId] = ts;
        emit FeatureScheduled(featureId, ts);
    }

    /// @notice Activation timestamp (ms) of `featureId`, or 0 if never scheduled.
    function activationOf(uint256 featureId) external view returns (uint256) {
        return _activation[featureId];
    }

    /// @notice Whether `featureId` is active as of the current block.
    function isActive(uint256 featureId) external view returns (bool) {
        uint256 ts = _activation[featureId];
        return ts != 0 && block.timestamp >= ts;
    }

    /// @notice The health beacon, unpacked: how many blocks the health check has
    ///         recorded, and the number/timestamp of the most recent one.
    ///         `(0, 0, 0)` means it has never run.
    function health() external view returns (uint64 count, uint64 blockNumber, uint64 timestampMs) {
        uint256 b = healthBeacon;
        return (uint64(b), uint64(b >> 64), uint64(b >> 128));
    }
}
