// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {KardamomUUPSBase} from "../factory/KardamomUUPSBase.sol";

/// @notice A minimal view of the output oracle. The lockbox reads it when
///         it finalizes a withdrawal.
interface IWithdrawalOutputOracle {
    function outputRootAt(uint256 index) external view returns (bytes32);
    function isFinalizable(uint256 index) external view returns (bool);
    function finalizationWindow() external view returns (uint64);
}

/// @notice A minimal view of the factory's ownership. The lockbox reads this
///         to authorize upgrade transactions. The factory is
///         `Ownable2StepUpgradeable`. Its owner is the chain's L1 authority
///         (a Safe contract in production).
interface IOwnable {
    function owner() external view returns (address);
}

/// @title ETHLockbox
/// @notice The L1 ETH bridge. It holds ETH deposited through `depositETH`
///         (the on-ramp). It releases ETH through `finalizeWithdrawal` (the
///         off-ramp) after a withdrawal proves inclusion in an attested,
///         finalized output root. An egress cap bounds the value that
///         leaves through the off-ramp per window, in total and per
///         L2 account.
contract ETHLockbox is KardamomUUPSBase {
    /// @notice An L2-to-L1 withdrawal, as recorded by the L2
    ///         `L2ToL1MessagePasser` contract. The leaf hash is
    ///         `keccak256(abi.encode(nonce, sender, target, value))`.
    struct WithdrawalTransaction {
        uint256 nonce; // the global withdrawal index on L2
        address sender; // the L2 initiator
        address target; // the L1 recipient of the released ETH
        uint256 value; // the wei amount to release
    }

    /// @notice The output-root version byte. It must match
    ///         `WithdrawalOutputOracle`.
    uint8 internal constant OUTPUT_VERSION = 0;

    /// @notice Domain tags for the withdrawals tree. Leaves and internal
    ///         nodes get distinct one-byte prefixes before hashing. This
    ///         stops an internal-node preimage from ever replaying as a
    ///         leaf, even if the withdrawal leaf format changes shape. It
    ///         must match
    ///         `kardamom_types::withdrawals::{LEAF_DOMAIN, NODE_DOMAIN}`.
    bytes1 internal constant LEAF_DOMAIN = 0x00;
    bytes1 internal constant NODE_DOMAIN = 0x01;

    uint64 public depositNonce;
    address public l2Minter;

    /// @notice The L1 output oracle that this lockbox checks when it
    ///         finalizes a withdrawal.
    address public outputOracle;
    /// @notice A replay guard. Maps a withdrawal leaf hash to whether it is
    ///         already paid.
    mapping(bytes32 => bool) public finalizedWithdrawals;

    /// @notice A counter that increases with each upgrade. It only helps
    ///         with observability and idempotence checks. The L2 side
    ///         dedups on the L1 log position, not on this counter.
    /// @dev    This variable must stay after `finalizedWithdrawals`.
    ///         `depositNonce` and `l2Minter` share slot 0, and
    ///         `outputOracle` and `finalizedWithdrawals` follow them. A new
    ///         variable inserted above would shift these slots and corrupt
    ///         a live proxy's state on upgrade. Appending a variable is safe
    ///         for storage layout, and a zero value is the correct start
    ///         value, so this needs no `reinitializer`. The egress cap
    ///         variables are appended after this one.
    uint64 public upgradeNonce;

    // -------------------------------------------------------------------------
    // Egress cap. The variables below are appended after `upgradeNonce`.
    // Every later addition must go after them.
    // -------------------------------------------------------------------------

    /// @notice The total value (wei) that can finalize per window (`E`).
    ///         Zero disables the total cap.
    uint256 public egressCapPerWindow;
    /// @notice The value (wei) that one L2 account can finalize per window.
    ///         This is the per-account share of `E`. Zero disables the
    ///         per-account cap.
    uint256 public egressAccountCapPerWindow;
    /// @notice Value finalized per window id.
    mapping(uint256 => uint256) public egressUsed;
    /// @notice Value finalized per window id and L2 sender.
    mapping(uint256 => mapping(address => uint256)) public egressUsedBy;

    event DepositInitiated(
        uint64 indexed depositNonce,
        address indexed from,
        address indexed to,
        uint256 mint,
        uint64 gasLimit,
        bytes data
    );
    event WithdrawalFinalized(
        bytes32 indexed withdrawalHash, address indexed target, uint256 value
    );
    event EgressLimitsUpdated(uint256 capPerWindow, uint256 accountCapPerWindow);

    /// @notice The upgrade transaction. It is an L1-authorized instruction
    ///         that schedules an L2 feature flag. The DA watcher derives a
    ///         system deposit from this log, the same way it derives user
    ///         deposits from `DepositInitiated`. This gives the instruction
    ///         L1's ordering and finality.
    /// @param activationTimestamp The L2 activation time, in epoch
    ///        milliseconds (L2 `block.timestamp` uses milliseconds on this
    ///        chain). A value of 0 activates the flag immediately.
    event UpgradeInitiated(
        uint64 indexed upgradeNonce, uint256 indexed featureId, uint64 activationTimestamp
    );

    error ZeroDeposit();
    error AlreadyFinalized();
    error BadInclusionProof();
    error OutputRootMismatch();
    error NotFinalizable();
    error TransferFailed();
    error NotUpgradeAuthority();
    error EgressCapExceeded();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address _l2Minter, address _outputOracle) external initializer {
        l2Minter = _l2Minter;
        outputOracle = _outputOracle;
    }

    /// @notice The V2 migration entry point. It sets or resets the output
    ///         oracle on a proxy that the one-arg V1 `initialize` set up,
    ///         before the withdrawal off-ramp existed. Without this
    ///         function, such a proxy stays deposit-only forever, because
    ///         `initializer` blocks a second call. Only the factory can
    ///         call this function. Call it through the factory's
    ///         `upgradeToAndCall`, whose delegatecall keeps the factory as
    ///         `msg.sender`.
    function initializeV2(address _outputOracle) external reinitializer(2) {
        if (msg.sender != FACTORY) revert NotFactory();
        outputOracle = _outputOracle;
    }

    receive() external payable {
        revert();
    }

    /// @notice Set the egress caps. Only the Kardamom factory can call
    ///         this. The caps are operator parameters of the trust set.
    ///         The share rule is public: each L2 account can finalize at
    ///         most `accountCapPerWindow` per window, and all accounts
    ///         together at most `capPerWindow`. A withdrawal above a cap
    ///         reverts and can retry in a later window. Zero disables a cap.
    function setEgressLimits(uint256 capPerWindow, uint256 accountCapPerWindow) external {
        if (msg.sender != FACTORY) revert NotFactory();
        egressCapPerWindow = capPerWindow;
        egressAccountCapPerWindow = accountCapPerWindow;
        emit EgressLimitsUpdated(capPerWindow, accountCapPerWindow);
    }

    /// @notice The current egress window id. The window length is the
    ///         oracle's finalization window, so the cap and the delay use
    ///         one clock.
    function egressWindowId() public view returns (uint256) {
        uint64 window = IWithdrawalOutputOracle(outputOracle).finalizationWindow();
        return block.timestamp / uint256(window);
    }

    // -------------------------------------------------------------------------
    // On-ramp (deposit)
    // -------------------------------------------------------------------------

    function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable {
        if (msg.value == 0) revert ZeroDeposit();
        unchecked {
            depositNonce += 1;
        }
        emit DepositInitiated(depositNonce, msg.sender, to, msg.value, gasLimit, data);
    }

    // -------------------------------------------------------------------------
    // Upgrades (L1-governed feature flags)
    // -------------------------------------------------------------------------

    /// @notice Schedule an L2 feature flag from L1. Only the factory owner
    ///         can call this function: the chain's L1 authority, a Safe
    ///         contract in production. Changing factory ownership changes
    ///         this authority too, so there is only one root of trust.
    /// @dev    This function only emits an event. The state change happens
    ///         on L2. The DA watcher turns this log into a system deposit
    ///         that calls `KardamomChainState.setFeature`. Every node
    ///         executes this call at the same canonical position.
    /// @param featureId           The flag to schedule.
    /// @param activationTimestamp The activation time, in epoch
    ///                            milliseconds (see the event docs). A
    ///                            value of 0 activates the flag immediately.
    function initiateUpgrade(uint256 featureId, uint64 activationTimestamp) external {
        if (msg.sender != IOwnable(FACTORY).owner()) revert NotUpgradeAuthority();
        unchecked {
            upgradeNonce += 1;
        }
        emit UpgradeInitiated(upgradeNonce, featureId, activationTimestamp);
    }

    // -------------------------------------------------------------------------
    // Off-ramp (withdrawal)
    // -------------------------------------------------------------------------

    /// @notice Finalize a withdrawal and release its ETH to `wtx.target`.
    /// @dev    This function checks, in order:
    ///         1. The withdrawal leaf is included in `withdrawalsRoot`,
    ///            through a positional Merkle proof.
    ///         2. The revealed `(stateRoot, withdrawalsRoot)` pair hashes
    ///            to the output root that the oracle holds at
    ///            `outputIndex`.
    ///         3. That output is finalizable: its challenge window has
    ///            ended, and no one deleted it.
    ///         A replay guard on the leaf hash blocks a repeat call.
    ///
    ///         This milestone finalizes in one step, with no separate
    ///         prove call. The oracle's challenge window starts when the
    ///         output is proposed, so a challenger has the full window to
    ///         delete a bad output before any withdrawal under it can
    ///         finalize.
    ///
    ///         The egress cap is checked last. A withdrawal that would push
    ///         the window total or the L2 sender's total above its cap
    ///         reverts with `EgressCapExceeded`. It is not consumed and can
    ///         retry in a later window.
    function finalizeWithdrawal(
        WithdrawalTransaction calldata wtx,
        uint256 outputIndex,
        bytes32 stateRoot,
        bytes32 withdrawalsRoot,
        uint256 leafIndex,
        bytes32[] calldata proof
    ) external {
        bytes32 wh = hashWithdrawal(wtx);
        if (finalizedWithdrawals[wh]) revert AlreadyFinalized();

        if (_merkleRoot(wh, leafIndex, proof) != withdrawalsRoot) revert BadInclusionProof();

        bytes memory packed = abi.encodePacked(OUTPUT_VERSION, stateRoot, withdrawalsRoot);
        bytes32 outputRoot = keccak256(packed);
        if (IWithdrawalOutputOracle(outputOracle).outputRootAt(outputIndex) != outputRoot) {
            revert OutputRootMismatch();
        }
        if (!IWithdrawalOutputOracle(outputOracle).isFinalizable(outputIndex)) {
            revert NotFinalizable();
        }
        _recordEgress(wtx.sender, wtx.value);

        finalizedWithdrawals[wh] = true;
        // Paying an arbitrary target is this contract's purpose. The Merkle
        // inclusion proof against an attested, finalized output root
        // (checked above) authorizes the recipient and the amount. The
        // replay guard is set before the transfer, following the
        // checks-effects-interactions pattern.
        // slither-disable-next-line arbitrary-send-eth
        (bool ok,) = wtx.target.call{value: wtx.value}("");
        if (!ok) revert TransferFailed();
        emit WithdrawalFinalized(wh, wtx.target, wtx.value);
    }

    /// @dev Charge `value` against the window caps, or revert. Both caps
    ///      disabled means no accounting and no storage writes.
    function _recordEgress(address l2Sender, uint256 value) internal {
        uint256 cap = egressCapPerWindow;
        uint256 accountCap = egressAccountCapPerWindow;
        if (cap == 0 && accountCap == 0) return;
        uint256 windowId = egressWindowId();
        if (cap != 0) {
            uint256 used = egressUsed[windowId] + value;
            if (used > cap) revert EgressCapExceeded();
            egressUsed[windowId] = used;
        }
        if (accountCap != 0) {
            uint256 usedBy = egressUsedBy[windowId][l2Sender] + value;
            if (usedBy > accountCap) revert EgressCapExceeded();
            egressUsedBy[windowId][l2Sender] = usedBy;
        }
    }

    /// @notice The canonical withdrawal leaf hash. This matches
    ///         `L2ToL1MessagePasser`.
    function hashWithdrawal(WithdrawalTransaction calldata wtx) public pure returns (bytes32) {
        return keccak256(abi.encode(wtx.nonce, wtx.sender, wtx.target, wtx.value));
    }

    /// @notice Recompute a Merkle root from `leaf`, its `index`, and the
    ///         sibling `proof`. Each bit of `index` selects left or right
    ///         at one level (a positional proof). Hashing uses
    ///         domain-separated keccak256: `LEAF_DOMAIN` for the leaf and
    ///         `NODE_DOMAIN` for internal pairs. This matches the
    ///         validator's tree builder (`kardamom_types::withdrawals`).
    ///         The function reverts if `index` has set bits beyond
    ///         `proof.length`. This binds the claimed leaf position to the
    ///         proof depth; otherwise, many indices would verify the same
    ///         proof.
    function _merkleRoot(bytes32 leaf, uint256 index, bytes32[] calldata proof)
        internal
        pure
        returns (bytes32)
    {
        bytes32 node = keccak256(abi.encodePacked(LEAF_DOMAIN, leaf));
        for (uint256 i = 0; i < proof.length; i++) {
            if (index & 1 == 0) {
                node = keccak256(abi.encodePacked(NODE_DOMAIN, node, proof[i]));
            } else {
                node = keccak256(abi.encodePacked(NODE_DOMAIN, proof[i], node));
            }
            index >>= 1;
        }
        if (index != 0) revert BadInclusionProof();
        return node;
    }
}
