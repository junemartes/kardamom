# Recovery lines: checkpoint markers, L1 output rollback, egress cap

Date: 2026-09-07. Status: implemented (contracts). Engine follow-up listed
in §5.

Chains that one operator runs inside one trust domain can exchange
messages with no verification delay. The safety story for that tier is
operational: fast detection, an automatic pause, and a ceremonial revert
of every member chain to one consistent recovery line. This document
specifies the three protocol pieces that the revert needs. The operator
procedures live outside this repository.

## 1. Terms

- **Lane.** A dense FIFO message channel from one chain to another
  (`Outbox` on the origin, `Inbox` on the destination).
- **Cut.** Per chain, a block number plus the lane cursor vector at that
  block: `sent[lane]` and `delivered[lane]` for every lane.
- **Consistent cut.** For every lane A→B, `delivered_B <= sent_A` at the
  cut. Nothing is delivered that the surviving history did not send.
- **Round.** One coordinated cut across the set, identified by a round id.
- **W.** The revert window of the trust set. A boundary effect (an L1
  withdrawal) must not settle in less than `W`.
- **E.** The value that can leave through the L1 off-ramp per window.

## 2. Checkpoint markers (`CheckpointMarker`, L2 predeploy)

Address: `0x42000000000000000000000000000000000000E2`
(`kardamom_types::xchain::CHECKPOINT_MARKER`).

### 2.1 The rule

The predeploy applies the Chandy-Lamport marker rule:

1. A chain opens a round on the first marker it sees for that round. The
   marker is its own timer, or a marker received on any inbound lane.
2. In the same transaction, the chain sends the round's marker on every
   registered outbound lane.

The snapshot for a round is the state at `roundBlock[round]`. Because
lanes are dense FIFO, the in-flight set of a lane at the cut is
`sent - delivered`. The destination re-derives it from the restored
origin. No channel state is recorded.

### 2.2 Round ids and the timer

The round id is `block.timestamp / ROUND_INTERVAL_MS`. `block.timestamp`
is epoch milliseconds on this chain. `ROUND_INTERVAL_MS` is a protocol
constant (600 000 ms). A change to it needs a coordinated genesis change
on every member, the same as `KardamomChainState`.

`startRound()` is permissionless. It succeeds only when the time-derived
round is greater than `lastRound`. A caller cannot pick the round id, so
a call is the same as the timer firing. An operator process calls it
once per interval.

### 2.3 Markers are system messages

A marker is an ordinary lane message. Its sender and its target are the
predeploy address. `onMarker(round)` on the destination accepts a call
only when:

- `msg.sender` is the `Inbox`, and
- `Inbox.xDomainSender()` reports the predeploy address as the origin
  sender.

No key exists for a predeploy address, so no user transaction can
produce that origin sender. A message that only looks like a marker
reverts inside the delivery. The delivery records status 2. Nothing else
happens.

A stale or duplicate round is a no-op. So is a round more than one
interval ahead of this chain's clock: a peer with a bad clock must not
park `lastRound` in the future and stop this chain's own timer. In both
cases the delivery succeeds, and the predeploy emits `MarkerIgnored`.

### 2.4 Peers

The outbound lane set is the predeploy's peer list. `registerPeer(chainId)`
is permissionless and idempotent. It succeeds only when
`Inbox.nextSeq(chainId) > 0`: that chain has delivered at least one
message here. Only the derivation pipeline can deliver, and it delivers
only from the peers its registry admits. So the list is the on-chain view
of the peer registry. A received marker also registers its origin.
`MAX_PEERS` is 32.

The `Inbox` is not changed. A storage write on the delivery path would
reduce the delivery gas margin.

### 2.5 Gas

Every marker carries `MARKER_GAS = 100 000 + 80 000 * MAX_PEERS`. That
covers the destination's own fan-out to every peer. The value is below
`Outbox.MAX_MESSAGE_GAS`, and a test pins that.

## 3. L1 output rollback (`WithdrawalOutputOracle`)

A revert discards a suffix of L2 blocks. The outputs posted for that
suffix must not finalize a withdrawal, because the restored chain has no
record of them. The oracle gets a recovery path:

- `recovery`: an address that the factory sets with `setRecovery`. Zero
  disables the path. The key belongs to the chain's dedicated recovery
  principal, not to the attester or challenger.
- `rollbackOutputs(fromIndex)`: recovery only. Marks every output from
  `fromIndex` to the newest one as deleted. Already deleted outputs are
  skipped.
- `pause()` and `unpause()`: recovery only. While paused, `isFinalizable`
  is false and the finalization clock stops. `unpause` gives every output
  that was still inside its window a fresh timestamp, so it waits a full
  window again.

**Settlement floor.** `rollbackOutputs` reverts with
`BelowSettlementFloor` when any output in the range has already ended its
window. Withdrawals under that output may be paid on L1. No revert may go
below it. Older problems are handled forward, with a new cut.

**Operator invariant.** `finalizationWindow >= W`. Otherwise a withdrawal
can finalize before the operator can declare the incident that reverts it.

The deployer's `initialize` signature is unchanged. The recovery address
is set after deployment, through the factory.

## 4. Egress cap (`ETHLockbox`)

The lockbox enforces two caps on `finalizeWithdrawal`:

| Parameter | Meaning |
|---|---|
| `egressCapPerWindow` | Total value that can finalize per window (`E`). |
| `egressAccountCapPerWindow` | Value one L2 sender can finalize per window. |

The window length is the oracle's `finalizationWindow`, so the cap and
the delay use one clock. The window id is `block.timestamp / window`.
A withdrawal that would push either total above its cap reverts with
`EgressCapExceeded`. It is not consumed. It can retry in a later window.
Zero disables a cap. The factory sets both caps with `setEgressLimits`.

The share rule is public: each account gets at most the per-account cap
per window, and all accounts together at most `E`.

## 5. Follow-ups

- **Block boundary after a marker.** The sealer stamps block boundaries
  at ticks. One destination block can carry two remote-epoch records from
  the same origin. A block that opens a round can then also deliver
  messages the origin sent after its own cut. A marker-bearing record must
  force a block boundary after it. The sealer already forces a boundary
  for an L1 origin record, so the mechanism exists. Until that lands, the
  operator's revert procedure verifies lane-cursor consistency per lane
  before it accepts a cut.
- **First round on a new lane.** A lane's first round after its
  first-ever traffic in one direction can lack a marker in the other
  direction. The same consistency check catches it. Operators register
  peers at onboarding.
- **Timer driver.** `startRound` needs a caller. An operator cron is the
  first driver. A derived system transaction is a later option.
