# Runbooks

This directory has one file for each `RecoveryId` of `kardamom_obs::halt`. A halted service names its runbook in three places:

- the `recovery` label of `kardamom_halt`
- the `/halt` record on its metrics port
- the alert annotation

A unit test in `crates/obs` asserts that every `RecoveryId` has a file here with the four sections below.

Every runbook has the same shape:

- `## Cause`: what happened, in one sentence.
- `## Confirm`: how to see that this is the cause, not another one.
- `## Steps`: what to do, in order.
- `## Clear`: how the halt ends.

The metrics ports of the deploy (`deploy/cluster/nomad`) are below. Other services that can halt also serve `/halt` on their own metrics port.

| Service | Port |
|---|---|
| sequencer | 9001 + 10 × lane (9001 for lane 0, 9011 for lane 1) |
| batcher | 9002 |
| executor | 9004 |
| da-watcher | 9005 |
| ingress | 9006 |
| validator | 9006 |
| l1-indexer (the L1 follower) | 9009 |

The `/halt` route and the `POST /halt/clear` command sit beside `/metrics` on that port. The clear accepts a loopback peer only. Run it on the node of the service:

```sh
curl -s http://127.0.0.1:<port>/halt
curl -s -X POST http://127.0.0.1:<port>/halt/clear
```

An `auto` halt needs no clear. The service retries its cause and resumes when the cause goes. A clear of an `auto` halt is harmless. The service raises the halt again on the next failed retry.

A service that waits on the halt of another service is `paused`, not halted.

- It names the root in its `/halt` record and in `kardamom_paused{root_service, cause}`.
- It resumes by itself when the root clears. Follow the runbook of the root, not of the paused service.
- `kardamom_chainStatus` on an ingress lists every root and every pause.

An operator can pause a service for maintenance. Run these commands on the node of the service:

```sh
curl -s -X POST 'http://127.0.0.1:<port>/pause?note=disk-swap'
curl -s -X POST http://127.0.0.1:<port>/resume
```

## Alerts without a halt

Two alerts of the L1 follower have a runbook and no halt:

- [`l1_follower_lag.md`](l1_follower_lag.md): `KardamomL1FollowerLag`, no
  instance publishes the finalized blocks.
- [`l1_follower_wake_overdue.md`](l1_follower_wake_overdue.md):
  `KardamomL1FollowerWakeOverdue`, one instance does not read on its plan.

## Procedures

A procedure runbook has no `RecoveryId`: no halt names it, and an operator
starts it. It has the same four sections.

- [`sealer-fleet-rebuild.md`](sealer-fleet-rebuild.md): every sealer member lost its state. The chain
  restarts after the posted head from a state rebuilt from L1, and the blocks
  after the posted head are reverted.
