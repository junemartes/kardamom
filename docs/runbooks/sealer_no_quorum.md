# sealer_no_quorum

## Cause

The sealer emits no block boundary: the Aeron Cluster has no leader, or no
quorum of members to commit one. Nothing is ordered while it lasts.

## Confirm

1. Call `kardamom_chainStatus` on an ingress. The service `sealer` (instance
   `cluster`) is `halted` with the cause `sealer_no_quorum`. The ingresses and
   the sequencers are `paused` on it.
2. Read the cluster job's logs on each sealer node. A member without a leader
   logs its election state; a member that is gone has no allocation.
3. Read `/halt` on a second ingress. Both ingresses see the same silence. If
   only one ingress sees it, the fault is that ingress's cluster session, not
   the sealer: restart that ingress.

## Steps

1. Count the live members. A cluster of three needs two for a quorum.
2. Start a member that is gone (`nomad job status cluster`). A member whose
   disk is lost rejoins through a snapshot: follow the cluster member rejoin
   procedure in `docs/failure-modes.md`.
3. If every member runs but none leads, read the members' logs for a clock or
   network fault between the sealer nodes, and fix it.

## Clear

This halt clears by itself (`auto`). The ingress clears it on the first
boundary it receives, and the paused ingresses and sequencers resume with it.
