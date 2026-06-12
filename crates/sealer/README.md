# kardamom-sealer

Block sealer. Single instance for v1: no election, no failover. Emits
`BlockBoundaryStart` markers onto tx_ordering every 250 ms wall-clock. If
the process dies the L2 stops producing blocks until an operator restarts
it — acceptable because the sealer is off the transaction hot path.

On restart, the sealer reads the most recent `BlockBoundaryStart` from
tx_ordering's tail to resume `block_number` without gaps.
