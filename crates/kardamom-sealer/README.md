# kardamom-sealer

S5 of the kardamom sequencer. One sealer process per recorder host; the
[`kardamom-leases`] lease primitive deterministically picks the leader
(lowest recorder id among caught-up recorders). The leader emits
`BlockBoundaryStart` markers onto tx_ordering every 250 ms; hot standbys
sit on the same election input and take over within
`(caught_up_stale_ms + tick_interval_ms)` if the leader's watermark
stops advancing.

All state is reconstructable from B's tail; failover is mechanical.

Spec: `` §2.6, §4.5.
Plan: `` (live decisions tracked
inline in the crate's module docs; the plan is the historical reference).
