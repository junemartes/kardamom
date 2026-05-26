# kardamom-leases

Lease primitive consumed by S2 (sequencer hot-standby), S5 (sealer leader election), and S7 (L1 batcher leader election). Per .

V0 implementation: a host holds the lease iff it has the lowest host id among recorders whose latest `FsyncWatermark.position` is within `caught_up_window` bytes of the current `QuorumWatermark`. Fully deterministic; no external KV or consensus library.
