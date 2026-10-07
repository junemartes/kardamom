# record_lag

The record-lag guard is in the sealer, but it is off by default. The budget is
`0` until a later release turns it on. In this release a budget above `0` stops
the sealer start. So this halt cannot occur yet.

## Cause

The last ordered canonical index is more than `KARDAMOM_RECORD_LAG_BUDGET`
records past the highest index that an executor recorded. The sealer refuses
new transactions until an executor records again.

## Confirm

1. Call `kardamom_chainStatus` on an ingress. The root is `sealer` (instance
   `cluster`) halted on `record_lag`. Its detail names the best recorded index
   and the budget. The ingresses are `paused` on it, not halted.
2. Read the sealer logs. The line `cluster RECORD-LAG-REJECT` names the sealed
   index, the best recorded cursor and the budget.
3. Read `/halt` and the logs of each executor. No executor records: every one
   is gone, frozen, parked at one entry, or its archive is stalled.
4. A client sees the JSON-RPC error `chain halted: record_lag at sealer` (code
   -32010) on `eth_sendRawTransaction`. The `txError` frame has the reason
   `record-lag`. Deposits and block boundaries continue.

## Steps

1. Find why no executor records.
   - If every executor is gone, start them (`nomad job status executor`).
   - If every executor is parked at one entry, follow the void section of
     [`failure-modes.md`](../failure-modes.md#removal-of-an-entry-that-no-consumer-can-execute-void).
   - If the archives of the executors stall, read the Aeron archive logs on
     the executor nodes.
2. Follow the [executor failure modes](../failure-modes.md#executor) for the
   fault that you find. One live executor that records is enough.
3. Do not set the budget to zero to make the chain live again.
   - Zero turns the guard off. The chain then orders entries that no executor
     recorded.
   - The guard exists to keep those entries inside the void window.
   - Every member must use the same budget. A change needs a coordinated
     restart of all the sealer members.

## Clear

This halt clears by itself (`auto`).

- An executor that records again sends its recorded cursor to the sealer.
- When the best cursor comes within the budget of the sealed index, the sealer accepts transactions again.
- The ingress clears the sealer halt on the next status frame from the cluster.
- The ingresses then resume submits. No pause needs an operator.
