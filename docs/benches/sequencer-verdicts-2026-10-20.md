# Sequencer bench: the verdicts of the v2 loop (2026-10-20)

With `[admission] verdicts = true`, the v2 loop sends the receipt of each forwarded transaction to the ingress that published it, on the `tx_verdicts` stream of that ingress. The ingress then reads no `tx_receipts` stream. See `docs/aeron-discovery.md` and the ingress part of `docs/failure-modes.md`.

## The bench

`cargo bench -p kardamom-sequencer --bench v2_loop` has a third case, `with_verdicts`:

- The input of `in_order`: 256 senders, 16 nonces each, from 8 ingress ids.
- Then one block for each nonce: the receipts of every sender, with their rows, then the `Rows` event of the block.
- The loop matches each receipt to the ingress of its transaction, puts the rows of the senders of each frame into it, and queues one frame for each ingress and block.
- The bench checks that every receipt went out.

The host has 12 cores and a load average of 17 to 22 from other work, so the numbers are lower bounds.

| Case | Throughput on one core |
|---|---|
| `in_order` | 1.27M tx/s |
| `reversed` | 1.12M tx/s |
| `with_verdicts` | **880k tx/s** |

`with_verdicts` does more work for each transaction than `in_order`: it also applies the rows of 16 blocks, matches each receipt, and builds the frames. A version of the frames without rows ran 1.06M tx/s. So the rows of the frames cost about 0.2 µs for each transaction.

The goal is 1M tx/s over 8 lanes, so one lane needs 125,000 tx/s. One core of the loop with verdicts has about 7 times that.

## The e2e suite

`KARDAMOM_E2E_ADMISSION=1 KARDAMOM_E2E_VERDICTS=1` runs the local stack with verdicts: every sequencer on the v2 loop with verdicts, and the ingress with `--verdicts true --admission-checks false`. On this host, 38 of the 40 chain-semantics scenarios pass with it:

- S11 fails on every loop on this host: the validator lags.
- S10d is flaky on this host in every mode, the default mode included.
- S13c and S13d keep the v1 loop (`StackConfig::unpayable_spends`).
- S17c and S17g read the local account layer of the ingress, which only the full receipt stream fills. They keep `tx_receipts` (`StackConfig::ingress_rows`).

## What the e2e suite found

- A new verdict publication is not connected for its first frames. Each offer is therefore best effort: the Aeron thread holds the frame until the offer deadline. A lossy offer dropped the first verdicts of each ingress.
- The v2 filter dropped the rows of the first transaction of a chain. Position 0 names both the empty chain and the state after its first transaction, and the filter took only a higher position. It now also takes position 0 with a higher nonce.
- The v2 filter marked a forward with the block of the last rows that it saw. A forward before any rows, or on a quiet chain, then expired with the next rows. A mark now takes the block of the first rows after the forward.
- A shard that restarted asked an executor for the state of a sender. The state database commits after the receipts go out, so the answer was one nonce behind the rows that the shard missed before its start. The next transaction of the sender parked behind a gap that did not exist, and no new rows came to close it. Now a sender whose state came from a lookup asks again after `relook_ms` (500 ms), while a transaction parks behind a gap over that state. A gap over rows is a missing transaction and asks nothing.
- An ingress that missed a receipt answered a copy of an executed transaction with `DuplicatedTx`. It now looks up the receipt by hash, from its cache or an executor, and answers with the receipt.
