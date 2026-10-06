# Priority fees

Priority fees let a sender pay a tip for an earlier place in the order. The chain also charges an
EIP-1559 base fee. This page follows a fee through the pipeline: the switch, the genesis values,
the admission checks, the ordering, the settlement, and the client view.

- The tip is an amount, not a rate: the tip rate times the gas limit. The sender pays the whole amount.
- There is no floor on the tip. A tip of zero is always valid.
- The setting is off by default. With it off, the order is arrival order and the base fee is zero.

## The switch

One value turns the feature on for every role: the deploy variable `PRIORITY_FEES` (`on` or `off`).
The Nomad variable `priority_fees` has the same values and the default `off`.

| Role | What `on` sets |
|---|---|
| Sequencer | `[fees] priority = true`. The env var `KARDAMOM_PRIORITY_FEES` (`true` or `false`) also sets it. The three admission checks run, and the tip of each transaction rides its offer. |
| Sealer | `-Dkardamom.cluster.orderingWindow=20`. With `off` the value is `0`. |
| Executor and validator | The deploy appends `deploy/cluster/config/genesis/fees.toml` to the genesis. The fee schedule applies. |

- The deploy sets all three from the one value, so they cannot disagree.
- Every replica of a role must use the same value.
- The jobs that read the switch: `sequencer`, `executor`, `validator` and `cluster` (the sealer).

Warning: turning the feature on or off for a chain that has blocks is a chain upgrade.

- The fee schedule is a protocol value in the genesis. It changes how a block executes and what a receipt holds.
- Every executor, every validator and every tool that rebuilds state must read the same genesis.
- An executor that resumes from a committed block takes the base fee from that block. A committed base fee of zero stays zero. So a restart alone does not start a schedule on a chain with history.
- Plan the change with an activation block, as for any protocol change.

## Genesis `[fees]`

The `[fees]` section of the genesis sets the chain values of the schedule.

```toml
[fees]
base_fee_initial = 1000000000
beneficiary = "0x8626f6940E2eb28930eFb4CeF49B2d1F2C9C1199"
```

| Key | Type | Meaning |
|---|---|---|
| `base_fee_initial` | integer, not zero | The base fee of the first block, in wei per gas. |
| `beneficiary` | address | The account that collects every tip. |

- The section has no other keys. An unknown key is an error.
- `base_fee_initial` is never zero. The schedule can lower a base fee by only one eighth in a block, so one wei is the lowest value. A zero base fee means "no schedule".
- No `[fees]` section means no schedule:
  - the base fee is zero in every block
  - the tip of every transaction burns at the zero address
  - the receipts have no tip fields, so `priorityFeePerGas` and `priorityFeePaid` are `0x0`
- The dev fragment `deploy/cluster/config/genesis/fees.toml` sets 1 gwei and the Anvil account #19 as beneficiary. The key of that account is public. Do not use it on a real chain.

## Base fee rule

The base fee follows EIP-1559. Each block gets its base fee from the previous block.

| Parameter | Value |
|---|---|
| Block gas limit | 30000000 |
| Gas target | 15000000 (half the limit) |
| Change denominator | 8 |
| First block | `base_fee_initial` |

- If `gas_used` equals the target, the base fee does not change.
- If `gas_used` is above the target, the base fee rises by `max(1, base_fee * (gas_used - target) / target / 8)`.
- If `gas_used` is below the target, the base fee falls by `base_fee * (target - gas_used) / target / 8`.
- A full block raises the base fee by one eighth. An empty block lowers it by one eighth. Example: 900 wei after an empty block gives 788 wei.
- The base fee on a transaction burns. The tip goes to the beneficiary.
- Each block header row in the state database holds the `base_fee` and the `gas_used` of the block. The block boundary on the receipt stream carries both.
- The gas limit of one transaction is at most 16777216 (EIP-7825).

## Admission at the sequencer

With priority fees on, the sequencer checks every transaction before it parks or releases it. A transaction that fails a check never enters the nonce state machine. It holds no nonce slot.

| Reason | Check | JSON-RPC message starts with | What the client does |
|---|---|---|---|
| `FeeInvalid` | The tip rate is above the fee cap. | `max priority fee per gas higher than max fee per gas` | Sign again with a tip rate within the cap. |
| `FeeTooLow` | The fee cap is below the next base fee. | `max fee per gas less than block base fee` | Sign again with a higher cap. |
| `InsufficientFunds` | The balance is below `gas_limit * max_fee_per_gas + value`. | `insufficient funds for gas * price + value` | Fund the account, then submit again. |

- The checks run in this order: `FeeInvalid`, `FeeTooLow`, `InsufficientFunds`.
- The base fee for `FeeTooLow` is the base fee of the next block. The sequencer computes it from the newest block boundary that it has seen. It can lag by a few blocks. Before the first boundary it is zero, so a cold replica admits every cap.
- The balance comes from the local account layer. A sender that the layer does not hold passes. The executor is the truth: a transaction that cannot pay at its block becomes a skip receipt.
- The ingress maps all three reasons to JSON-RPC code `-32000`. The messages have the shape of the geth messages. See [Client JSON-RPC API](json-rpc.md#errors).
- The same reasons reach a subscriber as `fee-invalid`, `fee-too-low` and `insufficient-funds`. See [Transaction status events](tx-status-events.md).
- The metric is `kardamom_sequencer_fee_rejected_total{partition}`. It counts the rejections of the three checks.
- There is no tip floor. The sequencer never rejects a transaction for a low tip.
- With priority fees off, the sequencer skips these checks. Every transaction gets a bid of zero.

### The bid

An admitted transaction gets a bid in wei. The bid rides the offer to the sealer, in the guard header.

- A typed transaction (EIP-1559 and later) bids `max_priority_fee_per_gas * gas_limit`.
- A legacy or EIP-2930 transaction bids `(gas_price - base_fee) * gas_limit`, and not below zero.

## Ordering at the sealer

The sealer holds a window of records and flushes it in order of the bid.

- The window holds up to 20 records. It closes on the count, on a 5 ms timer, on a block boundary, on an origin record, or on a snapshot.
- The flush order is bid descending, then arrival ascending. The records of one sender stay in nonce order.
- A tip buys a place only against the records in the same window. It is not a global queue.
- With the window at `0`, the sealer keeps arrival order.
- Every member of the sealer cluster must use the same window size.
- For the details, see [the sealer README](../cluster/sealer-service/README.md#ordering-window).

## Settlement at the executor

With a schedule, the executor settles each transaction in this way:

- The sender pays the base fee on the gas that it uses. This amount burns.
- The sender pays the tip in full: the tip rate times the gas limit. This holds also for the gas that the transaction does not use.
- The beneficiary receives the whole tip.
- The tip rate is the committed `max_priority_fee_per_gas`, bounded by what the fee cap leaves above the base fee.
- A transaction with a fee cap below the base fee is not valid at that block. It becomes a skip receipt.
- A derived transaction runs with the base fee check off and pays nothing. It is a deposit or a cross-chain delivery. Its receipt has no tip.

Without a schedule, nothing moves: the base fee is zero and the tip burns at the zero address.

## Receipt fields

A receipt has the Ethereum fields and two more. See [Client JSON-RPC API](json-rpc.md#receipts).

| Field | Value |
|---|---|
| `effectiveGasPrice` | The base fee of the block, in wei per gas (with a schedule). |
| `priorityFeePerGas` | The tip rate that the transaction paid, in wei per gas. |
| `priorityFeePaid` | The tip that the transaction paid, in wei. It is the tip rate times the gas limit. |

- A skip receipt, a deposit and any transaction on a chain with no schedule have `priorityFeePerGas` and `priorityFeePaid` at `0x0`.

## Fee RPCs

The ingress serves the fee methods. A wallet prices a transaction as on Ethereum.

| Method | Answer |
|---|---|
| `eth_feeHistory` | The base fee, the gas used ratio and the reward percentiles of recent blocks. |
| `eth_maxPriorityFeePerGas` | The median tip rate of the receipts in the newest 20 closed blocks. It is zero if there are none. |
| `eth_gasPrice` | The base fee of the next block plus the value of `eth_maxPriorityFeePerGas`. |

- The ingress keeps a ring of the last 1024 blocks for these methods. The ring is in memory.
- The ring comes from the block boundaries and the receipts on the `tx_receipts` stream. The ingress holds no block headers.
- For the parameters and the errors, see [Client JSON-RPC API](json-rpc.md#fee-methods).

## Further reading

- [Client JSON-RPC API](json-rpc.md)
- [Transaction status events](tx-status-events.md)
- [Sealer README](../cluster/sealer-service/README.md)
- [Deploy README](../deploy/cluster/README.md)
- [Design record: priority fees](specs/2026-10-01-priority-fees.md)
