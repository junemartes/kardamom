# Priority fees: admission at the sequencer, ordering at the sealer

Status: designed, not built.

## 1. Problem

The pipeline orders transactions first seen. The sealer never parses a payload; it sees a
guard header `[sender][nonce][deadline][canonical_id]` and relays the bytes. Fees reach a
contract only in the executor: the base fee is zero, the beneficiary is the zero address,
and the priority fee a transaction offers is burned. A trader cannot pay for a place in a
block, and the chain earns nothing from priority.

Wanted:

- The sequencer admits a transaction only when the sender can pay the fee it offers. There
  is no floor on the priority fee: zero is a valid tip at all times.
- The base fee follows EIP-1559: it moves by up to one eighth per block toward a gas target,
  computed from the previous block's gas used.
- The sealer orders transactions by priority fee within a window of 20 entries or 5 ms,
  whichever closes first, and keeps every other rule (one nonce order per sender, the
  inclusion deadline, the dedup window).
- A setting turns it on or off; off is first come, first served, as today.

## 2. What constrains the design

- The sealer is a Raft state machine: deterministic, no wall clock, no payload parsing.
  Every member must compute the same order from the same log, so the fee must be in the
  log entry and the window must close on log events (an entry count or a cluster timer),
  never on a member's clock.
- Two sequencer replicas race per lane and must send byte-identical offers; the fee field
  must derive from the transaction alone.
- A sender's records must arrive at the sealer in nonce order (`CONTIGUITY_REJECT`
  otherwise), so a reorder keeps each sender's relative order.
- A block is a contiguous index range closed by the tick (2 s). Reordering inside a
  window never crosses a block: a window closes at a boundary.
- The inclusion deadline is 64 blocks. Holding a low-fee entry for one window (5 ms) does
  not touch it.

## 3. Design

### 3.1 Two prices, one ordering key

Two different things, kept apart everywhere in this spec:

- the gas price: what the sender pays per gas, the base fee plus the tip; `max_fee_per_gas`
  is its cap on a type-2 transaction, `gas_price` is it whole on a legacy one;
- the priority fee, the tip: what the sender bids for its place, above the base fee;
  `max_priority_fee_per_gas` on a type-2 transaction.

The ordering key is the tip and only the tip. The base fee is the same for every
transaction of a block, so it says nothing about order; a high `max_fee_per_gas` with a
zero tip is a transaction that can afford any base fee and bids nothing.

The sequencer decodes, beside the nonce, the fee fields of the transaction. For a type-2
transaction the bid is `max_priority_fee_per_gas`, a pure function of the bytes, so both
racing replicas send the same offer. A legacy transaction carries one price and no tip;
Ethereum reads it as `max_fee = max_priority = gas_price`, so its tip at a block is
`gas_price - base_fee`. The sequencer computes that from the base fee of the latest block
it has seen, clamped at zero; two replicas may differ by one block's step (one eighth), and
the sealer's first-seen dedup settles which offer defines the key. The sequencer puts the
bid in the offer, `RefOffer { …, tip: u128 }`, and in the sealer's guard header as
`[tip:16]`. What the sender pays is settled at execution (3.4), not here.

With the setting off, the sequencer writes zero. A zero fee everywhere makes the sealer's
order equal to arrival order, so the sealer's code path is one, and the setting changes
the data, not the algorithm.

### 3.2 Admission at the sequencer

With the setting on, the sequencer's nonce state machine gains two checks before a
transaction is parked or released:

- `max_fee_per_gas >= base_fee` of the latest block the sequencer has seen (it reads the
  base fee from the block boundaries it already taps); below it the transaction cannot be
  included at that price and is rejected on `tx_errors` with a new kind `FeeTooLow`. The
  base fee moves by at most one eighth per block, so a view a few blocks old errs by a
  few eighths; a transaction admitted on a stale view that cannot pay at its block becomes
  a skip receipt at execution, as any unpayable transaction does today.
- the sender's balance covers `gas_limit * max_fee_per_gas + value`, the same check the
  ingress makes against a fresh balance, repeated at the sequencer with its own account
  view (the live map and Redis). A stale balance passes; the executor is the truth.

There is no floor on the tip. A zero tip is admitted and ordered last within its window.

Both are per-transaction and constant time; they add nothing to the hot path beyond the
fee decode, which is a few fields of the already-decoded transaction.

### 3.3 Ordering at the sealer

The sealer gains a window in front of `onRecord`:

```
Window {
    entries:   bounded array of 20 (fee, arrival_seq, entry)
    by_sender: small map sender -> position of that sender's last entry in the window
    opened_at: the log position of the first entry
}
```

Rules:

1. An entry joins the window with its fee and its arrival sequence. If the window already
   holds an entry of the same sender, the new entry is chained after it: it takes the
   sender's fee (the head entry's fee) and a sub-order after the head, so the sender's
   nonce order holds whatever the fees.
2. The window flushes when it holds 20 entries, or when the cluster timer fires 5 ms after
   it opened, or when a boundary or an origin record arrives. The timer is a Raft cluster
   timer scheduled on the first entry; it is a log event, so every member flushes at the
   same point of the log.
3. A flush sorts the window by `(fee descending, arrival ascending)`, with chained entries
   kept behind their head, and runs the existing `onRecord` on each in that order: dedup,
   deadline, window full, contiguity, index assignment. The egress relays in that order.

The window is twenty entries, so the sort is a sort of twenty elements and the chaining map
has at most twenty keys. A binary heap or a balanced tree would not beat an insertion into
a twenty-element array; the data structure's job is to keep the sender chaining exact, and
the small fixed array does that at the cost of at most 20 comparisons per entry. The work
per entry is bounded and does not grow with the chain.

With the setting off at the sequencer, every fee is zero, the sort is stable on arrival,
and the window's only effect is the 5 ms hold. The sealer's own setting
(`-Dkardamom.cluster.orderingWindow=0|20`) turns the hold off too; it must match on every
member, and a member checks it against the leader's value at join and halts on a mismatch,
the way it checks `retention` and `inclusionHorizonBlocks`.

### 3.4 The base fee follows EIP-1559; the tip goes to the beneficiary

The executor computes each block's base fee as Ethereum does: from the previous block's
gas used against the gas target, with the target at half the block gas limit
(`BLOCK_GAS_LIMIT` is 30M, so the target is 15M) and a change of at most one eighth per
block (`BASE_FEE_MAX_CHANGE_DENOMINATOR = 8`); the genesis sets the first base fee. The
computation is a pure function of the chain, so every executor and the validator agree,
and the block header carries it. The sealer needs no gas knowledge: it orders by the tip,
and the executor settles the price.

At execution the sender pays `gas_used * (base_fee + effective_tip)` with
`effective_tip = min(max_priority, max_fee - base_fee)` for type 2 and
`gas_price - base_fee` for legacy, as on Ethereum: the base fee is burned, the tip goes to
the block's beneficiary, a chain value (the operator's fee account) in the genesis beside
the chain id. The receipt's `effective_gas_price` reports what was charged, which fixes the
present inconsistency where it reports `max_fee_per_gas` at a zero base fee.

The ordering key (the tip bid) and the settlement (the gas price charged) are two
different numbers on purpose: the bid is knowable before the block, the charge only in it,
and a window of 5 ms never spans a base fee change the sender could not have priced.

### 3.5 The setting

| Where | Name | Effect |
|---|---|---|
| sequencer `sequencer.toml` | `[fees] priority = true/false` | the tip in the offer, the base fee check and the balance check |
| sealer `-Dkardamom.cluster.orderingWindow` | `20` or `0` | the window hold; must match on every member |
| executor | the beneficiary and the base fee schedule from the chain config | the charge and where the tip goes |

The deploy passes all three from one value (`PRIORITY_FEES=on`), so they cannot disagree.
Turning the setting off is a rolling deploy with the fee at zero and the window at zero;
nothing in the chain's history depends on the setting, because the order is in the log
either way.

### 3.6 Protocol values

Three values live in the chain's genesis, beside the chain id and the block gas limit, and
change only by a chain upgrade with an activation block: the initial base fee, the
beneficiary, and the gas target (half the limit). There is no priority floor: it is zero at
all times by design, so a transaction never waits on an operator's price.

## 4. Order and estimate

| Step | Work | Proof |
|---|---|---|
| 1 | the fee decode in the sequencer, the field in `RefOffer` and the guard header, zero when off | the racing-replica test still produces byte-identical offers |
| 2 | the sealer window, the timer, the flush order, the sender chaining | Java unit tests: fee order within a window, nonce order of one sender across fees, flush on 20, on the timer, on a boundary; the replica-determinism test |
| 3 | admission at the sequencer: the base fee check and the balance | sequencer unit tests; a chain-semantics case with a transaction under the base fee rejected on the feed |
| 4 | the EIP-1559 base fee in the executor and the header; the beneficiary; the receipt's price | executor tests against Ethereum's base fee vectors; the receipt vectors |
| 5 | the deploy value and the three settings | the deploy isolation test |
| 6 | a load shard with fees on: latency within the window's 5 ms of the baseline | the load campaign's comparison |

## 5. Open questions

1. The window's 5 ms as a cluster timer: Aeron Cluster timers are scheduled in
   milliseconds on the leader's clock and recorded in the log; the follower replays them at
   the same log position. Confirm the minimum timer resolution on the pinned version.
2. Whether `eth_feeHistory` and `eth_maxPriorityFeePerGas` on the ingress read the base
   fee history and the tips of recent blocks, so wallets price transactions as they do on
   Ethereum; the ingress has the headers and the receipts for it.
3. The base fee at genesis and the gas target are chain values; the first values for
   staging come with the chain upgrade that activates the schedule.
