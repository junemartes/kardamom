# Priority fees: admission at the sequencer, ordering at the sealer

Status: designed, not built.

## 1. Problem

The pipeline orders transactions first seen. The sealer never parses a payload; it sees a
guard header `[sender][nonce][deadline][canonical_id]` and relays the bytes. Fees reach a
contract only in the executor: the base fee is zero, the beneficiary is the zero address,
and the priority fee a transaction offers is burned. A trader cannot pay for a place in a
block, and the chain earns nothing from priority.

Wanted:

- The sequencer admits a transaction only when the sender can pay the priority fee it
  offers, and only when that fee reaches a floor.
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

### 3.1 The fee travels in the guard header

The sequencer decodes, beside the nonce, the fee fields of the transaction: for a type-2
transaction `max_priority_fee_per_gas` and `max_fee_per_gas`; for a legacy transaction
`gas_price`. It computes the effective priority fee the chain would collect at the current
base fee (zero today, so `min(max_priority, max_fee)` for type 2 and `gas_price` for
legacy), in wei, and puts it in the offer: `RefOffer { …, priority_fee: u128 }`, and in the
sealer's guard header as `[priority_fee:16]`. The field is a pure function of the
transaction bytes, so both replicas send the same offer.

With the setting off, the sequencer writes zero. A zero fee everywhere makes the sealer's
order equal to arrival order, so the sealer's code path is one, and the setting changes
the data, not the algorithm.

### 3.2 Admission at the sequencer

With the setting on, the sequencer's nonce state machine gains two checks before a
transaction is parked or released:

- `priority_fee >= min_priority_fee_per_gas`, the chain's floor (3.6); below it the
  transaction is rejected on `tx_errors` with a new kind `FeeTooLow`.
- the sender's balance covers `gas_limit * (base_fee + priority_fee) + value`. The ingress
  checks `gas_limit * max_fee + value` today against a fresh balance; the sequencer has the
  same account view through the live map and Redis, and checks the effective price. A
  stale balance passes (the executor is the truth; an unpayable transaction becomes a
  skip receipt, as today).

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

### 3.4 Where the fee goes

The executor pays the priority fee to the block's beneficiary. The beneficiary is a config
value of the chain (the operator's fee account), not the zero address, when the setting is
on. The base fee stays zero in this spec; an EIP-1559 base fee that follows demand is a
separate design, and it needs the sealer to know gas, which it does not.

One existing inconsistency gets fixed on the way: the receipt reports `effective_gas_price`
as `max_fee_per_gas` while revm charges `min(max_fee, priority)` at a zero base fee; the
receipt reports what was charged.

### 3.5 The setting

| Where | Name | Effect |
|---|---|---|
| sequencer `sequencer.toml` | `[fees] priority = true/false` | the fee in the offer, the floor (from the chain config) and the balance check |
| sealer `-Dkardamom.cluster.orderingWindow` | `20` or `0` | the window hold; must match on every member |
| executor | `--beneficiary <address>` | where the priority fee goes |

The deploy passes all three from one value (`PRIORITY_FEES=on`), so they cannot disagree.
Turning the setting off is a rolling deploy with the fee at zero and the window at zero;
nothing in the chain's history depends on the setting, because the order is in the log
either way.

### 3.6 The floor is a protocol value

`min_priority_fee_per_gas` is a field of the chain's genesis, beside the chain id and the
block gas limit, in wei per gas. The sequencer reads it from the chain config it already
loads, never from an operator setting, so two sequencers cannot disagree and the validator
can check that every sealed transaction cleared the floor. A change is a chain upgrade: a
new value with an activation block in the chain config, applied by every service at that
block. Zero is a valid value and means no floor.

## 4. Order and estimate

| Step | Work | Proof |
|---|---|---|
| 1 | the fee decode in the sequencer, the field in `RefOffer` and the guard header, zero when off | the racing-replica test still produces byte-identical offers |
| 2 | the sealer window, the timer, the flush order, the sender chaining | Java unit tests: fee order within a window, nonce order of one sender across fees, flush on 20, on the timer, on a boundary; the replica-determinism test |
| 3 | admission at the sequencer: the floor and the balance | sequencer unit tests; a chain-semantics case with a below-floor transaction rejected on the feed |
| 4 | the beneficiary and the receipt's price | an executor test; the receipt vectors |
| 5 | the deploy value and the three settings | the deploy isolation test |
| 6 | a load shard with fees on: latency within the window's 5 ms of the baseline | the load campaign's comparison |

## 5. Open questions

1. The window's 5 ms as a cluster timer: Aeron Cluster timers are scheduled in
   milliseconds on the leader's clock and recorded in the log; the follower replays them at
   the same log position. Confirm the minimum timer resolution on the pinned version.
2. Whether `Offered` (the status feed) should carry the fee, so a trader sees what it paid
   for its place.
3. The floor's unit is wei per gas at a zero base fee; when a base fee exists, the floor
   applies to the priority part only.
