# Sequencer bench: the v2 loop (2026-10-20)

The v2 loop of #697 against the v1 loop. Both benches run on one core of the same host: 12 cores, a load average of about 12 from other work. So the numbers are lower bounds.

## The benches

- `cargo bench -p kardamom-sequencer --bench throughput`: the v1 loop, 64 senders with 16 nonces each.
- `cargo bench -p kardamom-sequencer --bench v2_loop`: the v2 loop, 256 senders with 16 nonces each. Each sender has a row, so no transaction waits for a lookup. Two orders:
  - `in_order`: each transaction goes out at once;
  - `reversed`: each sender's nonces arrive last first, so each one parks until nonce 0 frees the run.
- `cargo bench -p kardamom-sequencer --bench admission`: the filter alone, with no decode and no offer.

In each loop bench the proxy supplies the sender, so the loop does no secp256k1 work. The loop decodes the fee fields, runs the fee checks and the filter, and offers each reference to an in-memory sealer port.

## Results

| Bench | Throughput on one core |
|---|---|
| v1 loop | 1.13M tx/s |
| v2 loop, `in_order` | **1.33M tx/s** |
| v2 loop, `reversed` | **1.10M tx/s** |
| filter alone | 2.18M tx/s |

The goal is 1M tx/s over 8 lanes, so one lane needs 125,000 tx/s. One core of the v2 loop has about 10 times that headroom, with every transaction parked first.

Two changes in the loop gave most of the speed:
- The loop computes the vslot of a sender once, in the guard, and keeps it with the transaction. The vslot is a keccak of the address, and the parked depth needs it again at the verdict. Before this change the loop ran 751k tx/s.
- The verdict vector keeps its capacity from turn to turn.

The host does not permit `perf` (`perf_event_paranoid` is 4), so there is no profile.

## The settings

The `[admission]` section of the sequencer config turns the v2 loop on. Off, the sequencer runs the v1 loop.

| Setting | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Run the v2 loop in place of the v1 loop. |
| `max_parked` | 16 | The most transactions of one sender that wait for a nonce gap. |
| `max_forwarded` | 64 | The most forwarded transactions of one sender that no row confirmed. |
| `loss_blocks` | 16 | The blocks after which an unconfirmed forwarded transaction is lost. |
| `park_width_units` | 131,072 | The 32-byte units of an image after which a parked transaction expires. |
| `max_senders` | 1,048,576 | The senders above which a sweep forgets the idle ones. |
| `sweep_every_ms` | 50 | The time between two sweeps. |
| `relook_ms` | 500 | The time after which a sender whose state came from a lookup asks again, while a transaction parks behind a gap over that state. |
| `verdicts` | `false` | Send the receipt of each forwarded transaction to the ingress that published it, on `tx_verdicts`. |
| `outbox_cap` | 4,096 | The offers that may wait for the sealer before the loop stops taking envelopes. |

A parked transaction also expires after `tx_ttl_ms`, the lifetime of the sequencer.

## The e2e suite

`KARDAMOM_E2E_ADMISSION=1` runs every sequencer of the local stack on the v2 loop. On this host, 39 of the 40 chain-semantics scenarios pass with it. S11 fails on both loops on this host: the validator is about 180 blocks behind the sealer when the 60-second wait ends. S11 passes in CI.

S13c and S13d send transactions that cannot pay, so that the executor writes skip receipts. The v2 filter refuses such a transaction before the sealer. So these two scenarios keep the v1 loop (`StackConfig::unpayable_spends`).
