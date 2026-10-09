# exec_record_mismatch

This halt occurs only on a validator that reads the executor stream
(`--tx-source exec-stream`).

## Cause

At one canonical index, every executor archive holds a record, and no record
passes the validator's check: its `tx_ref` is not the canonical `TxRef`, or the
keccak of its bytes is not the canonical `tx_hash`.

## Confirm

1. Read `/halt` on the validator (port 9006). The cause is
   `exec_record_mismatch`. The detail names the index, the canonical `tx_hash`,
   and the count of executors.
2. Read the validator log before `service halted`. The lines
   `executor stream record fails the check against the canonical TxRef` name
   the index and the reason (`tx_ref` or `hash`) of each copy. The line
   `every executor archive holds a record at the index that fails the check`
   comes last.
3. Check the counters on the validator:
   - `kardamom_exec_stream_record_rejected_total{reason}` increased.
   - `kardamom_exec_stream_refetch_total{outcome="mismatch"}` increased once for
     each executor.
4. This is not `validator_divergence`. The validator did not execute the
   record. Its state ends at the block before the index.

## Steps

1. Treat the entry as suspect. The executors joined bytes that do not hash to
   the hash that the sealer ordered, or the validator checks the record wrong.
   The validator does not execute the entry and does not drop it. Its attester
   posts no output root past the last verified block.
2. Compare the binaries first. A validator and executors of different releases
   can disagree on the record layout. If the versions differ, deploy one
   release to every executor and to the validator, then clear the halt.
3. If the versions agree, read the record at the index from one executor:
   - Ask `kardamom_getExecLocator` with `[index, tx_hash]` on the executor query
     endpoint (port 9024).
   - Compare `keccak256(raw_tx)` with the `tx_hash` of the canonical `TxRef`,
     and compare the `tx_ref` fields with the `TxRef` on the canonical order.
4. If the bytes do not hash to the canonical hash on every executor, the
   executors executed bytes that the sealer did not order. Stop the attesters
   of every validator. Find the ingress or the sequencer that published the
   wrong bytes or the wrong hash. The repair of the executors' state is a
   chain decision: follow [`revert_to_posted_head.md`](revert_to_posted_head.md)
   only after the cause is known.
5. If the bytes hash correctly and only the `tx_ref` differs, the record
   layout or the check is wrong. Fix the binary before you clear.

## Clear

This halt waits for an operator (`operator`).

- Clear it with `POST /halt/clear` on the validator node.
- The validator then runs the pipeline again from its cursor and checks the
  record again. If every archive still holds a mismatched record, it halts
  again.
- No file keeps this halt. A restart of the validator meets the same index
  and halts again.
