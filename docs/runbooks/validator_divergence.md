# validator_divergence

## Cause

The validator's re-execution of a block disagreed with the executor's published
receipts, block access list, or state root.

## Confirm

1. Read `/halt` on the validator. The detail carries the divergence reason:
   the block, and what disagreed. A divergence that replica results proved
   ends with `[k of n replica sessions differ on <stream>: session <id>, ...]`.
   It names every session whose result differs. The other `n - k` sessions
   agree with the validator.
2. Check `validator_divergence_total`. It increased once at the halt.
   `validator_replica_divergence_total{replica, check}` increased once for
   each named session, with the check (`bal`, `receipt`, or `rows`).
3. Read the validator log for the lines before `service halted`. A forged
   epoch says `epoch verification failed`; a receipt or write-set mismatch
   names the transaction.

## Steps

1. Treat the chain as suspect until the cause is known. The executors keep
   serving. The output attester pauses on this halt (`kardamom_chainStatus`
   shows `attester` paused on `validator_divergence`): no output root reaches
   L1 until the halt clears.
2. Compare the validator's view with the executors': the receipts of the
   block, the block access list on `tx_bal`, and the state root. One of the
   three sides is wrong: an executor, the validator's binary, or the stream.
3. Decide which side is wrong from the counts in the reason.
   - Some sessions differ and others agree (`1 of 3`, `2 of 3`): the named
     sessions are the suspects. The validator and the agreeing replicas
     computed one result.
   - Every session differs, and the reason ends with `the replicas agree
     with each other, so the validator is the suspect`: suspect the
     validator's binary or state first (step 4).
   - Every session differs, and the replicas also differ from each other:
     no side is clear from the counts. Compare the binaries of every side.
   - A session can name two processes: two replicas that share one media
     driver, or a restart that attached to a live publication, publish
     under one session.
4. If the validator's binary is wrong (a mixed-version fleet, an old
   `derive_epoch`), deploy the right binary. Its state from the halted block
   on can be wrong too: rebuild it from a peer checkpoint or from L1
   (`kardamom-reconstruct`) before you clear the halt.
5. If an executor is wrong, stop it and let the other executors serve.
   Rebuild its state from a peer checkpoint or from L1 (`kardamom-reconstruct`).
   - Find the replica from the session id in the reason. Search the executor
     logs for `tx_bal publication open` or `tx_receipts publication open`
     with that `session`. With discovery, the publisher record with that
     `session_id` meta names the host.
   - The halt names the differing sessions of one block only. Before the
     clear, compare the other replicas' results of the next blocks too.
6. If the stream carried a forged record, find the producer. The canonical id
   of the record names it.

## Clear

This halt needs an operator (`operator`). After the investigation, run
`POST /halt/clear` on the validator's node. The validator resumes from its
cursor.

- A divergence that replica results proved is not checked again. The
  resumed run opens new, empty buffers, and `tx_bal` and `tx_receipts` do
  not replay old frames. So the halted block, and every block that the
  executors published while the validator was halted, commit unverified.
  They count in `validator_bal_missing_total` and
  `validator_receipt_missing_total`. A block that was already committed
  stays committed. The clear does not prove the fix: finish steps 3 to 5
  first.
- For the other causes, the validator verifies the block again. If the cause
  is not fixed, it halts again at the same block.
