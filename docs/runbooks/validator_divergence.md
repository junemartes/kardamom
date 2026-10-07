# validator_divergence

## Cause

The validator's re-execution of a block disagreed with the executor's published
receipts, block access list, or state root.

## Confirm

1. Read `/halt` on the validator. The detail carries the divergence reason:
   the block, and what disagreed. A divergence that an executor replica
   proved ends with `[replica session <id> on <stream>]`.
2. Check `validator_divergence_total`. It increased once at the halt.
   `validator_replica_divergence_total{replica, check}` names the same
   replica and the check (`bal`, `receipt`, or `rows`).
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
3. If an executor is wrong, stop it and let the other executors serve. Rebuild
   its state from a peer checkpoint or from L1 (`kardamom-reconstruct`).
   - Find the replica from the session id in the reason. Search the executor
     logs for `tx_bal publication open` or `tx_receipts publication open`
     with that `session`. With discovery, the publisher record with that
     `session_id` meta names the host.
   - The validator compares every replica, so the named replica is wrong
     even when the other replicas agree with the validator. The halt names
     the first wrong result only. After the clear, the validator verifies
     the block again and halts again on any other wrong replica.
4. If the validator's binary is wrong (a mixed-version fleet, an old
   `derive_epoch`), deploy the right binary.
5. If the stream carried a forged record, find the producer. The canonical id
   of the record names it.

## Clear

This halt needs an operator (`operator`). After the investigation, run
`POST /halt/clear` on the validator's node. The validator resumes from its
cursor and verifies the block again. If the cause is not fixed, it halts again
at the same block.
