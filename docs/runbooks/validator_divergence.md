# validator_divergence

## Cause

The validator's re-execution of a block disagreed with the executor's published
receipts, block access list, or state root.

## Confirm

1. Read `/halt` on the validator. The detail carries the divergence reason:
   the block, and what disagreed.
2. Check `validator_divergence_total`. It increased once at the halt.
3. Read the validator log for the lines before `service halted`. A forged
   epoch says `epoch verification failed`; a receipt or write-set mismatch
   names the transaction.

## Steps

1. Treat the chain as suspect until the cause is known. The executors keep
   serving, so nothing stops on its own.
2. Compare the validator's view with the executors': the receipts of the
   block, the block access list on `tx_bal`, and the state root. One of the
   three sides is wrong: an executor, the validator's binary, or the stream.
3. If an executor is wrong, stop it and let the other executors serve. Rebuild
   its state from a peer checkpoint or from L1 (`kardamom-reconstruct`).
4. If the validator's binary is wrong (a mixed-version fleet, an old
   `derive_epoch`), deploy the right binary.
5. If the stream carried a forged record, find the producer. The canonical id
   of the record names it.

## Clear

This halt needs an operator (`operator`). After the investigation, run
`POST /halt/clear` on the validator's node. The validator resumes from its
cursor and verifies the block again. If the cause is not fixed, it halts again
at the same block.
