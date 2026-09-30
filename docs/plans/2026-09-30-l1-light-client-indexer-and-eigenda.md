# Next phase: a verified L1 view, an inbox indexer, and EigenDA

Date: 2026-09-30. Status: proposal. Tracks: kardamom-infra #9 (C1), #163
(validator L1 trust), #401 (rejoin from an L1 rebuild), #455 (the
batcher's lost cursor).

Staging runs on anvil. This plan replaces anvil with a real testnet L1,
moves the data availability of the batches from EIP-4844 blobs to
EigenDA, and gives every L1 reader a view it can verify. It answers two
questions first: what does each service need from the L1, and does that
need a full node.

## 1. What each service needs from the L1 today

| Service | Reads | Writes | Code |
|---|---|---|---|
| batcher | the oracle's `lastFinalizedBatch`, receipts of its own transactions | blob transactions to `KardamomProofOracle.claimBatch` (EIP-4844 sidecars), proof submissions | `crates/batcher/src/l1.rs`, `optimistic.rs`, `prover_submit.rs`, `blob.rs` |
| da-watcher | finalized block numbers and hashes, the lockbox logs of each finalized block (deposits, upgrades) | none | `crates/da_watcher/src/source.rs`, `rpc_source.rs` |
| validator | the same finalized blocks and logs, to re-derive every epoch | none | `crates/validator/src/epoch_verify.rs` |
| executor rebuild | the batch payloads of the inbox, from genesis | none | `docs/specs/2026-09-20-rejoin-from-l1-rebuild.md`, `crates/reconstruct` |
| light client | a consensus endpoint and an execution endpoint of a provider | none | `nomad/l1-light-client.nomad.hcl` (helios), unused on staging |

Every reader takes its view from one JSON-RPC endpoint and trusts it
(#163). The batch payloads live in blobs, which the beacon chain prunes
after about 18 days; a rebuild from L1 older than that has no source.

## 2. Full node, or light client and indexer

Three properties matter, and they separate cleanly:

1. **Correctness of headers and logs.** A consensus light client (helios)
   proves a finalized header through the sync committee and the finality
   branch, and verifies receipts against `receiptsRoot`. That is
   cryptographic, not a lower-grade approximation. The residual trust is
   the weak-subjectivity checkpoint (one auditable constant, rotated) and
   the sync committee (2/3 of 512, cheaper to corrupt than the validator
   set; it bites only together with a dishonest sequencer). A full node
   adds no correctness beyond this.
2. **Availability of the L1 view.** A light client verifies what a
   provider hands it; a provider that withholds a block or a log stalls
   the readers. The answer is two providers behind the light client and
   the alert on `validator_epochs_unverified_total` (#163, item 4), not a
   node. A full node removes the provider dependency; it costs a synced
   Sepolia node (about 1 TB of NVMe, days of sync) and its operation.
3. **Availability of our own data.** The batch payloads are ours; no L1
   node keeps 4844 blobs past the retention window, and EigenDA keeps a
   blob for its own window. Only an archive we run keeps a rebuild
   possible: the indexer.

**Decision:** a light client plus an indexer is enough; no full node.
Staging pairs helios with the validator and the da-watcher, reads
through it, and archives the inbox in the indexer. An own L1 node stays
the long-term option of #163 for the day a provider withholds; it is a
liveness improvement, and the alert tells us whether we need it.

The batcher's writes stay with a provider execution endpoint: a light
client does not build or broadcast transactions. With EigenDA (section
5) those writes become calldata transactions with a certificate, which
every provider accepts; the EIP-4844 path needs a provider that accepts
blob transactions, which is the case for the common ones.

## 3. The indexer

A service that follows the finalized L1 through the light client and
keeps what the chain needs from its inbox, for as long as we say:

- **What it stores.** Every batch of the oracle: the index, the L1 block
  and transaction, the claim's fields, and the payload (the blob data
  today, the EigenDA blob tomorrow), keyed by batch index. Every epoch's
  inputs: the finalized block hash and the lockbox logs, keyed by L1
  block. The finalization events of the oracle.
- **Where it reads.** Headers and logs through helios (verified). Blob
  data from the beacon API of the provider while the sidecar is retained,
  later from EigenDA by the certificate; the payload's commitment is
  checked against the claim before it is stored, so the source of the
  bytes does not matter.
- **What it serves.** A small HTTP API on the private network: a batch
  by index, the epoch inputs of an L1 block range, the last finalized
  batch. Consumers: the executor rebuild (`kardamom-reconstruct` reads
  batches instead of the L1), the validator's epoch check (a second
  source for its content checks, still re-derived), the batcher's resume
  (its cursor is L1-as-truth; after a node loss it asks the indexer for
  the last finalized batch and the cluster for the egress after it, the
  fix of #455), and later the da-watcher (it publishes from indexed
  finalized blocks instead of polling).
- **What it is not.** Not a source of truth: everything it holds is
  re-derivable from L1 and EigenDA, and every consumer checks a
  commitment. Not a second sequencer or validator. Losing it costs a
  re-index from the checkpoint, not the chain.
- **Shape.** One Rust crate, `kardamom-l1-indexer`, on the existing
  `L1Source` trait of the da-watcher (which gains the light-client
  source), an embedded KV store (the executor's libmdbx is already a
  dependency), one node class `indexer` in the placement model. On
  staging it rides on `da-watcher-0` through its role set
  (`node_roles=da-watcher,indexer`): the node is a cx23 with room, and
  the two read the same finalized stream. A 50 GB volume holds the
  archive.

## 4. Staging on a real L1 (C1)

Testnet: **Sepolia**, the Ethereum testnet with the long horizon and
EigenDA's active testnet. Providers: two (a primary and a fallback),
free tiers at staging's rate; their execution and consensus endpoints
become values of the GitHub environment (`L1_RPC`,
`L1_LIGHT_CLIENT_EXECUTION_RPC`, `L1_LIGHT_CLIENT_CONSENSUS_RPC`,
`L1_LIGHT_CLIENT_CHECKPOINT`). The settlement contracts deploy once on
Sepolia through the signed deployer; `SETTLEMENT_ADDRESS` and
`LOCKBOX_ADDRESS` then hold real values (the B6 that anvil made moot).
The batcher's key is funded from a faucet, with a balance alert.

Public changes: the workloads role skips the anvil job when `L1_RPC` is
set (the known gap C1 of the runbook); the da-watcher and the validator
take the light client's endpoint (`workloads_light_host`) as their L1
source; parent-hash chaining in `RpcL1Source` (#163, item 1); the alert
on unverified epochs (#163, item 4) in the monitoring job's rules.

## 5. EigenDA for the batches

Why: a 4844 blob is 128 KB, an L1 block targets three and tops at six,
shared with every rollup; that is about 64 KB/s for everyone. EigenDA's
testnet disperses tens of MB/s per client, and a batch is no longer
priced in L1 blob gas. The chain's own throughput (3,000 tx/s in the
load campaigns) needs the second, not the first.

How, in three parts:

1. **The batcher posts to EigenDA.** It disperses a batch through the
   EigenDA proxy sidecar (`POST /put`, the standard rollup integration)
   and receives a certificate; it then calls `claimBatch` with the
   certificate in calldata in place of the blob versioned hashes. The
   EIP-4844 path goes: no backend switch, no feature flag, no fallback
   (decision of 2026-09-30). The payload framing (version 3 blobs, the
   records commitment) does not change: what the bytes are stays; where
   they live changes.
2. **The contract takes a certificate.** `KardamomProofOracle`'s DA
   reference becomes the EigenDA certificate. On Sepolia the certificate
   is checked against EigenDA's cert verifier contract (the disperser's
   signatures and quorum); the optimistic claim window covers the rest
   as it does today. The existing challenge path reads the payload
   through the certificate.
3. **Every reader retrieves by certificate.** The indexer, the validator
   and the rebuild fetch a blob from the proxy (`GET /get/<cert>`) and
   check its commitment. The indexer archives it, so a rebuild works
   after EigenDA's own retention (about two weeks) has passed.

Cost on the testnet: none for dispersal; one proxy sidecar per node that
disperses or retrieves (a small container).

## 6. Order and estimate

| Step | Work | Proof | Cost |
|---|---|---|---|
| 1 | #163 items 1 and 4: parent-hash chaining, the unverified-epochs alert | unit tests; the alert fires in a chaos case that withholds a block | none |
| 2 | Staging on Sepolia: skip anvil, contracts deployed once, funded key, provider values, helios with a checkpoint as the source of the validator and the da-watcher | `just launch staging` and a deploy; the smoke; epochs verified through the light client | provider free tiers; faucet ETH |
| 3 | The indexer: crate, API, the role set of da-watcher-0, a volume; the rebuild reads it; the batcher resumes from it | a rebuild from the indexer matches the chain's state root; a batcher node loss recovers (#455) | one 50 GB volume, about 4 €/month |
| 4 | EigenDA: the batcher disperses and claims by certificate, the contract's DA reference, the proxy sidecar, retrieval by certificate; the blob path removed | a soak at the load campaign's rate; a rebuild from EigenDA-archived batches | none on the testnet |
| 5 | Two providers, and the decision on an own node from the alert's history | a provider outage keeps the readers on the fallback | a second free tier |

Step 1 is small and lands first. Steps 2 and 3 can run in parallel; 4
needs 2 (a real L1) and profits from 3 (the archive). No new Cloud
server: helios and the proxy are sidecars, the indexer shares the
da-watcher's node.

## 7. Open questions

1. **Log verification in helios.** The plan assumes helios verifies
   `eth_getLogs` against `receiptsRoot`. Confirm on the pinned release
   before step 2; if it does not, the indexer fetches receipts with
   proofs itself, and the light client covers headers only.
2. **The certificate on L1.** EigenDA's verifier contract addresses and
   the certificate format of its current testnet release decide the
   contract change; pin the release before step 4.
3. **Retention and rebuild.** How far back must a rebuild reach? The
   indexer's archive size follows from it (batches per day at the target
   rate, times the horizon).
4. **The checkpoint.** Who rotates the weak-subjectivity checkpoint, and
   from where (two independent sources, or our own node once).
