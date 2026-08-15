# Attested identity and pod-integrity plan

Status: PLAN v1 — 2026-08-15
Companion to `docs/specs/interop-outbox-messaging-spec.md` (§10 postures, §12
registry): interop lets a *peer chain* verify us by re-execution; this plan is
about verifying **our own services** — and eventually proving to peers that the
keys they trust live inside software we can name.

## What a certificate must come to mean

Plain mTLS proves possession of a key; a fully compromised service presents its
valid certificate happily. Each phase below strengthens what "this peer has a
valid cert" implies:

| Phase | A valid credential implies | Trust root |
|---|---|---|
| P0 | the running image is the digest the deploy pinned | our registry + deploy pipeline |
| P1 (SPIRE) | a workload matching {node, image digest, Nomad task} held this key minutes ago | SPIRE server + agent |
| P2 (TPM/Keylime) | …AND its host booted measured firmware/kernel and IMA-measured files match the allowlist, checked continuously | host TPMs (manufacturer EK certs) |
| P3 (TEE/RA-TLS) | …AND the TLS key exists only inside a CPU-measured enclave running build M | AMD/Intel attestation chains |
| zk endgame | the *output* proves itself; machine integrity stops mattering for chain state | proof system |

Standing limits, phase-independent: attestation proves what was **loaded**, not
that it has not since been exploited through its own legitimate interfaces;
freshness comes from short-lived credentials re-issued under re-attestation;
and every measurement is meaningless without **reproducible builds** mapping it
to a reviewed commit — reproducibility is the semantic link, not an
optimization (cross-cutting workstream below).

## P0 — pin what runs, sweep what changed (this branch)

1. **Digest pinning through the deploy pipeline.** Jobs currently run mutable
   tags (`192.168.56.10:5000/kardamom-*:dev`) with force-pull — anyone who can
   push to the registry changes what the next restart runs, silently. Change:
   the deploy scripts capture each image's registry digest at push time and
   pass it through the jobs' existing HCL variable system; task stanzas run
   `repo@sha256:…`. `force_pull` becomes redundant-but-harmless (digests are
   immutable). The deploy log becomes an audit record: what ran is exactly
   what was deployed, checkable later with one `docker inspect` per task.
2. **Integrity sweep tooling** (`deploy/cluster/scripts/integrity/`):
   - `image-drift.sh` — for every kardamom allocation on every node: running
     image digest vs the deployed digest; any mismatch is a finding.
   - `fs-drift.sh` — `docker diff` per container, filtered against each job's
     declared writable paths; output should be empty. Noisy paths found during
     rollout get either mounted writable explicitly or fixed.
   - `egress-inventory.sh` — snapshot live connections per task and diff
     against the expected-peer set derivable from `channels.toml` + job
     configs (Aeron endpoints, L1 RPC, registry, metrics scrapes). Inventory
     first; enforcement (per-task egress rules) after the inventory is quiet.
3. **Read-only rootfs**: attempted per job (`readonly_rootfs = true` +
   explicit writable mounts), validated by the cluster-e2e suite; any job that
   genuinely needs a writable root gets documented instead of forced.

## P1 — SPIRE: identity issued against what is running

Artifacts land in this branch (`deploy/cluster/spire/`); turning them on is an
operator action.

- SPIRE server as a Nomad service job; SPIRE agents as a system job on every
  client node; join-token or TPM-based node attestation (upgrade path to P2).
- Registration entries per kardamom service with selectors on **docker image
  digest** + Nomad task identity, so SVIDs are issued only to workloads
  running the pinned build. Short TTLs; rotation is SPIRE's job.
- First consumers, in order: (1) the interop feed WS endpoints (server certs
  from SVIDs, peers verify against our trust bundle); (2) validator↔validator
  attestation gossip when P2 of the interop spec lands; (3) operator tooling.
  Aeron cluster traffic stays out of scope (its integrity story is Raft +
  §10 re-execution, and its latency budget does not want a proxy).

## P2 — host attestation: TPM + Keylime

- Measured boot on all cluster hosts; IMA policy measuring the binaries that
  matter (nomad, docker/containerd, kernel modules); Keylime verifier off the
  cluster, agents on each host; continuous quote checking with nonces.
- Gate credential *renewal* on verified quotes: SPIRE node attestation moves
  from join tokens to TPM identity, so a host that fails appraisal stops
  being able to renew its workloads' SVIDs — quarantine by expiry, no active
  kill needed.
- Allowlists generated from the reproducible-build pipeline, never curated by
  hand (hand-curated allowlists are where these deployments rot).

## P3 — TEE + RA-TLS for the keys peers trust

Hardware-gated (SEV-SNP or TDX capable hosts): run `kardamom-validator` — or
at minimum its signing side — inside a measured VM enclave; TLS keys for the
interop feed and the P2 attestation signing keys generated inside it; RA-TLS
embeds the attestation report in the certificate so a *peer chain* verifying
our feed proves the key exists only inside build M. Registry hook (interop
spec §12/§16): peer records gain optional `measurements` — when present, a
peer's attestation quorum means "Q keys that provably live inside named
builds", hardening posture B from operator-trust toward hardware-trust.

## Cross-cutting: reproducible builds

- Rust: pin toolchain, `--locked`, strip build paths (`--remap-path-prefix`);
  verify by double-build in CI comparing binary hashes.
- Java fat-jar: normalize timestamps/entry order (Gradle `reproducibleFileOrder`
  + `preserveFileTimestamps=false`).
- Images: deterministic Dockerfiles (pinned base digests, no `apt-get update`
  at build time without a snapshot mirror), double-build digest comparison in
  CI.
- Deliverable: CI job asserting build-twice-compare on every release artifact;
  its output hashes are the P2 allowlist and the P3 measurement inputs.

## Sequencing and effort

P0: days (this branch). P1: days of artifact work + an operator rollout
window. P2: 1–2 weeks incl. host enrolment. P3: hardware procurement decision
first. Each phase is independently useful; none blocks the interop series —
P3 only *strengthens* interop posture B, which ships without it.
