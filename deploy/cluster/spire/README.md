# SPIRE artifacts (attested-identity plan, P1)

Identity issued against *what is running*: a SPIRE server + per-node agents
issue short-lived SVIDs only to workloads whose docker image digest and
Nomad task identity match a registration entry generated from the deploy's
digest manifest (P0.1). After this, "this peer has a valid cert" means "a
workload matching {node, image digest, Nomad task} held this key minutes
ago" — see `docs/specs/attested-identity-plan.md`.

**These are deployable artifacts, not part of the default deploy.**
`deploy.sh` does not touch them; nothing in the cluster depends on them
yet. Turning SPIRE on — and later pointing consumers at the Workload API —
is an operator action, done deliberately and observably.

Files:

| file | what |
|---|---|
| `spire-server.nomad.hcl` | server (service job, control node, digest-pinned image, sqlite datastore on `/opt/spire/data`) |
| `spire-agent.nomad.hcl`  | agents (system job, all non-control nodes, join-token node attestation — `TODO(P2: TPM)` — docker + unix workload attestors) |
| `register.sh`            | registration entries per kardamom service: selectors on image digest (from `images.digests`) + Nomad task labels |
| `export-trust-bundle.sh` | trust-bundle (PEM) export; `--distribute` stages agent bootstrap certs on every node |

## Bring-up order (runbook)

0. **One-time client prerequisite**: the docker attestor selects on Nomad's
   task labels; the clients' docker plugin config must stamp them
   (`extra_labels = ["job_name", "task_name"]` in the Nomad client config —
   an ansible change, not included here). Also create the host dirs on the
   relevant nodes: `/opt/spire/data` (control: server datastore; workers:
   agent state) and `/opt/spire/sockets` (workers: Workload API socket).
1. **Server**: `nomad job run spire/spire-server.nomad.hcl`; wait until its
   alloc is running (`nomad job status spire-server`).
2. **Trust bundle**: `spire/export-trust-bundle.sh --distribute` — stages
   `/opt/spire/bootstrap.crt` on every workload node.
3. **Agents**: join tokens are SINGLE-USE, one per node, all generated for
   the same agent identity so `register.sh` needs one parent:
   ```
   spire-server token generate -spiffeID spiffe://kardamom.internal/agent/kardamom-node
   ```
   (run inside the server container, once per node), then
   `nomad job run -var join_token=<token> spire/spire-agent.nomad.hcl` per
   token window. Yes, this is clumsy for a fleet — deliberately accepted
   for P1's operator-driven first rollout; the P2 TPM node attestor
   (`TODO(P2: TPM)` in the agent job) replaces the ceremony entirely and
   gates *renewal* on verified host quotes.
4. **Registration**: after any pinned deploy,
   `spire/register.sh` (add `--dry-run` first; `--resolve-image-id
   executor-0` if the deployed SPIRE reports local image config ids — see
   the script header). Re-run after every deploy that changes digests.
5. **Verify**: from a workload node,
   `spire-agent api fetch x509 -socketPath /opt/spire/sockets/agent.sock`
   inside (or as) a registered task returns an SVID for
   `spiffe://kardamom.internal/svc/<name>`; an unregistered process gets
   nothing.

## First consumers (in order — from the plan)

1. **Interop feed WS endpoints**: server certs minted from the validator's
   SVID; peer chains verify against our exported trust bundle. This is the
   first place a *peer* benefits: the cert they check stops meaning "has a
   key" and starts meaning "is the pinned build".
2. Validator↔validator attestation gossip (when interop spec P2 lands).
3. Operator tooling (mTLS to admin endpoints).

Aeron cluster traffic stays out of scope: its integrity story is Raft +
re-execution (interop spec §10), and its latency budget does not want a
proxy in the path.

## Notes

- **SVID TTLs are short (1h x509 / 5m JWT)** and rotation is SPIRE's job;
  nothing here hands out long-lived credentials.
- **The server is a trust root** (plan table, P1 row): whoever controls the
  control node can mint identities. That is the same trust class as the
  registry + deploy pipeline it joins (P0); P2 moves node trust to host
  TPMs, P3 moves key custody into enclaves.
- Removing SPIRE is `nomad job stop spire-agent spire-server` — consumers
  fall back to whatever they used before, because none exist yet.
