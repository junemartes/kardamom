# Hetzner hybrid infrastructure: dedicated core and elastic Cloud nodes

- Status: Proposed; specification only.
- Date: 2026-09-11
- Baseline: `main` at `fa6d04b9`.
- Decision: Hetzner dedicated servers host the core; Hetzner Cloud VMs host
  elastic ingress and sequencer pools.
- Dependency: [dynamic MDC and Consul discovery, PR #277](https://github.com/junemartes/kardamom/pull/277).
  Transport implementation belongs to the separate MDC session. Rebase the
  infrastructure implementation onto it before enabling application deployment.

## Problem and intended result

The current deployment couples machine ordinals, fixed IP lanes, application
configuration and several shell entry points. Increasing capacity requires
coordinating changes across inventory, jobs and peer lists. This makes node
replacement and autoscaling harder than they need to be.

Use Terraform for provider resources, Ansible for machine configuration and
deployment, Consul for service discovery, and Nomad Autoscaler for elastic
capacity. A new ingress or sequencer VM must join with a provider-assigned private
address, configure itself from a pinned Ansible release, and become eligible
without editing application peers. Removing it must preserve lane coverage,
accepted-work semantics and required archive data.

The existing Ansible implementation branch carries the shell-to-playbook
migration; this spec defines its destination and the remaining integration work.
No production rollout or completion of that migration is implied by this PR.

## Placement and availability

| Component | Placement | Scaling authority |
| --- | --- | --- |
| Consul and Nomad servers | Dedicated core; three voting servers for each control plane | Explicit infrastructure change |
| Aeron Cluster sealers | Three fixed members on distinct dedicated machines | Existing membership procedure |
| Executors, retained archives, validator, batcher and DA/L1 support services | Dedicated core with persistent storage | Explicit workload/capacity change |
| Ingress | Hetzner Cloud pool, at least two healthy replicas | Application policy plus node-capacity policy |
| Sequencers | Hetzner Cloud pool, at least two nodes and two racing replicas per active lane | Node-capacity policy; lane resizing remains a separate operation |
| Autoscaler, metrics and RPC proxies | Dedicated core, outside elastic pool deletion selectors | Explicit platform deployment |

These are placement classes, not necessarily one physical machine per row.
Control-plane colocation requires reserved resources and independent voting
members across physical hosts. CPU and I/O saturation in an executor must not
starve a colocated control-plane quorum. Keep core data directories persistent;
neither an elastic policy nor local test teardown may select these hosts.

Choose a Cloud location near the dedicated core and measure the actual private
path before setting production limits. A shared network zone does not establish
a latency budget or independent rack failure domains. Initial HA covers tested
node failures within this deployment; regional disaster recovery is separate.

## Ownership: one writer for each resource

| Owner | Responsibility |
| --- | --- |
| Terraform | Cloud Network, cloud/vSwitch subnets, placement groups, public firewall resources, SSH-key references, RPC edge resources, bootstrap DNS through its authoritative provider, and pool definitions/bounds |
| Ansible | Dedicated host networking, OS/packages, host firewall, Docker, Consul/Nomad agents, image preparation, signed application image publication, Nomad job deployment, diagnostics and local lifecycle |
| Nomad | Allocation placement/replacement, service checks, application rollout and drain execution |
| Nomad Autoscaler | Elastic VM creation/deletion through the Hetzner target; ingress allocation count through a distinct application policy |
| Application runtime | Dynamic MDC endpoint registration/reconciliation, application readiness, accepted-work handling and archive coverage |

Hetzner's selected target creates/deletes individual VMs. Terraform must **not**
also manage those VMs with `hcloud_server.count` or `for_each`: it owns the
resources and policy inputs around each pool, while the Autoscaler owns its
runtime instances. Initial minimum capacity must use that same capacity owner,
not temporary Terraform instances followed by state surgery. Disabling the
Autoscaler leaves existing VMs in place; removing a pool definition must not
implicitly delete them.

Dedicated servers and their existing Robot vSwitch are inventory inputs in the
first implementation. Terraform attaches the vSwitch to the Cloud Network;
Ansible configures its host interfaces. Purchasing/reinstalling dedicated
servers and taking ownership of existing Robot resources are separate explicit
changes, not prerequisites hidden in an elastic deployment.

Keep provider-specific Terraform roots and Autoscaler targets behind the same
bootstrap contract. An AWS adapter can later supply ASGs and its native target;
Terraform then owns ASG configuration/bounds while leaving desired capacity to
the Autoscaler. The shared Ansible roles and jobs must also work on owned
machines. This PR chooses Hetzner as the first adapter, without making the
application depend on Hetzner APIs or claiming an AWS adapter already exists.

## Private network and discovery

### Cloud-to-dedicated connectivity

Create one Hetzner Cloud Network, a cloud subnet for elastic VMs, and a separate
`vswitch` subnet connected to the dedicated core's vSwitch. Validate containment,
non-overlap and address capacity. Leave elastic VM addresses to the provider.
On dedicated hosts, Ansible renders the VLAN, its allocated address and the route
to the Cloud Network. The address allocation belongs to infrastructure/IPAM and
DNS, not an application ordinal-to-IP formula.

Hetzner documents this connection and the dedicated VLAN's MTU of 1400 in
[Connect Dedicated Servers](https://docs.hetzner.com/networking/networks/connect-dedi-vswitch/).
Validate the effective MTU across the full path, set Aeron datagram sizing to fit
that path, and test loss/retransmits without relying on IP fragmentation. Support
the dynamic MDC control, receive and replay ports in both directions. Opening
only publication control ports is insufficient.

Cloud Firewalls protect the public interface; they do not currently filter
private Network traffic. Configure the private host firewall through Ansible,
including the Docker/host-networking interaction. Consul/Nomad management ports
and Aeron UDP must not be publicly exposed. See the
[Hetzner firewall FAQ](https://docs.hetzner.com/cloud/firewalls/faq/).

### Bootstrap and service lookup

Consul bootstrap uses infrastructure DNS names resolving the dedicated Consul
servers' private addresses. That DNS must work before any Consul agent starts;
do not bootstrap Consul through `*.service.consul`. Terraform manages records
where the authoritative DNS provider supports it; otherwise require an explicit
external DNS contract and validate it before provisioning clients.

Each host runs its own Consul agent. Configure Nomad to advertise and discover
its servers through that local agent, with explicit `auto_advertise`,
`server_auto_join` and `client_auto_join`; remove static `client.servers` lists.
Scope Nomad's Consul service names to the deployment so two clusters cannot
accidentally join. The local-agent requirement is documented in
[Nomad's Consul integration](https://developer.hashicorp.com/nomad/docs/configuration/consul).

Applications receive cluster/chain identity, local Consul access, an unambiguous
private interface selector and allocated ports. Resolve the current private
address at runtime; fail if the selector is missing or ambiguous. Infrastructure
may allocate addresses and Consul records necessarily contain resolved addresses.
Source-controlled application peer lists, fixed interface subnets and generated
ordinal-to-IP mappings are prohibited in the deployment profile.

Use PR #277's version-1 records: `kardamom-mdc-publisher`,
`kardamom-aeron-archive` and `kardamom-cluster-member`. Every migrated publication
uses **dynamic MDC** (`control-mode=dynamic`). Consumers discover all relevant
publishers; Consul DNS round-robin is insufficient for that fan-in. Keep the
runtime's last known membership on discovery errors, and preserve its readiness
and recovery rules. Infrastructure does not introduce another watcher or relay.

The same removal of peer lists applies to executor nonce queries, archive
refetch, L1/DA services and RPC backends. Discovering Aeron Cluster endpoints does
not change its fixed member IDs or voting set.

### Public RPC edge

Replace the singleton proxy's embedded ingress addresses with at least two
Consul-aware proxy allocations on distinct core hosts. A provider load balancer
provides the stable public entry point; its proxy targets come from infrastructure
inventory, while each proxy resolves healthy ingress services through Consul.
Use supported Nomad/Consul templating or proxy integration, not shell polling.
Retain the last valid configuration on lookup failure and define empty-membership
behavior explicitly. Confirm HTTP and WebSocket readiness/drain behavior; a TCP
accept alone does not establish recording or application readiness.

## Ansible bootstrap and deployment contract

Proposed entry points share roles across dedicated, Cloud and local environments:

```text
deploy/cluster/
  terraform/hetzner/          network, vSwitch attachment, edge and pool outputs
  ansible/bootstrap.yml       configure a host and join the substrate
  ansible/image.yml           prepare a clean, versioned elastic-node image
  ansible/deploy.yml          validate and converge Nomad workloads
  ansible/autoscaler.yml      install/configure the pinned target and policies
  ansible/run.yml             local cluster lifecycle and test entry point
```

Names above describe the intended interface, not files already shipped on main.
Provider roots may be split further without duplicating shared roles.

The bootstrap input contract contains:

| Input | Meaning |
| --- | --- |
| `cluster_id`, `chain_id`, `datacenter`, Nomad region | Deployment/discovery isolation; datacenter names must agree with jobs |
| Role, tier and capacity-pool identity | Placement and scaling scope; never a lane assignment |
| Private interface selector and network identity | Select this host's current private address; never guess from the default public route |
| Consul bootstrap DNS names and expected server quorum | Join discovery independently of application service records |
| Server/client flags | Elastic machines are clients; only the dedicated control-plane inventory enables servers |
| Image/release revision and registry identity | Immutable bootstrap and application artifact selection |
| Credentials by protected file/reference | Agent enrollment, discovery and registry access; no tokens in labels or logs |
| Port allocation contract and storage paths | Reachable UDP sockets and persistent archive/state locations |

Prepare an immutable node image using Ansible, optionally through a Packer
Ansible provisioner. It contains pinned tools, collections and the playbook
release. An image must contain no Consul/Nomad node IDs, runtime data, private
keys, enrollment secrets or cached machine identity.

At first boot, declarative cloud-init writes nonsecret instance inputs and
enables a systemd unit that invokes `ansible-playbook` directly, after network
configuration is ready. The unit retries boundedly and reports bootstrap
failure. It does not run a shell deployment wrapper or pull a floating Git
branch. The host's OS hostname must retain the unique Cloud server name because
the selected target maps Nomad `unique.hostname` to that name.

Bootstrap obtains per-node credentials through the deployment's authenticated
enrollment mechanism before enabling agents. The implementation must document
and test that mechanism; it cannot depend on a running local Consul agent to
obtain the credentials required to start that agent. Keep the Hetzner API token
on the Autoscaler host, outside Terraform state and VM user-data. Production
requires Consul/Nomad ACLs, agent TLS and gossip configuration, with secrets
injected through the established secret store and protected Ansible inputs.

Ansible discovers real CPU/memory capacity on production machines. The local
Docker CPU override, insecure registry and Vagrant user are local-profile
settings. Do not apply them to Cloud or dedicated hosts. Node eligibility must
wait for correct interface selection, agent configuration, writable required
storage and the host prerequisites for Aeron (directories, mounts and buffers).
Allow required Nomad system jobs to start before gating application admission;
waiting for those jobs before enabling the Nomad client would deadlock bootstrap.
Job readiness adds transport, recording and application checks; a running VM is
not usable capacity.

Native Ansible modules and typed HTTP API calls own deployment mutations. CLI
invocation is acceptable for a tool with no suitable module (for example HCL
compilation or a validator), using argument arrays and explicit change/error
semantics. Do not use Terraform `remote-exec`, `local-exec`, shell `user_data`,
curl-to-shell installers or a script that wraps the old deployment sequence.
Load/chaos test scripts can remain test tools. Pure job renderers and contract
validators can remain; migrate `scale-sequencers.sh` orchestration to Ansible
while retaining the existing resize protocol.

## Elastic pool contract and Autoscaler integration

Terraform exports a versioned, nonsecret pool description consumed by Ansible:
provider/network/firewall/placement IDs, image ID and bootstrap revision,
location/server type, SSH-key references, unique pool label, Nomad node class,
role/tier, minimum/maximum capacity and discovery inputs. Ansible renders
Autoscaler configuration from these outputs; do not duplicate their values in
hand-maintained HCL. Record applied policy revision for audit and rollback.

Use separate ingress and sequencer pools. A pool ID includes environment and
cluster identity, and maps one-to-one to a Nomad `node_class` and the target's
`group-id` label. A selector must never match core nodes or another deployment.
Record cloud identity separately from lane IDs and allocation ordinals.

The initial target candidate is the community
[`hcloud-server` plugin](https://github.com/AndrewChubatiuk/nomad-hcloud-autoscaler),
listed among [Nomad's external plugins](https://developer.hashicorp.com/nomad/tools/autoscaling/plugins/external).
Pin a reviewed revision and artifact checksum, including Nomad/Autoscaler
compatibility. At inspected revision `9fff034a224511eb4a535ff0cfbf95577446e3d7`,
it selects running servers by labels, maps node identity through hostname and
uses Nomad pre-scale-in tasks before deleting servers. Its
[deletion path](https://github.com/AndrewChubatiuk/nomad-hcloud-autoscaler/blob/9fff034a224511eb4a535ff0cfbf95577446e3d7/plugin/hcloud.go)
logs deletion errors and proceeds to post-scale-in processing. This is a
candidate, not an approved production dependency: require verified deletion,
correct handling of non-running/in-flight VMs and bounded partial-failure
reconciliation before adoption. Patch or replace the target if necessary.

Use spread placement groups for elastic replicas. Hetzner currently limits a
spread group to ten servers and guarantees different physical hosts within
that group; this does not establish rack diversity. Initial pool maxima must
fit that limit, including rollout headroom. Expansion beyond it needs explicit
additional pool/placement policy rather than silently dropping anti-affinity.
See [Hetzner placement groups](https://docs.hetzner.com/cloud/placement-groups/overview/).

### Three distinct changes

1. **Ingress allocation count:** an application policy requests more/fewer
   replicas from measured admission load, pending work and latency. Keep at
   least two ready replicas, distinct-host placement and the existing collision-
   free ingress identity contract. Scale-in uses the drain rules below.
2. **Node capacity:** pool policies supply CPU/memory/placement headroom for
   allocations. Include pending allocations, bootstrap delay and reserved Aeron
   resources. Metrics must be scoped to each pool. Growth cannot resolve an
   invalid constraint or an unavailable image; detect those separately.
3. **Active sequencer lanes:** preserve the implemented
   [dynamic sequencer sizing protocol](../specs/dynamic-sequencer-sizing.md).
   Its overlap/steady phases and vslot ownership are an explicit Ansible
   operation. Each active lane retains two racing replicas on distinct hosts.
   Node scaling must not change `partition_count`, the shard map, or `seq-<lane>`
   group counts. Automatic lane resizing is outside this first implementation.

An added idle client is not evidence of higher sequencer throughput. Verify
that pending allocations can place or use a controlled rollout to redistribute
existing lane replicas onto added capacity, preserving their counts and coverage.
If more active lanes are required, run the separate resize operation. Expose
both available node capacity and useful application capacity in scaling metrics.

Enable policies only after measuring real CPU, NIC/fanout, pending-work and
latency signals. Missing/stale metrics must not mean zero load. Use bounded
steps, minimum/maximum limits, hysteresis and cooldown covering observed
bootstrap/convergence time. One active capacity controller owns each pool;
failover must fence the previous writer before mutations. Account for pending,
failed and draining machines so retries cannot exceed bounds or leak VMs.

Terraform apply preserves runtime VM counts. Ansible job redeployment must also
preserve the current Autoscaler-owned ingress count: merge it from Nomad using
the same job version checked during registration, retrying boundedly on a
concurrent scale/update. On first deployment only, use the declared initial
count. Intentional count/bounds changes must be explicit. Never apply this
preservation rule to the lane counts owned by the sequencer resize operation.

## Drain, archives and deletion

Ingress is not disposable merely because its application can be restarted:
current deployments record transaction data on ingress hosts. An Aeron system
job or a host-mounted archive can outlive the ingress allocation, and deleting
the VM removes that storage.

Node scale-in must implement this state machine with observable outcomes:

1. Select a node in exactly one elastic pool, respecting minimum healthy capacity
   and the placement required for two replicas of every active sequencer lane.
2. Withdraw new RPC admission as appropriate and mark the node ineligible for new
   allocations. Allow accepted work to complete or follow the existing explicit
   expiry/retry policy. Keep services needed for drain/recovery reachable.
3. Converge replacement allocations and verify application readiness and lane
   coverage. For an ingress allocation reduction without replacement, verify the
   remaining admission capacity and the same archive requirements.
4. Prove that every recording range still required for refetch/recovery is
   accessible on surviving durable storage, including remote publisher images.
   Verify archive identity, session and position coverage; catalog health or a
   local spy recording alone is insufficient. Retain the old archive if proof
   fails. Dedicated archives are a placement objective, not assumed coverage.
5. Stop/deregister the remaining allocation and archive endpoints in the
   transport-defined order, then delete the VM and confirm provider completion.
   Only then purge its Nomad node and stale infrastructure registrations.

A drain deadline is an alert/failure boundary, not permission to destroy
required data. Ordinary Nomad drain, `kill_timeout` and
`node_drain_ignore_system_jobs` do not establish archive safety. Integrate the
proof into the deletion path through a tested target/runtime API contract; do
not implement it as an external shell pre-delete hook. If the target cannot
enforce this contract, keep automatic scale-in disabled.

Initial rollout enables bounded scale-out after substrate tests. Enable ingress
allocation reduction and VM scale-in independently only after their respective
drain tests pass. Sequencer node removal must also preserve the existing rejoin
semantics; it must not replay old refs indiscriminately or derive lanes from
   ephemeral hostnames.

## Migration and rollback

1. Finish the Ansible lifecycle/image/workload migration and move remaining
   deployment mutations out of scripts. Keep local Docker tests reproducible.
2. Add the shared host bootstrap roles, explicit production profile and Consul-
   based Nomad join. Test nodes with changed addresses and no static peer list.
3. Add the Hetzner Terraform root, DNS/vSwitch inputs, image pipeline and pool
   outputs. Validate plans without purchasing/reinstalling core servers.
4. Test the target against provider/API doubles, then an isolated Cloud pool:
   minimum bootstrap, growth, failure reconciliation and non-destructive drain.
5. Rebase on the MDC implementation, consume its final config schema and signed
   images, and remove active multicast/peer-IP templates from the new profile.
   A preflight gate must reject old binaries or a missing discovery schema;
   substrate-only tests must not be reported as a working application cluster.
6. Run application correctness and latency tests across the Cloud/vSwitch path;
   enable bounded scale-out, then the independently proven scale-in paths.

Keep development topology compatible during migration, but do not carry its
fixed address lanes into the production profile. Avoid two permanent deploy
implementations: once callers use Ansible, delete superseded deployment scripts
and update Make, CI and documentation to the same entry points.

Roll back by disabling scaling decisions, preserving healthy machines and
required recordings, and redeploying a known-good compatible image/policy
revision through Ansible. Do not use `terraform destroy` as a rollout rollback.
The first multicast-to-MDC cutover is coordinated with the transport migration;
mixed-mode rolling compatibility and old archive URI compatibility must not be
assumed. Document the archive/checkpoint restore procedure before production use.

## Acceptance evidence

- Terraform format/validation and mocked plans verify subnet/vSwitch resources,
  isolated selectors, bounds/placement limits, and no Terraform-owned elastic
  instance count. State and user-data contain no provider/enrollment credentials.
- Ansible syntax/lint and isolated role tests verify rendering, interface
  selection, restart/change detection and failure propagation. A second converge
  is idempotent. No shell deployment path is required by Make or CI.
- Real Docker substrate tests use arbitrary assigned addresses and infrastructure
  DNS. Clients join through local Consul, survive a server restart and reconnect
  after a node address changes, without application peer edits.
- An isolated Hetzner Cloud/dedicated test verifies VLAN routes, MTU, bidirectional
  MDC/replay traffic, host firewall rules and replacement bootstrap. Docker tests
  alone do not establish vSwitch or provider behavior.
- A fresh elastic image joins with unique machine/agent identities, correct role
  and real resource fingerprints. Invalid discovery/interface/credentials fail
  closed; broken bootstrap is visible and cannot cause unbounded VM creation.
- Policy tests cover missing metrics, capacity errors, in-flight/stopped VMs,
  stale labels, duplicate requests, controller failover, delete errors and
  cross-cluster isolation. No failed deletion is reported as reclaimed capacity.
- Concurrent deployment and ingress scaling preserve the current intended count;
  conflict retries cannot overwrite a lane resize or reintroduce an old shard map.
- Under traffic, node growth and drain preserve two racing replicas per active
  lane and all existing ordering, nonce, receipt and refetch correctness gates.
  Ingress removal with the last required archive copy is refused, including
  timeout cases and surviving Aeron system jobs.
- Consul/API/metrics outages preserve established data-plane connections and
  suppress unsafe scaling. Core quorum/member identities never enter elastic
  deletion selectors. Loss of one tested core host preserves quorum.
- Measure p50/p99/p99.9 latency, sustainable throughput, publisher CPU/NIC usage,
  retransmits, recovery lag and startup/drain time on the actual chosen hardware.
  Use those results to set pool limits and policy thresholds; no HFT latency
  guarantee follows solely from choosing dedicated machines or dynamic MDC.

## Deployment-specific inputs still required

Implementation can proceed against this contract. Applying it needs the actual
dedicated inventory and vSwitch ID, Cloud project/location, network/IPAM ranges,
bootstrap DNS zone, enrollment/secret-store integration, image registry and
measured machine sizes/capacity bounds. These are environment inputs, not a
reason to reintroduce hardcoded application endpoints or deployment scripts.
