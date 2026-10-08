# The production profile

This document describes the `production` deployment profile: how jobs find their nodes, how to bootstrap an environment, how an elastic node enrolls, and which host settings the roles apply. The common deploy flow, the recipes and the local profile are in [`README.md`](./README.md).

The infrastructure code of a production deployment is not in this repository. It owns the cloud network, the pools, the enrollment service and the public entry points. This repository holds the Ansible roles, the Nomad jobs and an example inventory (`ansible/inventories/production/`).

## Placement by role set

A node has one class and a set of roles. A job selects its nodes by class or by role.

| Term | Where it is set | Meaning |
|------|-----------------|---------|
| `role` | The inventory (`role=`), or the class name in `node_classes` | The class of the node. It is one value. The node name and the index derive from it. |
| `tier` | The inventory, or `node_classes` | A coarse group (`control`, `sequencer`, `worker`). |
| `node_roles` | The inventory, as a comma list | The class plus the service classes that the node hosts. A node without it hosts its class only. |
| `roles`, `instance_roles` | `node_classes` in `ansible/group_vars/all.yml` (the local profile and the container root) | The service classes that every node of a class hosts, and the service classes that one node hosts by index. |
| `meta.role`, `meta.roles` | Stamped by `roles/nomad` | The Nomad node meta of the class, and of the role set (a comma list). |
| `node_pool` | The inventory or the node user data | The Nomad node pool. Default: `default`. |

How jobs place themselves:

- The pipeline jobs constrain on `meta.role`. These are the sealer (`cluster`), `executor`, `state-mirror`, `ingress`, `sequencer`, `notifier` and `anvil`.
- The service jobs constrain on `meta.roles` with `set_contains`.
- No job names a node or a node index.

| Service job | Role it needs |
|-------------|---------------|
| `redis` primary | `redis-primary` |
| `redis` replica | `redis-replica` |
| `redis` sentinels | `redis` (three nodes) |
| `batcher` | `batcher` |
| `da-proxy`, `da-store`, `l1-fault-proxy` | `batcher` |
| `monitoring` | `monitoring` |
| `validator` | `validator` |
| `l1-light-client` | `validator` |
| `da-watcher` | `da-watcher` |
| `l1-indexer` | `indexer` |

Pools of elastic nodes:

- The `ingress` and `sequencer` jobs have a `node_pool` variable. The workloads role fills it for each job from `workloads_node_pools`, a map from job name to pool id. A job without an entry stays in the `default` pool, with the fixed servers.
- A core job cannot land on an elastic node, because a job selects one pool.
- A node also gets `node_class` (default: its `role`). The Nomad Autoscaler selects its target nodes by `node_class`.

The local profile packs the service classes onto `aux-0` and the ingress nodes. See the table in [`README.md`](./README.md#topology). The production profile gives each service class its own nodes.

### Service classes

The example inventory (`ansible/inventories/production/hosts.example.ini`) has these groups:

| Group | Nodes | Role set |
|-------|-------|----------|
| `control` | 3 (or 1) | `control`. These nodes run the Consul and Nomad servers (`control_plane=true`). |
| `sealer` | 3 | `sealer` |
| `executor` | 3 | `executor` |
| `redis` | 3 | `redis` on all three nodes. `redis-primary` on `redis-0`. `redis-replica` on `redis-1`. |
| `batcher` | 1 | `batcher`. It keeps state on its node. |
| `monitoring` | 1 | `monitoring`. It keeps state on its node. |
| `validator` | 1 | `validator`. It keeps state on its node. |
| `da_watcher` | 1 | `da-watcher` |

- The ingress and sequencer nodes are not in the inventory. They are elastic nodes. See [Elastic first boot](#elastic-first-boot-with-kardamom-enroll).
- The edge is the load balancer of the provider. The ingress servers are its targets.
- The `l1-indexer` job (the L1 follower) runs two instances, each on its own node with the role `indexer`. Add `node_roles=indexer` to two nodes, or a group of two. A deployment with one such node sets `L1_INDEXER_COUNT=1` and loses the second instance's cover. The Aeron archive of each node records `l1_blocks` (`archive_topics_follower`).

### The da-watcher class

The da-watcher has its own node class in the production profile.

- The role `da-watcher` selects the node.
- The Aeron archive of that node records the topic `tx_deposits`. The da-watcher publishes it.
- The `archive_topics` node meta carries this. The ingress nodes record `tx_data`. Each executor node records the `exec_txs` stream of its own executor. Other nodes record nothing. See [`../../docs/aeron-discovery.md`](../../docs/aeron-discovery.md).
- The local profile keeps the da-watcher on `aux-0`.

### One server or three

- `consul_server_expect` and `nomad_server_expect` accept 1 or 3.
- The production profile fails with another value.
- `consul_retry_join` must have at least as many names as `consul_server_expect`.
- One server suits a staging environment without control-plane high availability. Three servers give a voting quorum on the dedicated core.
- The example profile values (`group_vars/production/profile.yml`) use 3.
- Mark as many hosts with `control_plane=true` as the server count. A deployment with one server sets both `*_server_expect` values to 1 and marks one host.

## First bootstrap of an environment

The first bootstrap of an environment runs with placeholder tokens. With the ACLs on, every Consul read is a 403 until the ACL bootstrap. Follow these steps.

1. Copy `ansible/inventories/production/` outside the repository. Fill in the inventory, the vSwitch inputs and the profile values.
2. Create the encrypted vault `group_vars/production/vault.yml` from `secrets.example.yml`. It holds `consul_gossip_key`, `consul_agent_token`, `consul_dns_token` and `nomad_consul_token`. The profile refuses an empty value.
3. Run the first pass with the DNS wait off:
   `ansible-playbook -i <copy> -u root -e consul_wait_for_dns=false ansible/bootstrap.yml`
4. Bootstrap the Consul and Nomad ACLs, and create the real tokens. This repository does not automate this step.
5. Put the real tokens in the vault.
6. Run the playbook again without the override. `consul_wait_for_dns` is `true` by default. The role then waits for the agent to answer its own name (`consul.service.consul`).

Notes:

- `roles/profile` refuses a production run that lacks gossip encryption, ACLs, agent TLS, a DNS token, one or three servers, enough join names, or `private_cidrs`.
- The Nomad HTTP API listens on `127.0.0.1` and on the node address. The RPC and serf ports stay on the node address.
  - The CLI on the node, the ACL bootstrap and the deploy runner use `NOMAD_ADDR=https://127.0.0.1:4646`.
  - The agent certificate names `127.0.0.1`.
- The Consul HTTP and DNS listeners stay on the loopback (`consul_client_addr: "127.0.0.1"`).
- The Consul DNS token (`consul_dns_token`) is the default token of the agent for DNS and tokenless local HTTP queries. It needs only this policy:

```hcl
service_prefix "" { policy = "read" }
node_prefix "" { policy = "read" }
```

- Every production agent needs the DNS token. The local profile has no ACLs and needs none.
- The agents need the TLS material in `consul_tls_dir` and `nomad_tls_dir` (`ca.pem`, `cert.pem`, `key.pem`). The example profile uses `/etc/consul.d/tls` and `/etc/nomad.d/tls`.
- A dedicated host on the vSwitch VLAN gets its address from `vswitch_address` (`roles/vswitch`, `roles/netinfo`). Another host uses `private_interface`. The role fails when the interface has no IPv4 address or more than one.
- `roles/firewall` installs an nftables rule set. It accepts the private ranges (`private_cidrs`), SSH from `ssh_allowed_cidrs`, and the `public_tcp_ports` list. It drops the rest.

After the bootstrap, deploy the workloads. See [Ansible workload deployment](./README.md#ansible-workload-deployment). A production deployment uses signed images. See [Release images](./README.md#release-images).

## Elastic first boot with `kardamom-enroll`

An elastic cloud node has no inventory entry. It configures itself at first boot from the elastic node image.

The image (`roles/elastic_image`) holds:

- The pinned ansible-core and collections, and the playbook release (`/opt/kardamom/ansible`).
- The profile values and the first-boot inventory.
- The enrollment client `kardamom-enroll`, and its argument vector.
- The CA bundle of the environment, `/etc/kardamom/ca.pem`.
- The unit `kardamom-bootstrap.service`.
- No machine identity: the build removes the host keys and the machine id.

The first boot:

1. Cloud-init writes `/etc/kardamom/node.yml` from the user data of the pool. This file enables the unit.
2. `kardamom-bootstrap.service` runs `bootstrap.yml` against the local host. It passes `node.yml` as extra variables.
3. `roles/enroll` runs the enrollment client before any agent starts, and loads the credentials.
4. `roles/profile` checks the node. Then the Consul and Nomad agents start.

The `node.yml` fields:

| Field | Use |
|-------|-----|
| `enroll_endpoints` | The list of enrollment service URLs. The client reads it. |
| `cluster_id` | The cluster of the node. The client sends it. |
| `node_pool` | The pool of the node. The client sends it. |
| `role` | The class of the node. The client sends it. |
| `tier` | Required by `roles/profile`, with `role`. |
| `bootstrap_revision` | Optional. The playbook release that the pool expects. |

- The client also sends the hostname. The service verifies the node by its cloud identity: the private address of the request must belong to a running server of the project with the labels of the pool.
- If `bootstrap_revision` is set, `/opt/kardamom/ansible/RELEASE` must equal it. A mismatch means that the pool points at the wrong image, and the node is refused.
- Nothing in the image is a secret. The image carries only the CA bundle.

The client (`/usr/local/bin/kardamom-enroll`):

- It posts the request as JSON to `<endpoint>/v1/enroll` over TLS. It trusts only `/etc/kardamom/ca.pem`.
- It tries the endpoints in order. An endpoint that does not answer makes it try the next one.
- It writes the TLS material of Consul and Nomad into `/etc/consul.d/tls` and `/etc/nomad.d/tls`.
- It writes `/etc/kardamom/secrets.yml` (mode `0600`) with `consul_gossip_key`, `consul_agent_token`, `consul_dns_token` and `nomad_consul_token`.
- One attempt is bounded by `enroll_timeout_s` (120 s).

| Exit status | Meaning |
|-------------|---------|
| 0 | Every file is in place. |
| 2 | `node.yml` has no `enroll_endpoints`. |
| 3 | An endpoint answered and refused the node. A refusal is final: the client does not try another endpoint. |
| 4 | No endpoint answered. |
| 5 | The answer lacks a credential, a certificate or the CA. |

The node fails closed:

- Any status other than 0 leaves the node without credentials. The play fails, and the agents stay disabled.
- The role also refuses a missing `secrets.yml`, or one with a mode other than `0600`.
- `elastic_image_ca_file` is empty by default. Without a CA bundle, the client cannot verify a service, and the node fails closed. Set it when you build the image.
- An empty `elastic_image_enrollment_argv` means no enrollment. The node then fails at the profile check.
- A failed bootstrap leaves the unit failed and visible in `systemctl`. The unit retries a bounded number of times (60 s apart, at most 5 starts in 30 minutes).
- A failed VM still counts toward the bound of the pool, so a broken image cannot cause unbounded creation.

## Host settings

The `common` role sets these on every host. Some also apply inside a node container.

| Setting | Value | Why |
|---------|-------|-----|
| `fixed_port_block` | `40000-40299` | The block of fixed Aeron ports. `common` reserves it. |
| `net.ipv4.ip_local_reserved_ports` | the `fixed_port_block` value | It takes the block out of the ephemeral range. The file is `/etc/sysctl.d/99-kardamom-ports.conf`. |
| `net.core.rmem_max`, `wmem_max`, `rmem_default`, `wmem_default` | `16777216` (16 MiB) | Large socket buffers for Aeron. The file is `/etc/sysctl.d/99-kardamom-aeron.conf`. |
| `aeron.mtu.length` | `1344` | The Aeron MTU of the media driver and of the sealer JVM. |

The fixed port block:

- The Aeron ports of the cluster lie in the ephemeral range of Linux (32768 to 60999). An outbound socket can take one of them first. The service that owns the port then fails to bind and crash-loops.
- The block holds the multicast fallback channel ports (from 40000, see `config/channels.toml.tpl`) and the sealer ports `cluster_ports` (40200 to 40205).
- `ansible/tests/test_ports.py` fails when a fixed port in the ephemeral range lies outside the block.

The socket buffers:

- `net.core.*mem_*` is not namespaced on a kernel before 6.11. A node container cannot set it.
- The role skips the task when `kardamom_in_container` is true, or when `/proc/sys/net/core/rmem_max` does not exist. In a container, the host machine sets the values.

The MTU:

- The vSwitch VLAN path has an MTU of 1400 bytes. The Aeron default MTU is 1408, which fragments or drops on that path.
- 1400, minus 20 bytes of IP header and 8 bytes of UDP header, is 1372. The largest multiple of 32 below 1372 is 1344.
- Both `aeron.system.nomad.hcl` and `cluster.nomad.hcl` set `-Daeron.mtu.length=1344`. The value also fits a Docker bridge, a cloud network and the loopback.

Related sections of [`README.md`](./README.md): [Aeron-in-Docker](./README.md#aeron-in-docker) and [Deployment profiles](./README.md#deployment-profiles).
