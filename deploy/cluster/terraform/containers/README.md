# terraform/containers

The container substrate of the local deployment profile: one privileged
systemd and Docker-in-Docker container per node of the node-class model.

## Inputs

The root reads `node_classes` from `../../ansible/group_vars/all.yml`
(`contract_file`). The model stays in one place; `ansible/contract.yml`
checks that this root reads it. The network range is the `subnet` variable.
Each node gets a stable address from it, in name order from `address_offset`
(host 10), so a node keeps its address across a container restart. This root
is the one place with an address plan; nothing else names an address.

A class in `node_classes` has `count` and `tier`. It can also have `roles`
and `instance_roles`. The root turns them into the role set of each node.

| Variable | Default | Meaning |
|----------|---------|---------|
| `node_generation` | `{}` | The replacement generation of a node, by name. A higher generation moves the node past every generation-0 address and renames its volumes, so the node starts with empty disks. |
| `address_offset` | `10` | The host number of the first node address. It must be 2 or more. |
| `ready_timeout` | `180` | The seconds to wait for systemd in a node. |
| `prebuilt_node_image` | `""` | An image already loaded on the Docker host. Empty keeps the local image build. |

## Resources

- `docker_network.this`: `kardamom-net` on the Linux bridge `kardamom-br0`
  with the `subnet` range.
- `docker_image.node[0]`: `kardamom-node:ci` from `../../docker/node.Dockerfile` when `prebuilt_node_image` is empty. A moved block preserves existing image state.
- `data.docker_image.prepared[0]`: the already-loaded image when `prebuilt_node_image` is set. This path performs no image build or registry pull. A different image ID replaces the node containers.
- `docker_volume.node`: `kardamom-<node>-docker` and
  `kardamom-<node>-containerd` for the inner Docker engine. A node with a
  generation above 0 uses `kardamom-<node>-<volume>-g<generation>`.
- `docker_container.node`: `kardamom-<node>` at the address the network
  assigns; the contract reads it back. Apply waits for `systemctl is-system-running` to report `running` or
  `degraded` (`ready_timeout`, 180 s).

## Shared CI node image

The `node-image` job builds `docker/node-prepared.Dockerfile` once and shares its saved image with every chaos shard. BuildKit caches its layers. The shard checks the source revision label, loads the image and sets `TF_VAR_prebuilt_node_image`. Node replacement reuses that loaded image too.

`ansible/node-image.yml` invokes the same package, Docker, Consul and Nomad installation tasks as ordinary bootstrap. It records the versions, architecture and task hashes in `/etc/kardamom-node-software.json`. Bootstrap skips installation only when that contract matches. A stale marker fails with a rebuild instruction. An ordinary image without a marker keeps the installation path.

Image preparation starts no cluster service and bakes no node address, resolver, membership or credentials. Runtime bootstrap still configures Docker, Consul and Nomad, sets up directories and mounts, and performs readiness checks. `ansible/node-image-check.yml` verifies the prepared software with network access disabled. Workload images and deployment are separate from node-software preparation; this image alone does not establish the two-minute startup target.

## Output

`node_contract` (version 1): the network, the image and one entry per node.
`ansible/containers.yml` reads it from `node-contract.json`.

| Field of a node | Meaning |
|-----------------|---------|
| `name` | The node name, `<class>-<index>`. |
| `container` | The container name, `kardamom-<name>`. |
| `role` | The class of the node. |
| `tier` | The tier of the class. |
| `roles` | The role set: the class, the `roles` of the class and the `instance_roles` of this index. |
| `index` | The 0-based index of the node in its class. |
| `control_plane` | `true` for the `control` class. |
| `ip` | The address that the network assigned, read back from the container. |
| `generation` | The replacement generation of the node. |

## Use

`just container-up` in `deploy/cluster` runs `init`, `apply`, writes the
contract and converges the cluster. `just container-down` runs `destroy`.
Do not run `tofu apply` while a `just container-up` is in progress; the
state lock refuses it.

## Tests

```sh
tofu test
```

The tests use a mocked provider and a small model under `tests/`. They check
the instance naming, the container names, the read-back addresses and the
health check.
