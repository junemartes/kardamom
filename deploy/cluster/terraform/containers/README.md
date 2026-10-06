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

## Resources

- `docker_network.this`: `kardamom-net` on the Linux bridge `kardamom-br0`
  with the `subnet` range.
- `docker_image.node`: `kardamom-node:ci` from `../../docker/node.Dockerfile`.
  A change of the Dockerfile rebuilds the image and replaces every node.
- `docker_volume.node`: `kardamom-<node>-docker` and
  `kardamom-<node>-containerd` for the inner Docker engine. A node with a
  generation above 0 uses `kardamom-<node>-<volume>-g<generation>`.
- `docker_container.node`: `kardamom-<node>` at the address the network
  assigns; the contract reads it back. Apply waits for `systemctl is-system-running` to report `running` or
  `degraded` (`ready_timeout`, 180 s).

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
