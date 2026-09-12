# terraform/containers

The container substrate of the local deployment profile: one privileged
systemd and Docker-in-Docker container per node of the node-class model.

## Inputs

The root reads `node_classes` from `../../ansible/group_vars/all.yml`
(`contract_file`). The model stays in one place; `scripts/check-contract.py`
checks that this root reads it. The network range is the `subnet` variable;
Docker assigns every node address from it.

## Resources

- `docker_network.this`: `kardamom-net` on the Linux bridge `kardamom-br0`
  with the `subnet` range.
- `docker_image.node`: `kardamom-node:ci` from `../../docker/node.Dockerfile`.
  A change of the Dockerfile rebuilds the image and replaces every node.
- `docker_volume.node`: `kardamom-<node>-docker` and
  `kardamom-<node>-containerd` for the inner Docker engine.
- `docker_container.node`: `kardamom-<node>` at the address the network
  assigns; the contract reads it back. Apply waits for `systemctl is-system-running` to report `running` or
  `degraded` (`ready_timeout`, 180 s).

## Output

`node_contract` (version 1): the network, the image and one entry per node
with `name`, `container`, `role`, `tier`, `index`, `ip` and `control_plane`.
`ansible/containers.yml` reads it from `node-contract.json`.

## Use

`make container-up` in `deploy/cluster` runs `init`, `apply`, writes the
contract and converges the cluster. `make container-down` runs `destroy`.
Do not run `tofu apply` while a `make container-up` is in progress; the
state lock refuses it.

## Tests

```sh
tofu test
```

The tests use a mocked provider and a small model under `tests/`. They check
the instance naming, the container names, the read-back addresses and the
health check.
