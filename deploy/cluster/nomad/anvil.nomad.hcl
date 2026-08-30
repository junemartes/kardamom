# This is the in-cluster L1 (anvil), for the smoke test and the
# da_watcher and batcher L1 endpoint.
#
# This runs on the control node r1 (192.168.56.11), and exposes
# JSON-RPC on 0.0.0.0:8546 (ports.anvil_l1 in group_vars/all.yml).
# da_watcher points its --l1-rpc at http://192.168.56.10:8546.
#
# This uses the upstream Foundry image, not the local registry, since
# anvil is not a kardamom service. It uses host networking, so :8546 is
# reachable at the VM IP.

job "anvil" {
  datacenters = ["dc1"]
  type        = "service"

  # Pin this to control-plane node r1.
  constraint {
    attribute = "${meta.role}"
    value     = "control"
  }

  group "anvil" {
    count = 1

    network {
      mode = "host"
    }

    task "anvil" {
      driver = "docker"

      config {
        # This pins both a release tag and a content digest. The tag
        # alone is mutable upstream;
        # ghcr.io lets the publisher re-point it. The digest names
        # immutable bytes. This is the multi-arch index digest of
        # foundry-rs/foundry:v1.5.0, resolved from ghcr.io at pin time.
        # The tag stays in the ref only for readability; docker pulls
        # by the digest when both are present.
        image = "ghcr.io/foundry-rs/foundry:v1.5.0@sha256:2a4c7c28807504292da7f76a069dae6d027e7993e9b274f6bf01eb41b0f4bdc6"
        # This skips readonly_rootfs on
        # purpose. This is an upstream image, not a kardamom build.
        # anvil keeps its chain state in the container filesystem,
        # with no bind mounts here, so a read-only root would break it
        # outright.
        network_mode = "host"
        # The foundry image's ENTRYPOINT is ["/bin/sh", "-c"], which
        # would swallow a command-and-args list: `sh -c "anvil"
        # "--host" ...` runs anvil with no args, bound to
        # 127.0.0.1:8545. Override the entrypoint, so the args reach
        # anvil unchanged.
        entrypoint = ["anvil"]
        args = [
          "--host", "0.0.0.0",
          "--port", "8546",
        ]
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
