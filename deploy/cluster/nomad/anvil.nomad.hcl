# In-cluster L1 (anvil) for the smoke test + da_watcher / batcher L1 endpoint.
#
# Runs on the control node r1 (192.168.56.11), exposing JSON-RPC on
# 0.0.0.0:8546 (ports.anvil_l1 in group_vars/all.yml). da_watcher points its
# --l1-rpc at http://192.168.56.10:8546.
#
# Uses the upstream Foundry image (not the local registry) since anvil is not a
# kardamom service. Host networking so :8546 is reachable as the VM IP.

job "anvil" {
  datacenters = ["dc1"]
  type        = "service"

  # Pin to the control-plane recorder r1.
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
        # Pinned release tag AND content digest (attested-identity P0.1): the
        # tag alone is mutable upstream — ghcr.io lets the publisher re-point
        # it — while the digest names immutable bytes. The digest is the
        # multi-arch index digest of foundry-rs/foundry:v1.5.0, resolved from
        # ghcr.io at pin time; the tag stays in the ref for readability only
        # (docker pulls by the digest when both are present).
        image = "ghcr.io/foundry-rs/foundry:v1.5.0@sha256:2a4c7c28807504292da7f76a069dae6d027e7993e9b274f6bf01eb41b0f4bdc6"
        # NO readonly_rootfs (attested-identity P0.3, deliberately skipped):
        # upstream image, not a kardamom build; anvil keeps its chain state in
        # the container filesystem (no bind mounts here), so a read-only root
        # would break it outright.
        network_mode = "host"
        # The foundry image's ENTRYPOINT is ["/bin/sh", "-c"], which would eat
        # a command+args list (sh -c "anvil" "--host" ... runs anvil with NO
        # args, bound to 127.0.0.1:8545). Override the entrypoint so the args
        # reach anvil verbatim.
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
