# In-cluster L1 (anvil) for the smoke test + da_watcher / batcher L1 endpoint.
#
# Runs on the control node r1 (192.168.56.11), exposing JSON-RPC on
# 0.0.0.0:8546 (ports.anvil_l1 in group_vars/all.yml). da_watcher points its
# --l1-rpc at http://192.168.56.11:8546.
#
# Uses the upstream Foundry image (not the local registry) since anvil is not a
# kardamom service. Host networking so :8546 is reachable as the VM IP.

job "anvil" {
  datacenters = ["dc1"]
  type        = "service"

  # Pin to the control-plane recorder r1.
  constraint {
    attribute = "${meta.kardamom_node}"
    value     = "r1"
  }

  group "anvil" {
    count = 1

    network {
      mode = "host"
    }

    task "anvil" {
      driver = "docker"

      config {
        # Pinned release tag; the foundry image also publishes :latest/:nightly
        # but an unpinned tag is a "works on my laptop" bug waiting to happen.
        image        = "ghcr.io/foundry-rs/foundry:v1.5.0"
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
