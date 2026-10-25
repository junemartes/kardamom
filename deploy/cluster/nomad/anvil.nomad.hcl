# This is the in-cluster L1 (anvil), for the smoke test and the
# da_watcher and batcher L1 endpoint.
#
# This runs on the control node, and exposes
# JSON-RPC on 0.0.0.0:8546 (ports.anvil_l1 in group_vars/all.yml).
# da_watcher points its --l1-rpc at http://anvil.service.consul:8546.
#
# This runs the upstream Foundry image, since anvil is not a kardamom
# service. The container path runs the copy of it in the in-cluster
# registry (var.anvil_image). It uses host networking, so :8546 is
# reachable at the VM IP.

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

# The tag pins a release, and the digest pins the immutable bytes. This is
# the multi-arch index digest of foundry-rs/foundry:v1.5.0, resolved from
# ghcr.io at pin time. Docker pulls by the digest when both are present.
variable "anvil_image" {
  type        = string
  description = "The image reference. The default is the pinned upstream image (upstream_images in ansible/group_vars/all.yml). The container path passes the in-cluster copy."
  default     = "ghcr.io/foundry-rs/foundry:v1.5.0@sha256:2a4c7c28807504292da7f76a069dae6d027e7993e9b274f6bf01eb41b0f4bdc6"
}

# anvil's "finalized" tag is the head minus two epochs, 32 slots each by
# default: 64 blocks behind. The followers (the da-watcher, the indexer)
# walk only finalized blocks, so on a young chain they see nothing. One
# slot per epoch puts finality two blocks behind the head.
variable "slots_in_an_epoch" {
  type        = string
  description = "anvil's --slots-in-an-epoch. Empty: anvil's default, 32. The chaos-l1 shard sets 1, so the followers see finality two blocks behind the head."
  default     = ""
}

job "anvil" {
  datacenters = [var.datacenter]
  type        = "service"

  # Pin this to control-plane node r1.
  constraint {
    attribute = "${meta.role}"
    value     = "control"
  }

  group "anvil" {
    count = 1

    # anvil keeps its chain in the container, and the constraint above
    # allows one node only. A replacement starts on the same node with an
    # empty chain: no factory, no settlement, no deposits. So a node that
    # misses its Nomad heartbeat keeps its allocation. Nomad reports it as
    # unknown until the client reports again, and never starts a second
    # anvil. A real L1 replaces anvil outside the local profile.
    disconnect {
      replace = false
    }

    network {
      mode = "host"
      port "l1" {
        static = 8546
      }
    }

    # The L1 is a Consul service: da-watcher and the batcher reach it as
    # anvil.service.consul and the controller discovers it through Nomad.
    service {
      name     = "anvil"
      port     = "l1"
      provider = "consul"
      check {
        type     = "tcp"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "anvil" {
      driver = "docker"

      config {
        image = var.anvil_image
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
        args = concat(
          [
            "--host", "0.0.0.0",
            "--port", "8546",
          ],
          var.slots_in_an_epoch != "" ? ["--slots-in-an-epoch", var.slots_in_an_epoch] : [],
        )
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
