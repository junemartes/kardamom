# kardamom-node-exporter: the host metrics of every node.
#
# A system job, so each node runs one exporter: CPU, memory, disk, the
# file systems and the network of the host, read through the host root
# mounted at /host. The exporter registers the node-exporter Consul
# service, and the Prometheus of the monitoring job discovers the targets
# through Consul, so an elastic node is scraped from the moment it joins.
#
# The container profile runs every node on one host. There, twelve
# exporters read the same host hardware files in /sys (cpu, cpufreq,
# mdadm, nvme), each read takes tens of seconds under load, and the stuck
# threads pile up in D state until the host stops. So that profile sets
# host_hardware = "off" and reads only the cheap /proc collectors.

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "host_hardware" {
  type        = string
  description = "on: every default collector. off: only the /proc collectors (load, memory, network, pressure, vmstat), for nodes that share one host."
  default     = "on"
  validation {
    condition     = contains(["on", "off"], var.host_hardware)
    error_message = "The host_hardware value must be on or off."
  }
}

locals {
  collectors = {
    # The pseudo file systems and the container layers are not disks.
    on = ["--collector.filesystem.mount-points-exclude=^/(dev|proc|sys|run|var/lib/docker/.+|host/(dev|proc|sys|run|var/lib/docker/.+))($|/)"]
    off = [
      "--collector.disable-defaults",
      "--collector.loadavg",
      "--collector.meminfo",
      "--collector.netdev",
      "--collector.pressure",
      "--collector.vmstat",
    ]
  }
}

job "node-exporter" {
  datacenters = [var.datacenter]
  type        = "system"
  # Every pool: a system job stays in the default pool without this, and
  # an elastic node joins a pool of its own.
  node_pool = "all"

  group "node-exporter" {
    network {
      mode = "host"
      port "metrics" {
        static = 9100
      }
    }

    service {
      name     = "node-exporter"
      port     = "metrics"
      provider = "consul"
      check {
        type     = "tcp"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "node-exporter" {
      driver = "docker"

      config {
        image        = "prom/node-exporter:v1.12.1@sha256:1b4e4438faca4dd7e001dd445d161a4a2091b0fededa84093b3a8dfeae1f1be0"
        network_mode = "host"
        pid_mode     = "host"
        # The host root, read-only, with the default mount propagation: a
        # container node has a private root, and a slave propagation cannot
        # bind it. A file system the host mounts after the start appears at
        # the next restart of the exporter.
        volumes = ["/:/host:ro"]
        args = concat(
          ["--path.rootfs=/host", "--web.listen-address=:9100"],
          local.collectors[var.host_hardware],
        )
      }

      resources {
        cpu    = 50
        memory = 64
      }
    }
  }
}
