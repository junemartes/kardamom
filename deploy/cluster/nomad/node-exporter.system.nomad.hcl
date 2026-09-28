# kardamom-node-exporter: the host metrics of every node.
#
# A system job, so each node runs one exporter: CPU, memory, disk, the
# file systems and the network of the host, read through the host root
# mounted at /host. The exporter registers the node-exporter Consul
# service, and the Prometheus of the monitoring job discovers the targets
# through Consul, so an elastic node is scraped from the moment it joins.

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

job "node-exporter" {
  datacenters = [var.datacenter]
  type        = "system"

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
        args = [
          "--path.rootfs=/host",
          "--web.listen-address=:9100",
          # The pseudo file systems and the container layers are not disks.
          "--collector.filesystem.mount-points-exclude=^/(dev|proc|sys|run|var/lib/docker/.+|host/(dev|proc|sys|run|var/lib/docker/.+))($|/)",
        ]
      }

      resources {
        cpu    = 50
        memory = 64
      }
    }
  }
}
