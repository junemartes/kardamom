# data-dirs: the data directories of the services, on every node. Each
# deploy registers this job before the services. It makes each directory
# that is missing and sets the mode of each directory. The common role
# makes the same directories when it provisions a node, but a node keeps
# the directories of the services that existed at that time. Without this
# job, Docker makes a missing bind-mount source as root with mode 0755,
# and a service that runs as uid 10001 cannot write to it.
#
# The workloads role passes the list and the mode from node_data_dirs in
# group_vars/all.yml, the one list of the data directories.

variable "datacenter" {
  type    = string
  default = "dc1"
}

variable "base" {
  type        = string
  default     = "/opt/kardamom"
  description = "The directory that holds every data directory: paths.base."
}

variable "dirs" {
  type        = string
  default     = "/opt/kardamom"
  description = "The data directories, separated by spaces: node_data_dirs.paths."
}

variable "mode" {
  type        = string
  default     = "0777"
  description = "The mode of each data directory: node_data_dirs.mode."
}

job "data-dirs" {
  datacenters = [var.datacenter]
  type        = "sysbatch"
  # Every node of every pool, the elastic pools included.
  node_pool = "all"

  group "data-dirs" {
    task "data-dirs" {
      driver = "docker"

      config {
        image = "busybox:1.37.0@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e"
        volumes = [
          "${var.base}:${var.base}",
        ]
        command = "/bin/sh"
        args = concat([
          "-c",
          "mkdir -p \"$@\" && chmod ${var.mode} \"$@\"",
          "data-dirs",
        ], split(" ", var.dirs))
      }

      resources {
        cpu    = 50
        memory = 32
      }
    }
  }
}
