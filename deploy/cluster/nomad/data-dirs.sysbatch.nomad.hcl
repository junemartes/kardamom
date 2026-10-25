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
#
# The task runs the Aeron image of the release manifest, as root, with a
# shell as its entrypoint. The release signs that image, and most nodes
# already hold it, so the job needs no other image.

variable "image_ref" {
  type        = string
  default     = ""
  description = "The Aeron image of the release manifest (repo:tag@sha256:...). Empty = mutable :dev tag fallback (dev-only)."
}

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

variable "run" {
  type        = string
  default     = ""
  description = "The start of the deploy. A new value each deploy gives the job a new version, so Nomad runs it again on every node."
}

job "data-dirs" {
  datacenters = [var.datacenter]
  type        = "sysbatch"
  # Every node of every pool, the elastic pools included.
  node_pool = "all"

  # A sysbatch job runs once for each version. The deploy waits for the
  # allocations of the current version, so each deploy needs a new one:
  # with the same version, Nomad runs nothing, and the allocations of the
  # last run can be gone.
  meta {
    run = var.run
  }

  group "data-dirs" {
    task "data-dirs" {
      driver = "docker"
      # Root makes a directory under a root-owned base and sets its mode.
      user = "root"

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-aeron:dev"
        volumes = [
          "${var.base}:${var.base}",
        ]
        # The shell replaces the entrypoint of the media driver. The
        # directories are the positional arguments of the script.
        entrypoint = [
          "/bin/sh",
          "-c",
          "mkdir -p \"$@\" && chmod ${var.mode} \"$@\"",
          "data-dirs",
        ]
        args = split(" ", var.dirs)
      }

      resources {
        cpu    = 50
        memory = 32
      }
    }
  }
}
