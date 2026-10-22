# The backup sealer (issue #746): an Aeron ClusterBackup that follows the
# sealer cluster from outside. It is not a member, and it never votes. It
# copies each new snapshot and the log after it from a follower, through
# archive replication and a replay that stops at the commit position, so
# its disk never holds the leader back. Its copy is the recovery source
# after a power loss on a majority of the members.
#
# The job runs on the node of role "backup" (box-dev-4 on staging). The
# deploy registers it only when the inventory has a backup node. The task
# runs the sealer image with -Dkardamom.cluster.backup=true
# (cluster/sealer-service/README.md, "Backup mode").

variable "datacenter" {
  type    = string
  default = "dc1"
}

variable "image_ref" {
  type        = string
  description = "The sealer image (the image of the cluster job). Empty means the :dev image of the local registry."
  default     = ""
}

variable "sealer_count" {
  type        = number
  description = "The sealer node count (node_classes.sealer.count). The backup asks each member for its snapshots."
  default     = 3
}

# The sync level of the archive of the backup and of its catalog. The
# backup is outside the quorum, so a sync never delays a commit, and its
# copy must survive a power loss of its own host: 1 syncs the data of each
# write batch.
variable "backup_file_sync_level" {
  type    = string
  default = "1"
}

# How often the backup asks the cluster for a new snapshot, in seconds.
variable "backup_interval_s" {
  type    = number
  default = 30
}

# The members that the backup copies from (ClusterBackup.SourceType).
# FOLLOWER keeps the replay load off the leader.
variable "backup_source_type" {
  type    = string
  default = "FOLLOWER"

  validation {
    condition     = contains(["ANY", "LEADER", "FOLLOWER"], var.backup_source_type)
    error_message = "The backup_source_type value must be ANY, LEADER or FOLLOWER."
  }
}

# The heap of the backup JVM in MB. The backup holds no retained frames.
variable "backup_heap_mb" {
  type    = number
  default = 512
}

# The longest idle wait of the backup threads, in microseconds
# (KARDAMOM_IDLE_CAP_US), as in the cluster job.
variable "idle_cap_us" {
  type    = number
  default = 50
}

# The member list in the format of the cluster job
# (nomad/cluster.nomad.hcl, locals): the backup reads the consensus
# endpoint of each member from it. Both jobs must build the same list.
locals {
  member_ports = [40200, 40201, 40202, 40203, 40204]
  admin_port   = 40205
  members = join("|", [
    for i in range(var.sealer_count) :
    "${i},${join(",", [for p in local.member_ports : "sealer-${i}.node.${var.datacenter}.consul:${p}"])}"
  ])
}

job "cluster-backup" {
  datacenters = [var.datacenter]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "backup"
  }

  group "backup" {
    count = 1

    # The backup restarts until it runs again. A dead backup harms no
    # commit, but a backup that gave up leaves no recovery copy.
    restart {
      attempts = 5
      interval = "5m"
      delay    = "15s"
      mode     = "delay"
    }

    # The task state decides the health: the backup reports ready only in
    # BACKING_UP, and a first copy of the snapshots can take longer than
    # a deployment waits.
    update {
      max_parallel      = 1
      health_check      = "task_states"
      min_healthy_time  = "30s"
      healthy_deadline  = "5m"
      progress_deadline = "10m"
      auto_revert       = false
    }

    network {
      mode = "host"
      # The backup status: GET /status, GET /ready and GET /metrics.
      port "admin" {
        static = local.admin_port
      }
    }

    task "backup" {
      driver = "docker"

      service {
        name     = "kardamom-cluster-backup"
        port     = "admin"
        address  = "${meta.node_ip}"
        provider = "consul"
        check {
          type     = "http"
          port     = "admin"
          path     = "/ready"
          interval = "10s"
          timeout  = "2s"
        }
      }

      # JVM options for the image ENTRYPOINT, as in the cluster job: docker
      # args land after the main class, so the -D properties go through
      # JAVA_TOOL_OPTIONS.
      env {
        JAVA_TOOL_OPTIONS        = "-Daeron.mtu.length=1344 -Dkardamom.cluster.backup=true -Dkardamom.cluster.nodeIp=${meta.node_ip} -Dkardamom.cluster.members=${local.members} -Daeron.dir=/opt/kardamom/aeron-mount/backup-dir -Dkardamom.cluster.dir=/opt/kardamom/cluster -Dkardamom.archive.dir=/opt/kardamom/archive -Dkardamom.cluster.adminPort=${local.admin_port} -Dkardamom.backup.fileSyncLevel=${var.backup_file_sync_level} -Dkardamom.backup.intervalS=${var.backup_interval_s} -Dkardamom.backup.sourceType=${var.backup_source_type}"
        KARDAMOM_IDLE_CAP_US     = var.idle_cap_us
        KARDAMOM_CLUSTER_HEAP_MB = var.backup_heap_mb
      }

      config {
        image        = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-cluster:dev"
        force_pull   = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/cluster:/opt/kardamom/cluster",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
      }

      resources {
        cpu    = 1000
        memory = var.backup_heap_mb + 640
      }
    }
  }
}
