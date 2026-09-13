# The container substrate of the local profile: one privileged systemd and
# Docker-in-Docker container per node of the node-class model. The model
# lives in ansible/group_vars/all.yml. This root reads it and materialises
# the nodes. ansible/containers.yml reads the node contract output and
# provisions the nodes with bootstrap.yml.
#
# A change of the node image replaces every container and its root
# filesystem. Use `tofu destroy` and `tofu apply` for a fresh chain.

locals {
  contract_path = startswith(var.contract_file, "/") ? var.contract_file : "${path.module}/${var.contract_file}"
  docker_path   = startswith(var.docker_dir, "/") ? var.docker_dir : "${path.module}/${var.docker_dir}"
  contract      = yamldecode(file(local.contract_path))
  ip_prefix     = local.contract.ip_prefix
  node_classes  = local.contract.node_classes
  subnet        = "${local.ip_prefix}.0/24"

  # <class>-<i> at ip_prefix.<ip_start + i>, the lane model of the contract.
  nodes = merge([
    for class, spec in local.node_classes : {
      for i in range(spec.count) : "${class}-${i}" => {
        name          = "${class}-${i}"
        container     = "kardamom-${class}-${i}"
        role          = class
        tier          = spec.tier
        index         = i
        ip            = "${local.ip_prefix}.${spec.ip_start + i}"
        control_plane = class == "control"
      }
    }
  ]...)

  lane_ends = [for class, spec in local.node_classes : spec.ip_start + spec.count]
  addresses = [for n in values(local.nodes) : n.ip]
  volumes   = toset(["docker", "containerd"])
  ready     = "s=$(systemctl is-system-running); [ \"$s\" = running ] || [ \"$s\" = degraded ]"
}

resource "docker_network" "this" {
  name   = var.network_name
  driver = "bridge"

  options = {
    "com.docker.network.bridge.name" = var.bridge_name
  }

  # The provider reads the gateway back into the set, so declare it.
  ipam_config {
    subnet  = local.subnet
    gateway = "${local.ip_prefix}.1"
  }

  labels {
    label = var.label
    value = var.network_name
  }

  lifecycle {
    precondition {
      condition     = length(local.nodes) > 0
      error_message = "The node-class model declares no node."
    }
    precondition {
      condition     = length(distinct(local.addresses)) == length(local.addresses)
      error_message = "The node class lanes overlap."
    }
    precondition {
      condition     = alltrue([for e in local.lane_ends : e <= 255])
      error_message = "A node class lane runs past .254."
    }
  }
}

resource "docker_image" "node" {
  name         = var.image_name
  keep_locally = true

  build {
    context    = local.docker_path
    dockerfile = "node.Dockerfile"
    tag        = [var.image_name]
  }

  triggers = {
    dockerfile = filesha256("${local.docker_path}/node.Dockerfile")
  }
}

resource "docker_volume" "node" {
  for_each = { for pair in setproduct(keys(local.nodes), local.volumes) : "${pair[0]}-${pair[1]}" => pair }

  name = "kardamom-${each.key}"

  labels {
    label = var.label
    value = var.network_name
  }
}

resource "docker_container" "node" {
  for_each = local.nodes

  name     = each.value.container
  hostname = each.value.name
  image    = docker_image.node.image_id

  # systemd and the inner dockerd need a privileged container on the host
  # cgroup namespace with a writable cgroup tree and tmpfs run directories.
  privileged    = true
  cgroupns_mode = "host"
  shm_size      = 512
  tmpfs = {
    "/run"      = ""
    "/run/lock" = ""
  }

  volumes {
    host_path      = "/sys/fs/cgroup"
    container_path = "/sys/fs/cgroup"
  }

  volumes {
    volume_name    = docker_volume.node["${each.key}-docker"].name
    container_path = "/var/lib/docker"
  }

  volumes {
    volume_name    = docker_volume.node["${each.key}-containerd"].name
    container_path = "/var/lib/containerd"
  }

  networks_advanced {
    name         = docker_network.this.name
    ipv4_address = each.value.ip
  }

  labels {
    label = var.label
    value = var.network_name
  }

  # Apply returns when systemd inside the node is ready for Ansible.
  healthcheck {
    test         = ["CMD-SHELL", local.ready]
    interval     = "2s" # Docker reports durations in its own form; keep the values in that form.
    timeout      = "5s"
    retries      = 3
    start_period = "1m0s"
  }

  wait         = true
  wait_timeout = var.ready_timeout
}
