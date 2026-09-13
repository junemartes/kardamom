# The node contract read by ansible/containers.yml.

output "node_contract" {
  description = "Version 1 of the node contract: the network and one entry per node."
  value = {
    version = 1
    network = {
      name   = docker_network.this.name
      bridge = var.bridge_name
      subnet = var.subnet
    }
    image = {
      name = docker_image.node.name
      id   = docker_image.node.image_id
    }
    # The address is the one Docker assigned; it is read back after
    # creation and changes on every replacement of the container.
    nodes = {
      for k, n in local.nodes : k => merge(n, {
        ip = one([for net in docker_container.node[k].network_data : net.ip_address if net.network_name == docker_network.this.name])
      })
    }
  }
}
