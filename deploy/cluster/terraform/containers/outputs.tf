# The node contract read by ansible/containers.yml.

output "node_contract" {
  description = "Version 1 of the node contract: the network and one entry per node."
  value = {
    version = 1
    network = {
      name   = docker_network.this.name
      bridge = var.bridge_name
      subnet = local.subnet
    }
    image = {
      name = docker_image.node.name
      id   = docker_image.node.image_id
    }
    nodes = local.nodes
  }
}
