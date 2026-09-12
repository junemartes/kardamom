# The versioned, nonsecret pool contract. ansible/autoscaler.yml renders
# the Autoscaler configuration and the pool policies from it:
#   tofu output -json pool_contract > pool-contract.json
output "pool_contract" {
  description = "Version 1 of the pool contract consumed by ansible/autoscaler.yml."
  value = {
    version            = 1
    cluster_id         = var.cluster_id
    datacenter         = var.datacenter
    location           = var.location
    network_zone       = var.network_zone
    network_id         = hcloud_network.this.id
    cloud_subnet       = var.cloud_subnet_cidr
    vswitch_subnet     = var.vswitch_subnet_cidr
    image              = var.elastic_image
    bootstrap_revision = var.bootstrap_revision
    ssh_keys           = [for k in sort(keys(var.ssh_keys)) : hcloud_ssh_key.this[k].name]
    consul_retry_join  = local.consul_join_names
    load_balancer = {
      id   = hcloud_load_balancer.rpc.id
      ipv4 = hcloud_load_balancer.rpc.ipv4
    }
    pools = {
      for k, p in var.pools : local.pool_ids[k] => {
        pool_id             = local.pool_ids[k]
        name                = k
        role                = p.role
        tier                = p.tier
        node_class          = local.pool_ids[k]
        node_pool           = local.pool_ids[k]
        server_type         = p.server_type
        min                 = p.min
        max                 = p.max
        private_interface   = p.private_interface
        node_drain_deadline = p.node_drain_deadline
        placement_group_id  = hcloud_placement_group.pool[k].id
        firewall_id         = hcloud_firewall.pool[k].id
        group_label         = { key = "group-id", value = local.pool_ids[k] }
        labels = {
          "group-id"         = local.pool_ids[k]
          "kardamom.cluster" = var.cluster_id
          "kardamom.pool"    = local.pool_ids[k]
          "kardamom.role"    = p.role
          "kardamom.tier"    = p.tier
        }
        user_data = local.user_data[k]
      }
    }
  }
}
