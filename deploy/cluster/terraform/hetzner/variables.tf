variable "cluster_id" {
  description = "Deployment identity. It prefixes every resource name and every pool id."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.cluster_id))
    error_message = "cluster_id is 3 to 31 lowercase letters, digits or dashes and starts with a letter."
  }
}

variable "datacenter" {
  description = "The Nomad and Consul datacenter name. It must agree with the jobs."
  type        = string
  default     = "dc1"
}

variable "location" {
  description = "The Hetzner Cloud location of the elastic pools and the load balancer, near the dedicated core."
  type        = string
}

variable "network_zone" {
  description = "The Hetzner network zone of the Cloud Network subnets."
  type        = string
  default     = "eu-central"
}

variable "network_cidr" {
  description = "The Cloud Network range. It contains the cloud subnet and the vswitch subnet."
  type        = string
}

variable "cloud_subnet_cidr" {
  description = "The subnet of the elastic Cloud VMs. The provider assigns their addresses."
  type        = string
}

variable "vswitch_subnet_cidr" {
  description = "The subnet connected to the Robot vSwitch of the dedicated core. The dedicated hosts take addresses from it (roles/vswitch)."
  type        = string
}

variable "vswitch_id" {
  description = "The id of the existing Robot vSwitch. The vSwitch is an inventory input; this root attaches it, it does not create it."
  type        = number
}

variable "dns_zone" {
  description = "The Hetzner DNS zone that holds the Consul bootstrap records. An empty value skips the records; then an external DNS contract must publish them before any client bootstraps."
  type        = string
  default     = ""
}

variable "consul_servers" {
  description = "The dedicated Consul servers: record name (relative to dns_zone) to private address in the vswitch subnet."
  type        = map(string)

  validation {
    condition     = length(var.consul_servers) == 3
    error_message = "consul_servers lists exactly the three voting Consul servers."
  }
}

variable "ssh_keys" {
  description = "SSH keys for the elastic VMs: name to public key. The bootstrap needs no SSH access; the keys serve diagnostics."
  type        = map(string)
}

variable "ssh_allowed_cidrs" {
  description = "The ranges that may reach SSH on the public interface of an elastic VM."
  type        = list(string)
  default     = []
}

variable "elastic_image" {
  description = "The id or name of the elastic node image that image.yml prepared."
  type        = string
}

variable "bootstrap_revision" {
  description = "The playbook release inside the elastic image. The bootstrap unit refuses a node whose image carries another release."
  type        = string
}

variable "pools" {
  description = "The elastic pools. The key is the pool name; the pool id is <cluster_id>-<key>. The Autoscaler owns the VMs; this root owns the placement group, the firewall and the bounds."
  type = map(object({
    role                = string
    tier                = string
    server_type         = string
    min                 = number
    max                 = number
    private_interface   = optional(string, "enp7s0")
    node_drain_deadline = optional(string, "15m")
  }))

  validation {
    condition     = alltrue([for p in var.pools : p.max <= 10])
    error_message = "A spread placement group holds at most 10 servers; a pool max above 10 needs an additional pool."
  }

  validation {
    condition     = alltrue([for p in var.pools : p.min >= 2 && p.min <= p.max])
    error_message = "Every pool keeps at least two nodes and min is at most max."
  }

  validation {
    condition     = alltrue([for p in var.pools : contains(["ingress", "sequencer"], p.role)])
    error_message = "Only the ingress and sequencer roles are elastic."
  }

  validation {
    condition     = alltrue([for k in keys(var.pools) : can(regex("^[a-z][a-z0-9-]{1,20}$", k))])
    error_message = "A pool name is 2 to 21 lowercase letters, digits or dashes and starts with a letter."
  }
}

variable "rpc_proxy_target_ips" {
  description = "The public addresses of the dedicated hosts that run the RPC proxies. The load balancer reaches an IP target over the public network."
  type        = list(string)

  validation {
    condition     = length(var.rpc_proxy_target_ips) >= 2
    error_message = "The RPC edge needs at least two proxy targets on distinct core hosts."
  }
}

variable "rpc_port" {
  description = "The JSON-RPC port of the proxies and of the load balancer."
  type        = number
  default     = 8545
}

variable "load_balancer_type" {
  description = "The Hetzner load balancer type of the public RPC entry point."
  type        = string
  default     = "lb11"
}
