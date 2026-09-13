# kardamom-monitoring: Prometheus and Grafana on the aux node.
#
# Prometheus scrapes every service's metrics endpoint by its Consul node
# name, rendered from the node-class counts: no address in this file. It
# evaluates the alert rules of deploy/alerts.yml. Grafana provisions the
# Prometheus datasource by the Consul service name and the dashboards
# from deploy/grafana/provisioning/dashboards-json. This job is the one
# monitoring stack of every profile. The autoscaler's Prometheus APM
# reads the same service.
#
# Placement: the aux node, next to the validator and the da-watcher,
# outside the chaos suite's blast radius. Ports on the aux node:
# Prometheus 9090, Grafana 3000.
#
# This job uses file() for its dashboards and alert rules, so submit it
# from deploy/cluster (the workloads role does).

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "executor_count" {
  type        = number
  description = "Executor nodes (node_classes.executor.count in group_vars/all.yml)."
  default     = 3
}

variable "sequencer_count" {
  type        = number
  description = "Sequencer nodes (node_classes.sequencer.count in group_vars/all.yml)."
  default     = 2
}

variable "ingress_count" {
  type        = number
  description = "Ingress nodes (node_classes.ingress.count in group_vars/all.yml)."
  default     = 2
}

variable "grafana_admin_password" {
  type        = string
  description = "The Grafana admin password. The local profile keeps the development default."
  default     = "kardamom"
}

locals {
  dc = var.datacenter
  # A sequencer node hosts one replica per active lane; the metrics port
  # forms the lane, 9001 + 10 * lane. Four lanes cover the resize cases;
  # an inactive lane's target reads as down.
  sequencer_targets = flatten([
    for i in range(var.sequencer_count) : [
      for lane in range(4) : "sequencer-${i}.node.${local.dc}.consul:${9001 + 10 * lane}"
    ]
  ])
  executor_targets = [for i in range(var.executor_count) : "executor-${i}.node.${local.dc}.consul:9004"]
  ingress_targets  = [for i in range(var.ingress_count) : "ingress-${i}.node.${local.dc}.consul:9006"]
  aux              = "aux-0.node.${local.dc}.consul"
  targets_yaml = <<-EOT
    scrape_configs:
      - job_name: kardamom-sequencer
        static_configs:
          - targets: ${jsonencode(local.sequencer_targets)}
      - job_name: kardamom-executor
        static_configs:
          - targets: ${jsonencode(local.executor_targets)}
      - job_name: kardamom-ingress
        static_configs:
          - targets: ${jsonencode(local.ingress_targets)}
      - job_name: kardamom-validator
        static_configs:
          - targets: ["${local.aux}:9006"]
      - job_name: kardamom-da-watcher
        static_configs:
          - targets: ["${local.aux}:9005"]
      - job_name: kardamom-batcher
        static_configs:
          - targets: ["${local.aux}:9002"]
  EOT
  dashboards = [
    "kardamom-overview", "kardamom-ingress", "kardamom-sequencer",
    "kardamom-executor", "kardamom-sealer", "kardamom-batcher", "kardamom-da-watcher",
    "kardamom-validator",
  ]
}

job "monitoring" {
  datacenters = [var.datacenter]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "monitoring" {
    count = 1

    restart {
      attempts = 3
      interval = "1m"
      delay    = "5s"
      mode     = "delay"
    }

    reschedule {
      delay          = "10s"
      delay_function = "exponential"
      max_delay      = "1m"
      unlimited      = true
    }

    network {
      mode = "host"
      port "prometheus" {
        static = 9090
      }
      port "grafana" {
        static = 3000
      }
    }

    service {
      name     = "prometheus"
      port     = "prometheus"
      provider = "consul"
      check {
        type     = "http"
        path     = "/-/ready"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name     = "grafana"
      port     = "grafana"
      provider = "consul"
      check {
        type     = "http"
        path     = "/api/health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "prometheus" {
      driver = "docker"

      config {
        image        = "prom/prometheus:v3.5.1@sha256:38c3b05c3bc744ff1b0b7b4eb82196026442845e62a1e2073795565da506d7a2"
        network_mode = "host"
        args = [
          "--config.file=/local/prometheus.yml",
          "--storage.tsdb.path=/alloc/data/prometheus",
          "--storage.tsdb.retention.time=24h",
          # The overview dashboard rates a multi-metric selector
          # ({__name__=~"kardamom_.+_total"}); without delayed name
          # removal, Prometheus rejects it once a service exports two
          # label-less counters.
          "--enable-feature=promql-delayed-name-removal",
        ]
      }

      # Every metric carries host_id (set through --host-id on the
      # binary), so the dashboards group by host without relabel rules.
      template {
        destination = "local/prometheus.yml"
        data        = <<-EOT
          global:
            scrape_interval: 1s
            evaluation_interval: 5s
          # Prometheus evaluates the rules and shows firing alerts on its
          # /alerts page. No Alertmanager is wired; route the alerts there
          # when a pager exists.
          rule_files:
            - /local/alerts.yml
          ${local.targets_yaml}
        EOT
      }

      # The alert rules carry Prometheus's own {{ }} templates, so the
      # consul-template delimiters move out of their way.
      template {
        destination     = "local/alerts.yml"
        data            = file("../alerts.yml")
        left_delimiter  = "[[["
        right_delimiter = "]]]"
      }

      resources {
        cpu    = 300
        memory = 512
      }
    }

    task "grafana" {
      driver = "docker"

      config {
        image        = "grafana/grafana:12.1.1@sha256:a1701c2180249361737a99a01bc770db39381640e4d631825d38ff4535efa47d"
        network_mode = "host"
        volumes = [
          "local/provisioning:/etc/grafana/provisioning:ro",
        ]
      }

      env {
        GF_SECURITY_ADMIN_USER       = "admin"
        GF_SECURITY_ADMIN_PASSWORD   = var.grafana_admin_password
        GF_AUTH_ANONYMOUS_ENABLED    = "true"
        GF_AUTH_ANONYMOUS_ORG_ROLE   = "Viewer"
        GF_USERS_DEFAULT_THEME       = "dark"
        GF_PATHS_DATA                = "/alloc/data/grafana"
      }

      template {
        destination = "local/provisioning/datasources/prometheus.yaml"
        data        = <<-EOT
          apiVersion: 1
          datasources:
            - name: Prometheus
              uid: prometheus
              type: prometheus
              access: proxy
              url: http://prometheus.service.consul:9090
              isDefault: true
              editable: false
        EOT
      }

      template {
        destination = "local/provisioning/dashboards/kardamom.yaml"
        data        = <<-EOT
          apiVersion: 1
          providers:
            - name: kardamom
              orgId: 1
              folder: ""
              type: file
              disableDeletion: false
              updateIntervalSeconds: 10
              allowUiUpdates: false
              options:
                path: /etc/grafana/provisioning/dashboards-json
                foldersFromFilesStructure: false
        EOT
      }

      # One template per dashboard. The JSON carries Grafana's own
      # {{ }} legend templates, so the delimiters move out of its way.
      dynamic "template" {
        for_each = local.dashboards
        content {
          destination     = "local/provisioning/dashboards-json/${template.value}.json"
          data            = file("../grafana/provisioning/dashboards-json/${template.value}.json")
          left_delimiter  = "[[["
          right_delimiter = "]]]"
        }
      }

      resources {
        cpu    = 300
        memory = 512
      }
    }
  }
}
