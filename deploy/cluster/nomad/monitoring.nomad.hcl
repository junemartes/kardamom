# kardamom-monitoring: Prometheus, Alertmanager and Grafana on the monitoring
# node.
#
# Prometheus scrapes every service's metrics endpoint by its Consul node
# name, rendered from the node-class counts: no address in this file. It
# evaluates the alert rules of deploy/alerts.yml and sends the firing
# alerts to the Alertmanager of the same allocation. Grafana provisions
# the Prometheus datasource by the Consul service name and the dashboards
# from deploy/grafana/provisioning/dashboards-json. This job is the one
# monitoring stack of every profile. The autoscaler's Prometheus APM
# reads the same service.
#
# The operator of an environment adds rules and the Alertmanager routing
# through the Nomad variable nomad/jobs/monitoring, with two items:
#   rules         a Prometheus rule file (groups of alerts and limits)
#   alertmanager  the complete Alertmanager configuration, receivers
#                 included
# The tasks render the two items and reload on a change (SIGHUP). Without
# the variable, Prometheus evaluates deploy/alerts.yml only and
# Alertmanager routes every alert to a receiver that notifies nobody.
#
# Placement: the node whose role set holds monitoring (the aux node by
# default), outside the chaos suite's blast radius. Ports on that node:
# Prometheus 9090, Alertmanager 9093, Grafana 3000.
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

variable "cluster_id" {
  type        = string
  description = "The cluster identity. The Nomad agents register <cluster_id>-nomad and <cluster_id>-nomad-client in Consul."
  default     = "kardamom-dev"
}

variable "nomad_region" {
  type        = string
  description = "The Nomad region. The agent certificate of a client names client.<region>.nomad."
  default     = "global"
}

variable "nomad_tls_dir" {
  type        = string
  description = "The directory with the agent TLS material on the monitoring node (ca.pem). Empty means the Nomad API speaks plain HTTP."
  default     = ""
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
  # One state mirror per executor node (nomad/state-mirror.nomad.hcl).
  state_mirror_targets = [for i in range(var.executor_count) : "executor-${i}.node.${local.dc}.consul:9007"]
  targets_yaml         = <<-EOT
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
      - job_name: kardamom-state-mirror
        static_configs:
          - targets: ${jsonencode(local.state_mirror_targets)}
      - job_name: kardamom-validator
        static_configs:
          - targets: [{{ range $i, $s := service "kardamom-validator" }}{{ if $i }}, {{ end }}"{{ $s.Node }}.node.${local.dc}.consul:{{ $s.Port }}"{{ end }}]
      - job_name: kardamom-da-watcher
        static_configs:
          - targets: [{{ range $i, $s := service "kardamom-da-watcher" }}{{ if $i }}, {{ end }}"{{ $s.Node }}.node.${local.dc}.consul:{{ $s.Port }}"{{ end }}]
      - job_name: kardamom-batcher
        static_configs:
          - targets: [{{ range $i, $s := service "kardamom-batcher" }}{{ if $i }}, {{ end }}"{{ $s.Node }}.node.${local.dc}.consul:{{ $s.Port }}"{{ end }}]
      - job_name: kardamom-l1-indexer
        static_configs:
          - targets: [{{ range $i, $s := service "kardamom-l1-indexer-metrics" }}{{ if $i }}, {{ end }}"{{ $s.Node }}.node.${local.dc}.consul:{{ $s.Port }}"{{ end }}]
      - job_name: kardamom-notifier
        static_configs:
          - targets: [{{ range $i, $s := service "kardamom-notifier-metrics" }}{{ if $i }}, {{ end }}"{{ $s.Node }}.node.${local.dc}.consul:{{ $s.Port }}"{{ end }}]
      # The host metrics of every node (nomad/node-exporter.system.nomad.hcl)
      # and the metrics of every Nomad agent, discovered through the local
      # Consul agent. The node label is the Consul node name.
      - job_name: node
        consul_sd_configs:
          - server: 127.0.0.1:8500
            datacenter: ${var.datacenter}
            services: [node-exporter]
        relabel_configs:
          - source_labels: [__meta_consul_node]
            target_label: node
      %{ for role in ["server", "client"] }
      - job_name: nomad-${role}
        metrics_path: /v1/metrics
        params:
          format: [prometheus]
        scheme: ${var.nomad_tls_dir != "" ? "https" : "http"}
        tls_config:
          ca_file: ${var.nomad_tls_dir != "" ? "/etc/kardamom/nomad-ca.pem" : ""}
          server_name: ${var.nomad_tls_dir != "" ? "${role}.${var.nomad_region}.nomad" : ""}
        consul_sd_configs:
          - server: 127.0.0.1:8500
            datacenter: ${var.datacenter}
            services: ["${var.cluster_id}-nomad${role == "client" ? "-client" : ""}"]
            # A server registers its http, rpc and serf ports under one
            # name; only the http port serves the metrics.
            tags: [http]
        relabel_configs:
          - source_labels: [__meta_consul_node]
            target_label: node
          - target_label: job
            replacement: nomad
      %{ if role == "client" }
          # A server can also register as a client. Its certificate has
          # the server name, and the server scrape already covers it.
          - source_labels: [__meta_consul_node]
            regex: '{{ range $i, $s := service "${var.cluster_id}-nomad" }}{{ if $i }}|{{ end }}{{ $s.Node }}{{ end }}'
            action: drop
      %{ endif }
      %{ endfor }
  EOT
  dashboards = [
    "kardamom-overview", "kardamom-ingress", "kardamom-sequencer",
    "kardamom-executor", "kardamom-sealer", "kardamom-batcher", "kardamom-da-watcher",
    "kardamom-validator", "kardamom-state-mirror", "kardamom-notifier",
    "kardamom-chain-status", "kardamom-hosts", "kardamom-nomad", "kardamom-aeron",
    "kardamom-da",
    "kardamom-l1-follower",
  ]
}

job "monitoring" {
  datacenters = [var.datacenter]
  type        = "service"

  # The nodes whose role set holds monitoring (group_vars/all.yml, node_classes).
  constraint {
    attribute = "${meta.roles}"
    operator  = "set_contains"
    value     = "monitoring"
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
      port "alertmanager" {
        static = 9093
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
      name     = "alertmanager"
      port     = "alertmanager"
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
        # The CA of the Nomad agents, when the API speaks TLS: the scrape
        # of the Nomad metrics verifies the agent certificate against it.
        volumes = var.nomad_tls_dir != "" ? ["${var.nomad_tls_dir}/ca.pem:/etc/kardamom/nomad-ca.pem:ro"] : []
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
      # The scrape targets follow the Consul service records, so the file
      # renders again whenever a job stops or starts. A reload keeps the
      # server, its alert states and its API up; the default restart
      # takes them down on every job change.
      template {
        destination   = "local/prometheus.yml"
        change_mode   = "signal"
        change_signal = "SIGHUP"
        data          = <<-EOT
          global:
            scrape_interval: 1s
            evaluation_interval: 5s
          alerting:
            alertmanagers:
              - static_configs:
                  - targets: ["127.0.0.1:9093"]
          rule_files:
            - /local/alerts.yml
            - /local/operator-rules.yml
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

      # The rules of the operator, from the Nomad variable. The variable
      # holds the file as one item, so its own {{ }} templates arrive as
      # data. A change reloads Prometheus in place.
      template {
        destination   = "local/operator-rules.yml"
        change_mode   = "signal"
        change_signal = "SIGHUP"
        data          = <<-EOT
          {{- if nomadVarExists "nomad/jobs/monitoring" -}}
          {{- with nomadVar "nomad/jobs/monitoring" }}{{ .rules }}{{ end -}}
          {{- else -}}
          groups: []
          {{- end }}
        EOT
      }

      resources {
        cpu    = 300
        memory = 512
      }
    }

    task "alertmanager" {
      driver = "docker"

      config {
        image        = "prom/alertmanager:v0.34.1@sha256:e9733bafb1bdef9b00e25a21f8f99dc26a22224bf16641ad754d1649f4c3357a"
        network_mode = "host"
        args = [
          "--config.file=/local/alertmanager.yml",
          "--storage.path=/alloc/data/alertmanager",
          # One instance: no peer gossip. The default listener takes port
          # 9094 on every interface of the host, outside the job's ports.
          "--cluster.listen-address=",
        ]
      }

      # The routing of the operator, from the Nomad variable. Without it,
      # the one receiver notifies nobody, and the alerts show on the
      # Alertmanager page only. A change reloads Alertmanager in place.
      template {
        destination   = "local/alertmanager.yml"
        change_mode   = "signal"
        change_signal = "SIGHUP"
        data          = <<-EOT
          {{- if nomadVarExists "nomad/jobs/monitoring" -}}
          {{- with nomadVar "nomad/jobs/monitoring" }}{{ .alertmanager }}{{ end -}}
          {{- else -}}
          route:
            receiver: nobody
          receivers:
            - name: nobody
          {{- end }}
        EOT
      }

      resources {
        cpu    = 100
        memory = 128
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
        GF_SECURITY_ADMIN_USER     = "admin"
        GF_SECURITY_ADMIN_PASSWORD = var.grafana_admin_password
        GF_AUTH_ANONYMOUS_ENABLED  = "true"
        GF_AUTH_ANONYMOUS_ORG_ROLE = "Viewer"
        GF_USERS_DEFAULT_THEME     = "dark"
        GF_PATHS_DATA              = "/alloc/data/grafana"
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
