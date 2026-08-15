# =============================================================================
# spire-agent.nomad.hcl — SPIRE agent on every workload node (P1).
# =============================================================================
# DEPLOYABLE ARTIFACT, NOT WIRED INTO deploy.sh: operator action, see
# spire/README.md. Nothing in the cluster depends on it yet.
#
# System job: one agent per non-control node (every node that can host a
# kardamom task — including the sealer nodes, which the aeron system job
# skips). The agent:
#   * attests its NODE to the server with a single-use join token (P1;
#     TODO(P2: TPM): move node attestation to TPM identity so a host that
#     fails Keylime appraisal can no longer renew its workloads' SVIDs);
#   * attests WORKLOADS with the docker attestor (inner dockerd socket) so
#     registration entries can select on the container's image digest +
#     the Nomad task labels;
#   * serves the Workload API on a host socket dir that consumer tasks
#     bind-mount later (first consumer: the interop feed WS endpoints).
#
# Join tokens are SINGLE-USE: a fleet bring-up generates one token per node
# and submits this job once per token window (runbook step 3) — clumsy on
# purpose; the P2 TPM attestor removes the ceremony. All tokens are
# generated against the same agent SPIFFE ID (var agent_spiffe_id) so
# register.sh needs exactly one -parentID.

# Pinned upstream image: tag AND multi-arch index digest of
# ghcr.io/spiffe/spire-agent:1.12.4, resolved from ghcr.io at pin time.
variable "image_ref" {
  type        = string
  description = "SPIRE agent image (digest-pinned)."
  default     = "ghcr.io/spiffe/spire-agent:1.12.4@sha256:163970884fba18860cac93655dc32b6af85a5dcf2ebb7e3e119a10888eff8fcd"
}

variable "trust_domain" {
  type    = string
  default = "kardamom.internal"
}

# Server endpoint as seen from the nodes (control node, static port 8081).
variable "server_address" {
  type    = string
  default = "192.168.56.10"
}

# Single-use node join token (spire-server token generate). Empty default
# fails attestation loudly — there is deliberately no insecure_bootstrap
# fallback.
variable "join_token" {
  type    = string
  default = ""
}

# The agent identity the tokens are generated for (README step 3 passes the
# same -spiffeID to every `token generate`), and register.sh's -parentID.
variable "agent_spiffe_id" {
  type    = string
  default = "spiffe://kardamom.internal/agent/kardamom-node"
}

job "spire-agent" {
  datacenters = ["dc1"]
  type        = "system"

  # Every node that can host kardamom tasks; the control node runs no
  # kardamom workloads (registry + anvil + servers only).
  constraint {
    attribute = "${meta.tier}"
    operator  = "!="
    value     = "control"
  }

  group "agent" {
    network {
      mode = "host"
    }

    task "agent" {
      driver = "docker"

      config {
        image        = var.image_ref
        network_mode = "host"
        # Host PID namespace: the docker workload attestor resolves the
        # CALLING workload's container from its PID (SO_PEERCRED on the
        # Workload API socket -> /proc/<pid> -> container id), which
        # requires seeing host PIDs.
        pid_mode = "host"
        args     = ["run", "-config", "/local/agent.conf"]
        volumes = [
          # Agent state (SVID cache, keys) — survives restarts.
          "/opt/spire/data:/opt/spire/data",
          # Server CA bundle for bootstrap, distributed by
          # export-trust-bundle.sh (runbook step 2).
          "/opt/spire/bootstrap.crt:/opt/spire/bootstrap.crt:ro",
          # Workload API socket dir on the HOST: consumer tasks bind-mount
          # /opt/spire/sockets and dial unix:///opt/spire/sockets/agent.sock.
          "/opt/spire/sockets:/opt/spire/sockets",
          # The node's inner dockerd — the docker workload attestor's view
          # of the containers it attests.
          "/var/run/docker.sock:/var/run/docker.sock",
        ]
      }

      template {
        destination = "local/agent.conf"
        data        = <<EOF
agent {
    data_dir          = "/opt/spire/data/agent"
    log_level         = "INFO"
    server_address    = "${var.server_address}"
    server_port       = "8081"
    trust_domain      = "${var.trust_domain}"
    trust_bundle_path = "/opt/spire/bootstrap.crt"
    socket_path       = "/opt/spire/sockets/agent.sock"

    # Single-use node join token (see the job header).
    # TODO(P2: TPM): swap join_token for TPM-based node attestation
    # (tpm_devid or Keylime-gated), making token ceremony and this variable
    # obsolete.
    join_token = "${var.join_token}"
}

plugins {
    NodeAttestor "join_token" {
        plugin_data {}
    }

    KeyManager "disk" {
        plugin_data {
            directory = "/opt/spire/data/agent"
        }
    }

    # Docker workload attestor: selectors on the attested container's image
    # + labels. Nomad's docker driver stamps com.hashicorp.nomad.* labels on
    # task containers; job/task name labels need `extra_labels` enabled in
    # the clients' docker plugin config (README step 0).
    WorkloadAttestor "docker" {
        plugin_data {
            docker_socket_path = "unix:///var/run/docker.sock"
        }
    }

    # Fallback attestor for non-container consumers (operator tooling).
    WorkloadAttestor "unix" {
        plugin_data {}
    }
}
EOF
      }

      resources {
        cpu    = 200
        memory = 192
      }
    }
  }
}
