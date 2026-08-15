# =============================================================================
# spire-server.nomad.hcl — SPIRE server (attested-identity plan, P1).
# =============================================================================
# DEPLOYABLE ARTIFACT, NOT WIRED INTO deploy.sh: turning SPIRE on is an
# operator action (see spire/README.md for the bring-up runbook). Nothing in
# the cluster depends on it yet.
#
# One SPIRE server on the control node, issuing SVIDs for the trust domain
# `kardamom.internal`. Agents (spire-agent.nomad.hcl, system job) attest
# nodes to it with join tokens (P1; TPM in P2) and workloads with the docker
# attestor; register.sh creates the per-service registration entries with
# selectors on the deployed image DIGEST + Nomad task identity, so SVIDs are
# only ever issued to workloads running the pinned build.
#
# Datastore + CA keys live on a host volume (/opt/spire/data on the control
# node — create it before first run, see README) so identities survive
# container restarts. Requires the Nomad clients' docker volume support
# (already enabled for the kardamom jobs' bind mounts).

# Pinned upstream image (attested-identity P0.1 discipline): tag AND content
# digest — the digest is the multi-arch index digest of
# ghcr.io/spiffe/spire-server:1.12.4, resolved from ghcr.io at pin time. The
# tag stays for readability; docker pulls by the digest.
variable "image_ref" {
  type        = string
  description = "SPIRE server image (digest-pinned)."
  default     = "ghcr.io/spiffe/spire-server:1.12.4@sha256:34147f27066ab2be5cc10ca1d4bfd361144196467155d46c45f3519f41596e49"
}

variable "trust_domain" {
  type    = string
  default = "kardamom.internal"
}

job "spire-server" {
  datacenters = ["dc1"]
  type        = "service"

  # Control node: co-located with the other cluster-trust roots (nomad/consul
  # servers, registry). P2 moves the stronger root of trust to the host TPMs.
  constraint {
    attribute = "${meta.role}"
    value     = "control"
  }

  group "server" {
    count = 1

    restart {
      attempts = 3
      interval = "2m"
      delay    = "10s"
      mode     = "delay"
    }

    network {
      mode = "host"
      port "api" {
        static = 8081
      }
    }

    task "server" {
      driver = "docker"

      config {
        image        = var.image_ref
        network_mode = "host"
        args         = ["run", "-config", "/local/server.conf"]
        volumes = [
          # Datastore (sqlite) + disk keymanager state. Host path must exist
          # on the control node before first run (runbook step 0).
          "/opt/spire/data:/opt/spire/data",
        ]
      }

      template {
        destination = "local/server.conf"
        data        = <<EOF
server {
    bind_address = "0.0.0.0"
    bind_port    = "8081"
    trust_domain = "${var.trust_domain}"
    data_dir     = "/opt/spire/data/server"
    log_level    = "INFO"
    ca_ttl       = "24h"
    # Short workload SVID TTLs: freshness comes from re-issuance under
    # re-attestation, not from long-lived credentials (plan, standing limits).
    default_x509_svid_ttl = "1h"
    default_jwt_svid_ttl  = "5m"
}

plugins {
    DataStore "sql" {
        plugin_data {
            database_type     = "sqlite3"
            connection_string = "/opt/spire/data/server/datastore.sqlite3"
        }
    }

    # P1 node attestation: join tokens, generated per node by the operator
    # (README step 3). TODO(P2: TPM): replace with tpm_devid / Keylime-gated
    # attestation so credential RENEWAL is contingent on verified host quotes
    # — a host that fails appraisal stops being able to renew its workloads'
    # SVIDs (quarantine by expiry).
    NodeAttestor "join_token" {
        plugin_data {}
    }

    KeyManager "disk" {
        plugin_data {
            keys_path = "/opt/spire/data/server/keys.json"
        }
    }
}
EOF
      }

      resources {
        cpu    = 300
        memory = 256
      }

      service {
        name     = "spire-server"
        port     = "api"
        provider = "consul"

        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }
    }
  }
}
