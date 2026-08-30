#!/usr/bin/env bash
# =============================================================================
# local-cluster.sh — runs the container cluster-e2e suite locally on Docker Desktop.
# =============================================================================
#
# This script is the dev-machine analogue of
# .github/workflows/cluster-e2e.yml. That workflow runs `cargo build
# --release` and ci-cluster.sh directly on a GitHub ubuntu-24.04 runner.
# This script reproduces that on a laptop, where the only Linux "host" is
# the Docker Desktop VM:
#
#   1. Builds the service binaries in a reproducible builder image
#      (docker/local-build.Dockerfile). This image provides the build
#      dependencies the runner image gives implicitly (cmake 3.30 or
#      later, libclang, rustfmt, a JDK). The script stages the binaries
#      under target/release, where ci-cluster.sh expects them.
#   2. Builds a runner-surrogate "orchestrator" image
#      (docker/orchestrator.Dockerfile), with the docker CLI, ansible, and
#      nomad.
#   3. Makes sure the Docker daemon trusts the in-cluster registry (plain
#      HTTP).
#   4. Runs the unmodified ci-cluster.sh from the orchestrator. It shares
#      the VM's network namespace and docker socket, so node containers
#      are siblings on the VM daemon. This matches the topology
#      ci-cluster.sh assumes on a runner.
#
# Usage:
#   deploy/cluster/scripts/local-cluster.sh            # build + bring up
#   KEEP=1 deploy/cluster/scripts/local-cluster.sh     # leave the cluster up to inspect
#   deploy/cluster/scripts/local-cluster.sh build      # only build binaries + images
#   deploy/cluster/scripts/local-cluster.sh up         # only run ci-cluster.sh
#
# The orchestrator ships foundry `cast`, so smoke.sh uses its preferred
# signed-tx path (Path A), the same path CI uses. To inspect the cluster
# for the multi-host Aeron work, run with KEEP=1, or use `docker exec
# kardamom-orch ...`.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${CLUSTER_DIR}/../.." && pwd)"
DOCKER_DIR="${CLUSTER_DIR}/docker"
REGISTRY_CIDR="192.168.56.0/24"

log() { echo "==> $*"; }

build_binaries() {
  log "building reproducible builder image (docker/local-build.Dockerfile)"
  docker build -f "${DOCKER_DIR}/local-build.Dockerfile" -t kardamom-local-build:latest "${DOCKER_DIR}"
  docker volume create kardamom-ltarget >/dev/null
  docker volume create kardamom-cargo-registry >/dev/null
  log "building service binaries in the builder (host arch)"
  # Mount the registry cache at a sub-path, so it does not shadow the
  # image's toolchain at /root/.cargo/bin. Save build outputs through
  # CARGO_TARGET_DIR.
  docker run --rm \
    -v "${ROOT}:/work" \
    -v kardamom-cargo-registry:/root/.cargo/registry \
    -v kardamom-ltarget:/ltarget \
    -e CARGO_TARGET_DIR=/ltarget -e CARGO_NET_RETRY=10 -e CARGO_HTTP_MULTIPLEXING=false \
    -w /work kardamom-local-build:latest bash -c '
      set -e
      cargo build --release \
        -p kardamom-ingress -p kardamom-sequencer -p kardamom-executor \
        -p kardamom-validator -p kardamom-da-watcher -p kardamom-batcher \
        --bins
      # ci-cluster.sh runs the sustained-load and chaos harness from
      # target/release.
      cargo build --release -p kardamom-bench --bin kardamom-load
      # Stage files exactly where ci-cluster.sh looks: binaries in
      # target/release, and libaeron*.so under a build/*/out/build/lib
      # path it greps for. kardamom-recorder is removed; durability now
      # happens at the sealer archive.
      mkdir -p /work/target/release/build/aeronstage/out/build/lib
      for b in ingress sequencer executor validator da-watcher batcher; do
        cp -f "/ltarget/release/kardamom-$b" /work/target/release/
      done
      cp -f /ltarget/release/kardamom-load /work/target/release/
      find /ltarget/release/build -path "*/out/build/lib/libaeron*.so" \
        -exec cp -f {} /work/target/release/build/aeronstage/out/build/lib/ \;
      echo "staged: $(ls /work/target/release/build/aeronstage/out/build/lib/ | tr "\n" " ")"
    '
  log "service binaries staged under target/release"
}

build_orchestrator() {
  log "building orchestrator image (docker/orchestrator.Dockerfile)"
  docker build -f "${DOCKER_DIR}/orchestrator.Dockerfile" -t kardamom-orchestrator:latest "${DOCKER_DIR}"
}

ensure_registry_trusted() {
  # This check is informational only, and never fails the build.
  # ci-cluster.sh pushes through REGISTRY_PUSH_NODE: it saves the image and
  # loads it engine-to-engine (`docker save | docker exec control-0 docker
  # load`), then pushes from inside control-0 against its own registry.
  # The host/VM docker daemon never sends registry traffic, so its
  # insecure-registry trust does not matter here. Only the in-node dockers
  # need that trust, and Ansible configures those. An earlier hard gate
  # here was also racy: a transient `docker info` hiccup during a Docker
  # Desktop restart aborted the whole run before it started.
  if docker info 2>/dev/null | grep -q "${REGISTRY_CIDR}"; then
    log "host daemon trusts insecure registry ${REGISTRY_CIDR} (not required for the in-node push)"
  else
    log "note: host daemon doesn't list ${REGISTRY_CIDR} as insecure — fine; the in-node push doesn't route registry traffic through it"
  fi
}

run_cluster() {
  ensure_registry_trusted
  docker rm -f kardamom-orch >/dev/null 2>&1 || true
  log "starting orchestrator (--privileged --network=host --pid=host + docker.sock)"
  docker run -d --name kardamom-orch \
    --privileged --network=host --pid=host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${ROOT}:/work" \
    kardamom-orchestrator:latest >/dev/null
  log "running ci-cluster.sh inside orchestrator (KEEP=${KEEP:-0})"
  # REGISTRY_PUSH_NODE makes ci-cluster.sh's push_image() skip Docker
  # Desktop's HTTP proxy. That proxy cannot reach the VM-internal registry
  # IP, and would otherwise hang the push. Instead, the image loads
  # engine-to-engine into cp1's inner docker, and the push runs from
  # there, next to the registry. This setting is a no-op on CI.
  # Forward the load and chaos tuning knobs. Each `-e VAR` passes the
  # current value if set, or is a no-op otherwise. This lets a developer
  # adjust the local run.
  docker exec -e KEEP="${KEEP:-0}" -e REGISTRY_PUSH_NODE=control-0 \
    -e LOAD_DURATION_S -e LOAD_TARGET_TPS -e LOAD_SENDERS -e LOAD_MAX_GAP \
    -e CHAOS_TPS -e CHAOS_CASE_S -e CHAOS_CASES \
    -e CHAOS_RESTART_SLO_S -e CHAOS_RESCHEDULE_SLO_S \
    kardamom-orch \
    bash -lc 'cd /work && deploy/cluster/scripts/ci-cluster.sh'
}

case "${1:-all}" in
  build) build_binaries; build_orchestrator ;;
  up)    run_cluster ;;
  all)   build_binaries; build_orchestrator; run_cluster ;;
  *) echo "usage: $0 [build|up|all]   (KEEP=1 to leave the cluster up)" >&2; exit 2 ;;
esac
