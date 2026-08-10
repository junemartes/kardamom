# shellcheck shell=bash
# =============================================================================
# ci-images.sh — image build + registry push for ci-cluster.sh.
# =============================================================================
# SOURCED into ci-cluster.sh's shell (never executed as a child): a fatal
# build error must abort the ONE ci-cluster process (set -e / explicit exit).
# Reads the entry script's ROOT/REGISTRY/TAG/SERVICES constants at call time.
# This file must NOT install traps (ci-cluster.sh owns the single EXIT trap).
# Requires lib.sh (log).

# Push a locally-built image to the in-cluster registry.
#
# On a CI runner the host docker daemon reaches 192.168.56.10:5000 directly, so
# this is a plain `docker push` (REGISTRY_PUSH_NODE unset → unchanged behavior).
#
# On the local Docker-Desktop harness (local-cluster.sh) the VM daemon's registry
# traffic is hijacked by Docker Desktop's transparent HTTP proxy
# (http.docker.internal:3128), whose bypass list does NOT include the VM-internal
# 192.168.56.0/24 bridge — so a direct push routes through that proxy, which can't
# reach the bridge IP, and hangs until "context deadline exceeded" (the registry
# never receives the request). Side-step the proxy entirely by moving the image
# engine-to-engine over the docker socket (`docker save | docker exec … docker
# load`) into REGISTRY_PUSH_NODE's inner docker, then pushing from THERE: that node
# is co-located with the registry (cp1), so its push is node-local and never touches
# the proxy. The other nodes still pull from the registry over the bridge as usual.
push_image() {
  local img="$1"
  if [[ -n "${REGISTRY_PUSH_NODE:-}" ]]; then
    log "push ${img} via kardamom-${REGISTRY_PUSH_NODE} (proxy-safe engine-to-engine load)"
    docker save "${img}" | docker exec -i "kardamom-${REGISTRY_PUSH_NODE}" docker load
    docker exec "kardamom-${REGISTRY_PUSH_NODE}" docker push "${img}"
  else
    docker push "${img}"
  fi
}

# Thin service images from prebuilt binaries (§5): the workflow ran
# `cargo build --release` for each BIN; wrap each into a thin image and push
# to the in-cluster registry (reachable on the bridge IP).
build_service_images() {
  local staging found so svc bin
  log "building + pushing service images to ${REGISTRY}"
  # Aeron image (same canonical Dockerfile as make images).
  docker build -f "${ROOT}/crates/log/docker/aeron/Dockerfile" \
    -t "${REGISTRY}/kardamom-aeron:${TAG}" "${ROOT}/crates/log/docker/aeron"
  push_image "${REGISTRY}/kardamom-aeron:${TAG}"

  # The binaries link Aeron DYNAMICALLY (rusteron's static feature is broken on
  # Linux), so the thin runtime image must carry libaeron.so /
  # libaeron_archive_c_client.so. rusteron builds them under the cargo build dir;
  # stage them into the image build context (target/release) so the Dockerfile
  # can COPY them in. ldconfig in the image then makes them resolvable.
  staging="${ROOT}/target/release/_aeronlibs"
  rm -rf "${staging}"; mkdir -p "${staging}"
  found=0
  while IFS= read -r so; do
    cp -f "${so}" "${staging}/"; found=$((found + 1))
  done < <(find "${ROOT}/target/release/build" -path '*/out/build/lib/libaeron*.so' 2>/dev/null)
  log "staged ${found} Aeron shared lib(s): $(ls "${staging}" 2>/dev/null | tr '\n' ' ')"
  [[ "${found}" -gt 0 ]] || { echo "ERROR: no libaeron*.so found under target/release/build" >&2; exit 1; }

  for svc in "${SERVICES[@]}"; do
    bin="kardamom-${svc}"
    docker build -f docker/ci-service.Dockerfile --build-arg "BIN=${bin}" \
      -t "${REGISTRY}/${bin}:${TAG}" "${ROOT}/target/release"
    push_image "${REGISTRY}/${bin}:${TAG}"
  done
}

# Cluster image — the Java Aeron-Cluster node (§5b). The clustered sealer
# (cluster.nomad.hcl, Phase 3) runs a PURE-JVM Aeron Cluster member, NOT a Rust
# SERVICE — so it's built separately from the SERVICES loop above: the workflow
# runs `./gradlew :service:shadowJar` (before ci-cluster.sh) to produce
# kardamom-cluster-node.jar; here we stage that jar into the Dockerfile's
# build context and build/push the image deploy.sh's cluster.nomad.hcl pulls
# (192.168.56.10:5000/kardamom-cluster:dev). cluster.Dockerfile COPYs the jar.
build_cluster_image() {
  local CLUSTER_JAR="${ROOT}/cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar"
  if [[ -f "${CLUSTER_JAR}" ]]; then
    log "building + pushing cluster image (Java Aeron-Cluster node) to ${REGISTRY}"
    cp "${CLUSTER_JAR}" "${ROOT}/deploy/cluster/docker/kardamom-cluster-node.jar"
    docker build -f "${ROOT}/deploy/cluster/docker/cluster.Dockerfile" \
      -t "${REGISTRY}/kardamom-cluster:${TAG}" "${ROOT}/deploy/cluster/docker"
    push_image "${REGISTRY}/kardamom-cluster:${TAG}"
  else
    # The deploy uses the CLUSTERED sealer (cluster.nomad.hcl), so a missing jar is
    # fatal: deploy.sh would fail pulling kardamom-cluster:${TAG}. Fail loudly here.
    echo "ERROR: cluster jar not found at ${CLUSTER_JAR}" >&2
    echo "       Build it first: (cd cluster/sealer-service && ./gradlew :service:shadowJar)" >&2
    exit 1
  fi
}
