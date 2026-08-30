# shellcheck shell=bash
# =============================================================================
# ci-images.sh — image build + registry push for ci-cluster.sh.
# =============================================================================
# This file is sourced into ci-cluster.sh's shell, never run as a child
# process. A fatal build error must abort the one ci-cluster process,
# through set -e or an explicit exit. It reads the entry script's
# ROOT/REGISTRY/TAG/SERVICES/DIGEST_MANIFEST constants at call time.
# This file must not install traps; ci-cluster.sh owns the single EXIT
# trap. It needs lib.sh (for log) and lib-signing.sh (for
# sign_pushed_image). ci-cluster.sh signs the completed manifest itself,
# with sign_digest_manifest, after the last push. A signature over a
# half-written manifest would still verify, but it would lie.

# Push a locally-built image to the in-cluster registry.
#
# On a CI runner, the host docker daemon reaches 192.168.56.10:5000
# directly. So this is a plain `docker push`, and REGISTRY_PUSH_NODE stays
# unset with no change in behavior.
#
# On the local Docker Desktop harness (local-cluster.sh), Docker Desktop's
# transparent HTTP proxy (http.docker.internal:3128) hijacks the VM
# daemon's registry traffic. Its bypass list does not include the
# VM-internal 192.168.56.0/24 bridge. So a direct push routes through
# that proxy, which cannot reach the bridge IP, and hangs until "context
# deadline exceeded" — the registry never gets the request. To avoid the
# proxy, move the image engine-to-engine over the docker socket
# (`docker save | docker exec … docker load`) into REGISTRY_PUSH_NODE's
# inner docker, then push from there. That node (cp1) sits next to the
# registry, so its push is node-local and never touches the proxy. Other
# nodes still pull from the registry over the bridge as usual.
push_image() {
  local img="$1"
  local out
  if [[ -n "${REGISTRY_PUSH_NODE:-}" ]]; then
    log "push ${img} via kardamom-${REGISTRY_PUSH_NODE} (proxy-safe engine-to-engine load)"
    docker save "${img}" | docker exec -i "kardamom-${REGISTRY_PUSH_NODE}" docker load
    out="$(docker exec "kardamom-${REGISTRY_PUSH_NODE}" docker push "${img}" | tee /dev/stderr)"
  else
    out="$(docker push "${img}" | tee /dev/stderr)"
  fi
  # Digest capture: record the digest of this exact push, so deploy.sh
  # can pin the jobs. The digest comes from the
  # push output's last "digest: sha256:..." line, not from
  # `docker inspect --format='{{index .RepoDigests 0}}'`, for two reasons:
  #   1. It works the same for both push paths above. In the
  #      REGISTRY_PUSH_NODE path, the push happens inside the node's
  #      inner docker, where a host-side inspect would find no
  #      RepoDigests at all.
  #   2. RepoDigests is an unordered list. It can hold digests for other
  #      repos or registries that share the same image ID, so index 0 is
  #      not guaranteed to be this registry's digest. The push output
  #      line always names the digest of this exact push, to this exact
  #      repo.
  # Strip stray characters and validate the shape. An invisible \r or
  # space would print clean in every log, but fail docker's reference
  # parser at pull time. Fail loudly instead of falling back. A deploy
  # that silently lost its digest would run the mutable :dev tag while
  # claiming an audit record.
  local digest
  digest="$(awk '/digest: sha256:/ {d=$3} END {print d}' <<<"${out}" | tr -d '[:space:]\r')"
  if [[ -z "${digest}" ]]; then
    echo "ERROR: could not capture the pushed digest for ${img} from the push output" >&2
    exit 1
  fi
  # Manifest line: "<svc> <repo>:<tag>@sha256:...". <svc> is the image
  # basename without its kardamom- prefix (aeron, cluster, ingress, ...).
  # deploy.sh maps each job to its <svc> line, and passes the ref through
  # -var image_ref=... .
  #
  # The combined repo:tag@digest form is deliberate; a bare repo@digest is
  # not enough. Nomad 1.9.5's docker driver
  # (drivers/docker/utils.go parseDockerImage) cannot parse a bare digest
  # ref on a registry host with a port. The registry-port colon makes it
  # take the "tag contains /" branch, append :latest, and the pull fails
  # with "invalid reference format". With repo:tag@digest, the driver
  # pulls the advisory tag, then resolves the container image by the
  # digest ref (ImageInspectWithRaw on the full pinned string). So the
  # digest still pins what runs: a moved tag fails the task, instead of
  # running unpinned bytes.
  local ref name ref_re='^[a-z0-9./:-]+@sha256:[0-9a-f]{64}$'
  ref="${img}@${digest}"
  if [[ ! "${ref}" =~ ${ref_re} ]]; then
    echo "ERROR: captured image ref fails shape validation: '${ref}'" >&2
    exit 1
  fi
  name="${img##*/}"
  name="${name%%:*}"
  echo "${name#kardamom-} ${ref}" >>"${DIGEST_MANIFEST}"
  log "pinned ${name#kardamom-} -> ${ref}"
  # Sign the digest just pinned, with keyless signing to the public
  # Rekor log. Outside CI with OIDC, this is a
  # logged no-op. The local harness, and the REGISTRY_PUSH_NODE
  # proxy-safe path (local dev by definition), deploy unsigned, the same
  # as deploy.sh's :dev-tag fallback.
  sign_pushed_image "${ref}"
}

# Build thin service images from prebuilt binaries (§5). The workflow ran
# `cargo build --release` for each BIN. This function wraps each binary
# into a thin image, and pushes it to the in-cluster registry, reachable
# on the bridge IP.
build_service_images() {
  local staging found so svc bin
  log "building + pushing service images to ${REGISTRY}"
  # Start a fresh digest manifest for this deploy. push_image appends one
  # line per image. deploy.sh reads the file to pin every job to
  # repo@sha256:....
  : >"${DIGEST_MANIFEST}"
  # Aeron image (same canonical Dockerfile as make images).
  docker build -f "${ROOT}/crates/log/docker/aeron/Dockerfile" \
    -t "${REGISTRY}/kardamom-aeron:${TAG}" "${ROOT}/crates/log/docker/aeron"
  push_image "${REGISTRY}/kardamom-aeron:${TAG}"

  # The binaries link Aeron dynamically, since rusteron's static feature
  # is broken on Linux. So the thin runtime image must carry libaeron.so
  # and libaeron_archive_c_client.so. rusteron builds these under the
  # cargo build directory. Stage them into the image build context
  # (target/release), so the Dockerfile can copy them in. ldconfig in the
  # image then makes them resolvable.
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

# Build the cluster image, the Java Aeron Cluster node (§5b). The
# clustered sealer (cluster.nomad.hcl, Phase 3) runs a pure-JVM Aeron
# Cluster member, not a Rust service. So this function builds it
# separately from the SERVICES loop above. The workflow runs
# `./gradlew :service:shadowJar` before ci-cluster.sh, to produce
# kardamom-cluster-node.jar. This function stages that jar into the
# Dockerfile's build context, then builds and pushes the image
# deploy.sh's cluster.nomad.hcl pulls
# (192.168.56.10:5000/kardamom-cluster:dev). cluster.Dockerfile copies the
# jar in.
build_cluster_image() {
  local CLUSTER_JAR="${ROOT}/cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar"
  if [[ -f "${CLUSTER_JAR}" ]]; then
    log "building + pushing cluster image (Java Aeron-Cluster node) to ${REGISTRY}"
    cp "${CLUSTER_JAR}" "${ROOT}/deploy/cluster/docker/kardamom-cluster-node.jar"
    docker build -f "${ROOT}/deploy/cluster/docker/cluster.Dockerfile" \
      -t "${REGISTRY}/kardamom-cluster:${TAG}" "${ROOT}/deploy/cluster/docker"
    push_image "${REGISTRY}/kardamom-cluster:${TAG}"
  else
    # The deploy uses the clustered sealer (cluster.nomad.hcl), so a
    # missing jar is fatal. deploy.sh would fail to pull
    # kardamom-cluster:${TAG}. Fail loudly here.
    echo "ERROR: cluster jar not found at ${CLUSTER_JAR}" >&2
    echo "       Build it first: (cd cluster/sealer-service && ./gradlew :service:shadowJar)" >&2
    exit 1
  fi
}
