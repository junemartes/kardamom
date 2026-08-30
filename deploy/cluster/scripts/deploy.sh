#!/usr/bin/env bash
# Deploy the kardamom Nomad job pipeline to the cluster.
#
# Order: the Aeron substrate (system) and anvil L1 first. Wait until
# their allocations are running. Then the service jobs (cluster,
# sequencer, executor, ingress, da-watcher). Then register the batcher
# periodic job.
#
# This talks to the Nomad server on r1. By default, it uses the HTTP
# API over the host-only network (NOMAD_ADDR). All values derive from
# ansible/group_vars/all.yml (control_ip 192.168.56.11, nomad_http
# 4646).
#
# Usage (works from any directory; paths resolve relative to this script):
#   deploy/cluster/scripts/deploy.sh
#   NOMAD_ADDR=http://192.168.56.10:4646 deploy/cluster/scripts/deploy.sh
#   LOCKBOX_ADDRESS=0x... deploy/cluster/scripts/deploy.sh   # for da-watcher
#   KARDAMOM_REQUIRE_SIGNED=1 deploy/cluster/scripts/deploy.sh   # prod posture
set -euo pipefail

usage() {
  cat <<'EOF'
usage: deploy.sh [-h|--help]

Deploys the kardamom Nomad job pipeline (Aeron substrate -> anvil L1 ->
cluster/sequencer/ingress/executor/validator/da-watcher -> batcher). All
configuration is via environment variables; see the header comment for
NOMAD_ADDR / LOCKBOX_ADDRESS / SETTLEMENT_ADDRESS / DIGEST_MANIFEST.

Signature verification (attested-identity P0.5, cosign keyless + public
Rekor):

  KARDAMOM_REQUIRE_SIGNED=1
      Before ANY job run: verify the digest manifest's signature bundle
      (<manifest>.sigbundle, written by the CI push path) and every pinned
      image's keyless signature, both against the pinned CI identity. Any
      failure — missing manifest, missing bundle, bad signature, unverifiable
      image — REFUSES the deploy (fail closed). This is the PRODUCTION
      posture.

  DEFAULT: OFF. Unsigned local deploys are a dev affordance for the local
  cluster, loudly warned about at run time — a production deploy without
  KARDAMOM_REQUIRE_SIGNED=1 has no supply-chain integrity gate.

  Identity pinning (defaults for junemartes/kardamom's CI live in ONE place,
  scripts/lib-signing.sh signing_defaults; override to re-pin):
      KARDAMOM_CERT_IDENTITY_RE    certificate identity regexp
                                   (org/repo/workflow ref of the signing run)
      KARDAMOM_CERT_OIDC_ISSUER    OIDC issuer (GitHub Actions)
EOF
}
case "${1:-}" in
  -h|--help) usage; exit 0 ;;
  "") ;;
  *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
esac

# --- locate the nomad/ job dir relative to this script ----------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NOMAD_DIR="${CLUSTER_DIR}/nomad"

# The job specs pull their template payloads with file("config/...").
# Those paths resolve against the CLI's working directory, so run from
# CLUSTER_DIR.
cd "${CLUSTER_DIR}"

export NOMAD_ADDR="${NOMAD_ADDR:-http://192.168.56.10:4646}"
LOCKBOX_ADDRESS="${LOCKBOX_ADDRESS:-}"
# The live batcher's DA batch inbox (KardamomL2Settlement). Phase 2b
# deploys it with kardamom-deploy, unless supplied here. Without
# either, the batcher job gets its placeholder address and cannot
# post.
SETTLEMENT_ADDRESS="${SETTLEMENT_ADDRESS:-}"
L1_RPC="${L1_RPC:-http://192.168.56.10:8546}"
ROOT_DIR="$(cd "${CLUSTER_DIR}/../.." && pwd)"
DEPLOY_BIN="${DEPLOY_BIN:-${ROOT_DIR}/target/release/kardamom-deploy}"
# anvil dev account #0: factory owner and deploy gas payer. This is a
# well-known dev key.
L1_OWNER="${L1_OWNER:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}"
L1_OWNER_KEY="${L1_OWNER_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
# anvil dev account #2: the batcher EOA (`l1Batcher`). This must match
# the job's batcher_key; see nomad/batcher.nomad.hcl.
BATCHER_EOA="${BATCHER_EOA:-0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC}"
L2_CHAIN_ID="${L2_CHAIN_ID:-412346}"

echo "==> Nomad endpoint: ${NOMAD_ADDR}"
echo "==> Job specs:      ${NOMAD_DIR}"

if ! command -v nomad >/dev/null 2>&1; then
  echo "ERROR: 'nomad' CLI not found on PATH. Install Nomad or run this on r1." >&2
  exit 1
fi

# --- digest-pinned image refs -----------------------------------------------
# The image build/push step (ci-images.sh push_image, or `make images`)
# records each pushed image's registry digest in the manifest below,
# one line per image: "<svc> <repo>:<tag>@sha256:...". Every kardamom
# job exposes an `image_ref` HCL variable. Passing the digest ref
# through it makes the task run the exact bytes this deploy pushed. A
# digest is immutable, so nobody who can push to the registry can
# change what a restart runs. The combined repo:tag@digest form
# matters: Nomad 1.9.5's docker driver cannot parse a bare repo@digest
# on a registry host with a port ("invalid reference format" at pull;
# see ci-images.sh push_image). The manifest and this deploy log are
# the audit record of what this deploy pinned. Operational tooling can
# audit the running fleet against them later.
#
# Fallback: a missing manifest, or a missing per-service line, leaves
# image_ref empty, and the job runs its mutable :dev tag default. This
# fallback exists so a manual `nomad job run <job>.nomad.hcl` during
# debugging still works. It is a dev affordance, not a production path,
# and it logs a loud warning.
DIGEST_MANIFEST="${DIGEST_MANIFEST:-${CLUSTER_DIR}/images.digests}"
if [[ -f "${DIGEST_MANIFEST}" ]]; then
  echo "==> Image digests:  ${DIGEST_MANIFEST}"
  sed 's/^/    /' "${DIGEST_MANIFEST}"
else
  echo "WARNING: digest manifest not found at ${DIGEST_MANIFEST}." >&2
  echo "         Jobs will run mutable :dev tags (dev fallback, NOT a" >&2
  echo "         production path). Run the image build/push step to pin." >&2
fi

# --- signed-manifest verification ---------------------------------------------
# A deploy-time signature proves origin, not current health. This gate
# checks that the manifest, and every image it pins, came from the
# pinned CI identity, before any job run. The continuous half lives in
# the private repo's image-drift sweep, which verifies the same bundle
# before trusting the manifest as its expected-state anchor. This gate
# fails closed on any failure: missing manifest, missing bundle, or bad
# signature. This matches the corrupt-manifest posture in
# image_ref_args below.
if [[ "${KARDAMOM_REQUIRE_SIGNED:-0}" == "1" ]]; then
  # shellcheck source=deploy/cluster/scripts/lib-signing.sh
  source "${SCRIPT_DIR}/lib-signing.sh"
  echo "==> KARDAMOM_REQUIRE_SIGNED=1: verifying manifest + image signatures (cosign, public Rekor)"
  if [[ ! -f "${DIGEST_MANIFEST}" ]]; then
    echo "ERROR: KARDAMOM_REQUIRE_SIGNED=1 but there is no digest manifest at" >&2
    echo "       ${DIGEST_MANIFEST}. A signed deploy cannot fall back to mutable" >&2
    echo "       tags; refusing to deploy." >&2
    exit 1
  fi
  if ! verify_manifest_signature "${DIGEST_MANIFEST}"; then
    echo "ERROR: digest manifest signature verification FAILED — refusing to deploy." >&2
    exit 1
  fi
  SIGNED_REF_RE='^[a-z0-9./:-]+@sha256:[0-9a-f]{64}$'
  VERIFIED_IMAGES=0
  # Read from fd 3, not stdin, so cosign cannot swallow manifest lines.
  while read -r -u 3 svc ref _; do
    [[ -z "${svc}" || "${svc}" == \#* ]] && continue
    if [[ ! "${ref}" =~ ${SIGNED_REF_RE} ]]; then
      echo "ERROR: malformed digest ref for '${svc}' in the SIGNED manifest: '${ref}'" >&2
      echo "       (signature verified but the content is not a pinned ref) — refusing to deploy." >&2
      exit 1
    fi
    if ! verify_image_signature "${ref}"; then
      echo "ERROR: image signature verification FAILED for '${svc}' (${ref}) — refusing to deploy." >&2
      exit 1
    fi
    VERIFIED_IMAGES=$((VERIFIED_IMAGES + 1))
  done 3<"${DIGEST_MANIFEST}"
  echo "==> signatures OK: manifest bundle + ${VERIFIED_IMAGES} image ref(s) verified"
else
  echo "WARNING: KARDAMOM_REQUIRE_SIGNED is not set — deploying WITHOUT" >&2
  echo "         signature verification. This is the local-dev posture only;" >&2
  echo "         production deploys must set KARDAMOM_REQUIRE_SIGNED=1" >&2
  echo "         (see --help)." >&2
fi

# Populate IMAGE_REF_ARGS=(-var image_ref=<repo>:<tag>@sha256:...) for
# a service. Leave it empty, falling back to the tag, when the manifest
# has no line for that service. The last matching line wins, mirroring
# push order. A present but malformed ref is a hard error, not a
# fallback. It means the manifest is corrupt, from a hand edit or a
# capture bug, and deploying unpinned while a manifest exists would
# silently break the audit record.
IMAGE_REF_ARGS=()
image_ref_args() {
  local svc="$1" ref="" ref_re='^[a-z0-9./:-]+@sha256:[0-9a-f]{64}$'
  IMAGE_REF_ARGS=()
  if [[ -f "${DIGEST_MANIFEST}" ]]; then
    ref="$(awk -v s="${svc}" '$1 == s { r = $2 } END { print r }' "${DIGEST_MANIFEST}" | tr -d '[:space:]\r')"
  fi
  if [[ -n "${ref}" ]]; then
    if [[ ! "${ref}" =~ ${ref_re} ]]; then
      echo "ERROR: malformed digest ref for '${svc}' in ${DIGEST_MANIFEST}: '${ref}'" >&2
      echo "       expected <repo>:<tag>@sha256:<64 hex>; refusing to deploy from a corrupt manifest." >&2
      exit 1
    fi
    IMAGE_REF_ARGS=(-var "image_ref=${ref}")
  else
    echo "WARNING: no pinned digest for '${svc}'; its job falls back to the mutable :dev tag." >&2
  fi
}

# --- helpers ----------------------------------------------------------------

run_job() {
  local file="$1"
  shift
  echo "==> nomad run ${file} $*"
  # `nomad job run` exits 2 on "failed to place all allocations". This
  # is expected for a system job that constrains itself off some nodes.
  # For example, aeron (tier != control) is correctly filtered from the
  # control node, which Nomad reports as one unplaceable alloc. The job
  # is still registered. wait_running() below checks the real
  # placement, so this tolerates exit 2 and fails only on other errors.
  local rc=0
  nomad job run "$@" "${NOMAD_DIR}/${file}" || rc=$?
  if [[ "${rc}" -ne 0 && "${rc}" -ne 2 ]]; then
    return "${rc}"
  fi
}

# Poll until a job has at least one running allocation and none
# pending, or until timeout. A failed alloc that has been replaced
# stays in the job's alloc history forever, so "every alloc is running"
# would never become true after a single restart. Instead, this
# function treats "nothing pending, something running" as converged,
# and reports any failed allocs as a warning.
wait_running() {
  local job="$1"
  local timeout="${2:-120}"
  local waited=0
  echo "==> Waiting for job '${job}' allocations to be running (timeout ${timeout}s)..."
  while true; do
    local statuses running pending failed
    statuses="$(nomad job allocs -t '{{range .}}{{.ClientStatus}}{{"\n"}}{{end}}' "${job}" 2>/dev/null || true)"
    running="$(grep -cx 'running' <<<"${statuses}" || true)"
    pending="$(grep -cx 'pending' <<<"${statuses}" || true)"
    failed="$(grep -cx 'failed' <<<"${statuses}" || true)"
    if (( running >= 1 && pending == 0 )); then
      echo "    job '${job}': ${running} allocation(s) running."
      if (( failed > 0 )); then
        echo "    WARNING: job '${job}' also has ${failed} failed alloc(s) in its history." >&2
      fi
      return 0
    fi
    if (( waited >= timeout )); then
      echo "ERROR: timed out waiting for '${job}' (last ${timeout}s)." >&2
      nomad job status "${job}" >&2 || true
      return 1
    fi
    sleep 3
    waited=$((waited + 3))
  done
}

# --- 1. Aeron substrate (system job on all nodes) ---------------------------
echo
echo "### Phase 1: Aeron substrate"
image_ref_args aeron
run_job "aeron.system.nomad.hcl" ${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"}
# Use a generous timeout. Every worker node force-pulls the aeron
# image from the single in-cluster registry at once, which is slow
# under CI CPU contention; the dedicated cluster nodes add more
# concurrent pullers. A healthy run finishes well under this cap; the
# cap only guards against a premature failure when bring-up is slow.
wait_running "aeron" 600

# --- (removed) recorder quorum ----------------------------------------------
# The custom recorders and the Q-of-N quorum aggregator are removed, in
# favor of archive-at-the-sealer durability. The sealer (Phase 3,
# launched first) records its own tx_ordering MDC publication, and
# publishes the durable watermark that ingress --ack-policy on-quorum
# gates on. There are no separate recorder or quorum jobs to bring up.

# --- 2. In-cluster L1 (anvil on r1) -----------------------------------------
echo
echo "### Phase 2: anvil L1"
run_job "anvil.nomad.hcl"
wait_running "anvil" 240

# --- 2b. Settlement contract (the live batcher's DA batch inbox) ------------
# Deployed with kardamom-deploy, using embedded bytecode and a
# deterministic proxy address. anvil's dev accounts are pre-funded, so
# no funding step is needed.
if [[ -z "${SETTLEMENT_ADDRESS}" ]]; then
  if [[ -x "${DEPLOY_BIN}" ]]; then
    echo
    echo "### Phase 2b: KardamomL2Settlement deploy (${L1_RPC})"
    # A fresh anvil has no ERC-7955 CREATE2 factory. Install its
    # runtime with anvil_setCode. This bootstrap is dev-chain only, and
    # idempotent.
    "${DEPLOY_BIN}" --rpc-url "${L1_RPC}" --owner "${L1_OWNER}" bootstrap-7955-anvil
    "${DEPLOY_BIN}" --rpc-url "${L1_RPC}" --owner "${L1_OWNER}" \
      deploy --private-key "${L1_OWNER_KEY}" \
      KardamomL2Settlement --l2-chain-id "${L2_CHAIN_ID}" --l2-minter "${BATCHER_EOA}"
    # `addresses` prints registry ids as hashes, not names. The
    # settlement is the only contract this deploy registers for the
    # chain id, so the first proxy line is it.
    SETTLEMENT_ADDRESS="$("${DEPLOY_BIN}" --rpc-url "${L1_RPC}" --owner "${L1_OWNER}" \
      addresses --l2-chain-id "${L2_CHAIN_ID}" \
      | awk '/proxy/ && !found {print $2; found=1}')"
    if [[ -z "${SETTLEMENT_ADDRESS}" ]]; then
      echo "ERROR: could not resolve the KardamomL2Settlement proxy address" >&2
      exit 1
    fi
    echo "    settlement proxy: ${SETTLEMENT_ADDRESS}"
  else
    echo "WARNING: ${DEPLOY_BIN} not found and SETTLEMENT_ADDRESS unset;" >&2
    echo "         batcher will use its PLACEHOLDER address and cannot post." >&2
  fi
fi

# --- 3. Service pipeline ----------------------------------------------------
# The 3-member Aeron Cluster (Raft) sealer launches first. It owns both
# ordering and durability; durability folds into the Raft log and the
# per-member archive. So the cluster must elect a leader and start
# accepting ingress before the sequencer cluster-clients connect and
# the executor subscribes to its egress. The standalone single-sealer
# job is removed; the Aeron Cluster (cluster.nomad.hcl) is the sole
# orderer and durability layer.
echo
echo "### Phase 3: service pipeline"

# da-watcher needs the chain-specific Lockbox address. If not
# supplied, it falls back to the job's placeholder default, which will
# not work against a real chain.
DA_WATCHER_ARGS=()
if [[ -n "${LOCKBOX_ADDRESS}" ]]; then
  DA_WATCHER_ARGS=(-var "lockbox_address=${LOCKBOX_ADDRESS}")
else
  echo "WARNING: LOCKBOX_ADDRESS not set; da-watcher will use its PLACEHOLDER" >&2
  echo "         default address (deposit path will not function)." >&2
fi

# Bring the cluster up first, and wait until it converges (leader
# elected, archive ready) before the sequencer cluster-clients connect
# and the executor subscribes to its egress. Election and archive
# recovery take longer than the old single-sealer start, hence the
# 180s budget.
# Egress retention override (retention-overrun chaos case): the
# chaos-retention shard exports KARDAMOM_CLUSTER_RETENTION, and
# chaos.sh reads the same env var to size its freeze. This keeps the
# deployed window and the case's expectations from drifting apart. When
# unset, this uses the jobspec default, the sealer's own default.
CLUSTER_ARGS=()
if [[ -n "${KARDAMOM_CLUSTER_RETENTION:-}" ]]; then
  echo "==> cluster: egress retention override: ${KARDAMOM_CLUSTER_RETENTION} frames"
  CLUSTER_ARGS+=(-var "cluster_retention=${KARDAMOM_CLUSTER_RETENTION}")
fi
if [[ -n "${KARDAMOM_CLUSTER_SNAPSHOT_S:-}" ]]; then
  echo "==> cluster: snapshot interval override: ${KARDAMOM_CLUSTER_SNAPSHOT_S}s"
  CLUSTER_ARGS+=(-var "cluster_snapshot_interval_s=${KARDAMOM_CLUSTER_SNAPSHOT_S}")
fi
image_ref_args cluster
CLUSTER_ARGS+=(${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"})
run_job "cluster.nomad.hcl" ${CLUSTER_ARGS[@]+"${CLUSTER_ARGS[@]}"}
wait_running "cluster" 300
image_ref_args sequencer
run_job "sequencer.nomad.hcl" ${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"}
# Bring the ingress up before the executor. The ingress subscribes to
# tx_receipts, and the executor publishes them. Starting the subscriber
# first lets the executor's receipt publications connect right away,
# instead of stalling against a not-yet-present subscriber during
# bring-up, which would back-pressure the exec-to-commit channel and
# freeze state. The ingress also subscribes to the quorum watermark
# here, from the already-running aggregator.
image_ref_args ingress
run_job "ingress.nomad.hcl" ${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"}
wait_running "ingress" 240
image_ref_args executor
run_job "executor.nomad.hcl" ${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"}
# The validator is a passive follower. It subscribes to the same
# multicast channels, plus its own cluster egress session. It can come
# up alongside the executors, since it re-executes from genesis
# regardless of join order.
# L1 light client: when configured, the validator verifies epochs
# against a beacon-authenticated L1 view, instead of a blindly trusted
# RPC.
#
# This is off by default, on purpose. Every kardamom test environment
# runs anvil as L1, and anvil is execution-only, with no beacon chain
# for a consensus light client to sync from. This path works only
# against a real network, so nothing in CI exercises it. Enable it by
# setting the three env variables below.
VALIDATOR_ARGS=()
if [ -n "${L1_LIGHT_CLIENT_EXECUTION_RPC:-}" ] && [ -n "${L1_LIGHT_CLIENT_CONSENSUS_RPC:-}" ]; then
  : "${L1_LIGHT_CLIENT_CHECKPOINT:?set L1_LIGHT_CLIENT_CHECKPOINT (weak-subjectivity beacon block root) to run the light client}"
  echo "==> l1-light-client: verifying L1 view via helios (${L1_LIGHT_CLIENT_NETWORK:-mainnet})"
  run_job "l1-light-client.nomad.hcl" \
    -var "execution_rpc=${L1_LIGHT_CLIENT_EXECUTION_RPC}" \
    -var "consensus_rpc=${L1_LIGHT_CLIENT_CONSENSUS_RPC}" \
    -var "checkpoint=${L1_LIGHT_CLIENT_CHECKPOINT}" \
    -var "network=${L1_LIGHT_CLIENT_NETWORK:-mainnet}"
  wait_running "l1-light-client" 240
  # The validator reads L1 through the light client, never the
  # upstream RPC. Routing it back to the upstream would silently
  # restore the blind trust this exists to remove. Both jobs are
  # constrained to the aux node (ip_start 61 in ansible group_vars), so
  # this is a node-local hop.
  VALIDATOR_ARGS=(
    -var "l1_rpc_url=http://${AUX_IP:-192.168.56.61}:${L1_LIGHT_CLIENT_PORT:-8548}"
    -var "lockbox_address=${LOCKBOX_ADDRESS}"
  )
fi
image_ref_args validator
run_job "validator.nomad.hcl" ${VALIDATOR_ARGS[@]+"${VALIDATOR_ARGS[@]}"} ${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"}
# The ${arr[@]+...} expansion keeps `set -u` happy on bash 3.2, the
# version in macOS /bin/bash, where expanding an empty array is an
# "unbound variable" error.
image_ref_args da-watcher
DA_WATCHER_ARGS+=(${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"})
run_job "da-watcher.nomad.hcl" ${DA_WATCHER_ARGS[@]+"${DA_WATCHER_ARGS[@]}"}

# The cluster was already waited-on above (before the sequencer connected).
wait_running "sequencer" 240
wait_running "executor" 240
wait_running "validator" 240
wait_running "da-watcher" 240

# --- 4. Batcher (live L1 service) --------------------------------------
echo
echo "### Phase 4: batcher (live L1 service)"
BATCHER_ARGS=()
if [[ -n "${SETTLEMENT_ADDRESS}" ]]; then
  BATCHER_ARGS=(-var "settlement_address=${SETTLEMENT_ADDRESS}")
fi
image_ref_args batcher
BATCHER_ARGS+=(${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"})
run_job "batcher.nomad.hcl" ${BATCHER_ARGS[@]+"${BATCHER_ARGS[@]}"}
if [[ -n "${SETTLEMENT_ADDRESS}" ]]; then
  wait_running "batcher" 240
else
  # With a placeholder settlement address, the batcher cannot read or
  # post, and will crash-loop until redeployed with a real address.
  # Register it, but do not gate the deploy on it converging.
  echo "    batcher registered (placeholder settlement address; posting disabled)."
fi

echo
echo "==> Deploy complete. Run scripts/smoke.sh to exercise the pipeline."
