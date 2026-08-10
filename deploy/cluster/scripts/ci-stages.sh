# shellcheck shell=bash
# =============================================================================
# ci-stages.sh — inventory generation + the env-gated pipeline stages of
# ci-cluster.sh.
# =============================================================================
# SOURCED into ci-cluster.sh's shell (never executed as a child): stage
# failures must abort the ONE ci-cluster process (set -e / explicit exit),
# and the RUN_LOAD/RUN_SEMANTICS/RUN_CHAOS stage gating stays in the entry
# script so the shard semantics are readable there. This file must NOT
# install traps (ci-cluster.sh owns the single EXIT trap — see the sampler
# note in stage_load). Requires lib.sh (log, running_alloc, on_control) and
# the entry script's ROOT/LOAD_BIN.

# Generate the Ansible container inventory from the topology_load instances:
# one group per role (site.yml provisions `all` + `control`), every host
# carrying its node_ip (consul/nomad bind_addr) + role (Nomad node meta for
# ${meta.role} placement). Written to a temp file so the repo carries no
# hand-maintained container inventory.
CONTAINER_INVENTORY="/tmp/kardamom-inventory.containers.ini"
gen_container_inventory() {
  : >"${CONTAINER_INVENTORY}"
  local roles_seen=() r n extra
  for n in "${NODES[@]}"; do
    r="${NODE_ROLE[$n]}"
    [[ " ${roles_seen[*]} " == *" ${r} "* ]] || roles_seen+=("${r}")
  done
  for r in "${roles_seen[@]}"; do
    echo "[${r}]" >>"${CONTAINER_INVENTORY}"
    for n in "${NODES[@]}"; do
      [[ "${NODE_ROLE[$n]}" == "${r}" ]] || continue
      extra=""; [[ "${r}" == "control" ]] && extra=" control_plane=true"
      # node_index = the <i> of <class>-<i>: the node's 0-based index within
      # its class, stamped as Nomad node meta (see nomad.hcl.j2).
      echo "${n} ansible_host=kardamom-${n} kardamom_node=${n} node_ip=${NODE_IP[$n]} role=${r} tier=${NODE_TIER[$n]} node_index=${n##*-}${extra}" >>"${CONTAINER_INVENTORY}"
    done
    echo "" >>"${CONTAINER_INVENTORY}"
  done
  cat >>"${CONTAINER_INVENTORY}" <<EOF
[all:vars]
ansible_connection=community.docker.docker
ansible_python_interpreter=/usr/bin/python3
kardamom_in_container=true
EOF
}

# --- 7. Sustained-load invariant gate (Rust harness: FIXED-RATE soak;
# must-deliver + drop accounting + keep-pace).
stage_load() {
  local LOADAVG_SAMPLER_PID
  # Runner-identity stamping (#124): the load ceiling swings 800→18 on
  # identical code, and "degraded runner window" has only ever been an
  # inference. Stamp WHO is running this shard and HOW LOADED the host is —
  # at start and sampled through the load stages — so collapses correlate
  # with runner hosts/windows instead of being guessed at. RUNNER_NAME is
  # exported by the workflow (github's runner.name); absent locally.
  log "load runner: name=${RUNNER_NAME:-local} host=$(hostname) cpus=$(nproc) loadavg=[$(cut -d' ' -f1-3 /proc/loadavg)] mem_avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo)"
  (
    while true; do
      log "load runner sample: loadavg=[$(cut -d' ' -f1-3 /proc/loadavg)] mem_avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo)"
      sleep 30
    done
  ) &
  LOADAVG_SAMPLER_PID=$!
  # No EXIT trap here — it would CLOBBER on_exit's teardown trap. The
  # sampler is killed explicitly after the load stages; on a mid-load
  # failure exit the orphan is reaped by the CI job's process cleanup.

  # FIXED-RATE invariant gate, not edge discovery: this shard runs on
  # 4-core GH-hosted VMs where the whole stack + harness share the host —
  # a ramp-to-edge there measures the hypervisor (ceilings swung 800→18 on
  # identical code) and soaking at 0.8×edge parks the run AT the edge on a
  # bad VM, failing deadlines sized for healthy hosts. Correctness (zero
  # loss, no gaps, keep-pace, no divergence) is rate-independent: gate on
  # it at a rate the weakest runner sustains. Throughput/latency numbers
  # in the report are informational; real perf comes from the perf suite
  # on dedicated hardware.
  log "load test: kardamom-load fixed-rate invariant gate (duration=${LOAD_DURATION_S:-60}s rate=${LOAD_TARGET_TPS:-200}tps)"
  # --chain-id is passed explicitly (412346, from group_vars/all.yml) rather
  # than probed via eth_chainId: ingress.toml sets no chain_id, so its
  # eth_chainId returns a default that does NOT match the executors' chain, and
  # txs signed with it would never execute. smoke.sh hardcodes 412346 likewise.
  # Sender offset 1 reserves account #0 for the single-tx smoke gate above.
  "${LOAD_BIN}" --rpc http://192.168.56.31:8545 --chain-id 412346 --fixed-rate \
    --duration "${LOAD_DURATION_S:-60}s" --target-tps "${LOAD_TARGET_TPS:-200}" \
    --senders "${LOAD_SENDERS:-6}" --sender-offset 1 --assert-all-delivered \
    --completeness accepted --max-gap "${LOAD_MAX_GAP:-5}" \
    --scrape executor,ingress,sequencer --output /tmp/kardamom-load.json

  # DeFi stage: CLOB + swap-pool + vault mix (contracts deployed by the
  # harness from its first sender). Contract-shaped gas + write sets —
  # exercises the BAL attribution + parallel-validation path under
  # realistic storage churn, reported gas-centrically (Mgas/s). Fresh
  # nonces continue from the transfer stage via --nonce-start probing:
  # kardamom-load starts at 0, so this stage uses DIFFERENT senders
  # (offset past the transfer stage's) to keep nonce bookkeeping trivial.
  log "load test: kardamom-load DEFI stage (duration=${DEFI_DURATION_S:-45}s rate=${DEFI_TARGET_TPS:-100}tps)"
  "${LOAD_BIN}" --rpc http://192.168.56.31:8545 --chain-id 412346 --fixed-rate \
    --workload defi \
    --duration "${DEFI_DURATION_S:-45}s" --target-tps "${DEFI_TARGET_TPS:-100}" \
    --senders "${DEFI_SENDERS:-6}" --sender-offset "$((1 + ${LOAD_SENDERS:-6}))" \
    --assert-all-delivered --completeness accepted --max-gap "${LOAD_MAX_GAP:-5}" \
    --scrape executor,ingress,sequencer --output /tmp/kardamom-load-defi.json

  # Load stages done — stop the runner sampler (#124).
  kill "${LOADAVG_SAMPLER_PID}" 2>/dev/null || true
  LOADAVG_SAMPLER_PID=""
}

# --- chain-semantics suite (Target C) ----------------------------------------
# The SAME scenario drivers the Target-L tests run
# (crates/e2e/src/scenarios, docs/agents/chain-semantics-e2e-suite-spec.md),
# pointed at this cluster instead of a single-host stack: nonce ordering,
# gap non-processing, RPC liveness and validator/executor consistency, all
# observed through the ingress JSON-RPC + the services' /metrics.
#
# OFF by default (RUN_SEMANTICS=0) so it only runs on its own shard and can
# never affect the load / chaos shards. Executor, sequencer and validator
# metrics all bind 0.0.0.0 in their Nomad jobs, so the runner scrapes them
# directly (the same way chaos.sh's progress probe does); the ingress binds
# loopback-only, so the one probe that needs ingress metrics self-skips.
#
# Accounts: this shard runs no load/chaos, so only the smoke gate (#0) has
# been used — the semantics cases own #1..#15 (see the ledger in ci-cluster.sh).
stage_semantics() {
  local SEMANTICS_BIN="${ROOT}/target/release/kardamom-semantics"
  if [[ -x "${SEMANTICS_BIN}" ]]; then
    # The l1-batch case (#39) asserts the live batcher's L2 → L1 round trip
    # against the in-cluster anvil. The settlement proxy address is
    # deterministic on-chain state — re-resolve it the same way deploy.sh's
    # Phase 2b did rather than plumbing it through a file.
    local SETTLEMENT_ADDRESS="${SETTLEMENT_ADDRESS:-}"
    local DEPLOY_BIN="${ROOT}/target/release/kardamom-deploy"
    if [[ -z "${SETTLEMENT_ADDRESS}" && -x "${DEPLOY_BIN}" ]]; then
      # Registry ids print as hashes; the settlement is the only contract
      # registered for this chain id (deploy.sh Phase 2b), so first proxy
      # line wins.
      SETTLEMENT_ADDRESS="$("${DEPLOY_BIN}" --rpc-url http://192.168.56.10:8546 \
        --owner 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
        addresses --l2-chain-id 412346 2>/dev/null \
        | awk '/proxy/ && !found {print $2; found=1}' || true)"
    fi
    if [[ -z "${SETTLEMENT_ADDRESS}" ]]; then
      log "ERROR: could not resolve the settlement address for the l1-batch case"
      exit 1
    fi
    log "chain-semantics suite (Target C): ${SEMANTICS_CASES:-nonce-unordered,nonce-gap,rpc-liveness,rpc-vectors,consistency,l1-batch} (settlement ${SETTLEMENT_ADDRESS})"
    "${SEMANTICS_BIN}" \
      --rpc http://192.168.56.31:8545 --chain-id 412346 \
      --executor-metrics 192.168.56.41:9004,192.168.56.42:9004,192.168.56.43:9004 \
      --sequencer-metrics 192.168.56.21:9001,192.168.56.22:9001 \
      --validator-metrics 192.168.56.61:9006 \
      --pending-receipt-timeout-ms "${SEMANTICS_PARK_MS:-30000}" \
      --account-base "${SEMANTICS_ACCOUNT_BASE:-1}" \
      --l1-rpc http://192.168.56.10:8546 \
      --settlement "${SETTLEMENT_ADDRESS}" \
      --cases "${SEMANTICS_CASES:-nonce-unordered,nonce-gap,rpc-liveness,rpc-vectors,consistency,l1-batch}"
  else
    log "ERROR: RUN_SEMANTICS=1 but ${SEMANTICS_BIN} is not staged"
    exit 1
  fi
}

# --- 8. chaos/resilience suite -----------------------------------------------
stage_chaos() {
  # The chaos suite kills pipeline components under steady load and asserts they
  # auto-recover AND every accepted tx still receipts. The COMPONENT cases
  # (graceful/hard-executor, etc.) exercise the executor's same-node restart: it
  # reopens its persistent /opt/kardamom/state and runs Phase-2 crash recovery
  # (replays tx_ordering + tx_data + tx_deposits from the archive, skip-counts
  # past its durable cursor) — so a broken recovery surfaces here as a missed
  # receipt or a crash-loop that never restores the executor count. (Needs the
  # executor's archive replay endpoints + ingress/da-watcher --archive-durability,
  # wired in the jobs.) The CLUSTER cases exercise the Raft sealer (leader /
  # follower kill, quorum loss + recovery).
  log "chaos suite (kills components under steady load; asserts auto-recovery)"
  # Default cases are the CLUSTERED-sealer Raft cases (Phase 3): the deploy uses
  # cluster.nomad.hcl, so the always-on cluster gate exercises leader-kill /
  # follower-kill / quorum-loss-recover. A shard can override CHAOS_CASES to run
  # the component (executor/ingress/sequencer/sealer) cases instead.
  CHAOS_TPS="${CHAOS_TPS:-50}" CHAOS_CASE_S="${CHAOS_CASE_S:-45}" \
    CHAOS_CASES="${CHAOS_CASES:-cluster-leader-kill cluster-follower-kill cluster-quorum-loss-recover}" \
    CHAOS_RESTART_SLO_S="${CHAOS_RESTART_SLO_S:-60}" \
    CHAOS_RESCHEDULE_SLO_S="${CHAOS_RESCHEDULE_SLO_S:-150}" \
    CHAOS_LEADER_SLO_S="${CHAOS_LEADER_SLO_S:-30}" \
    LOAD_BIN="${LOAD_BIN}" LOAD_MAX_GAP="${LOAD_MAX_GAP:-5}" \
    ./scripts/chaos.sh
}

# Fallback (kardamom-load not staged): the legacy bash load smoke (accounts
# #1..#N) + single subscriber-churn check. Kept so a cluster bring-up without the
# harness still exercises a sustained stream + a basic resilience event.
stage_fallback_load() {
  local alloc
  log "WARN: ${LOAD_BIN} not found — running legacy load smoke + subscriber-churn"
  # The load smoke starts its sender set at account #1 (offset 1) so its nonces
  # never collide with account #0 (reserved for the smoke gate + churn re-smoke).
  SMOKE_SENDER_OFFSET=1 ./scripts/smoke-load.sh
  log "subscriber-churn: stopping one executor alloc and re-running smoke"
  alloc="$(running_alloc executor || true)"
  [ -n "${alloc}" ] && on_control 'nomad alloc stop "$1"' "${alloc}" || true
  sleep 5
  # Re-smoke from a dedicated account (#17), disjoint from the gate (#0) and the
  # ingress-churn re-smoke (#16) — every check owns its own nonce-0 account.
  PK="0x689af8efa8c651a91ad287602527f3af2fe9f6501a7ac4b061667b5a93e037fd" \
    ./scripts/smoke.sh
}

# --- 7b. Ingress active/active failover + multicast-receipts freeze guard ----
# Active/active ingress (docs/agents/resilient-ingress-spec.md): kill ingress-0
# (@.31) and re-smoke against the surviving ingress-1 (@.32). This validates two
# things at once:
#   (a) FAILOVER — a client that loses one ingress is served by another replica
#       (both accept txs, recover the sender, and route to all sequencer shards).
#   (b) The 2a MULTICAST-RECEIPTS FREEZE GUARD — ingress-0 leaving the shared
#       tx_receipts multicast group must NOT freeze the image for ingress-1 (the
#       exact subscriber-churn pathology MDS was introduced to avoid; see the
#       TxReceipts section of config/channels.toml.tpl). If the group froze,
#       ingress-1 would never receive the receipt and this re-smoke would time
#       out — i.e. this IS the freeze reproduction. If it fails here, tighten the
#       Aeron image-liveness / no_unavailable_image handling until it does not.
stage_ingress_churn() {
  log "ingress-churn: stopping ingress-0 and re-smoking against ingress-1 (.32)"
  docker exec kardamom-control-0 bash -lc 'export NOMAD_ADDR=http://192.168.56.10:4646; \
    alloc=$(nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.NodeName}} {{.ID}}{{\"\n\"}}{{end}}{{end}}" ingress 2>/dev/null | awk "/ingress-0/{print \$2; exit}"); \
    [ -n "$alloc" ] && nomad alloc stop "$alloc" || true' || true
  sleep 5
  # Re-smoke against the surviving ingress-1 from a DEDICATED funded account (#16,
  # genesis dev.toml) — disjoint from the gate (#0), load (#1..#6), chaos
  # (#7..#15), and the fallback executor-churn (#17), so there is no cross-stage
  # nonce coordination regardless of which branch above ran.
  PK="0xea6c44ac03bff858b476bba40716402b03e41b8e97e276d1baec7c37d42484a0" \
    RPC_URL="http://192.168.56.32:8545" ./scripts/smoke.sh
}
