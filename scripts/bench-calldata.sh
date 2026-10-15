#!/usr/bin/env bash
# Run the calldata bench matrix on the local container cluster and write a
# Markdown table. `just bench-calldata` builds kardamom-load and calls this
# script.
#
# Each cell runs a warm-up load, then a measured load at a fixed rate. Each
# load starts every sender at its nonce on the chain, so the cells run one
# after the other on one chain.
#
# The settings come from the environment:
#   RATES      the rates in tx/s (default "500 1000 2000 4000 6000")
#   SIZES      the calldata sizes in bytes (default "0 1938 8082"). The raw
#              size of a transfer is about the calldata size + 110 bytes, so
#              the defaults give raw sizes of about 110, 2048 and 8192 bytes.
#   EXTRA      more cells, as "rate/calldata" words; empty for none. The calldata is a size or
#              a weighted mix (default: the 5,000 tx/s target rows, 8 KB only
#              and the mix of 80 % transfers, 15 % 2 KB and 5 % 8 KB).
#   WARMUP     the warm-up of each cell (default 60s)
#   DURATION   the measured part of each cell (default 300s)
#   SENDERS    the sender count (default 15); the first sender is account 1
#   MAX_IN_FLIGHT  the limit on open submits (default 4096)
#   NODE_CONTRACT  the node contract of the container cluster
#   LOAD_BIN   the kardamom-load binary
set -euo pipefail

out=${1:?usage: bench-calldata.sh OUT.md}
RATES=${RATES:-500 1000 2000 4000 6000}
SIZES=${SIZES:-0 1938 8082}
EXTRA=${EXTRA-5000/8082 5000/0:16,1938:3,8082:1}
WARMUP=${WARMUP:-60s}
DURATION=${DURATION:-300s}
SENDERS=${SENDERS:-15}
MAX_IN_FLIGHT=${MAX_IN_FLIGHT:-4096}
NODE_CONTRACT=${NODE_CONTRACT:-deploy/cluster/terraform/containers/node-contract.json}
LOAD_BIN=${LOAD_BIN:-target/release/kardamom-load}
METRIC=kardamom_ingress_seal_latency_seconds

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ip_of() { jq -er --arg n "$1" '.nodes[$n].ip' "$NODE_CONTRACT"; }
ingress_ip=$(ip_of ingress-0)
rpc="http://$ingress_ip:8545"
mapfile -t sealers < <(jq -r '.nodes[] | select(.role == "sealer") | .container' "$NODE_CONTRACT" | sort)

# The cumulative buckets of the seal latency histogram on ingress-0, as
# "le count" lines.
scrape_buckets() {
    curl -sf "http://$ingress_ip:9006/metrics" \
        | awk -v m="${METRIC}_bucket" 'index($0, m "{") == 1 {
            match($0, /le="[^"]*"/); le = substr($0, RSTART + 4, RLENGTH - 5); print le, $NF }'
}

# The quantile q of the bucket counts in $2 minus those in $1, in
# milliseconds. A value inside a bucket is interpolated linearly. A value
# above the top bucket prints as ">top". "-" means no sample.
quantile_ms() {
    join <(sort "$1") <(sort "$2") | sort -g -k1,1 | awk -v q="$3" '
        { le[NR] = $1; n[NR] = $3 - $2 }
        END {
            total = n[NR]
            if (total <= 0) { print "-"; exit }
            want = q * total; lo = 0; prev = 0
            for (i = 1; i <= NR; i++) {
                if (n[i] >= want && le[i] == "+Inf") { printf ">%.0f\n", lo * 1000; exit }
                if (n[i] >= want) {
                    hi = le[i]
                    span = n[i] - prev
                    frac = (span > 0) ? (want - prev) / span : 1
                    printf "%.1f\n", (lo + (hi - lo) * frac) * 1000
                    exit
                }
                lo = le[i]; prev = n[i]
            }
        }'
}

# The busy and total jiffies of the host, from /proc/stat.
host_jiffies() { awk '/^cpu / { print $2 + $3 + $4 + $7 + $8, $2 + $3 + $4 + $5 + $6 + $7 + $8 }' /proc/stat; }

# The member that logged LEADER last, as its container name.
leader() {
    local c
    for c in "${sealers[@]}"; do
        docker exec "$c" sh -c 'grep -h "role=" /opt/nomad/alloc/*/alloc/logs/cluster.stdout.* 2>/dev/null | tail -1' \
            | awk -v c="$c" '/role=LEADER/ { print c }'
    done | head -1
}

# The "sealer snapshot TAKEN" lines that every member logged after $1.
snapshots_since() {
    local c
    for c in "${sealers[@]}"; do
        docker exec "$c" sh -c 'grep -h "sealer snapshot TAKEN" /opt/nomad/alloc/*/alloc/logs/cluster.stdout.* 2>/dev/null' \
            | awk -v t="$1" 'substr($1, 1, 19) >= t'
    done
}

# Sample the CPU of one container every 5 s until killed.
sample_cpu() {
    while sleep 5; do
        docker stats --no-stream --format '{{.CPUPerc}}' "$1" | tr -d '%'
    done
}

# Wait until the nonce of the first sender holds for 15 s, at most 10
# minutes: the backlog of the last cell is then sealed and executed.
settle() {
    local addr=0x70997970C51812dc3A010C7d01b50e0d17dc79C8 last="" now deadline=$((SECONDS + 600))
    while ((SECONDS < deadline)); do
        now=$(curl -sf -X POST -H 'content-type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getTransactionCount\",\"params\":[\"$addr\",\"latest\"]}" \
            "$rpc" | jq -r .result || true)
        [[ -n "$now" && "$now" == "$last" ]] && return
        last=$now
        sleep 15
    done
    echo ">> the chain did not settle in 10 minutes" >&2
}

load() {
    "$LOAD_BIN" --rpc "$rpc" --fixed-rate --target-tps "$1" --workload calldata \
        --calldata-bytes "$2" --senders "$SENDERS" --sender-offset 1 --nonce-start chain \
        --duration "$3" --scrape executor,ingress,sequencer --subscribe \
        --max-in-flight "$MAX_IN_FLIGHT" "${@:4}"
}

cell() {
    local rate=$1 calldata=$2 dir="$work/$1-${2//[:,]/_}"
    mkdir -p "$dir"
    echo ">> cell rate=$rate calldata=$calldata: warm-up $WARMUP" >&2
    load "$rate" "$calldata" "$WARMUP" --drain-timeout 60s >"$dir/warmup.log" 2>&1 || true
    local lead since
    lead=$(leader)
    since=$(date -u +%Y-%m-%dT%H:%M:%S)
    scrape_buckets >"$dir/before"
    read -r busy0 total0 < <(host_jiffies)
    sample_cpu "$lead" >"$dir/cpu" &
    local sampler=$!
    echo ">> cell rate=$rate calldata=$calldata: measure $DURATION, leader $lead" >&2
    load "$rate" "$calldata" "$DURATION" --assert-all-delivered --output "$dir/report.json" \
        >"$dir/load.log" 2>&1 || true
    kill "$sampler" 2>/dev/null || true
    scrape_buckets >"$dir/after"
    read -r busy1 total1 < <(host_jiffies)
    local verdict delivered offered receipt seal cpu snap load1 host_cpu
    verdict=$(jq -r 'if .verdict.pass then "PASS" else "FAIL" end' "$dir/report.json" 2>/dev/null || echo "NO REPORT")
    delivered=$(jq -r '"\(.verdict.receipted)/\(.verdict.offered)"' "$dir/report.json" 2>/dev/null || echo "-")
    offered=$(jq -r '.verdict.offered / .duration_secs | floor' "$dir/report.json" 2>/dev/null || echo "-")
    receipt=$(jq -r '"\(.lat_p50_us / 1000 | floor) / \(.lat_p99_us / 1000 | floor)"' "$dir/report.json" 2>/dev/null || echo "-")
    seal="$(quantile_ms "$dir/before" "$dir/after" 0.5) / $(quantile_ms "$dir/before" "$dir/after" 0.99)"
    cpu=$(awk '{ s += $1; if ($1 > m) m = $1 } END { if (NR) printf "%.0f / %.0f", s / NR, m; else print "-" }' "$dir/cpu")
    snap=$(snapshots_since "$since" | sed -n 's/.* ms=\([0-9]*\).*/\1/p' \
        | awk '{ if ($1 > m) m = $1 } END { if (NR) printf "%d (%d)", m, NR; else print "-" }')
    load1=$(cut -d' ' -f1 /proc/loadavg)
    host_cpu=$(awk -v b="$((busy1 - busy0))" -v t="$((total1 - total0))" 'BEGIN { printf "%.0f", t ? 100 * b / t : 0 }')
    echo "| $rate | $offered | $calldata | $verdict | $delivered | $receipt | $seal | $cpu | $snap | $load1 | $host_cpu |"
}

cells=()
for size in $SIZES; do
    for rate in $RATES; do cells+=("$rate/$size"); done
done
read -r -a extra <<<"$EXTRA"
cells+=("${extra[@]}")

{
    echo "# Calldata bench: $(date -u +%Y-%m-%d)"
    echo
    echo "- Revision: \`$(git rev-parse --short HEAD 2>/dev/null || jj log -r @- --no-graph -T 'commit_id.short()' 2>/dev/null || echo unknown)\`"
    echo "- Host: $(nproc) CPUs, $(awk '/MemTotal/ { printf "%.0f GiB", $2 / 1048576 }' /proc/meminfo), load average at start $(cut -d' ' -f1-3 /proc/loadavg)"
    echo "- Each cell: warm-up $WARMUP, then $DURATION measured, $SENDERS senders, \`--fixed-rate --subscribe --max-in-flight $MAX_IN_FLIGHT\`. Offered tx/s is the offered count over the measured time."
    echo "- Calldata: the calldata bytes of each transaction. The raw size is about the calldata + 110 bytes. \`a:w,b:w\` is a weighted mix."
    echo "- Receipt latency: \`kardamom-load\`, p50 / p99 in ms. Seal latency: \`$METRIC\` on ingress-0, p50 / p99 in ms, interpolated in the histogram buckets."
    echo "- Leader CPU: \`docker stats\` of the leader node container every 5 s, mean / max in % of one CPU."
    echo "- Snapshot: the largest \`ms=\` of the \`sealer snapshot TAKEN\` lines of all members in the measured part, and the line count."
    echo "- Host load and host CPU: the 1-minute load average at the end of the cell, and the busy % of all host CPUs in the measured part."
    echo
    echo "| tx/s | offered tx/s | calldata | verdict | receipted/offered | receipt ms | seal ms | leader CPU % | snapshot ms (n) | host load | host CPU % |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|"
} >"$out"

for c in "${cells[@]}"; do
    settle
    cell "${c%%/*}" "${c#*/}" | tee -a "$out"
done
echo ">> wrote $out" >&2
