#!/usr/bin/env python3
"""Verify that values mirrored from ansible/group_vars/all.yml stay in sync.

group_vars/all.yml is the canonical cluster contract, but several artifacts
cannot read YAML (Vagrantfile, Makefile, Nomad job specs, shell scripts) and
mirror its values as literals. This script extracts the contract with plain
regexes (no YAML dependency) and fails if any mirror drifts.

Run from anywhere; paths resolve relative to this file. Exits nonzero and
prints one line per violation.
"""

import re
import subprocess
import sys
from pathlib import Path

CLUSTER = Path(__file__).resolve().parent.parent
REPO = CLUSTER.parent.parent
GROUP_VARS = CLUSTER / "ansible" / "group_vars" / "all.yml"

errors: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)


def scalar(text: str, key: str) -> str:
    """Extract a top-level `key: value` scalar (strips quotes + comments)."""
    m = re.search(rf"^{re.escape(key)}:\s*([^#\n]+)", text, re.M)
    if not m:
        err(f"group_vars/all.yml: missing key '{key}'")
        return ""
    return m.group(1).strip().strip('"').strip("'")


def must_not_contain(path: Path, needle: str, why: str) -> None:
    rel = path.relative_to(REPO)
    if path.exists() and needle in path.read_text():
        err(f"{rel}: must not contain {needle!r} ({why})")


def must_contain(path: Path, needle: str, why: str) -> None:
    rel = path.relative_to(REPO)
    if not path.exists():
        err(f"{rel}: file missing (expected to contain {needle!r} — {why})")
        return
    if needle not in path.read_text():
        err(f"{rel}: expected {needle!r} ({why})")


gv = GROUP_VARS.read_text()

registry_host = scalar(gv, "registry_host")
registry_port = scalar(gv, "registry_port")
image_tag = scalar(gv, "image_tag")
chain_id = scalar(gv, "chain_id")
control_ip = scalar(gv, "control_ip")
ingress_ip = scalar(gv, "ingress_ip")
sealer_ip = scalar(gv, "sealer_ip")
aeron_version = scalar(gv, "aeron_version")
nomad_version = scalar(gv, "nomad_version")
registry = f"{registry_host}:{registry_port}"

# Indented `ports:` entries.
ports = dict(re.findall(r"^\s{2}(\w+):\s*(\d+)", gv, re.M))
for k in ("ingress_rpc", "anvil_l1", "nomad_http"):
    if k not in ports:
        err(f"group_vars/all.yml: missing ports.{k}")
ingress_rpc = ports.get("ingress_rpc", "")
anvil_l1 = ports.get("anvil_l1", "")
nomad_http = ports.get("nomad_http", "")
nomad_addr = f"http://{control_ip}:{nomad_http}"

# Node name -> ip from the legacy hand-enumerated cluster_nodes mapping (if any).
nodes = dict(re.findall(r"^\s{2}(\w+):\n\s{4}ip:\s*([\d.]+)", gv, re.M))
if nodes:
    # --- legacy cluster_nodes model: Vagrantfile + inventory mirror node IPs ---
    EXPECTED_NODES = [
        "batcher1", "cp1", "dawatcher1", "exec1", "ingress1",
        "r1", "r2", "r3", "sealer1", "sq1", "sq2",
    ]
    if sorted(nodes) != EXPECTED_NODES:
        err(f"group_vars/all.yml: unexpected cluster_nodes set: {sorted(nodes)}")
    for name, ip in nodes.items():
        must_contain(CLUSTER / "Vagrantfile", f'"{ip}"', f"static IP of {name}")
        must_contain(
            CLUSTER / "ansible" / "inventory.ini",
            f"{name} ansible_host={ip}",
            f"inventory entry for {name}",
        )
        must_contain(
            CLUSTER / "ansible" / "inventory.containers.ini",
            f"{name} ansible_host=kardamom-{name}",
            f"container inventory entry for {name}",
        )
else:
    # node-class model: terraform/containers materialises nodes from
    # `node_classes` at apply time (names <class>-<i>, static IPs from each
    # class's ip_start lane), so there are no hand-written per-node IP mirrors to
    # cross-check here. Just assert the model is actually declared.
    if "node_classes:" not in gv:
        err("group_vars/all.yml: neither cluster_nodes nor node_classes is defined")

# --- the one bootstrap entry point ---------------------------------------------
# bootstrap.yml is the only host configuration playbook. Every caller (the
# Makefile, the Vagrantfile, containers.yml) runs it, and the Nomad agents
# find their servers through Consul, never through a static server list.
ANSIBLE = CLUSTER / "ansible"
if (ANSIBLE / "site.yml").exists():
    err("ansible/site.yml exists: bootstrap.yml is the one entry point; delete site.yml")
must_contain(ANSIBLE / "bootstrap.yml", "provision_hosts | default('all')", "the bootstrap playbook configures every host")
for caller in ("Makefile", "Vagrantfile", "ansible/containers.yml"):
    must_contain(CLUSTER / caller, "bootstrap.yml", "runs the one bootstrap playbook")
    must_not_contain(CLUSTER / caller, "site.yml", "site.yml is replaced by bootstrap.yml")
NOMAD_TPL = ANSIBLE / "roles" / "nomad" / "templates" / "nomad.hcl.j2"
must_not_contain(NOMAD_TPL, "\n  servers", "Nomad clients discover servers through Consul")
for key in ("server_auto_join", "client_auto_join", "auto_advertise"):
    must_contain(NOMAD_TPL, key, "Consul-based Nomad join")
must_contain(NOMAD_TPL, 'server_service_name = "{{ cluster_id }}-nomad"', "Nomad service names carry cluster_id")
profile = scalar(gv, "deployment_profile")
if profile != "local":
    err(f"group_vars/all.yml: deployment_profile must default to local, got {profile!r}")

# --- the Hetzner Terraform root -----------------------------------------------------
# Terraform owns the network, the placement groups, the firewalls, the DNS
# records and the pool bounds. The Nomad Autoscaler owns the elastic VMs,
# so the root declares no server resource and no provider token variable.
TF_ROOT = CLUSTER / "terraform" / "hetzner"
for tf in sorted(TF_ROOT.glob("*.tf")):
    must_not_contain(tf, 'resource "hcloud_server"', "the Autoscaler owns the elastic VMs")
    must_not_contain(tf, "hcloud_token", "the API token is the HCLOUD_TOKEN environment variable, never a variable")
must_contain(TF_ROOT / "outputs.tf", "version            = 1", "the pool contract is version 1")

# --- the container Terraform root ---------------------------------------------
# terraform/containers owns the node containers of the local profile and
# reads the node-class model from group_vars/all.yml. The lifecycle around
# it (converge, test, diagnose, destroy) is the Makefile's, so no root
# runs a provisioner and no Ansible lifecycle playbook remains.
TF_CONTAINERS = CLUSTER / "terraform" / "containers"
must_contain(TF_CONTAINERS / "main.tf", 'yamldecode(file(local.contract_path))', "the root reads the node-class model of group_vars")
must_contain(TF_CONTAINERS / "outputs.tf", "version = 1", "the node contract is version 1")
for tf in sorted(TF_ROOT.glob("*.tf")) + sorted(TF_CONTAINERS.glob("*.tf")):
    for provisioner in ("local-exec", "remote-exec"):
        must_not_contain(tf, provisioner, "the Makefile owns the lifecycle, not a provisioner")
if (ANSIBLE / "run.yml").exists():
    err("ansible/run.yml exists: the container lifecycle is the Makefile's container-* targets")
for f in ("containers.yml",):
    must_contain(ANSIBLE / f, "node-contract.json", "the container playbook reads the node contract")
must_contain(CLUSTER / "Makefile", "tofu -chdir=$(TF_CONTAINERS) apply", "container-up applies the container root")

# --- Makefile -----------------------------------------------------------------
must_contain(CLUSTER / "Makefile", f"REGISTRY := {registry}", "registry host:port")
must_contain(CLUSTER / "Makefile", f"NOMAD_ADDR := {nomad_addr}", "nomad HTTP API")
must_contain(CLUSTER / "Makefile", f"TAG := {image_tag}", "image tag")

# --- Nomad job specs ------------------------------------------------------------
jobs = CLUSTER / "nomad"
for svc in ("ingress", "sequencer", "executor", "cluster", "da-watcher", "batcher"):
    must_contain(
        jobs / f"{svc}.nomad.hcl",
        f"{registry}/kardamom-{svc}:{image_tag}",
        "image ref from the local registry",
    )
must_contain(
    jobs / "aeron.system.nomad.hcl",
    f"{registry}/kardamom-aeron:{image_tag}",
    "aeron image ref",
)
# NOTE: cluster-only. Ordering + durability are provided by the Aeron Cluster
# (Raft) job (cluster.nomad.hcl); the standalone sealer + its --archive-durability
# sidecar are gone, so the durable watermark ingress --ack-policy on-quorum gates
# on now comes from cluster egress progress (ClusterWatermark).
must_contain(jobs / "anvil.nomad.hcl", f'"{anvil_l1}"', "anvil L1 port")
must_contain(
    jobs / "da-watcher.nomad.hcl",
    f"http://{control_ip}:{anvil_l1}",
    "L1 RPC endpoint (anvil on the control node)",
)
must_contain(jobs / "ingress.nomad.hcl", f"static = {ingress_rpc}", "ingress RPC port")
must_contain(jobs / "executor.nomad.hcl", f'"{chain_id}"', "L2 chain id")

# --- shard count (M) and the lane plane ------------------------------------------
# partition_count is the active shard count. The lane plane is fixed at 8
# lanes in code (kardamom_types::shard_map::LANE_COUNT), and the identity
# map v0 needs a count that divides 256. So M must be 1, 2, 4, or 8. The
# ingress and both sequencer replica groups take M. The consumers (the
# executor, the validator, the batcher) open every lane and take no count.
# See docs/specs/dynamic-sequencer-sizing.md.
partition_count = scalar(gv, "partition_count")
if partition_count not in ("1", "2", "4", "8"):
    err(f"group_vars/all.yml: partition_count must be 1, 2, 4, or 8, got {partition_count!r}")
must_contain(
    jobs / "ingress.nomad.hcl",
    f'"--shards", "{partition_count}"',
    "ingress active shard count (M) mirrors partition_count",
)
seq_job = (jobs / "sequencer.nomad.hcl").read_text()
seq_count_flags = seq_job.count(f'"--partition-count", "{partition_count}"')
if seq_count_flags != 2:
    err(
        f"nomad/sequencer.nomad.hcl: expected both replica groups to pass "
        f'"--partition-count", "{partition_count}" (found {seq_count_flags})'
    )
must_contain(
    CLUSTER / "config" / "sequencer.toml.tpl",
    f"partition_count = {partition_count}",
    "sequencer template partition_count mirrors group_vars",
)
for job in ("executor", "validator", "batcher"):
    must_not_contain(
        jobs / f"{job}.nomad.hcl",
        '"--shards"',
        f"{job} opens the fixed lane plane and takes no shard count",
    )

# --- transaction lifetime (tx_ttl) --------------------------------------------------
# One value drives the sequencer expiry and the ingress submit park. The
# chain-semantics stage parks for the same time by default.
tx_ttl_ms = scalar(gv, "tx_ttl_ms")
if not tx_ttl_ms.isdigit() or int(tx_ttl_ms) == 0:
    err(f"group_vars/all.yml: tx_ttl_ms must be a positive integer, got {tx_ttl_ms!r}")
seq_ttl_flags = seq_job.count(f'"--tx-ttl-ms", "{tx_ttl_ms}"')
if seq_ttl_flags != 2:
    err(
        f"nomad/sequencer.nomad.hcl: expected both replica groups to pass "
        f'"--tx-ttl-ms", "{tx_ttl_ms}" (found {seq_ttl_flags})'
    )
must_contain(
    jobs / "ingress.nomad.hcl",
    f'"--pending-receipt-timeout-ms", "{tx_ttl_ms}"',
    "ingress submit park equals tx_ttl_ms",
)
must_contain(
    CLUSTER / "scripts" / "ci-stages.sh",
    f"SEMANTICS_PARK_MS:-{tx_ttl_ms}",
    "chain-semantics park default equals tx_ttl_ms",
)

# --- the shard map and the generated sequencer job ------------------------------------
# config/shard-map.toml is the routing truth. The checked-in sequencer job
# must equal render-sequencer-job.py's steady render for it, and the ingress
# job must read it. Outside a resize the map is the identity map over
# partition_count, at version 0, and no resize marker exists.
SHARD_MAP = CLUSTER / "config" / "shard-map.toml"
if (CLUSTER / "config" / "shard-map.next.toml").exists():
    err("config/shard-map.next.toml exists: a resize is in flight; finish it before committing")
if not SHARD_MAP.exists():
    err("config/shard-map.toml missing (render: scripts/render-shard-map.py --identity <lanes>)")
else:
    map_text = SHARD_MAP.read_text()
    m_tab = re.search(r"^table\s*=\s*\[([^\]]*)\]", map_text, re.M | re.S)
    m_ver = re.search(r"^version\s*=\s*(\d+)", map_text, re.M)
    table = [int(x) for x in re.findall(r"\d+", m_tab.group(1))] if m_tab else []
    if len(table) != 256:
        err(f"config/shard-map.toml: table has {len(table)} entries, expected 256")
    elif partition_count.isdigit():
        pc = int(partition_count)
        if max(table) + 1 != pc:
            err(f"config/shard-map.toml: {max(table) + 1} active lanes != partition_count {pc}")
        if m_ver and m_ver.group(1) == "0" and table != [v % pc for v in range(256)]:
            err("config/shard-map.toml: version 0 must be the identity map lane = vslot % partition_count")
    rendered = subprocess.run(
        [sys.executable, str(CLUSTER / "scripts" / "render-sequencer-job.py")],
        capture_output=True,
        text=True,
    )
    if rendered.returncode != 0:
        err(f"render-sequencer-job.py failed: {rendered.stderr.strip()}")
    elif rendered.stdout != (jobs / "sequencer.nomad.hcl").read_text():
        err(
            "nomad/sequencer.nomad.hcl differs from the steady render; run "
            "scripts/render-sequencer-job.py > nomad/sequencer.nomad.hcl"
        )
must_contain(jobs / "ingress.nomad.hcl", '"--shard-map", "/local/shard-map.toml"', "ingress reads the shard map")
must_contain(jobs / "ingress.nomad.hcl", 'file("config/shard-map.toml")', "ingress job templates the shard map")
ingress_kill = re.search(r'kill_timeout\s*=\s*"(\d+)s"', (jobs / "ingress.nomad.hcl").read_text())
if not ingress_kill:
    err("nomad/ingress.nomad.hcl: missing kill_timeout for the graceful drain")
elif tx_ttl_ms.isdigit() and int(ingress_kill.group(1)) * 1000 < int(tx_ttl_ms) + 5000:
    err(f"nomad/ingress.nomad.hcl: kill_timeout {ingress_kill.group(1)}s must cover tx_ttl_ms {tx_ttl_ms} plus 5s")


# --- chaos.sh sender-to-shard table ------------------------------------------------
# chaos.sh pins a case's load to a shard through ACCT_SHARD, a table over the
# 16 funded Anvil dev accounts. Recompute it: vslot = keccak256(address)[7],
# then the shard map. keccak-256 is inlined, so this needs no dependency.
RC = [
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
    0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
    0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
    0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
    0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
    0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
]
ROT = [[0, 36, 3, 41, 18], [1, 44, 10, 45, 2], [62, 6, 43, 15, 61], [28, 55, 25, 21, 56], [27, 20, 39, 8, 14]]
MASK64 = (1 << 64) - 1


def _rol(x: int, n: int) -> int:
    n %= 64
    return ((x << n) | (x >> (64 - n))) & MASK64 if n else x


def _keccak_f(a: list[list[int]]) -> list[list[int]]:
    for rc in RC:
        c = [a[x][0] ^ a[x][1] ^ a[x][2] ^ a[x][3] ^ a[x][4] for x in range(5)]
        d = [c[(x - 1) % 5] ^ _rol(c[(x + 1) % 5], 1) for x in range(5)]
        a = [[a[x][y] ^ d[x] for y in range(5)] for x in range(5)]
        b = [[0] * 5 for _ in range(5)]
        for x in range(5):
            for y in range(5):
                b[y][(2 * x + 3 * y) % 5] = _rol(a[x][y], ROT[x][y])
        a = [[b[x][y] ^ ((~b[(x + 1) % 5][y]) & b[(x + 2) % 5][y]) for y in range(5)] for x in range(5)]
        a[0][0] ^= rc
    return a


def keccak256(data: bytes) -> bytes:
    rate = 136
    buf = bytearray(data)
    buf.append(0x01)
    while len(buf) % rate:
        buf.append(0)
    buf[-1] |= 0x80
    a = [[0] * 5 for _ in range(5)]
    for off in range(0, len(buf), rate):
        block = buf[off : off + rate]
        for i in range(rate // 8):
            a[i % 5][i // 5] ^= int.from_bytes(block[8 * i : 8 * i + 8], "little")
        a = _keccak_f(a)
    return b"".join(a[i % 5][i // 5].to_bytes(8, "little") for i in range(4))


assert keccak256(b"").hex() == "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"

# The Anvil dev mnemonic accounts 0..15. Genesis funds them (config/genesis/dev.toml).
ANVIL_ACCOUNTS = [
    "f39Fd6e51aad88F6F4ce6aB8827279cffFb92266", "70997970C51812dc3A010C7d01b50e0d17dc79C8",
    "3C44CdDdB6a900fa2b585dd299e03d12FA4293BC", "90F79bf6EB2c4f870365E785982E1f101E93b906",
    "15d34AAf54267DB7D7c367839AAf71A00a2C6A65", "9965507D1a55bcC2695C58ba16FB37d819B0A4dc",
    "976EA74026E726554dB657fA54763abd0C3a0aa9", "14dC79964da2C08b23698B3D3cc7Ca32193d9955",
    "23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f", "a0Ee7A142d267C1f36714E4a8F75612F20a79720",
    "Bcd4042DE499D14e55001CcbB24a551F3b954096", "71bE63f3384f5fb98995898A86B02Fb2426c5788",
    "FABB0ac9d68B0B445fB7357272Ff202C5651694a", "1CBd3b2770909D4e10f157cABC84C7264073C9Ec",
    "dF3e18d64BC6A983f673Ab319CCaE4f1a57C7097", "cd3B766CCDd6AE721141F452C550Ca635964ce71",
]
if SHARD_MAP.exists() and len(table) == 256:
    chaos_text = (CLUSTER / "scripts" / "chaos.sh").read_text()
    vslots = [keccak256(bytes.fromhex(a))[7] for a in ANVIL_ACCOUNTS]
    expected_shards = " ".join(str(table[v]) for v in vslots)
    m_acct = re.search(r"^ACCT_SHARD=\(([^)]*)\)", chaos_text, re.M)
    if not m_acct:
        err("scripts/chaos.sh: missing ACCT_SHARD table")
    elif " ".join(m_acct.group(1).split()) != expected_shards:
        err(f"scripts/chaos.sh: ACCT_SHARD=({m_acct.group(1)}) but the shard map gives ({expected_shards})")
    expected_vslots = " ".join(str(v) for v in vslots)
    m_vslot = re.search(r"^ACCT_VSLOT=\(([^)]*)\)", chaos_text, re.M)
    if not m_vslot:
        err("scripts/chaos.sh: missing ACCT_VSLOT table")
    elif " ".join(m_vslot.group(1).split()) != expected_vslots:
        err(f"scripts/chaos.sh: ACCT_VSLOT=({m_vslot.group(1)}) but keccak gives ({expected_vslots})")

    # The sequencer shard's account walk. chaos.sh pins the shard-0 cases to a
    # shard-0 sender and the resize case to a sender that moves under the
    # fewest-moves 2-to-3 render, and burns every skipped account. Walk that
    # allocation over the shard's case list (ci-stages.sh appends the two
    # dynamic-sizing cases), so a table or case-list change that runs out of
    # funded accounts fails here, not two hours into the shard.
    workflow = CLUSTER.parent.parent / ".github" / "workflows" / "cluster-e2e.yml"
    m_shard = re.search(r'chaos-sequencer\)\s*\{([^}]*)\}', workflow.read_text()) if workflow.exists() else None
    m_cases = re.search(r'CHAOS_CASES=([^"]*)"', m_shard.group(1)) if m_shard else None
    if not m_cases:
        err("workflows/cluster-e2e.yml: no chaos-sequencer shard with CHAOS_CASES")
    else:
        cases = m_cases.group(1).split()
        if "sequencer-replica-kill" in cases and "resize-scale-out-in" not in cases:
            cases += ["lookup-blackout", "resize-scale-out-in"]
        run_load = "0" if "RUN_LOAD=0" in m_shard.group(1) else "1"
        next_render = subprocess.run(
            [sys.executable, str(CLUSTER / "scripts" / "render-shard-map.py"), "--from", str(SHARD_MAP), "--lanes", "3"],
            capture_output=True,
            text=True,
        )
        m_next = re.search(r"^table\s*=\s*\[([^\]]*)\]", next_render.stdout, re.M | re.S)
        next_table = [int(x) for x in re.findall(r"\d+", m_next.group(1))] if m_next else table
        shard0 = {"sequencer-replica-kill", "sequencer-lapse", "graceful-sequencer", "hard-sequencer", "lookup-blackout"}

        def moves(acct: int) -> bool:
            return next_table[vslots[acct]] != table[vslots[acct]]

        acct = 7
        for case in cases:
            if case in shard0:
                while acct <= 15 and table[vslots[acct]] != 0:
                    acct += 1
            elif case == "resize-scale-out-in":
                while acct <= 15 and not moves(acct):
                    acct += 1
                if acct > 15 and run_load == "0":
                    acct = next((a for a in range(1, 7) if moves(a)), 16)
            if acct > 15:
                err(f"scripts/chaos.sh: the chaos-sequencer shard runs out of funded accounts at {case} (#{acct} > 15)")
                break
            acct = acct + 1 if acct >= 7 else 16

# --- executor nonce query -----------------------------------------------------------
# Every executor serves the query on ports.executor_nonce_query. Both sequencer
# groups carry the full executor list, derived from node_classes.executor.
nonce_query_port = ports.get("executor_nonce_query", "")
if not nonce_query_port:
    err("group_vars/all.yml: missing ports.executor_nonce_query")
must_contain(
    jobs / "executor.nomad.hcl",
    f'"--nonce-query-addr", "${{meta.node_ip}}:{nonce_query_port}"',
    "executor nonce query address",
)
m_exec_start = re.search(r"^\s{2}executor:\s*\{[^}]*?\bip_start:\s*(\d+)", gv, re.M)
m_exec_count = re.search(r"^\s{2}executor:\s*\{[^}]*?\bcount:\s*(\d+)", gv, re.M)
ip_prefix_for_exec = scalar(gv, "ip_prefix")
if m_exec_start and m_exec_count and ip_prefix_for_exec and nonce_query_port:
    urls = ",".join(
        f"http://{ip_prefix_for_exec}.{int(m_exec_start.group(1)) + i}:{nonce_query_port}"
        for i in range(int(m_exec_count.group(1)))
    )
    flag = f'"--executor-query-endpoints", "{urls}"'
    found = seq_job.count(flag)
    if found != 2:
        err(
            "nomad/sequencer.nomad.hcl: expected both replica groups to pass "
            f"{flag} (found {found})"
        )
else:
    err("group_vars/all.yml: missing node_classes.executor ip_start/count")

# --- config templates -----------------------------------------------------------
# The [discovery] section of channels.toml.tpl mirrors the chain id and reads
# the rest of its scope from the node through Nomad template placeholders.
# The Nomad agent template stamps that node meta, and the Nomad service
# records repeat the chain id and read the same cluster_id meta.
channels_tpl = CLUSTER / "config" / "channels.toml.tpl"
must_contain(channels_tpl, f"chain_id = {chain_id}", "[discovery] chain_id mirror")
for placeholder in (
    'cluster_id = "{{ env "meta.cluster_id" }}"',
    'datacenter = "{{ env "node.datacenter" }}"',
    'advertise_interface = "{{ env "meta.node_ip" }}/32"',
    'tx_data_archive_endpoints = [{{ range service "ingress.kardamom-aeron-archive" }}',
    'tx_deposits_archive_endpoints = [{{ range service "aux.kardamom-aeron-archive" }}',
):
    must_contain(channels_tpl, placeholder, "[discovery] scope comes from the node")
# Every multicast `interface=` value is a placeholder, never an address
# literal: the node's own address comes from meta.node_ip at render time.
for m in re.finditer(r"interface=([^|\"]+)", channels_tpl.read_text()):
    if re.search(r"\d+\.\d+\.\d+\.\d+", m.group(1)):
        err(
            f"{channels_tpl.relative_to(REPO)}: interface={m.group(1)} is an address literal;"
            " use the meta.node_ip placeholder"
        )
must_contain(
    jobs / "aeron.system.nomad.hcl",
    'tags = ["${meta.role}"]',
    "the archive record carries the role tag the channel template selects on",
)
nomad_tpl = CLUSTER / "ansible" / "roles" / "nomad" / "templates" / "nomad.hcl.j2"
for meta_key in ("cluster_id", "node_ip", "archive_topics"):
    must_contain(nomad_tpl, f"    {meta_key} ", f"node meta {meta_key} feeds discovery")
for job in ("aeron.system", "cluster"):
    must_contain(
        jobs / f"{job}.nomad.hcl",
        f'chain_id          = "{chain_id}"',
        "discovery service record chain_id mirror",
    )
    must_contain(
        jobs / f"{job}.nomad.hcl",
        'cluster_id        = "${meta.cluster_id}"',
        "discovery service record cluster_id comes from the node",
    )

# tx_ordering is carried by the Aeron Cluster (Raft). channels.toml.tpl is
# consumed via --log-config by every pipeline service; spot-check the flag is
# actually wired.
for job in ("ingress", "sequencer", "executor", "da-watcher"):
    must_contain(
        jobs / f"{job}.nomad.hcl",
        "--log-config",
        "channels config passed via --log-config (issue #36)",
    )
# Cluster-only: the sequencer publishes tx_ordering to the Aeron Cluster, not via
# an Aeron MDC control endpoint, so there is no --tx-ordering-mdc-control flag.
must_contain(
    CLUSTER / "config" / "genesis" / "dev.toml",
    f"chain_id = {chain_id}",
    "genesis chain id",
)
must_contain(REPO / "chains" / "dev.toml", f"chain_id = {chain_id}", "dev chain id")

# --- scripts --------------------------------------------------------------------
must_contain(
    CLUSTER / "scripts" / "smoke.sh",
    f"http://{ingress_ip}:{ingress_rpc}",
    "default ingress RPC URL",
)
must_contain(
    CLUSTER / "scripts" / "smoke.sh", f"CHAIN_ID:-{chain_id}", "default chain id"
)

# --- versions pinned elsewhere in the repo ---------------------------------------
must_contain(
    REPO / "justfile",
    f'AERON_JAR_VERSION := "{aeron_version}"',
    "Aeron version pin (host-native driver)",
)
must_contain(
    REPO / "crates" / "log" / "docker" / "aeron" / "Dockerfile",
    f"ARG AERON_VERSION={aeron_version}",
    "Aeron version pin (driver image)",
)
must_contain(
    REPO / "justfile",
    f'NOMAD_VERSION := "{nomad_version}"',
    "Nomad version pin (cluster-bootstrap host CLI)",
)
must_contain(
    REPO / "justfile",
    registry,
    "insecure-registry address in cluster-bootstrap/doctor",
)

# --- Aeron Cluster (Raft) sealer ------------------------------------------------
# The 3-member cluster topology is the canonical contract here, but it is mirrored
# as literals in four places that cannot read YAML:
#   - nomad/cluster.nomad.hcl    : the -Dkardamom.cluster.members string + ingressStreamId
#   - config/sequencer.toml.tpl  : the [cluster] ingress_endpoints + stream ids
#   - config/executor.toml       : the [cluster] ingress_endpoints + stream ids
#   - config/ingress.toml        : the [cluster] ingress_endpoints + stream ids
#                                  (on-quorum watermark client)
# Derive the expected member endpoints from node_classes.sealer (ip_start lane on
# ip_prefix) + cluster_member_count + cluster_ports, then assert each mirror agrees.
ip_prefix = scalar(gv, "ip_prefix")
cluster_member_count = scalar(gv, "cluster_member_count")
cluster_ingress_stream_id = scalar(gv, "cluster_ingress_stream_id")
cluster_egress_stream_id = scalar(gv, "cluster_egress_stream_id")
cluster_egress_port = scalar(gv, "cluster_egress_port")

# cluster_ports: indented `key: int` entries under the `cluster_ports:` block.
cp_block = re.search(r"^cluster_ports:\n((?:\s{2}\w+:.*\n?)+)", gv, re.M)
cluster_ports = (
    dict(re.findall(r"^\s{2}(\w+):\s*(\d+)", cp_block.group(1), re.M))
    if cp_block
    else {}
)
for k in ("ingress", "consensus", "log", "catchup", "archive_control"):
    if k not in cluster_ports:
        err(f"group_vars/all.yml: missing cluster_ports.{k}")

# sealer node-class ip_start lane (members at <ip_prefix>.<ip_start + i>).
m_sealer = re.search(r"^\s{2}sealer:\s*\{[^}]*?\bip_start:\s*(\d+)", gv, re.M)
m_sealer_count = re.search(r"^\s{2}sealer:\s*\{[^}]*?\bcount:\s*(\d+)", gv, re.M)
if not m_sealer:
    err("group_vars/all.yml: missing node_classes.sealer.ip_start")
if m_sealer_count and cluster_member_count and m_sealer_count.group(1) != cluster_member_count:
    err(
        "group_vars/all.yml: node_classes.sealer.count "
        f"({m_sealer_count.group(1)}) != cluster_member_count ({cluster_member_count}) "
        "— the cluster runs one Raft member per sealer node"
    )

if (
    ip_prefix
    and m_sealer
    and cluster_member_count.isdigit()
    and all(k in cluster_ports for k in ("ingress", "consensus", "log", "catchup", "archive_control"))
):
    n = int(cluster_member_count)
    ip_start = int(m_sealer.group(1))
    member_ips = [f"{ip_prefix}.{ip_start + i}" for i in range(n)]
    p = cluster_ports
    # Expected -Dkardamom.cluster.members string (id,ingress,consensus,log,catchup,archive|...).
    expected_members = "|".join(
        ",".join(
            [
                str(i),
                f"{ip}:{p['ingress']}",
                f"{ip}:{p['consensus']}",
                f"{ip}:{p['log']}",
                f"{ip}:{p['catchup']}",
                f"{ip}:{p['archive_control']}",
            ]
        )
        for i, ip in enumerate(member_ips)
    )
    must_contain(
        jobs / "cluster.nomad.hcl",
        expected_members,
        "cluster member endpoints (id,ingress,consensus,log,catchup,archive | per member)",
    )
    must_contain(
        jobs / "cluster.nomad.hcl",
        f"-Dkardamom.cluster.ingressStreamId={cluster_ingress_stream_id}",
        "cluster ingress stream id",
    )
    # memberId is derived from the node's own IP (alloc index != node), so the job
    # passes the per-node ${meta.node_ip} rather than a static index→IP mapping.
    must_contain(
        jobs / "cluster.nomad.hcl",
        "-Dkardamom.cluster.nodeIp=${meta.node_ip}",
        "per-node cluster memberId derivation (node IP, not alloc index)",
    )
    # Expected [cluster] ingress_endpoints: "id=ip:ingress,..." (client view).
    expected_endpoints = ",".join(
        f"{i}={ip}:{p['ingress']}" for i, ip in enumerate(member_ips)
    )
    for tpl in ("sequencer.toml.tpl", "executor.toml", "ingress.toml"):
        must_contain(
            CLUSTER / "config" / tpl,
            f'ingress_endpoints = "{expected_endpoints}"',
            f"[cluster] ingress_endpoints in {tpl}",
        )
        must_contain(
            CLUSTER / "config" / tpl,
            f"ingress_stream_id = {cluster_ingress_stream_id}",
            f"[cluster] ingress_stream_id in {tpl}",
        )
        must_contain(
            CLUSTER / "config" / tpl,
            f"egress_stream_id = {cluster_egress_stream_id}",
            f"[cluster] egress_stream_id in {tpl}",
        )
# Per-node egress endpoint is injected by the Nomad job (port differs only by
# node IP), so assert the job carries the flag with the canonical egress port.
# Ingress passes the same flag for its on-quorum watermark cluster client.
for job in ("sequencer", "executor", "ingress"):
    must_contain(
        jobs / f"{job}.nomad.hcl",
        f"${{meta.node_ip}}:{cluster_egress_port}",
        f"per-node cluster egress endpoint (--cluster-egress-endpoint) in {job}",
    )

if errors:
    print(f"check-contract: {len(errors)} mismatch(es) vs ansible/group_vars/all.yml:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)

print("check-contract: all mirrored values agree with ansible/group_vars/all.yml")
