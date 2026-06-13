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

# Node name -> ip from the cluster_nodes mapping.
nodes = dict(re.findall(r"^\s{2}(\w+):\n\s{4}ip:\s*([\d.]+)", gv, re.M))
if sorted(nodes) != ["r1", "r2", "r3", "w1", "w2"]:
    err(f"group_vars/all.yml: unexpected cluster_nodes set: {sorted(nodes)}")

# --- Vagrantfile + inventory mirror the node IPs -----------------------------
for name, ip in nodes.items():
    must_contain(CLUSTER / "Vagrantfile", f'"{ip}"', f"static IP of {name}")
    must_contain(
        CLUSTER / "ansible" / "inventory.ini",
        f"{name} ansible_host={ip}",
        f"inventory entry for {name}",
    )

# --- Makefile -----------------------------------------------------------------
must_contain(CLUSTER / "Makefile", f"REGISTRY := {registry}", "registry host:port")
must_contain(CLUSTER / "Makefile", f"NOMAD_ADDR := {nomad_addr}", "nomad HTTP API")
must_contain(CLUSTER / "Makefile", f"TAG := {image_tag}", "image tag")

# --- Nomad job specs ------------------------------------------------------------
jobs = CLUSTER / "nomad"
for svc in ("ingress", "sequencer", "executor", "sealer", "da-watcher", "batcher"):
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
# The recorder image (issue #38) is shared by the record + aggregate jobs.
for job in ("recorder.system.nomad.hcl", "quorum.nomad.hcl"):
    must_contain(
        jobs / job,
        f"{registry}/kardamom-recorder:{image_tag}",
        "recorder image ref",
    )
# recorder must be in the Makefile's build/push SERVICES list, else its image
# is never built.
must_contain(CLUSTER / "Makefile", "recorder", "recorder in Makefile SERVICES")
must_contain(jobs / "anvil.nomad.hcl", f'"{anvil_l1}"', "anvil L1 port")
must_contain(
    jobs / "da-watcher.nomad.hcl",
    f"http://{control_ip}:{anvil_l1}",
    "L1 RPC endpoint (anvil on the control node)",
)
must_contain(jobs / "ingress.nomad.hcl", f"static = {ingress_rpc}", "ingress RPC port")
must_contain(jobs / "executor.nomad.hcl", f'"{chain_id}"', "L2 chain id")

# --- config templates -----------------------------------------------------------
# The sealer overrides tx_ordering from its own channel_b_uri, so it MUST be
# byte-identical to channels.toml.tpl's tx_ordering_channel (else the sealer
# publishes to a different group than the sequencers/recorders subscribe).
import re as _re


def _extract(path, pattern, label):
    rel = path.relative_to(REPO)
    if not path.exists():
        err(f"{rel}: file missing ({label})")
        return None
    m = _re.search(pattern, path.read_text(), _re.M)
    if not m:
        err(f"{rel}: could not find {label}")
        return None
    return m.group(1).strip()


sealer_b = _extract(
    CLUSTER / "config" / "sealer.toml.tpl",
    r'^channel_b_uri\s*=\s*"([^"]+)"',
    "sealer channel_b_uri",
)
channels_ordering = _extract(
    CLUSTER / "config" / "channels.toml.tpl",
    r'^tx_ordering_channel\s*=\s*"([^"]+)"',
    "channels tx_ordering_channel",
)
if sealer_b is not None and channels_ordering is not None and sealer_b != channels_ordering:
    err(
        "sealer channel_b_uri != channels tx_ordering_channel "
        f"({sealer_b!r} vs {channels_ordering!r}) — the sealer would publish "
        "tx_ordering to a different channel than subscribers/recorders use"
    )
# channels.toml.tpl is consumed via --log-config by every pipeline service +
# the recorder/quorum jobs; spot-check the flag is actually wired.
for job in ("ingress", "sequencer", "executor", "sealer", "da-watcher"):
    must_contain(
        jobs / f"{job}.nomad.hcl",
        "--log-config",
        "channels config passed via --log-config (issue #36)",
    )
must_contain(
    CLUSTER / "config" / "genesis" / "dev.toml",
    f"chain_id = {chain_id}",
    "genesis chain id",
)
must_contain(REPO / "chains" / "dev.toml", f"chain_id = {chain_id}", "dev chain id")

# --- scripts --------------------------------------------------------------------
must_contain(
    CLUSTER / "scripts" / "deploy.sh", nomad_addr, "default NOMAD_ADDR"
)
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

if errors:
    print(f"check-contract: {len(errors)} mismatch(es) vs ansible/group_vars/all.yml:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)

print("check-contract: all mirrored values agree with ansible/group_vars/all.yml")
