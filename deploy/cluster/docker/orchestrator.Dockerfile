# =============================================================================
# orchestrator.Dockerfile — "runner surrogate" for running the container
# cluster-e2e LOCALLY (deploy/cluster/ansible/local.yml), the dev-machine
# analogue of the GitHub runner in .github/workflows/cluster-e2e.yml.
# =============================================================================
#
# On a GitHub runner, ansible/run.yml runs directly on the host: it has the docker
# CLI talking to the host daemon, ansible + the community.docker connection
# plugin, and the nomad CLI. On a dev machine the "host" is the Docker Desktop
# VM, which we can't get a Linux shell on directly — so we run ansible/run.yml
# from THIS image instead, started with:
#
#   docker run --privileged --network=host --pid=host \
#       -v /var/run/docker.sock:/var/run/docker.sock -v <repo>:/work ...
#
# `--network=host` puts it in the VM's network namespace (so the kardamom-net
# bridge + node containers it creates via the socket are reachable and its
# sysctl/sysfs writes hit the same kernel ansible/run.yml expects on a runner),
# and the mounted socket makes the node containers siblings on the VM daemon —
# exactly the topology ansible/run.yml assumes on a runner.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

# docker.io for the CLI (the daemon is unused — we drive the mounted socket);
# ansible + python3-docker for the community.docker connection plugin; the rest
# are what ansible/run.yml / Ansible deployment / smoke.sh shell out to.
RUN apt-get update && apt-get install -y --no-install-recommends \
        docker.io docker-buildx python3-venv python3-requests \
        curl unzip sudo iproute2 jq ca-certificates iptables kmod git \
    && rm -rf /var/lib/apt/lists/*

# Use a supported controller version instead of Ubuntu's older Ansible package.
RUN python3 -m venv /opt/ansible && \
    /opt/ansible/bin/pip install --no-cache-dir 'ansible-core>=2.18,<2.19' requests
ENV PATH=/opt/ansible/bin:${PATH}

# Same connection plugin the cluster-e2e workflow installs.
RUN ansible-galaxy collection install ansible.posix community.docker >/dev/null

# Nomad CLI — keep the version in sync with ansible/group_vars/all.yml
# (nomad_version). Arch-dynamic so the image builds on amd64 and arm64.
ARG NOMAD_VERSION=1.9.5
RUN arch="$(dpkg --print-architecture)" && \
    curl -fsSL "https://releases.hashicorp.com/nomad/${NOMAD_VERSION}/nomad_${NOMAD_VERSION}_linux_${arch}.zip" \
        -o /tmp/nomad.zip && \
    unzip -o /tmp/nomad.zip -d /usr/local/bin && rm /tmp/nomad.zip

# foundry `cast` for smoke.sh's preferred Path A (signed eth_sendRawTransaction +
# receipt poll). The cluster-e2e workflow installs this via foundry-toolchain on
# the runner; bundle it here so the LOCAL smoke matches CI instead of falling back
# to smoke.sh's curl path, whose hardcoded pre-signed RAW_TX only covers account
# #0 — it can't sign from the per-check dedicated accounts (#16/#17, via PK) the
# churn re-smokes use. foundryup downloads a prebuilt binary for the image arch
# (amd64/arm64).
RUN curl -L https://foundry.paradigm.xyz | bash && \
    /root/.foundry/bin/foundryup && \
    install -m0755 /root/.foundry/bin/cast /usr/local/bin/cast

WORKDIR /work
ENTRYPOINT ["sleep", "infinity"]
