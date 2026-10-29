# Node software is installed by the same tasks as ordinary bootstrap.
# Runtime provisioning supplies node identity, configuration and mounts.
FROM geerlingguy/docker-ubuntu2204-ansible:latest
COPY ansible/node-image.yml ansible/node-software.yml /opt/node-build/
COPY ansible/group_vars/all.yml /opt/node-build/group_vars/all.yml
COPY ansible/roles/common/tasks/install.yml /opt/node-build/roles/common/tasks/install.yml
COPY ansible/roles/docker/tasks/install.yml /opt/node-build/roles/docker/tasks/install.yml
COPY ansible/roles/consul/tasks/install.yml /opt/node-build/roles/consul/tasks/install.yml
COPY ansible/roles/nomad/tasks/install.yml /opt/node-build/roles/nomad/tasks/install.yml
RUN cd /opt/node-build && \
    ansible-playbook -i localhost, node-image.yml && \
    rm -rf /opt/node-build /var/lib/apt/lists/*
