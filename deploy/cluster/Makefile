# kardamom multi-node cluster orchestration.
# See README.md. All values derive from ansible/group_vars/all.yml
# (`make check-contract` verifies the mirrors below stay in sync).

SHELL := /bin/bash
REGISTRY := 192.168.56.10:5000
NOMAD_ADDR := http://192.168.56.10:4646
TAG := dev
# Per-deploy image digest manifest (attested-identity P0.1): written at push
# time by ansible/images.yml, read by ansible/deploy.yml to pin every job's
# image_ref variable to repo@sha256:... . Gitignored (audit record, not source).
DIGEST_MANIFEST := images.digests
export REGISTRY TAG DIGEST_MANIFEST
# The Rust service binaries that get their own image. There is NO kardamom-
# sealer binary (canonical ordering is the Java Aeron Cluster — see the
# `cluster` image below) and NO kardamom-recorder (durability is archive-at-
# the-sealer, folded into the cluster members).
SERVICES := ingress sequencer executor validator da-watcher batcher

.PHONY: up vms provision images deploy smoke status down clean validate check-contract

## up: full bring-up — VMs, provisioning, images, deploy. Run `make smoke`
## separately to exercise the pipeline once the jobs settle.
up: vms provision images deploy
	@echo
	@echo "==> Cluster is up. Run 'make smoke' to exercise the pipeline."

## vms: boot the VMs (one per node_classes instance)
vms:
	vagrant up

## provision: install docker/nomad/consul/registry via Ansible
provision:
	ansible-playbook -i ansible/inventory.ini ansible/bootstrap.yml

## images: build service + aeron images on the host and push to the registry.
## NOTE: pushing needs $(REGISTRY) in the HOST Docker daemon's
## insecure-registries (see README "Host prerequisites").
images:
	ansible-playbook -i localhost, ansible/images.yml

## deploy: converge workload jobs and settlement through Ansible
deploy:
	ansible-playbook -i localhost, ansible/deploy.yml

## smoke: submit transfers through ingress and assert receipts
smoke:
	./scripts/smoke.sh

## status: cluster + job health (Nomad binds the VM IP, not localhost, so the
## CLI inside the VM needs NOMAD_ADDR pointed at it)
status:
	vagrant ssh control-0 -c 'export NOMAD_ADDR=$(NOMAD_ADDR); nomad node status && echo && nomad job status && echo && consul members'

## validate: static checks (run on a host with the toolchain; CI runs these too)
validate:
	@rc=0; \
	yamllint ansible/ || rc=1; \
	ansible-lint ansible/ || rc=1; \
	for s in scripts/*.sh; do bash -n "$$s" || rc=1; done; \
	for j in nomad/*.hcl; do echo "== $$j =="; nomad job validate "$$j" || rc=1; done; \
	./scripts/check-contract.py || rc=1; \
	exit $$rc

## check-contract: verify the values mirrored from group_vars/all.yml agree
check-contract:
	./scripts/check-contract.py

## down: stop jobs and destroy VMs
down:
	vagrant destroy -f

## clean: remove local built images (+ the staged cluster jar)
clean:
	-docker image rm $(REGISTRY)/kardamom-aeron:$(TAG)
	-docker image rm $(REGISTRY)/kardamom-cluster:$(TAG)
	-rm -f docker/kardamom-cluster-node.jar
	-@for svc in $(SERVICES); do docker image rm $(REGISTRY)/kardamom-$$svc:$(TAG); done

.PHONY: container-up container-down container-reset
## Container lifecycle (Linux controller; prebuilt artifacts required).
container-up:
	ansible-playbook -i localhost, ansible/run.yml -e cluster_run_keep=true
container-down:
	ansible-playbook -i localhost, ansible/run.yml -e cluster_run_operation=down
container-reset:
	ansible-playbook -i localhost, ansible/run.yml -e cluster_run_operation=reset -e cluster_run_keep=true

.PHONY: local
local:
	ansible-playbook -i localhost, ansible/local.yml
