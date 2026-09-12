# kardamom multi-node cluster orchestration.
# See README.md. All values derive from ansible/group_vars/all.yml
# (`make check-contract` verifies the mirrors below stay in sync).

SHELL := /bin/bash
# The registry is a Consul service; every node resolves the name. The host
# pushes through the control node (REGISTRY_PUSH_NODE), where it resolves.
REGISTRY := registry.service.consul:5000
NOMAD_HTTP := 4646
# The control node address comes from the node contract of the container
# root at run time. Nothing in this tree names an address.
NODE_CONTRACT := terraform/containers/node-contract.json
CONTROL_ADDRESS_CMD := python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["nodes"]["control-0"]["ip"])' $(NODE_CONTRACT)
CONTROL_ADDRESS = $(shell $(CONTROL_ADDRESS_CMD) 2>/dev/null)
# Without a contract (the VM path) NOMAD_ADDR stays empty and the role uses
# its own default.
NOMAD_ADDR ?= $(if $(CONTROL_ADDRESS),http://$(CONTROL_ADDRESS):$(NOMAD_HTTP))
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

## status: cluster + job health, from the control node's own agents
status:
	vagrant ssh control-0 -c 'nomad node status && echo && nomad job status && echo && consul members'

## validate: static checks (run on a host with the toolchain; CI runs these too)
validate:
	@rc=0; \
	yamllint ansible/ || rc=1; \
	ansible-lint ansible/ || rc=1; \
	for s in scripts/*.sh; do bash -n "$$s" || rc=1; done; \
	for j in nomad/*.hcl; do echo "== $$j =="; nomad job validate "$$j" || rc=1; done; \
	./scripts/check-contract.py || rc=1; \
	for r in terraform/*/; do echo "== $$r =="; tofu -chdir=$$r fmt -check -recursive . || rc=1; \
	  tofu -chdir=$$r init -backend=false -input=false >/dev/null && tofu -chdir=$$r validate || rc=1; done; \
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

# --- container path (Linux controller; prebuilt artifacts required) ---------
# terraform/containers owns the node containers. The Makefile owns the
# lifecycle: apply, converge, test, diagnose, destroy. CI runs these targets
# as workflow steps; ansible/local.yml runs them inside the orchestrator.
TF_CONTAINERS := terraform/containers
# On the container path, images are pushed from inside the control node,
# where the registry name resolves; the host daemon needs no
# insecure-registry entry. The VM path pushes from the host.
container-up: export REGISTRY_PUSH_NODE ?= control-0
# Extra vars for ansible/cluster.yml, as one JSON object without a single quote.
CLUSTER_VARS :=
CLUSTER_VARS_ARG = $(if $(CLUSTER_VARS),--extra-vars '$(CLUSTER_VARS)')

.PHONY: container-up container-test container-diagnostics container-down container-reset
## container-up: create the node containers, provision them, publish the
## prebuilt images and deploy the workloads.
container-up:
	tofu -chdir=$(TF_CONTAINERS) init -input=false
	tofu -chdir=$(TF_CONTAINERS) apply -auto-approve -input=false
	tofu -chdir=$(TF_CONTAINERS) output -json node_contract > $(NODE_CONTRACT)
	NOMAD_ADDR=http://$$($(CONTROL_ADDRESS_CMD)):$(NOMAD_HTTP) \
	  ansible-playbook -i localhost, ansible/cluster.yml $(CLUSTER_VARS_ARG)

## container-test: the smoke, load and chaos gates (RUN_LOAD, RUN_CHAOS, ...)
container-test:
	NOMAD_ADDR=$(NOMAD_ADDR) bash ./scripts/run-tests.sh

## container-diagnostics: collect node and job state after a failure
container-diagnostics:
	NOMAD_ADDR=$(NOMAD_ADDR) bash ./scripts/ci-diagnostics.sh

## container-down: destroy the node containers and their volumes
container-down:
	tofu -chdir=$(TF_CONTAINERS) init -input=false
	tofu -chdir=$(TF_CONTAINERS) destroy -auto-approve -input=false
	rm -f $(NODE_CONTRACT)

## container-reset: destroy, then create a fresh chain
container-reset: container-down container-up

.PHONY: local
local:
	ansible-playbook -i localhost, ansible/local.yml
