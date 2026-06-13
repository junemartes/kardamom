# kardamom multi-node cluster orchestration.
# See README.md. All values derive from ansible/group_vars/all.yml
# (`make check-contract` verifies the mirrors below stay in sync).

SHELL := /bin/bash
REGISTRY := 192.168.56.11:5000
NOMAD_ADDR := http://192.168.56.11:4646
TAG := dev
# Workspace root (this file lives in deploy/cluster/).
ROOT := $(abspath ../..)
SERVICES := ingress sequencer executor sealer da-watcher batcher recorder

.PHONY: up vms provision images deploy smoke status down clean validate check-contract

## up: full bring-up — VMs, provisioning, images, deploy. Run `make smoke`
## separately: until the --log-config plumbing lands (issue #36) the services
## use single-host IPC defaults, so the cross-host pipeline smoke test is
## EXPECTED TO FAIL (see README "Required service changes").
up: vms provision images deploy
	@echo
	@echo "==> Cluster is up. Run 'make smoke' to exercise the pipeline."
	@echo "    NOTE: smoke is expected to FAIL until the channels-config plumbing"
	@echo "    lands (issue #36) — see README 'Required service changes'."

## vms: boot the 5 VMs
vms:
	vagrant up

## provision: install docker/nomad/consul/registry via Ansible
provision:
	ansible-playbook -i ansible/inventory.ini ansible/site.yml

## images: build service + aeron images on the host and push to the registry.
## NOTE: pushing needs $(REGISTRY) in the HOST Docker daemon's
## insecure-registries (see README "Host prerequisites").
images:
	docker build -f $(ROOT)/crates/log/docker/aeron/Dockerfile \
		-t $(REGISTRY)/kardamom-aeron:$(TAG) $(ROOT)/crates/log/docker/aeron
	docker push $(REGISTRY)/kardamom-aeron:$(TAG)
	@for svc in $(SERVICES); do \
		echo "==> building kardamom-$$svc"; \
		docker build -f docker/service.Dockerfile \
			--build-arg BIN=kardamom-$$svc \
			-t $(REGISTRY)/kardamom-$$svc:$(TAG) $(ROOT) || exit 1; \
		docker push $(REGISTRY)/kardamom-$$svc:$(TAG) || exit 1; \
	done

## deploy: run the Aeron system jobs, wait for quorum, then the service jobs
deploy:
	./scripts/deploy.sh

## smoke: submit transfers through ingress and assert receipts
smoke:
	./scripts/smoke.sh

## status: cluster + job health (Nomad binds the VM IP, not localhost, so the
## CLI inside the VM needs NOMAD_ADDR pointed at it)
status:
	vagrant ssh r1 -c 'export NOMAD_ADDR=$(NOMAD_ADDR); nomad node status && echo && nomad job status && echo && consul members'

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
	-vagrant ssh r1 -c 'export NOMAD_ADDR=$(NOMAD_ADDR); for j in $$(nomad job status -short 2>/dev/null | awk "NR>1{print \$$1}"); do nomad job stop -purge $$j; done' || true
	vagrant destroy -f

## clean: remove local built images
clean:
	-docker image rm $(REGISTRY)/kardamom-aeron:$(TAG)
	-@for svc in $(SERVICES); do docker image rm $(REGISTRY)/kardamom-$$svc:$(TAG); done
