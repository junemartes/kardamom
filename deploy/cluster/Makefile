# kardamom multi-node cluster orchestration.
# See README.md. All values derive from ansible/group_vars/all.yml.

SHELL := /bin/bash
REGISTRY := 192.168.56.11:5000
TAG := dev
# Workspace root (this file lives in deploy/cluster/).
ROOT := $(abspath ../..)
SERVICES := ingress sequencer executor sealer da-watcher batcher

.PHONY: up vms provision images deploy smoke status down clean validate

## up: full bring-up — VMs, provisioning, images, deploy, smoke test
up: vms provision images deploy smoke

## vms: boot the 5 VMs
vms:
	vagrant up

## provision: install docker/nomad/consul/registry via Ansible
provision:
	ansible-playbook -i ansible/inventory.ini ansible/site.yml

## images: build service + aeron images on the host and push to the registry
images:
	docker build -f docker/aeron.Dockerfile -t $(REGISTRY)/kardamom-aeron:$(TAG) $(ROOT)/crates/log/docker/aeron
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

## status: cluster + job health
status:
	vagrant ssh r1 -c 'nomad node status && echo && nomad job status && echo && consul members'

## validate: static checks (run on a host with the toolchain)
validate:
	yamllint ansible/ || true
	ansible-lint ansible/ || true
	@for j in nomad/*.hcl; do echo "== $$j =="; nomad job validate $$j || true; done

## down: stop jobs and destroy VMs
down:
	-vagrant ssh r1 -c 'for j in $$(nomad job status -short 2>/dev/null | awk "NR>1{print \$$1}"); do nomad job stop -purge $$j; done' || true
	vagrant destroy -f

## clean: remove local built images
clean:
	-docker image rm $(REGISTRY)/kardamom-aeron:$(TAG)
	-@for svc in $(SERVICES); do docker image rm $(REGISTRY)/kardamom-$$svc:$(TAG); done
