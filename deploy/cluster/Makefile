# kardamom multi-node cluster orchestration.
# See README.md. All values derive from ansible/group_vars/all.yml
# (`make check-contract` verifies the mirrors below stay in sync).

SHELL := /bin/bash
REGISTRY := 192.168.56.10:5000
NOMAD_ADDR := http://192.168.56.10:4646
TAG := dev
# Workspace root (this file lives in deploy/cluster/).
ROOT := $(abspath ../..)
# kardamom-recorder removed: durability is archive-at-the-sealer (the sealer
# image carries the --archive-durability sidecar).
SERVICES := ingress sequencer executor sealer da-watcher batcher

.PHONY: up images deploy smoke status down clean validate check-contract

## up: bring up the container cluster + run the e2e (scripts/local-cluster.sh).
## Works on macOS Docker Desktop and Linux Docker — no VMs. `KEEP=1 make up`
## leaves the cluster running to inspect.
up:
	./scripts/local-cluster.sh

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

## deploy: run the Aeron system job, then the service jobs (sealer first — it
deploy:
	./scripts/deploy.sh

## smoke: submit transfers through ingress and assert receipts
smoke:
	./scripts/smoke.sh

## status: container + Nomad/Consul health (cluster must be up — e.g. KEEP=1 make up)
status:
	@docker ps --filter name=kardamom- --format 'table {{.Names}}\t{{.Status}}'
	@docker exec kardamom-control-0 bash -lc 'export NOMAD_ADDR=$(NOMAD_ADDR); nomad node status && echo && nomad job status && echo && consul members' 2>/dev/null || echo "(control node not up; bring the cluster up first, e.g. KEEP=1 make up)"

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

## down: tear down the container cluster (containers + bridge network)
down:
	-docker rm -f $$(docker ps -aq --filter name=kardamom-) 2>/dev/null || true
	-docker network rm kardamom-net 2>/dev/null || true

## clean: remove local built images
clean:
	-docker image rm $(REGISTRY)/kardamom-aeron:$(TAG)
	-@for svc in $(SERVICES); do docker image rm $(REGISTRY)/kardamom-$$svc:$(TAG); done
