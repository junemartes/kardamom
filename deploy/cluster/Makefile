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
# The test suite and the operator commands are the kardamom-chaos crate.
# A shard test brings its own cluster up; the container-* targets do the
# same lifecycle by hand and run the gates against it (KARDAMOM_CHAOS_REUSE).
# KARDAMOM_STAGED=1 runs the binaries a build already staged under
# target/release (scripts/ci/stage-cluster-dist.sh): the shard test
# executable as kardamom-chaos-shards and the operator binary. CI's shard
# runners carry no toolchain. Unset, cargo builds and runs them.
ifdef KARDAMOM_STAGED
RUN_SHARDS := ../../target/release/kardamom-chaos-shards
CLUSTER_BIN := ../../target/release/kardamom-cluster
else
RUN_SHARDS := cargo test -p kardamom-chaos --release --features cluster-e2e --locked --test shards --
CLUSTER_BIN := cargo run --release -p kardamom-chaos --locked --bin kardamom-cluster --
endif
# The shard to run: load, semantics, chaos-executor, chaos-ingress,
# chaos-sequencer, chaos-cluster, chaos-retention, chaos-cache.
SHARD ?= load
SHARD_TEST = $(subst -,_,$(SHARD))
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

.PHONY: images deploy smoke clean validate check-contract

## images: wrap the prebuilt service binaries and the sealer jar into images
## and push them to the in-cluster registry through REGISTRY_PUSH_NODE.
images:
	ansible-playbook -i localhost, ansible/images.yml

## deploy: converge workload jobs and settlement through Ansible
deploy:
	ansible-playbook -i localhost, ansible/deploy.yml

## smoke: one signed transfer through the ingress, with a receipt poll
## (RPC_URL overrides the ingress-0 address of the node contract)
smoke:
	$(CLUSTER_BIN) smoke $(if $(RPC_URL),--rpc $(RPC_URL))

## validate: static checks (run on a host with the toolchain; CI runs these too)
validate:
	@rc=0; \
	yamllint ansible/ || rc=1; \
	ansible-lint ansible/ || rc=1; \
	for j in nomad/*.hcl; do echo "== $$j =="; nomad job validate "$$j" || rc=1; done; \
	./scripts/check-contract.py || rc=1; \
	for r in terraform/*/; do echo "== $$r =="; tofu -chdir=$$r fmt -check -recursive . || rc=1; \
	  tofu -chdir=$$r init -backend=false -input=false >/dev/null && tofu -chdir=$$r validate || rc=1; done; \
	exit $$rc

## check-contract: verify the values mirrored from group_vars/all.yml agree
check-contract:
	./scripts/check-contract.py

## clean: remove local built images (+ the staged cluster jar)
clean:
	-docker image rm $(REGISTRY)/kardamom-aeron:$(TAG)
	-docker image rm $(REGISTRY)/kardamom-cluster:$(TAG)
	-rm -f docker/kardamom-cluster-node.jar
	-@for svc in $(SERVICES); do docker image rm $(REGISTRY)/kardamom-$$svc:$(TAG); done

# --- container path (Linux controller; prebuilt artifacts required) ---------
# terraform/containers owns the node containers. The Makefile owns the
# lifecycle: apply, converge, test, diagnose, destroy. CI runs these targets
# as workflow steps.
TF_CONTAINERS := terraform/containers
# On the container path, images are pushed from inside the control node,
# where the registry name resolves; the host daemon needs no
# insecure-registry entry. The VM path pushes from the host.
container-up: export REGISTRY_PUSH_NODE ?= control-0
# Extra vars for ansible/cluster.yml, as one JSON object without a single quote.
CLUSTER_VARS :=
CLUSTER_VARS_ARG = $(if $(CLUSTER_VARS),--extra-vars '$(CLUSTER_VARS)')

.PHONY: container-up container-test shard container-diagnostics container-down container-reset
## container-up: create the node containers, provision them, publish the
## prebuilt images and deploy the workloads.
container-up:
	tofu -chdir=$(TF_CONTAINERS) init -input=false
	tofu -chdir=$(TF_CONTAINERS) apply -auto-approve -input=false
	tofu -chdir=$(TF_CONTAINERS) output -json node_contract > $(NODE_CONTRACT)
	NOMAD_ADDR=http://$$($(CONTROL_ADDRESS_CMD)):$(NOMAD_HTTP) \
	  ansible-playbook -i localhost, ansible/cluster.yml $(CLUSTER_VARS_ARG)

## container-test: run one shard's gates (SHARD=...) against the cluster
## container-up made; KARDAMOM_CHAOS_CASES narrows a chaos shard's cases.
container-test:
	KARDAMOM_CHAOS_REUSE=1 $(RUN_SHARDS) --ignored --exact $(SHARD_TEST) --nocapture

## shard: one shard end to end, the way CI runs it: the test brings the
## cluster up, runs its gates, and destroys the cluster on success.
shard:
	$(RUN_SHARDS) --ignored --exact $(SHARD_TEST) --nocapture

## container-diagnostics: collect node and job state after a failure
container-diagnostics:
	$(CLUSTER_BIN) diagnostics

## container-down: destroy the node containers and their volumes
container-down:
	tofu -chdir=$(TF_CONTAINERS) init -input=false
	tofu -chdir=$(TF_CONTAINERS) destroy -auto-approve -input=false
	rm -f $(NODE_CONTRACT)

## container-reset: destroy, then create a fresh chain
container-reset: container-down container-up
