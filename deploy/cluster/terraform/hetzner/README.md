# Hetzner root

The Terraform (OpenTofu) root of the hybrid deployment. See
[the specification](../../../../docs/agents/hetzner-hybrid-infra-spec.md).

## What it owns

- The Cloud Network, its cloud subnet for the elastic VMs, and its vswitch
  subnet attached to the Robot vSwitch of the dedicated core.
- One spread placement group and one public firewall per elastic pool.
- The SSH keys of the elastic VMs.
- The Consul bootstrap DNS records in the Hetzner DNS zone.
- The public RPC load balancer and its proxy targets.
- The pool contract: a versioned description of each pool that
  `ansible/autoscaler.yml` consumes.

## What it does not own

- The elastic VMs. The Nomad Autoscaler creates and deletes them. The
  contract check refuses a `hcloud_server` resource in this root.
- The dedicated servers and the Robot vSwitch. They are inventory inputs.
- The API token. Export `HCLOUD_TOKEN`; the token never enters a
  variable, the plan or the state.

## Use

```sh
cd deploy/cluster/terraform/hetzner
cp terraform.tfvars.example ../../../../../kardamom-prod.tfvars   # outside the repo
export HCLOUD_TOKEN=...
tofu init
tofu plan -var-file ../../../../../kardamom-prod.tfvars
tofu apply -var-file ../../../../../kardamom-prod.tfvars
tofu output -json pool_contract > pool-contract.json
```

`pool-contract.json` holds no secret. Pass it to
`ansible-playbook autoscaler.yml -e autoscaler_pool_contract_file=.../pool-contract.json`.
A Terraform apply never changes the number of running VMs.

## Checks

```sh
tofu fmt -check -recursive .
tofu validate
tofu test
```

The tests run against a mocked provider. They check the subnet
containment and non-overlap, the pool bounds against the placement group
limit, the pool id mapping, the DNS records and that the user data
carries no credential.
