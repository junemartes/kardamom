# deploy-rollback

## Cause

A release runs that must not run: a deploy failed part of the way, or a
deploy succeeded and the release degrades the chain after it.

## Confirm

1. Read the deploy record. The Nomad variable `kardamom/deploys/<env>` is
   the record. `deployed/<env>/attempt.json` is its mirror on the controller.

   ```sh
   nomad var get -out=go-template -template='{{ index .Items "attempt" }}' kardamom/deploys/<env> | jq .
   ```

   - `status: started` with no running deploy: the attempt failed. `changed`
     lists the jobs it registered, in order. The jobs after the last one in
     `changed` still run the accepted release.
   - `status: accepted`: the last deploy completed. The release runs on every
     job in `changed`.
   - `status: rolled_back`: the release is already rolled back.
   - `before.jobs.<job>.before` is the Nomad job version that ran before the
     release. `after` is the version the release registered.
   - `floor` is not `{}` when the release wrote a one-way format. A rollback
     below it is not safe.
2. Read `kardamom_chainStatus` on an ingress. A root or a pause names the
   failing service. Follow its runbook when the cause is the environment and
   not the release. A deploy cannot tell the two apart; neither can a
   rollback.
3. Decide. Roll back when:
   - the deploy failed and the fix is not ready;
   - the release crashes, fails readiness, or degrades after a successful
     deploy;
   - the validator raised a divergence verdict after an executor change.

   Roll a failed attempt back before you deploy a fix. A deploy over a
   failed attempt records the mixed versions that run, and a rollback of it
   goes back to that mixed state.

## Steps

1. Run the rollback from a checkout with the same `NOMAD_ADDR`,
   `NOMAD_TOKEN` and `NOMAD_NAMESPACE` as the deploy:

   ```sh
   just rollback <env>
   ```

   It reverts every job in `changed`, last job first, to its `before`
   version, with `EnforcePriorVersion` set to its `after` version. It waits
   for each job as the deploy does. The sealer rolls back member by member,
   followers first, under the same readiness checks. A job that the release
   added stops.
2. When it refuses with `Nomad no longer holds the pre-release version of
   <job>`, Nomad dropped the version. The rollback wrote
   `deployed/<env>/rollback.digests`, the manifest of the release before.
   Run:

   ```sh
   just rollback-rerender <env>
   ```

   It is a normal rolling deploy of the older images with the job files of
   the checkout. An old binary may not accept a flag that the checkout adds.
   Check out the revision of `before.revision` first when the job files
   changed.
3. When it refuses with `A rollback below this release is not safe`, the
   release has a rollback floor: it writes a format version that the release
   before cannot read. Confirm, for each format in `floor.formats`, that no
   writer wrote the new version. The rules per format are in
   `docs/formats.md`. Only then:

   ```sh
   KARDAMOM_ROLLBACK_BELOW_FLOOR=<ids> just rollback <env>
   ```

4. When it refuses with `is already rolled back`, the record holds no older
   versions. A deeper rollback is a deploy of an older manifest:
   `DIGEST_MANIFEST=<file> just deploy`.
5. When the sealer reverse roll stops with `The sealer has no settled leader
   or a member is down`, a member of the release does not run. The roll
   needs every member and one leader. Do not revert the whole cluster job:
   it replaces the members at once and the cluster loses its quorum. Bring
   the member back, or follow `sealer-fleet-rebuild.md`.
6. If the EnforcePriorVersion check fails (`400` from Nomad), another deploy
   changed the job after the release. Read the record and the job versions
   again before you continue.

## Clear

The rollback writes the record: the attempt gets `status: rolled_back`, and
`accepted` describes the release that runs again, with `restored_from` set.
Confirm:

1. `nomad job history <job>` shows the pre-release version as the newest
   one for each job in `changed`.
2. `kardamom_chainStatus` shows no root and no pause, and the heads advance.
3. The next deploy compares its formats with the restored release.

## The refusals of the release gate

A deploy refuses a release before it changes a job. Each message starts
with `Refused:`.

| Message | Cause | What to do |
|---|---|---|
| `the chain stands on a halt or a pause` | `kardamom_chainStatus` lists a root, or a pause stands on the sealer, the ingress or a service. | Follow the runbook of the root. Resume the paused service. |
| `the registry ... does not hold the image of <services>` | A digest of the manifest is not in the registry. | `just images`, or fix the manifest. |
| `a rolling deploy cannot carry a coordinated format change: <ids>` | The accepted release and the target cannot run as a mixed fleet. | Stop the writers, or follow the runbook of the format in `docs/formats.md`. |
| `the release writes a version that the accepted release cannot read: <ids>` | A one-way format change. A rollback is not safe after the first write. | `KARDAMOM_ALLOW_ONE_WAY=<ids> just deploy`. The record then carries the floor. |
| `KARDAMOM_ALLOW_ONE_WAY names <ids>, and the release has no one-way change of it` | A stale allowance. | Unset it. |
| `a rolling deploy cannot change a sealer setting that every member must match: <settings>` | A change of `daLagBudgetBlocks`, `voidVoters`, `remoteOrigins`, `orderingWindow` or another setting of the list. | A coordinated restart of the sealer. |
| `config/shard-map.toml ... differs from the shard map of the registered ingress` | A routing change in the rolling path. | `kardamom-cluster scale-sequencers`. |
