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
   - `rolled_back` lists the jobs a rollback has reverted so far.
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
   changed. After a dead attempt the re-render is a deploy over an attempt
   that is still `started`, so it needs `KARDAMOM_REPLACE_ATTEMPT=1`.
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
6. A job whose registered definition already equals the pre-release
   version is done: Nomad reverted it itself (`auto_revert` on the ingress
   and the sequencer), or an earlier run of the rollback did. The play
   reports it and moves on. When the job is at another version and another
   content, the play refuses with `Another change moved the job after the
   release`. Read the record and `nomad job history <job>` before you
   continue.
7. A rollback that stops resumes. Each job joins `rolled_back` in the record
   when it is done; the next `just rollback <env>` skips those jobs. The
   sealer is the exception: a reverse roll that stops between two members
   leaves the groups mixed, so `cluster` is neither at the release nor at
   the pre-release definition, and the resume refuses it with `Another
   change moved the job`. Finish the roll by hand: register the pre-release
   definition (`nomad job history -p -version <before> cluster`) one group
   at a time, as `roll_step.yml` does, followers first and the leader
   last, and wait for each member's `/ready`. Never revert the whole job:
   the members would restart at once. Then run `just rollback <env>` again
   for the jobs that remain.
8. When it refuses with `The deploy record ... changed under this run`,
   another controller writes the record of the environment at the same
   time. Let it finish, then read the record again.

### Rollback after the activation of the sealer rules

`FEATURE_SEALER_V3` is on for good on staging. A new network sends `ETHLockbox.initiateUpgrade(2, 0)` once after its first deploy. Every sealer member of this release runs decision version 3 from its start (`cluster/sealer-service/README.md`, "Decision version"). Off is no longer supported: `just sealer-upgrade-check <env> 2 <activation_ms>` refuses every activation time other than 0.

- A rollback of the sealer to the release before this one is a normal rolling rollback. That release reads snapshot version 14, holds the activation, and runs version 3 while the feature stays on. Do not send an off transaction of feature 2.
- Do not roll the sealer back past that release. A release that reads only up to snapshot version 13 stops on version 14. Fix forward.
- Never deploy a build whose sealer job names `decisionVersion`. The gate refuses a registered job that names `decisionVersion=3`.

### Rollback after the activation of the nonce burn

From the first block that executes at or after the activation time of `FEATURE_SEALER_V3` ("When a flag switches" in `docs/failure-modes.md`), a skipped transaction at the chain nonce of its sender uses that nonce ("A skipped transaction and its nonce" in `docs/failure-modes.md`). The executor, the validator and the zk guest of the prover run the rule; the ingress and the sequencer read it. A release before this one does not run it.

- After the activation, do not roll an executor, a validator, an ingress or a sequencer back to a release without the job meta `nonce_burn = "true"`. An older executor or validator does not use the nonce of a skip, so its state forks from the chain at the first such skip. An older ingress or sequencer takes such a skip as a skip that used no nonce. Fix forward.
- `just sealer-upgrade-check <env> <feature> <activation_ms>` refuses the upgrade transaction until every live allocation of the executor, validator, ingress and sequencer jobs runs a job version with the meta `nonce_burn`.
- The prover's zk guest links the same rule, so a guest of this release has a new verifying key. Roll the prover and its verifying key before the activation. A guest of an older release cannot prove a block with a skip after the activation.

### Rollback after the install of the chain state code

On a genesis without the `KardamomChainState` code (the staging genesis), the first `setFeature` system deposit installs the code before its call ("A genesis without the chain-state code" in `docs/failure-modes.md`). A release before this one runs that deposit as a call to an empty account.

- Do not roll an executor or a validator back to a release without the job meta `chain_state_install = "true"` while it can execute the block of that deposit: before the deposit, or in a replay or a rebuild from L1 that crosses it. Its state forks at that deposit. A node that starts after that block reads the code from its state, so the rule does not touch it.
- `just sealer-upgrade-check <env> <feature> <activation_ms>` refuses the upgrade transaction until every live allocation of the executor and validator jobs runs a job version with the meta `chain_state_install`.
- The prover's zk guest links the install too. Roll the prover and its verifying key before the first upgrade transaction.

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
| `the attempt of <time> by <operator> is still started` | The last attempt runs, or died. | Let it finish. If it died: `just rollback <env>`, or `KARDAMOM_REPLACE_ATTEMPT=1 just deploy` to deploy over it. |
| `the chain stands on a halt or a pause` | `kardamom_chainStatus` lists a root, or a pause stands on the sealer, the ingress or a service. When Nomad does not know the sealer job, the gate excuses `sealer_no_quorum` and the upstream pauses on it. | Follow the runbook of the root. Resume the paused service. |
| `the ingress job is registered and no allocation of it runs` | The chain status cannot be read: a failed release left the ingress down. | Bring the ingress back, or `just rollback <env>`. |
| `KARDAMOM_REGISTRY_URL=off skips the image check, and the <profile> profile does not allow that` | Only the local profile skips the image check. | Set `KARDAMOM_REGISTRY_URL` to the registry API. |
| `the registry ... does not hold the image of <services>` | A digest of the manifest is not in the registry. | `just images`, or fix the manifest. |
| `a rolling deploy cannot carry a coordinated format change: <ids>` | The accepted release and the target cannot run as a mixed fleet. | Stop the writers, or follow the runbook of the format in `docs/formats.md`. |
| `the release writes a version that the accepted release cannot read: <ids>` | A one-way format change. A rollback is not safe after the first write. | `KARDAMOM_ALLOW_ONE_WAY=<ids> just deploy`. The record then carries the floor. |
| `KARDAMOM_ALLOW_ONE_WAY names <ids>, and the release has no one-way change of it` | A stale allowance. | Unset it. |
| `a rolling deploy cannot carry a coordinated format change: <ids>`, with `set KARDAMOM_ALLOW_RETIRED=<ids>` | The release retires a format that is neither shared nor permanent. | Only when the release removes the service that owned the format: `KARDAMOM_ALLOW_RETIRED=<ids> just deploy`. For a renamed format of a running service, take the coordinated path. See `docs/formats.md`. |
| `KARDAMOM_ALLOW_RETIRED names <ids>, and the release retires no format of that id ...` | A stale allowance, or a shared or permanent format. | Unset it, or remove the id. |
| `a rolling deploy cannot change a sealer setting that every member must match: <settings>` | A change of `daLagBudgetBlocks`, `voidVoters`, `remoteOrigins`, `orderingWindow` or another setting of the list. | A coordinated restart of the sealer. A documented procedure, such as the return from a seeded start, names the settings in `KARDAMOM_ALLOW_MUST_MATCH=<settings>`. |
| `config/shard-map.toml ... differs from the shard map of the registered ingress` | A routing change in the rolling path. | `kardamom-cluster scale-sequencers`. |
| `the target release reads sealer snapshots only up to version <N> ..., and these sealer members hold a newer snapshot: <member> at version <M>` | The `snapshotLatest` of a member is above the sealer snapshot `reads_max` in the `formats.toml` of the target. After the roll that member cannot restore its own snapshot, and stops. | Deploy a release that reads snapshot version `<M>`. Do not roll the sealer back below it. Fix forward. |
| `the sealer members <members> do not run, so the gate cannot read the version of their newest snapshot` | The release changes the sealer job, and a configured sealer member has no running allocation. A release that does not change the sealer job passes. | Start the member and let it catch up, then deploy. When the member cannot restore its snapshot, follow `sealer-fleet-rebuild.md`. |
| `the target lowers the sealer retention from <old> to <new> frames` | A rolling deploy only raises `KARDAMOM_CLUSTER_RETENTION`. A member with the lower window answers more cursors with `REPLAY_UNAVAILABLE`. | Keep the value, or raise it. Lower it only on purpose: `KARDAMOM_ALLOW_MUST_MATCH=retention just deploy`. |
| `the sealer retention <n> frames at the mean frame size of the members ... is above the cap of 50331648 bytes` | A replay of the whole window would pass the 64 MiB backlog limit of a cluster session. | Set at most the value that the message names. A higher rate needs the retained file or a change to the replay path. |
| `KARDAMOM_SEALER_VERSION_FLOOR is <n>, and Nomad does not know the sealer job` | A new chain starts on decision version 3. | Deploy a new chain with the floor 0. |
| `the sealer members <members> do not run, so the gate cannot read their decision version` | The floor changes, and a member has no running allocation. | Start the member and let it catch up, then deploy. |
| `KARDAMOM_SEALER_VERSION_FLOOR is <n>, and a sealer member runs decision version <v>` | A floor above the lowest `decisionVersion` of the members. | Set the floor to the lowest `decisionVersion`. Before a fleet rebuild from a seed, set 0. |
| `the <job> job does not run a relayed full reference (job meta relayed_full_ref)` | A floor above 0 needs every executor, validator, batcher and egress of the release that reads a relayed full reference. | Deploy the release to every reader with the floor 0 first. |
| `the egress job does not keep the mark of the highest stored reference (job meta ref_mark)` | An egress of an older release runs, for example after a rollback of the egress. Its mark of the highest stored reference can be too low. | Deploy the release of the egress. Then follow "After a rollback of the egress" in `carry-window-rollout.md`. |
| `an egress instance does not report its reference mark` | The gate needs R from every egress instance, and one does not answer on its metrics port. | Bring the instance back, then deploy. |
| `the <reader> moves to the egress, and its block <blocks> ... is not above the block <b> of the last stored reference` | Below that block the egress range holds a reference that the reader cannot join. | Wait until the reader passes the block, then deploy. |
| `the executor groups <groups> do not run on the sealer nodes` | `KARDAMOM_CARRY_EXECUTORS` lists an executor that still runs on the executor nodes. | Deploy with `KARDAMOM_EXEC_ON_SEALERS=on` first. |
| `the executor job does not stop on a reference that no source can join (job meta carried_ref_fault)` | The registered executor is of an older release. | Deploy the release with the carry executors unchanged first. |
| `a carry executor needs the rules of the carry window (feature 5) on every member` | A member does not run, or reports `carryWindow` false. | Move the executors only after the switch of feature 5. |
| `<executor> moves to carry mode, and its member <member> reports the feed off` | The member of the executor runs no feed. | Deploy with `KARDAMOM_MEMBER_FEED=on` first. |
| `<executor> moves to carry mode, and its committed block ... minus 32 is not above the block <b>` | At its first carry start the executor compares the stream with its committed blocks of that window, and the window still holds a reference. | Wait until the executor passes the block, then deploy. |
| `KARDAMOM_MEMBER_FEED and KARDAMOM_EXEC_ON_SEALERS are on, off or empty` | A bad value, the executors on the sealer nodes without the feed, or an executor count other than the sealer count. | Fix the switches. Turn on the feed first. |
| `KARDAMOM_CARRY_EXECUTORS (...) is empty or a list of distinct member indexes` | A bad list, or a list without `KARDAMOM_EXEC_ON_SEALERS=on` and `KARDAMOM_EGRESS=1`, or with `KARDAMOM_EXEC_CURSOR=on`. | Fix the list or the switches. |
| `KARDAMOM_SEALER_VERSION_FLOOR (...) is empty or a decision version from 0 to 5` | A bad value. | Set empty, or a value from 0 to 5. |
| `KARDAMOM_CLUSTER_RETENTION (...) is empty or a frame count from 1 to 131072` | A value above the cap in frames. A replay of the whole window would pass the backlog limit of a cluster session. | Set at most 131072. A higher rate needs the retained file or a change to the replay path. |

The last four rows come from the preflight, before the release gate. Their messages do not start with `Refused:`.

The upgrade check (`just sealer-upgrade-check`) refuses with these messages too:

| Message | Cause | What to do |
|---|---|---|
| `the upgrade transaction of feature <3 or 4> with the activation time ... can turn decision version 5 off, and a member holds feature 5` | Without decision version 5 the rules of feature 5 stop, and a carry executor stops on the first reference. | Turn feature 5 off first, and empty `KARDAMOM_CARRY_EXECUTORS`. |
| `the upgrade transaction of FEATURE_CARRY_WINDOW (feature id 5) needs every sealer member to read snapshot version 18` | A member of an older release. | Deploy the release to every member. |
| `feature 5 needs decision version 5 on every member` | Features 3 and 4 are not on. A member below version 5 ignores the record (`EARLY`). | Activate features 3 and 4 first. |
| `feature 5 needs the retained frames on the heap of every member` | A member with the retained file never stores the record (`UNSUPPORTED`). | The chain cannot take feature 5. |
| `the sequencer job does not keep the highest sealer version across its sessions (job meta sealer_version_kept)` | A sequencer of an older release runs. It can send a reference without bytes after a new session. | Deploy the release to every sequencer. |
| `the <reader> job does not read the canonical stream from the egress (job meta reads_egress)` | The batcher or the validator still reads the cluster egress. Under the carry window it gets `REPLAY_UNAVAILABLE` below the window. | Do step 4 of `carry-window-rollout.md` first. |
| `the egress job does not keep the mark of the highest stored reference (job meta ref_mark)` | An egress of an older release runs. | Deploy the release of the egress. Then follow "After a rollback of the egress" in `carry-window-rollout.md`. |
| `an egress instance does not report its reference mark` | An egress instance does not answer on its metrics port. | Bring the instance back, then run the check again. |
| `the <reader> cursors <cursors> ... are not all above the index <r> of the last stored reference` | A reader cursor at or below R can still meet a reference in the egress range. | Wait until every reader passes R. |
