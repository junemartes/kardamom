# Runbooks

One file per `RecoveryId` of `kardamom_obs::halt`. A halted service names its
runbook in the `recovery` label of `kardamom_halt`, in the `/halt` record on its
metrics port, and in the alert annotation. A unit test in `crates/obs` asserts
that every `RecoveryId` has a file here with the four sections below.

Every runbook has the same shape:

- `## Cause`: what happened, in one sentence.
- `## Confirm`: how to see that this is the cause, not another one.
- `## Steps`: what to do, in order.
- `## Clear`: how the halt ends.

The metrics ports of the deploy (`deploy/cluster/nomad`): batcher 9002,
da-watcher 9005, ingress and validator 9006, l1-indexer 9009. The `/halt` route
and the `POST /halt/clear` command sit beside `/metrics` on that port. The clear
accepts a loopback peer only, so run it on the service's node:

```sh
curl -s http://127.0.0.1:<port>/halt
curl -s -X POST http://127.0.0.1:<port>/halt/clear
```

An `auto` halt needs no clear: the service retries its cause and resumes when the
cause goes. A clear of an `auto` halt is harmless; the service raises it again
on the next failed retry.
