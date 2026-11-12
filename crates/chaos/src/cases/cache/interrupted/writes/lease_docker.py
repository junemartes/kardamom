#!/usr/bin/python3
"""Run the lease commands with a writable task mount and a read-only image root."""
import json
import os
from pathlib import Path
import sys

root = Path(os.environ["LEASE_FIXTURE"])
state_file = root / "state.json"
state = json.loads(state_file.read_text())
args = sys.argv[1:]

def acl(command):
    state["allowed"] = "+eval" in command
    state["changes"].append(state["allowed"])
    state_file.write_text(json.dumps(state))
    print("OK")

if Path(sys.argv[0]).name == "redis-cli":
    acl(args)
    sys.exit(0)

assert args.pop(0) == "exec"
detached = args[0] == "-d"
if detached:
    args.pop(0)
args.pop(0)
if detached:
    assert args[:2] == ["sh", "-c"]
    state["workers"].append(args[2].replace("/local/", str(root / "local") + "/"))
    state_file.write_text(json.dumps(state))
elif args[0] in ("touch", "rm"):
    marker = args[-1]
    if not marker.startswith("/local/"):
        print("Read-only file system: " + marker, file=sys.stderr)
        sys.exit(1)
    destination = root / marker.removeprefix("/")
    if args[0] == "touch":
        destination.touch()
    else:
        destination.unlink(missing_ok=True)
elif args[0] == "redis-cli":
    acl(args[1:])
else:
    raise ValueError(args)
