#!/usr/bin/python3
"""A table fixture for running the exact fault and cleanup shell scripts."""

import json
import os
from pathlib import Path
import sys

store = Path(os.environ["LOOKUP_TABLE"])
table = json.loads(store.read_text())
args = sys.argv[3:]
failure = os.environ.get("LOOKUP_FAIL", "")
if failure and " ".join(args).startswith(failure):
    sys.exit(1)
operation, chain = args[:2]
rule = args[2:]
if rule and rule[0].isdigit():
    rule = rule[1:]

if operation == "-N":
    if chain in table:
        sys.exit(1)
    table[chain] = []
elif chain not in table:
    sys.exit(1)
elif operation in ("-A", "-I"):
    table[chain].append(rule)
elif operation == "-C":
    sys.exit(0 if rule in table[chain] else 1)
elif operation == "-D":
    table[chain].remove(rule)
elif operation == "-F":
    table[chain] = []
elif operation == "-X":
    del table[chain]
elif operation not in ("-S", "-Z"):
    raise ValueError(args)
store.write_text(json.dumps(table))
