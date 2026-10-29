#!/usr/bin/env python3
"""Build Docker recovery tests once and run exact cases from the shared bundle."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import subprocess

from component_tests import Bundle, SHARDS


class DependencyBundle(Bundle):
    ci_mode = "dependencies"
    include_ignored = True
    test_seconds = 180
    shard_seconds = 12 * 60

    @classmethod
    def build_command(cls, suites):
        features = set()
        for suite in suites.values():
            command = suite["command"]
            if "--features" in command:
                package = cls.target(suite)[0]
                features.update(f"{package}/{feature}" for feature in
                                command[command.index("--features") + 1].split(","))
        return super().build_command(suites) + ["--features", ",".join(sorted(features))]

    def run(self, shard, output):
        missing = [name for name in ("docker", "anvil") if not shutil.which(name)]
        if missing:
            raise ValueError(f"dependency runtime tools missing: {missing}")
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL)
        return super().run(shard, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["prepare", "run", "run-all"])
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--shard", type=int)
    args = parser.parse_args()
    bundle = DependencyBundle(args.bundle)
    if args.mode == "prepare":
        bundle.prepare(args.artifacts)
        return
    if args.output is None or (args.mode == "run" and args.shard is None):
        parser.error("execution needs --output, and run needs --shard")
    if args.mode == "run":
        success = bundle.run(args.shard, args.output)
    else:
        with ThreadPoolExecutor(max_workers=SHARDS) as pool:
            success = all(list(pool.map(lambda shard: bundle.run(shard, args.output / str(shard)), range(SHARDS))))
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
