#!/usr/bin/env python3
"""Build once and run every full-stack chain-semantics test on one shard."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from test_bundle import ExecutableBundle, ROOT

SHARDS = 4
SHARD_SECONDS = 12 * 60
TEST_BINARY = "bin/deps/chain_semantics"
SERVICES = {
    "ingress", "sequencer", "executor", "validator", "statecheck", "da-watcher",
    "l1-indexer", "batcher", "reconstruct", "notifier", "canary", "egress", "semantics",
}


class SemanticsBundle(ExecutableBundle):
    @staticmethod
    def build_command():
        packages = {"kardamom-" + name for name in SERVICES - {"statecheck", "semantics"}}
        packages.update({"kardamom-state", "e2e"})
        return ["cargo", "build", "--locked", "--test", "chain_semantics",
                "--features", "e2e/full-pipeline-e2e", "--message-format=json-render-diagnostics",
                *[arg for package in sorted(packages) for arg in ["-p", package]],
                *[arg for name in sorted(SERVICES) for arg in ["--bin", "kardamom-" + name]]]

    def environment(self):
        return super().environment() | {
            "KARDAMOM_AERON_ALL_JAR": str(self.directory / "aeron-all.jar"),
            "KARDAMOM_CLUSTER_JAR": str(self.directory / "sealer.jar"),
        }

    @staticmethod
    def partition(names):
        if len(names) < SHARDS or len(set(names)) != len(names):
            raise ValueError("the semantics inventory must have distinct tests and no empty shard")
        ordered = sorted(names)
        return [ordered[index::SHARDS] for index in range(SHARDS)]

    def prepare(self, artifacts=None):
        self.directory.mkdir(parents=True, exist_ok=False)
        if artifacts is None:
            artifacts = self.directory / "build.jsonl"
            with artifacts.open("w") as output:
                subprocess.run(self.build_command(), cwd=ROOT, stdout=output, check=True)
        messages = self.build_messages(artifacts)
        selected, paths = {}, []
        for item in messages:
            if item["reason"] == "build-script-executed":
                paths.extend(path.removeprefix("native=") for path in item["linked_paths"])
            if item["reason"] != "compiler-artifact" or not item.get("executable"):
                continue
            target = item["target"]
            if target["kind"] == ["bin"] and not item["profile"]["test"]:
                selected["bin/" + target["name"]] = Path(item["executable"])
            if target["name"] == "chain_semantics" and item["profile"]["test"]:
                selected[TEST_BINARY] = Path(item["executable"])
        required = {"bin/kardamom-" + name for name in SERVICES} | {TEST_BINARY}
        if missing := required - selected.keys():
            raise ValueError(f"missing semantics build artifacts: {sorted(missing)}")
        selected = {name: source for name, source in selected.items() if name in required}
        libraries = self.libraries(selected.values(), paths)
        binaries = {name: self.executable(source, name) for name, source in selected.items()}
        assets = {}
        for name, source in {
            "aeron-all.jar": ROOT / "crates/log/docker/aeron/aeron-all.jar",
            "sealer.jar": ROOT / "cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar",
        }.items():
            shutil.copy2(source, self.directory / name)
            assets[name] = self.digest(self.directory / name)
        tests = self.partition(self.names(self.directory / TEST_BINARY))
        manifest = {"revision": os.environ.get("GITHUB_SHA"), "tests": tests,
                    "shards": SHARDS, "binaries": binaries, "libraries": libraries, "assets": assets}
        (self.directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Prepared {sum(map(len, tests))} semantics tests for {SHARDS} shards", flush=True)

    def load(self):
        manifest = super().load()
        names = [name for shard in manifest["tests"] for name in shard]
        if manifest["shards"] != SHARDS or manifest["tests"] != self.partition(names):
            raise ValueError("incomplete or inconsistent semantics shard plan")
        if sorted(self.names(self.directory / TEST_BINARY)) != sorted(names):
            raise ValueError("the shard plan does not cover the compiled semantics suite")
        return manifest

    def run(self, shard, output):
        manifest = self.load()
        if not 0 <= shard < SHARDS:
            raise ValueError(f"shard must be in 0..{SHARDS - 1}")
        missing = [name for name in ("anvil", "java") if not shutil.which(name)]
        if missing:
            raise ValueError(f"semantics runtime tools missing: {missing}")
        names = manifest["tests"][shard]
        output.mkdir(parents=True, exist_ok=False)
        log = output / "tests.log"
        command = [str(self.directory / TEST_BINARY), "--ignored", "--exact", *names,
                   "--nocapture", "--test-threads=1"]
        code, timed_out, seconds = self.execute(command, log, SHARD_SECONDS, self.environment())
        text = log.read_text()
        executed = re.findall(r"^test ([\w:]+) \.\.\.", text, re.MULTILINE)
        summary = f"test result: ok. {len(names)} passed; 0 failed; 0 ignored;"
        success = (code == 0 and not timed_out and seconds <= SHARD_SECONDS and summary in text
                   and sorted(executed) == sorted(names) and "SKIP:" not in text)
        report = {"revision": manifest["revision"], "shard": shard, "tests": names,
                  "seconds": round(seconds, 3), "success": success,
                  "exit_code": code, "timed_out": timed_out}
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        print(text, end="", flush=True)
        return success


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["prepare", "run"])
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--shard", type=int)
    args = parser.parse_args()
    bundle = SemanticsBundle(args.bundle)
    if args.mode == "prepare":
        bundle.prepare(args.artifacts)
    else:
        raise SystemExit(0 if bundle.run(args.shard, args.output) else 1)


if __name__ == "__main__":
    main()
