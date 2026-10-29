#!/usr/bin/env python3
"""Build actor tests once, then execute exact tests with bounded shard budgets."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from test_bundle import ExecutableBundle
import time
import tomllib

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "docs/reviews/2026-10-28-component-recovery.json"
TEST_SECONDS = 60
SHARD_SECONDS = 240
SHARDS = 4


class Bundle(ExecutableBundle):
    @staticmethod
    def selection(catalog):
        return {name: suite for name, suite in catalog["local_suites"].items()
                if suite.get("ci_mode") == "in-process"}

    @staticmethod
    def target(suite):
        command = suite["command"]
        package = command[command.index("-p") + 1]
        target = command[command.index("--test") + 1] if "--test" in command else package.replace("-", "_")
        return package, target

    @classmethod
    def build_command(cls, suites):
        packages = sorted({cls.target(suite)[0] for suite in suites.values()})
        tests = sorted({cls.target(suite)[1] for suite in suites.values() if "--test" in suite["command"]})
        return ["cargo", "test", "--locked", "--no-run", "--message-format=json-render-diagnostics", "--lib",
                *[arg for package in packages for arg in ["-p", package]],
                *[arg for target in tests for arg in ["--test", target]]]

    @staticmethod
    def resolve(expected, names):
        matches = [name for name in names if name == expected or name.endswith("::" + expected)]
        if len(matches) != 1:
            raise ValueError(f"expected exactly one executable test for {expected}, found {matches}")
        return matches[0]

    def prepare(self, artifacts_file=None):
        suites = self.selection(json.loads(CATALOG.read_text()))
        if not suites:
            raise ValueError("the in-process recovery inventory is empty")
        self.directory.mkdir(parents=True, exist_ok=False)
        log = self.directory / "build.jsonl"
        if artifacts_file is None:
            with log.open("w") as output:
                subprocess.run(self.build_command(suites), cwd=ROOT, stdout=output, check=True)
        else:
            shutil.copyfile(artifacts_file, log)
        artifacts, paths = {}, []
        for item in self.build_messages(log):
            if item["reason"] == "build-script-executed":
                paths.extend(path.removeprefix("native=") for path in item["linked_paths"])
            if item["reason"] == "compiler-artifact" and item["profile"]["test"] and item.get("executable"):
                package = tomllib.loads(Path(item["manifest_path"]).read_text())["package"]["name"]
                artifacts[package, item["target"]["name"]] = Path(item["executable"])
        selected = {artifacts[self.target(suite)] for suite in suites.values()}
        libraries = self.libraries(selected, paths)
        tests, binaries = [], {}
        for suite_name, suite in suites.items():
            source = artifacts[self.target(suite)]
            names = self.names(source)
            binary = source.name
            if binary not in binaries:
                binaries[binary] = self.executable(source, binary)
            tests.extend({"suite": suite_name, "test": self.resolve(test, names), "binary": binary}
                         for test in suite["tests"])
        tests.sort(key=lambda test: (test["suite"], test["test"]))
        identities = {(test["binary"], test["test"]) for test in tests}
        if len(identities) != len(tests):
            raise ValueError("one executable test appears in more than one suite")
        manifest = {"revision": os.environ.get("GITHUB_SHA"), "shards": SHARDS,
                    "test_seconds": TEST_SECONDS, "shard_seconds": SHARD_SECONDS,
                    "binaries": binaries, "libraries": libraries, "tests": tests}
        (self.directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Prepared {len(tests)} tests in {len(binaries)} binaries for {SHARDS} shards", flush=True)

    def load(self):
        manifest = super().load()
        if not manifest["tests"] or manifest["shards"] != SHARDS:
            raise ValueError("empty test bundle or incompatible shard plan")
        return manifest

    def run(self, shard, output):
        started = time.monotonic()
        manifest = self.load()
        if not 0 <= shard < SHARDS:
            raise ValueError(f"shard must be in 0..{SHARDS - 1}")
        tests = manifest["tests"][shard::SHARDS]
        if not tests:
            raise ValueError("an empty shard cannot pass")
        output.mkdir(parents=True, exist_ok=False)
        results = []
        for index, test in enumerate(tests):
            remaining = SHARD_SECONDS - (time.monotonic() - started)
            log = output / f"{index:03}.log"
            if remaining <= 0:
                results.append(dict(test, success=False, timed_out=True, reason="shard budget exhausted"))
                continue
            command = [str(self.directory / test["binary"]), "--exact", test["test"],
                       "--nocapture", "--test-threads=1"]
            code, timed_out, seconds = self.execute(command, log, min(TEST_SECONDS, remaining), self.environment())
            text = log.read_text()
            passed = re.findall(r"^test ([\w:]+) \.\.\. ok$", text, re.MULTILINE)
            # Rust can print captured tracing between the test name and its verdict.
            summary = "test result: ok. 1 passed; 0 failed; 0 ignored;" in text
            success = (code == 0 and not timed_out and seconds <= TEST_SECONDS
                       and summary and (passed == [test["test"]] or not passed))
            results.append(dict(test, success=success, exit_code=code, timed_out=timed_out,
                                seconds=round(seconds, 3), log=str(log)))
            print(f"shard {shard}: {test['suite']}::{test['test']}: {'PASS' if success else 'FAIL'} ({seconds:.3f}s)", flush=True)
        elapsed = time.monotonic() - started
        report = {"revision": manifest["revision"], "shard": shard, "seconds": round(elapsed, 3),
                  "test_seconds": TEST_SECONDS, "shard_seconds": SHARD_SECONDS, "tests": results,
                  "success": all(test["success"] for test in results) and elapsed <= SHARD_SECONDS}
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        return report["success"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["prepare", "run", "run-all"])
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--artifacts", type=Path, help="Cargo JSON output from a shared build")
    parser.add_argument("--shard", type=int)
    args = parser.parse_args()
    bundle = Bundle(args.bundle)
    if args.mode == "prepare":
        bundle.prepare(args.artifacts)
        return
    if args.output is None or (args.mode == "run" and args.shard is None):
        parser.error("execution needs --output, and run needs --shard")
    if args.mode == "run":
        success = bundle.run(args.shard, args.output)
    else:
        # Local verification uses the same partition and execution path as CI.
        with ThreadPoolExecutor(max_workers=SHARDS) as pool:
            success = all(list(pool.map(lambda shard: bundle.run(shard, args.output / str(shard)), range(SHARDS))))
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
