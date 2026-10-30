#!/usr/bin/env python3
"""Build the component recovery tests once, then run fixed shards planned from measured runtimes.

Every catalogued suite has a ci_mode:
- in-process: a Cargo test binary with no external service.
- docker: a Cargo test binary that starts real containers.
- gradle: a JUnit method of the Java sealer.

The build job packages the Cargo binaries. The sealer job packages the Java test
runtime. Each shard job runs its planned tests from these two bundles and
compiles nothing.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
import tomllib
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "scripts/ci/component-recovery.json"
TIMINGS = ROOT / "scripts/ci/component-timings.json"
SEALER = ROOT / "cluster/sealer-service"
SEALER_RUNTIME = SEALER / "service/build/component-runtime"
TEST_SECONDS = 60
SHARD_SECONDS = 6 * 60
SHARDS = 4
CARGO_MODES = ("in-process", "docker")
# The estimate for a test with no measured runtime yet.
DEFAULT_SECONDS = {"in-process": 5.0, "docker": 30.0, "gradle": 15.0}
# The registry images that the docker suites start. A shard pulls them once before its first test.
IMAGES = ("redis:7-alpine", "python:3.13-alpine", "hashicorp/consul:1.21")
# The image that the docker suites build from the Aeron image context.
BUILT_IMAGES = {"kardamom-aeron:test": ROOT / "crates/log/docker/aeron"}


class Plan:
    """The fixed shard plan: longest measured test first, onto the least loaded shard."""

    def __init__(self, catalog, timings):
        self.suites = {name: suite for name, suite in catalog["local_suites"].items() if "ci_mode" in suite}
        self.timings = timings

    @classmethod
    def load(cls):
        return cls(json.loads(CATALOG.read_text()), json.loads(TIMINGS.read_text()))

    def tests(self, modes=None):
        return [{"suite": name, "test": test, "mode": suite["ci_mode"]}
                for name, suite in self.suites.items() if modes is None or suite["ci_mode"] in modes
                for test in suite["tests"]]

    def estimate(self, test):
        return self.timings.get(f"{test['suite']}::{test['test']}", DEFAULT_SECONDS[test["mode"]])

    def shards(self):
        ordered = sorted(self.tests(), key=lambda test: (-self.estimate(test), test["suite"], test["test"]))
        shards = [[] for _ in range(SHARDS)]
        loads = [0.0] * SHARDS
        for test in ordered:
            self.place(test, shards, loads)
        return [sorted(shard, key=lambda test: (test["mode"], test["suite"], test["test"])) for shard in shards]

    def place(self, test, shards, loads):
        index = loads.index(min(loads))
        shards[index].append(test)
        loads[index] += self.estimate(test)

    def describe(self):
        return "\n".join(f"shard {index}: {len(shard)} tests, estimate {sum(map(self.estimate, shard)):.0f}s"
                         for index, shard in enumerate(self.shards()))


class Bundle:
    """The Cargo test binaries of every in-process and docker suite, with their native libraries."""

    def __init__(self, directory):
        self.directory = directory.resolve()

    @staticmethod
    def selection(catalog):
        return {name: suite for name, suite in catalog["local_suites"].items()
                if suite.get("ci_mode") in CARGO_MODES}

    @staticmethod
    def selector(suite):
        return next((flag for flag in ("--test", "--bin") if flag in suite["command"]), None)

    @classmethod
    def target(cls, suite):
        command = suite["command"]
        package = command[command.index("-p") + 1]
        selector = cls.selector(suite)
        target = command[command.index(selector) + 1] if selector else package.replace("-", "_")
        return package, target

    @staticmethod
    def features(suite):
        command = suite["command"]
        package = command[command.index("-p") + 1]
        named = command[command.index("--features") + 1].split(",") if "--features" in command else []
        return {f"{package}/{feature}" for feature in named}

    @classmethod
    def build_command(cls, suites):
        packages = sorted({cls.target(suite)[0] for suite in suites.values()})
        targets = sorted({(cls.selector(suite), cls.target(suite)[1]) for suite in suites.values()
                          if cls.selector(suite)})
        features = sorted(set().union(*map(cls.features, suites.values())))
        return ["cargo", "test", "--locked", "--no-run", "--message-format=json-render-diagnostics", "--lib",
                *[arg for package in packages for arg in ["-p", package]],
                *[arg for target in targets for arg in target],
                *(["--features", ",".join(features)] if features else [])]

    @staticmethod
    def resolve(expected, names):
        matches = [name for name in names if name == expected or name.endswith("::" + expected)]
        if len(matches) != 1:
            raise ValueError(f"expected exactly one executable test for {expected}, found {matches}")
        return matches[0]

    @staticmethod
    def digest(path):
        with path.open("rb") as source:
            return hashlib.file_digest(source, "sha256").hexdigest()

    def environment(self):
        return dict(os.environ, RUST_BACKTRACE="1", LD_LIBRARY_PATH=str(self.directory / "native"))

    def libraries(self, artifacts, paths):
        native = self.directory / "native"
        native.mkdir()
        environment = dict(os.environ, LD_LIBRARY_PATH=os.pathsep.join(paths))
        libraries = {}
        for source in artifacts:
            libraries |= self.copy_libraries(source, environment, libraries)
        return libraries

    def copy_libraries(self, source, environment, known):
        listing = subprocess.check_output(["ldd", source], text=True, env=environment)
        if "not found" in listing:
            raise ValueError(f"missing native library for {source}: {listing}")
        found = [Path(match[1]) for match in re.finditer(r"=> (/\S+)", listing)]
        local = [library for library in found
                 if not (library.is_relative_to("/lib") or library.is_relative_to("/usr/lib"))]
        copied = {}
        for library in local:
            name, digest = "native/" + library.name, self.digest(library)
            if known.get(name, digest) != digest or copied.get(name, digest) != digest:
                raise ValueError(f"conflicting native library: {name}")
            shutil.copy2(library, self.directory / name)
            copied[name] = digest
        return copied

    @staticmethod
    def build_messages(log):
        messages = [json.loads(line) for line in log.read_text().splitlines()]
        finished = [item for item in messages if item["reason"] == "build-finished"]
        if len(finished) != 1 or not finished[0]["success"]:
            raise ValueError("artifact packaging requires one successful completed Cargo build")
        return messages

    def names(self, binary):
        listing = subprocess.check_output([binary, "--list", "--format", "terse"],
                                          text=True, timeout=30, env=self.environment())
        return [line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")]

    def executable(self, source):
        destination = self.directory / source.name
        shutil.copy2(source, destination)
        subprocess.run(["strip", "--strip-debug", destination], check=True)
        return self.digest(destination)

    def artifacts(self, log):
        artifacts, paths = {}, []
        for item in self.build_messages(log):
            if item["reason"] == "build-script-executed":
                paths.extend(path.removeprefix("native=") for path in item["linked_paths"])
            if item["reason"] == "compiler-artifact" and item["profile"]["test"] and item.get("executable"):
                package = tomllib.loads(Path(item["manifest_path"]).read_text())["package"]["name"]
                artifacts[package, item["target"]["name"]] = Path(item["executable"])
        return artifacts, paths

    def prepare(self, artifacts_file=None):
        suites = self.selection(json.loads(CATALOG.read_text()))
        if not suites:
            raise ValueError("the Cargo recovery inventory is empty")
        self.directory.mkdir(parents=True, exist_ok=False)
        log = self.directory / "build.jsonl"
        if artifacts_file is None:
            with log.open("w") as output:
                subprocess.run(self.build_command(suites), cwd=ROOT, stdout=output, check=True)
        else:
            shutil.copyfile(artifacts_file, log)
        artifacts, paths = self.artifacts(log)
        sources = {artifacts[self.target(suite)] for suite in suites.values()}
        libraries = self.libraries(sources, paths)
        binaries = {source.name: self.executable(source) for source in sources}
        listings = {source.name: self.names(source) for source in sources}
        tests = sorted(({"suite": name, "test": test, "binary": artifacts[self.target(suite)].name,
                         "name": self.resolve(test, listings[artifacts[self.target(suite)].name])}
                        for name, suite in suites.items() for test in suite["tests"]),
                       key=lambda test: (test["suite"], test["test"]))
        manifest = {"revision": os.environ.get("GITHUB_SHA"), "binaries": binaries,
                    "libraries": libraries, "tests": tests}
        self.validate_inventory(manifest)
        (self.directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Prepared {len(tests)} Cargo tests in {len(binaries)} binaries", flush=True)

    @classmethod
    def validate_inventory(cls, manifest):
        suites = cls.selection(json.loads(CATALOG.read_text()))
        expected = sorted(((name, test) for name, suite in suites.items() for test in suite["tests"]))
        present = [(test["suite"], test["test"]) for test in manifest["tests"]]
        if present != expected:
            raise ValueError("bundle tests differ from the complete ordered catalog selection")
        if any(test["binary"] not in manifest["binaries"] for test in manifest["tests"]):
            raise ValueError("a bundle test names an unhashed executable")
        identities = {(test["binary"], test["name"]) for test in manifest["tests"]}
        if len(identities) != len(manifest["tests"]):
            raise ValueError("one executable test appears in more than one suite")

    def load(self):
        manifest = json.loads((self.directory / "manifest.json").read_text())
        if os.environ.get("GITHUB_SHA") and manifest["revision"] != os.environ["GITHUB_SHA"]:
            raise ValueError("the bundle was built from another revision")
        for name, digest in (manifest["binaries"] | manifest["libraries"]).items():
            if Path(name).is_absolute() or ".." in Path(name).parts or self.digest(self.directory / name) != digest:
                raise ValueError(f"invalid executable bundle: {name}")
        self.validate_inventory(manifest)
        return {(test["suite"], test["test"]): test for test in manifest["tests"]}

    def command(self, entry):
        return ([str(self.directory / entry["binary"]), "--exact", entry["name"], "--nocapture",
                 "--test-threads=1", "--include-ignored"], self.environment())

    @staticmethod
    def verdict(entry, text, _reports):
        passed = re.findall(r"^test ([\w:]+) \.\.\. ok$", text, re.MULTILINE)
        # Rust can print captured tracing between the test name and its verdict.
        return ("test result: ok. 1 passed; 0 failed; 0 ignored;" in text
                and (passed == [entry["name"]] or not passed) and "SKIP:" not in text)


class SealerRuntime:
    """The compiled sealer test runtime: classes, dependency jars and the JVM arguments of the test task."""

    def __init__(self, directory):
        self.directory = directory.resolve()

    def prepare(self):
        subprocess.run([SEALER / "gradlew", "-p", SEALER, ":service:componentTestRuntime", "--no-daemon", "-q"],
                       check=True)
        shutil.copytree(SEALER_RUNTIME, self.directory)
        print(f"Prepared the sealer test runtime in {self.directory}", flush=True)

    def load(self):
        arguments = (self.directory / "jvm-args.txt").read_text().splitlines()
        if not (self.directory / "lib").is_dir() or not arguments:
            raise ValueError("the sealer test runtime is incomplete")
        return arguments

    def command(self, entry, reports):
        java = Path(os.environ.get("JAVA_HOME_17_X64") or os.environ.get("JAVA_HOME", "/usr")) / "bin/java"
        classpath = os.pathsep.join(str(self.directory / part) for part in ("test", "main", "lib/*"))
        method = entry["test"].replace("::", "#").removesuffix("()")
        return [str(java), *self.load(), "-cp", classpath, "org.junit.platform.console.ConsoleLauncher",
                "execute", "--disable-banner", "--fail-if-no-tests", "--details=summary",
                "--select-method", method, "--reports-dir", str(reports)], dict(os.environ)

    @staticmethod
    def verdict(entry, _text, reports):
        try:
            cases = [case for path in reports.glob("*.xml") for case in ET.parse(path).getroot().iter("testcase")]
        except ET.ParseError:
            return False
        name = entry["test"].split("::")[-1]
        return (len(cases) == 1 and cases[0].get("name") == name
                and not any(cases[0].find(tag) is not None for tag in ("failure", "error", "skipped")))


class Docker:
    """The images and containers of the docker suites on one runner."""

    @staticmethod
    def images():
        with ThreadPoolExecutor(max_workers=len(IMAGES) + len(BUILT_IMAGES)) as pool:
            pulls = [pool.submit(subprocess.run, ["docker", "pull", "-q", image], check=True) for image in IMAGES]
            builds = [pool.submit(subprocess.run, ["docker", "build", "-q", "-t", image, context], check=True)
                      for image, context in BUILT_IMAGES.items()]
            [future.result() for future in pulls + builds]
        print(f"Ready: {', '.join([*IMAGES, *BUILT_IMAGES])}", flush=True)

    @staticmethod
    def containers():
        return set(subprocess.check_output(["docker", "ps", "-aq", "--no-trunc"], text=True).split())

    @classmethod
    def remove_new(cls, before):
        """A killed test cannot stop its containers, so the shard removes every container that the test left."""
        left = sorted(cls.containers() - before)
        if left:
            subprocess.run(["docker", "rm", "-f", "-v", *left], check=False, stdout=subprocess.DEVNULL)
        return len(left)

    @staticmethod
    def unlisted():
        present = set(subprocess.check_output(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
                                              text=True).split())
        return sorted(present - set(IMAGES) - set(BUILT_IMAGES))


class Shard:
    """One shard job: run the planned tests with a per-test and a per-shard deadline."""

    def __init__(self, bundle, sealer, output):
        self.bundle = bundle
        self.sealer = sealer
        self.output = output

    @staticmethod
    def execute(command, log, seconds, environment=None):
        started = time.monotonic()
        timed_out = False
        with log.open("w") as output:
            process = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, env=environment)
            try:
                code = process.wait(timeout=seconds)
            except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                os.killpg(process.pid, signal.SIGKILL)
                code = process.wait(timeout=5)
                if isinstance(error, KeyboardInterrupt):
                    raise
                timed_out = True
        return code, timed_out, time.monotonic() - started

    def entries(self, tests):
        cargo = self.bundle.load() if any(test["mode"] in CARGO_MODES for test in tests) else {}
        return [dict(test, **cargo.get((test["suite"], test["test"]), {})) for test in tests]

    def run_test(self, index, entry, remaining):
        log = self.output / f"{index:03}.log"
        if remaining <= 0:
            return dict(entry, success=False, timed_out=True, seconds=0.0, reason="shard budget exhausted")
        reports = self.output / f"{index:03}-junit"
        runner = self.sealer if entry["mode"] == "gradle" else self.bundle
        command, environment = (runner.command(entry, reports) if runner is self.sealer
                                else runner.command(entry))
        before = Docker.containers() if entry["mode"] == "docker" else set()
        code, timed_out, seconds = self.execute(command, log, min(TEST_SECONDS, remaining), environment)
        left = Docker.remove_new(before) if entry["mode"] == "docker" else 0
        success = (code == 0 and not timed_out and seconds <= TEST_SECONDS
                   and runner.verdict(entry, log.read_text(errors="replace"), reports))
        return dict(entry, success=success, exit_code=code, timed_out=timed_out,
                    seconds=round(seconds, 3), removed_containers=left, log=str(log))

    def run(self, index, tests):
        started = time.monotonic()
        if not tests:
            raise ValueError("an empty shard cannot pass")
        entries = self.entries(tests)
        self.output.mkdir(parents=True, exist_ok=False)
        results = []
        for position, entry in enumerate(entries):
            result = self.run_test(position, entry, SHARD_SECONDS - (time.monotonic() - started))
            results.append(result)
            print(f"shard {index}: {entry['suite']}::{entry['test']}: "
                  f"{'PASS' if result['success'] else 'FAIL'} ({result['seconds']:.3f}s)", flush=True)
        elapsed = time.monotonic() - started
        report = {"revision": os.environ.get("GITHUB_SHA"), "shard": index, "seconds": round(elapsed, 3),
                  "test_seconds": TEST_SECONDS, "shard_seconds": SHARD_SECONDS, "tests": results,
                  "success": all(result["success"] for result in results) and elapsed <= SHARD_SECONDS}
        (self.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        print(self.table(report), flush=True)
        if any(entry["mode"] == "docker" for entry in entries) and Docker.unlisted():
            print(f"warning: images outside the pulled list: {Docker.unlisted()}", flush=True)
        return report["success"]

    @staticmethod
    def table(report):
        rows = sorted(report["tests"], key=lambda result: -result["seconds"])
        width = max(len(f"{row['suite']}::{row['test']}") for row in rows)
        lines = [f"{'seconds':>8}  {'mode':<10}  {'result':<6}  test",
                 *(f"{row['seconds']:>8.1f}  {row['mode']:<10}  {'PASS' if row['success'] else 'FAIL':<6}  "
                   f"{row['suite']}::{row['test']}" for row in rows),
                 f"{report['seconds']:>8.1f}  {'total':<10}  {'PASS' if report['success'] else 'FAIL':<6}  "
                 f"shard {report['shard']}: {len(rows)} tests"]
        return "\n".join(["-" * (width + 30), *lines])


class Timings:
    """The measured runtime of each test, taken from shard results."""

    @staticmethod
    def update(result_files):
        timings = json.loads(TIMINGS.read_text())
        measured = {f"{test['suite']}::{test['test']}": test["seconds"]
                    for path in result_files for test in json.loads(Path(path).read_text())["tests"]
                    if test.get("exit_code") is not None}
        known = {f"{test['suite']}::{test['test']}" for test in Plan.load().tests()}
        merged = {name: seconds for name, seconds in (timings | measured).items() if name in known}
        TIMINGS.write_text(json.dumps(dict(sorted(merged.items())), indent=2) + "\n")
        print(f"Recorded {len(measured)} measured runtimes; {len(merged)} of {len(known)} tests are measured")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["features", "prepare", "prepare-sealer", "images", "image-names", "plan", "run", "run-all", "timings"])
    parser.add_argument("--bundle", type=Path, help="the Cargo test bundle")
    parser.add_argument("--sealer", type=Path, help="the sealer test runtime")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--artifacts", type=Path, help="Cargo JSON output from a shared build")
    parser.add_argument("--shard", type=int)
    parser.add_argument("results", nargs="*", type=Path, help="results.json files for the timings mode")
    args = parser.parse_args()
    if args.mode == "features":
        suites = Bundle.selection(json.loads(CATALOG.read_text()))
        print(",".join(sorted(set().union(*map(Bundle.features, suites.values())))))
    elif args.mode == "prepare":
        Bundle(args.bundle).prepare(args.artifacts)
    elif args.mode == "prepare-sealer":
        SealerRuntime(args.sealer).prepare()
    elif args.mode == "images":
        Docker.images()
    elif args.mode == "image-names":
        print(" ".join([*IMAGES, *BUILT_IMAGES]))
    elif args.mode == "plan":
        print(Plan.load().describe())
    elif args.mode == "timings":
        Timings.update(args.results)
    else:
        raise SystemExit(0 if run(parser, args) else 1)


def run(parser, args):
    if args.output is None or args.bundle is None or args.sealer is None:
        parser.error("execution needs --bundle, --sealer and --output")
    shards = Plan.load().shards()
    if args.mode == "run":
        if args.shard is None or not 0 <= args.shard < SHARDS:
            parser.error(f"run needs --shard in 0..{SHARDS - 1}")
        return Shard(Bundle(args.bundle), SealerRuntime(args.sealer), args.output).run(args.shard, shards[args.shard])
    # Local verification uses the same plan and execution path as CI.
    with ThreadPoolExecutor(max_workers=SHARDS) as pool:
        return all(list(pool.map(
            lambda index: Shard(Bundle(args.bundle), SealerRuntime(args.sealer), args.output / str(index))
            .run(index, shards[index]), range(SHARDS))))


if __name__ == "__main__":
    main()
