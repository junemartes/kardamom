"""Shared Cargo artifact validation, native libraries and bounded execution."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]


class ExecutableBundle:
    def __init__(self, directory):
        self.directory = directory.resolve()

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
            listing = subprocess.check_output(["ldd", source], text=True, env=environment)
            if "not found" in listing:
                raise ValueError(f"missing native library for {source}: {listing}")
            for match in re.finditer(r"=> (/\S+)", listing):
                library = Path(match[1])
                if library.is_relative_to("/lib") or library.is_relative_to("/usr/lib"):
                    continue
                name = "native/" + library.name
                digest = self.digest(library)
                if name in libraries and libraries[name] != digest:
                    raise ValueError(f"conflicting native library: {name}")
                shutil.copy2(library, self.directory / name)
                libraries[name] = digest
        return libraries

    @staticmethod
    def build_messages(log):
        messages = [json.loads(line) for line in log.read_text().splitlines()]
        finished = [item for item in messages if item["reason"] == "build-finished"]
        if len(finished) != 1 or not finished[0]["success"]:
            raise ValueError("artifact packaging requires one successful completed Cargo build")
        return messages

    def executable(self, source, name):
        destination = self.directory / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        subprocess.run(["strip", "--strip-debug", destination], check=True)
        return self.digest(destination)

    def names(self, binary):
        listing = subprocess.check_output([binary, "--list", "--format", "terse"],
                                          text=True, timeout=30, env=self.environment())
        return [line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")]

    def load(self):
        manifest = json.loads((self.directory / "manifest.json").read_text())
        if os.environ.get("GITHUB_SHA") and manifest["revision"] != os.environ["GITHUB_SHA"]:
            raise ValueError("the bundle was built from another revision")
        files = manifest["binaries"] | manifest.get("libraries", {}) | manifest.get("assets", {})
        for name, digest in files.items():
            if Path(name).is_absolute() or ".." in Path(name).parts or self.digest(self.directory / name) != digest:
                raise ValueError(f"invalid executable bundle: {name}")
        return manifest

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
