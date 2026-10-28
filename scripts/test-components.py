"""Run catalogued production-actor checks and preserve exact local evidence."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "docs/reviews/2026-10-28-component-recovery.json"


class Run:
    def __init__(self, suites, directory):
        self.suites = suites
        self.directory = directory

    def suite(self, name):
        suite = self.suites[name]
        log = self.directory / f"{name}.log"
        reports = [ROOT / path for path in suite.get("junit_reports", [])]
        for report in reports:
            report.unlink(missing_ok=True)
        started = time.monotonic()
        print(f"{name}: {' '.join(suite['command'])}", flush=True)
        timed_out = False
        with log.open("w") as output:
            process = subprocess.Popen(suite["command"], cwd=ROOT, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True,
                                       env=dict(os.environ, CARGO_TERM_COLOR="never"))
            try:
                code = process.wait(timeout=600)
            except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                os.killpg(process.pid, signal.SIGKILL)
                code = process.wait(timeout=5)
                if isinstance(error, KeyboardInterrupt):
                    raise
                timed_out = True
        text = log.read_text()
        passed = re.findall(r"^test ([\w:]+) \.\.\. ok$", text, re.MULTILINE)
        failed = re.findall(r"^test ([\w:]+) \.\.\. FAILED$", text, re.MULTILINE)
        report_errors = []
        if reports:
            passed, failed, report_errors = self.junit(name, reports)
        missing = [name for name in suite["tests"]
                   if not any(test == name or test.endswith("::" + name) for test in passed)]
        result = {"command": suite["command"], "exit_code": code, "timed_out": timed_out,
                  "seconds": round(time.monotonic() - started, 3), "passed": passed, "failed": failed,
                  "report_errors": report_errors,
                  "missing_expected_tests": missing, "log": str(log),
                  "success": code == 0 and not missing and not failed and not report_errors}
        print(f"{name}: {'PASS' if result['success'] else 'FAIL'}; "
              f"{len(passed)} passed, {len(failed)} failed tests; {result['seconds']}s; {log}", flush=True)
        if not result["success"]:
            print("\n".join(text.splitlines()[-30:]), flush=True)
        return result

    def junit(self, name, reports):
        passed, failed, errors = [], [], []
        for path in reports:
            try:
                root = ET.parse(path).getroot()
                shutil.copyfile(path, self.directory / f"{name}-{path.name}")
            except (OSError, ET.ParseError) as error:
                errors.append(f"{path}: {error}")
                continue
            for test in root.iter("testcase"):
                identifier = f"{test.get('classname', '')}::{test.get('name', '')}"
                if test.find("failure") is not None or test.find("error") is not None:
                    failed.append(identifier)
                elif test.find("skipped") is None:
                    passed.append(identifier)
        return passed, failed, errors

    @staticmethod
    def revision():
        for command in (["jj", "log", "--no-graph", "-r", "@", "-T", "commit_id"],
                        ["git", "rev-parse", "HEAD"]):
            try:
                result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=10)
            except FileNotFoundError:
                continue
            if result.returncode == 0:
                return result.stdout.strip()
        return "unknown"

    def run(self, names, catalog):
        report = {"source_revision": self.revision(),
                  "started_utc": datetime.now(timezone.utc).isoformat(),
                  "cases_in_inventory": len(catalog["cases"]),
                  "full_suite_locally_validated": False,
                  "qualification_credit": 0, "suites": {}}
        for name in names:
            report["suites"][name] = self.suite(name)
        destination = self.directory / "results.json"
        destination.write_text(json.dumps(report, indent=2) + "\n")
        print(f"Evidence: {destination}")
        print("These are component checks. Full chaos-case equivalence and distributed qualification remain open.")
        return 0 if all(result["success"] for result in report["suites"].values()) else 1


if __name__ == "__main__":
    catalog = json.loads(CATALOG.read_text())
    suites = catalog["local_suites"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", nargs="?", default="all", choices=["all", *suites])
    args = parser.parse_args()
    directory = Path(tempfile.mkdtemp(prefix="kardamom-components-"))
    names = list(suites) if args.suite == "all" else [args.suite]
    sys_exit = Run(suites, directory).run(names, catalog)
    raise SystemExit(sys_exit)
