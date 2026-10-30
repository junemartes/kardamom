#!/usr/bin/env python3
"""Validate the component recovery catalog and summarize the port status per family."""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "scripts/ci/component-recovery.json"
CASE_KEYS = {"case", "family", "status", "local_tests", "full_case_locally_validated"}
OPTIONAL_CASE_KEYS = {"cluster_only_checks"}
STATUSES = ("planned", "partial", "ported")
NEGATIVE_CONTROL_KEYS = ("negative_controls", "clock_negative_controls", "negative_control")


class Catalog:
    def __init__(self, document):
        self.document = document
        self.suites = document["local_suites"]
        self.cases = document["cases"]

    @classmethod
    def load(cls, path=CATALOG):
        return cls(json.loads(Path(path).read_text()))

    def errors(self):
        names = [case.get("case") for case in self.cases]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        return ([f"duplicate case: {name}" for name in duplicates]
                + [error for case in self.cases for error in self.case_errors(case)])

    def case_errors(self, case):
        name = case.get("case", "<unnamed>")
        missing = sorted(CASE_KEYS - case.keys())
        unknown = sorted(case.keys() - CASE_KEYS - OPTIONAL_CASE_KEYS)
        if missing or unknown:
            return [f"{name}: missing keys {missing}, unknown keys {unknown}"]
        return [f"{name}: {error}" for error in [*self.field_errors(case), *self.test_errors(case),
                                                  *self.ported_errors(case)]]

    def field_errors(self, case):
        checks = case.get("cluster_only_checks", ["valid"])
        return [error for failed, error in [
            (case["family"] not in self.document["families"], f"unknown family {case['family']}"),
            (case["status"] not in STATUSES, f"status must be one of {list(STATUSES)}"),
            (not isinstance(case["full_case_locally_validated"], bool),
             "full_case_locally_validated must be a boolean"),
            (not isinstance(checks, list) or not checks
             or not all(isinstance(check, str) and check.strip() for check in checks),
             "cluster_only_checks must be a non-empty list of non-empty strings"),
            ((case["status"] == "ported") != case["full_case_locally_validated"],
             "status ported and full_case_locally_validated true must go together"),
        ] if failed]

    def test_errors(self, case):
        return [f"local test {test} is not in its catalogued suite" for test in case["local_tests"]
                if test.get("test") not in self.suites.get(test.get("suite"), {}).get("tests", [])]

    def ported_errors(self, case):
        if not case["full_case_locally_validated"]:
            return []
        suites = [self.suites.get(test.get("suite"), {}) for test in case["local_tests"]]
        return [error for failed, error in [
            (not suites, "a ported case needs at least one local test"),
            (not any(suite.get(key) for suite in suites for key in NEGATIVE_CONTROL_KEYS),
             "a ported case needs a negative control in one of its suites"),
        ] if failed]

    def summary(self):
        counts = Counter((case["family"], case["status"]) for case in self.cases)
        families = sorted({case["family"] for case in self.cases})
        rows = [(family, *(counts[family, state] for state in STATUSES)) for family in families]
        rows.append(("total", *(sum(row[index] for row in rows) for index in range(1, 4))))
        width = max(len(row[0]) for row in rows)
        header = f"{'family':<{width}}  " + "  ".join(f"{state:>7}" for state in STATUSES)
        lines = [f"{row[0]:<{width}}  " + "  ".join(f"{count:>7}" for count in row[1:]) for row in rows]
        return "\n".join([header, *lines])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["validate", "summary"])
    parser.add_argument("--catalog", type=Path, default=CATALOG)
    args = parser.parse_args()
    catalog = Catalog.load(args.catalog)
    errors = catalog.errors()
    for error in errors:
        print(error, file=sys.stderr)
    if args.mode == "summary":
        print(catalog.summary())
    raise SystemExit(1 if errors else 0)


if __name__ == "__main__":
    main()
