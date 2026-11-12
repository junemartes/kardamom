#!/usr/bin/env python3
"""Find the jobs of a nightly cluster-e2e run that need an issue.

The nightly-report job of .github/workflows/cluster-e2e.yml reads the
jobs of its run from the GitHub API and passes them on stdin, one JSON
object for each line. This script prints one tab-separated
line for each job that needs an issue: the kind (`stage` or `shard`),
the job id, the job URL, the conclusion, and the issue title.

With the argument `cases`, the script reads the log of one shard job on
stdin and prints one line for each case that did not pass: the case, a
colon and the reason that the log gives.

A job needs an issue when its conclusion is anything but `success`.
That covers `failure`, `cancelled` and `timed_out`: GitHub cancels a job
past its timeout-minutes, and the jobs API can report either of the last
two for it. A shard that a failed stage skipped gets no issue of its
own: the issue of the stage names the cause.
"""

import json
import re
import sys


STAGES = ("build cluster artifact", "prepare cluster node image", "prepare the chaos cluster")
SHARD_PREFIX = "container cluster e2e ("


class NightlyReport:
    """The jobs of one run and the issues that they need."""

    def __init__(self, jobs):
        self.jobs = jobs

    @staticmethod
    def broken(job):
        return job.get("conclusion") not in ("success", None)

    def stages(self):
        return [job for job in self.jobs if job["name"] in STAGES]

    def shards(self):
        return [job for job in self.jobs if job["name"].startswith(SHARD_PREFIX)]

    def findings(self):
        """The (kind, job, title) of each job that needs an issue."""
        stages = [job for job in self.stages() if self.broken(job)]
        found = [("stage", job, f"nightly: the {job['name']} job failed") for job in stages]
        found += [
            ("shard", job, f"nightly: the {job['name'][len(SHARD_PREFIX):-1]} shard failed")
            for job in self.shards()
            if self.broken(job) and not (stages and job.get("conclusion") == "skipped")
        ]
        return found

    def lines(self):
        return [
            "\t".join([kind, str(job["id"]), job["html_url"], job.get("conclusion") or "", title])
            for kind, job, title in self.findings()
        ]


class ShardLog:
    """The cases of one shard log that did not pass, each with the reason that the log gives."""

    COLOR = re.compile(r"\x1b\[[0-9;]*m")
    STAMP = re.compile(r"^\S*\d{4}-\d\d-\d\dT[\d:.]+Z ")
    STARTED = re.compile(r"= CHAOS CASE: ([a-z0-9-]+)")
    PASSED = re.compile(r"CHAOS CASE ([a-z0-9-]+): PASS")
    FAILED = re.compile(r"CHAOS FAIL: ([a-z0-9-]+): (.*)")
    PANIC = "panicked at crates/chaos/tests/shards.rs"
    NO_REASON = "the log gives no reason"

    def __init__(self, text):
        self.lines = [self.STAMP.sub("", self.COLOR.sub("", line)).strip() for line in text.splitlines()]

    def named(self, pattern):
        """The first match of `pattern` for each case, in the order of the log."""
        found = {}
        for match in filter(None, map(pattern.search, self.lines)):
            found.setdefault(match.group(1), match.groups()[-1])
        return found

    def last_words(self):
        """The last line before the shard test panics: the error of a case that prints no CHAOS FAIL line."""
        before = [line for line in self.lines[:self.panic_index()] if line]
        return before[-1] if before else self.NO_REASON

    def panic_index(self):
        return next((index for index, line in enumerate(self.lines) if self.PANIC in line), 0)

    def failed(self):
        """The (case, reason) of each case that started or failed and did not pass."""
        reasons = self.named(self.FAILED)
        passed = self.named(self.PASSED)
        cases = [case for case in {**self.named(self.STARTED), **reasons} if case not in passed]
        return [(case, reasons.get(case) or self.last_words()) for case in cases]


def main():
    if sys.argv[1:] == ["cases"]:
        for case, reason in ShardLog(sys.stdin.read()).failed():
            print(f"{case}: {reason}")
        return
    jobs = [json.loads(line) for line in sys.stdin if line.strip()]
    for line in NightlyReport(jobs).lines():
        print(line)


if __name__ == "__main__":
    main()
