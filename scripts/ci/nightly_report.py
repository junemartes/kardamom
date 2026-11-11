#!/usr/bin/env python3
"""Find the jobs of a nightly cluster-e2e run that need an issue.

The nightly-report job of .github/workflows/cluster-e2e.yml reads the
jobs of its run from the GitHub API and passes them on stdin, one JSON
object for each line. This script prints one tab-separated
line for each job that needs an issue: the kind (`stage` or `shard`),
the job id, the job URL, the conclusion, and the issue title.

A job needs an issue when its conclusion is anything but `success`.
That covers `failure`, `cancelled` and `timed_out`: GitHub cancels a job
past its timeout-minutes, and the jobs API can report either of the last
two for it. A shard that a failed stage skipped gets no issue of its
own: the issue of the stage names the cause.
"""

import json
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


def main():
    jobs = [json.loads(line) for line in sys.stdin if line.strip()]
    for line in NightlyReport(jobs).lines():
        print(line)


if __name__ == "__main__":
    main()
