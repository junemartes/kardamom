#!/usr/bin/env python3
"""Record complete CI rounds on one immutable head. Never rerun a failure."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import subprocess
import time

from component_tests import SHARDS as COMPONENT_SHARDS


WORKFLOWS = {
    "ci.yml", "cluster-e2e.yml", "docker-e2e.yml",
    "chain-semantics-e2e.yml", "cluster-validate.yml",
}
REQUIRED = 10
JOB_BUDGET_SECONDS = 15 * 60
COMPONENT_JOB_SECONDS = 10 * 60


class GitHub:
    def __init__(self, repository):
        self.repository = repository

    def read(self, path):
        result = subprocess.run(
            ["gh", "api", f"repos/{self.repository}/{path}"],
            check=True, text=True, capture_output=True,
        )
        return json.loads(result.stdout)

    def evidence(self, run_id):
        run = self.read(f"actions/runs/{run_id}")
        jobs = []
        page = 1
        while True:
            batch = self.read(f"actions/runs/{run_id}/jobs?per_page=100&page={page}")
            jobs.extend(batch["jobs"])
            if len(jobs) >= batch["total_count"]:
                break
            page += 1
        return {
            key: run[key] for key in (
                "id", "head_sha", "head_branch", "event", "path", "status",
                "conclusion", "created_at", "updated_at", "html_url", "run_attempt",
            )
        } | {"jobs": [{key: job[key] for key in ("name", "status", "conclusion", "started_at", "completed_at")} for job in jobs]}


class Streak:
    def __init__(self, head, branch, shards, state=None):
        self.shards = set(shards)
        self.state = state or {
            "version": 3, "head": head, "branch": branch,
            "required": REQUIRED, "streak": 0, "rounds": [],
            "job_budget_seconds": JOB_BUDGET_SECONDS,
            "component_job_seconds": COMPONENT_JOB_SECONDS,
        }
        if (self.state.get("job_budget_seconds") != JOB_BUDGET_SECONDS
                or self.state.get("component_job_seconds") != COMPONENT_JOB_SECONDS):
            raise ValueError("timing policy changed; retain the old audit and start a new state file")
        if self.state["head"] != head or self.state["branch"] != branch:
            raise ValueError("head or branch changed; retain this audit and start a new state file")

    def classify(self, runs):
        paths = [Path(run["path"]).name for run in runs]
        if len(paths) != len(WORKFLOWS) or set(paths) != WORKFLOWS:
            raise ValueError("a round needs one run of every required workflow")
        if any(run["head_sha"] != self.state["head"] for run in runs):
            raise ValueError("a workflow tested another head")
        if any(run["head_branch"] != self.state["branch"] for run in runs):
            raise ValueError("a workflow tested another branch")
        if any(run["event"] != "workflow_dispatch" for run in runs):
            raise ValueError("count explicit dispatches so checkout tests the exact head")
        if any(run["run_attempt"] != 1 for run in runs):
            raise ValueError("reruns cannot replace fresh complete validation rounds")
        failed = {"failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale"}
        if any(run["conclusion"] in failed or any(job["conclusion"] in failed for job in run["jobs"]) for run in runs):
            return "failed"
        if any(run["status"] != "completed" for run in runs):
            return "pending"
        if any(run["conclusion"] != "success" for run in runs):
            return "failed"
        if self.missing_coverage(runs):
            return "failed"
        if any(job["status"] != "completed" for run in runs for job in run["jobs"]):
            return "pending"
        if self.slow_jobs(runs):
            return "failed"
        return "green"

    @staticmethod
    def elapsed_seconds(runs):
        """The complete round includes dispatch gaps, queueing, builds and teardown."""
        intervals = [(datetime.fromisoformat(run["created_at"].replace("Z", "+00:00")),
                      datetime.fromisoformat(run["updated_at"].replace("Z", "+00:00")))
                     for run in runs]
        if any(start.tzinfo is None or end.tzinfo is None or end < start
               for start, end in intervals):
            raise ValueError("workflow timing needs ordered timestamps with time zones")
        return (max(end for _, end in intervals) - min(start for start, _ in intervals)).total_seconds()

    @staticmethod
    def slow_jobs(runs):
        """Measure execution per job; dependency and runner queue time are separate."""
        slow = []
        for run in runs:
            for job in run["jobs"]:
                if job["status"] != "completed" or job["conclusion"] == "skipped":
                    continue
                start = datetime.fromisoformat(job["started_at"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(job["completed_at"].replace("Z", "+00:00"))
                if start.tzinfo is None or end.tzinfo is None or end < start:
                    raise ValueError("job timing needs ordered timestamps with time zones")
                seconds = (end - start).total_seconds()
                budget = COMPONENT_JOB_SECONDS if job["name"].startswith("component recovery (") else JOB_BUDGET_SECONDS
                if seconds > budget:
                    slow.append({"workflow": run["path"], "job": job["name"], "seconds": seconds,
                                 "budget_seconds": budget})
        return slow

    def missing_coverage(self, runs):
        cluster = next(run for run in runs if Path(run["path"]).name == "cluster-e2e.yml")
        green = {job["name"] for job in cluster["jobs"] if job["conclusion"] == "success"}
        expected = {f"container cluster e2e ({shard})" for shard in self.shards}
        ci = next(run for run in runs if Path(run["path"]).name == "ci.yml")
        components = {f"component recovery ({shard})" for shard in range(COMPONENT_SHARDS)}
        completed = {job["name"] for job in ci["jobs"] if job["conclusion"] == "success"}
        return sorted((expected - green) | (components - completed))

    def record(self, runs):
        status = self.classify(runs)
        key = sorted(run["id"] for run in runs)
        existing = next((row for row in self.state["rounds"] if row["run_ids"] == key), None)
        if existing and existing["status"] != "pending" and existing["status"] != status:
            raise ValueError("retain completed round outcomes; dispatch a new complete round")
        previous = [row for row in self.state["rounds"] if row["run_ids"] != key]
        owned = {run_id for row in previous for run_id in row["run_ids"]}
        if owned.intersection(key):
            raise ValueError("a workflow run cannot supply evidence for two rounds")
        row = {
            "run_ids": key, "status": status,
            "started_at": min(run["created_at"] for run in runs),
            "updated_at": max(run["updated_at"] for run in runs), "workflows": runs,
            "missing_green_coverage": self.missing_coverage(runs),
            "elapsed_seconds": self.elapsed_seconds(runs),
            "slow_jobs": self.slow_jobs(runs) if status == "green" or all(
                job.get("started_at") and job.get("completed_at")
                for run in runs for job in run["jobs"] if job["status"] == "completed"
            ) else [],
            "job_budget_seconds": JOB_BUDGET_SECONDS,
            "component_job_seconds": COMPONENT_JOB_SECONDS,
        }
        rounds = sorted(previous + [row], key=lambda item: item["started_at"])
        self.validate_order(rounds)
        self.state["rounds"] = rounds
        self.state["streak"] = self.count(rounds)
        return status

    @staticmethod
    def validate_order(rounds):
        for before, after in zip(rounds, rounds[1:]):
            unfinished = any(run["status"] != "completed" for run in before["workflows"])
            if unfinished or after["started_at"] < before["updated_at"]:
                raise ValueError("rounds must finish before the next round starts")

    @staticmethod
    def count(rounds):
        streak = 0
        for row in rounds:
            if row["status"] == "green":
                streak += 1
            elif row["status"] == "failed":
                streak = 0
        return streak

    def save(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.state, indent=2) + "\n")
        temporary.replace(path)


class Command:
    @staticmethod
    def head(value):
        if not re.fullmatch(r"[0-9a-f]{40}", value):
            raise argparse.ArgumentTypeError("head must be a full commit hash")
        return value

    @staticmethod
    def shards(root):
        source = (root / "crates/chaos/src/shard.rs").read_text()
        shards = set(re.findall(r'=> "(chaos-[a-z0-9-]+)"', source))
        if "chaos-integrity" not in shards:
            raise ValueError("cannot find the full shard registry")
        return shards | {"load", "semantics"}

    @staticmethod
    def run():
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--repository", default="junemartes/kardamom")
        parser.add_argument("--branch", required=True)
        parser.add_argument("--head", required=True, type=Command.head)
        parser.add_argument("--run", required=True, action="append", type=int)
        parser.add_argument("--state", required=True, type=Path)
        parser.add_argument("--poll", action="store_true")
        args = parser.parse_args()
        if len(set(args.run)) != len(WORKFLOWS) or len(args.run) != len(WORKFLOWS):
            parser.error("provide five distinct --run IDs, one per workflow")
        root = Path(__file__).resolve().parents[2]
        previous = json.loads(args.state.read_text()) if args.state.exists() else None
        streak = Streak(args.head, args.branch, Command.shards(root), previous)
        github = GitHub(args.repository)
        while True:
            status = streak.record([github.evidence(run_id) for run_id in args.run])
            streak.save(args.state)
            print(f"{status}: {streak.state['streak']}/{REQUIRED} consecutive complete green rounds", flush=True)
            if status == "failed":
                raise SystemExit("stop: classify and fix the failed round before dispatching again")
            if status != "pending" or not args.poll:
                return
            time.sleep(30)


if __name__ == "__main__":
    Command.run()
