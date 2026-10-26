"""A streak needs complete, independent green rounds and retains every failure."""

from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from green_streak import Command, Streak, WORKFLOWS


HEAD = "a" * 40
BRANCH = "validation/final"
SHARDS = {"load", "semantics", "chaos-cache", "chaos-integrity"}


class StreakTest(unittest.TestCase):
    def setUp(self):
        self.streak = Streak(HEAD, BRANCH, SHARDS)

    @staticmethod
    def runs(number, conclusion="success"):
        return [{
            "id": number * 100 + index,
            "head_sha": HEAD, "head_branch": BRANCH,
            "event": "workflow_dispatch", "run_attempt": 1,
            "path": f".github/workflows/{name}",
            "status": "completed", "conclusion": conclusion,
            "created_at": f"2026-10-{number:02}T00:00:00Z",
            "updated_at": f"2026-10-{number:02}T02:00:00Z",
            "html_url": f"https://example.test/{number}/{index}",
            "jobs": [{"name": f"container cluster e2e ({shard})",
                      "status": "completed", "conclusion": "success"}
                     for shard in sorted(SHARDS)] if name == "cluster-e2e.yml" else [],
        } for index, name in enumerate(sorted(WORKFLOWS))]

    def green(self, count):
        for number in range(1, count + 1):
            self.streak.record(self.runs(number))

    def test_requires_ten_complete_rounds(self):
        self.green(9)
        self.assertEqual(self.streak.state["streak"], 9)
        self.streak.record(self.runs(10))
        self.assertEqual(self.streak.state["streak"], 10)

    def test_polling_a_green_round_does_not_count_it_again(self):
        runs = self.runs(1)
        self.streak.record(runs)
        self.streak.record(runs)
        self.assertEqual(self.streak.state["streak"], 1)
        self.assertEqual(len(self.streak.state["rounds"]), 1)

    def test_one_failed_workflow_resets_nine_green_rounds(self):
        self.green(9)
        runs = self.runs(10)
        runs[0]["conclusion"] = "failure"
        self.streak.record(runs)
        self.assertEqual(self.streak.state["streak"], 0)
        self.streak.record(self.runs(11))
        self.assertEqual(self.streak.state["streak"], 1)

    def test_a_failed_job_cannot_hide_under_a_cancelled_workflow(self):
        self.green(1)
        runs = self.runs(2)
        runs[0]["conclusion"] = "cancelled"
        self.streak.record(runs)
        self.assertEqual(self.streak.state["streak"], 0)

    def test_a_failed_job_resets_the_streak_before_other_jobs_finish(self):
        self.green(1)
        runs = self.runs(2)
        runs[0].update(status="in_progress", conclusion=None)
        runs[0]["jobs"].append({"name": "test", "status": "completed", "conclusion": "failure"})
        self.streak.record(runs)
        self.assertEqual(self.streak.state["streak"], 0)

    def test_incomplete_round_does_not_advance_the_streak(self):
        self.green(1)
        runs = self.runs(2)
        runs[0].update(status="in_progress", conclusion=None)
        self.assertEqual(self.streak.record(runs), "pending")
        self.assertEqual(self.streak.state["streak"], 1)
        runs[0].update(status="completed", conclusion="success")
        self.streak.record(runs)
        self.assertEqual(self.streak.state["streak"], 2)

    def test_an_early_failure_cannot_open_the_next_round_while_jobs_still_run(self):
        self.green(1)
        runs = self.runs(2)
        runs[0]["conclusion"] = "failure"
        runs[1].update(status="in_progress", conclusion=None)
        self.assertEqual(self.streak.record(runs), "failed")
        with self.assertRaisesRegex(ValueError, "must finish"):
            self.streak.record(self.runs(3))
        runs[1].update(status="completed", conclusion="success")
        self.streak.record(runs)
        self.streak.record(self.runs(3))
        self.assertEqual(self.streak.state["streak"], 1)

    def test_regular_shards_alone_do_not_complete_a_round(self):
        runs = self.runs(1)
        cluster = next(run for run in runs if run["path"].endswith("cluster-e2e.yml"))
        cluster["jobs"] = [job for job in cluster["jobs"] if "chaos-integrity" not in job["name"]]
        self.assertEqual(self.streak.record(runs), "failed")
        self.assertEqual(self.streak.state["streak"], 0)
        self.assertIn("container cluster e2e (chaos-integrity)", self.streak.state["rounds"][0]["missing_green_coverage"])

    def test_rejects_other_heads_branches_events_and_attempts(self):
        mutations = [
            ("head_sha", "b" * 40), ("head_branch", "another"),
            ("event", "pull_request"), ("run_attempt", 2),
        ]
        for key, value in mutations:
            runs = self.runs(1)
            runs[0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.streak.record(runs)

    def test_cannot_overwrite_a_failed_round_with_a_rerun(self):
        self.streak.record(self.runs(1, "failure"))
        with self.assertRaisesRegex(ValueError, "retain completed"):
            self.streak.record(self.runs(1))

    def test_rejects_overlapping_rounds_and_reused_run_ids(self):
        self.green(1)
        runs = self.runs(2)
        runs[0]["created_at"] = "2026-10-01T01:00:00Z"
        with self.assertRaisesRegex(ValueError, "must finish"):
            self.streak.record(runs)
        runs = self.runs(2)
        runs[0]["id"] = self.runs(1)[0]["id"]
        with self.assertRaisesRegex(ValueError, "two rounds"):
            self.streak.record(runs)

    def test_a_missing_workflow_is_not_a_round(self):
        with self.assertRaisesRegex(ValueError, "every required workflow"):
            self.streak.record(self.runs(1)[:-1])

    def test_state_keeps_failure_history_and_cannot_move_to_a_new_head(self):
        self.green(1)
        self.streak.record(self.runs(2, "failure"))
        original = deepcopy(self.streak.state)
        with self.assertRaisesRegex(ValueError, "head or branch changed"):
            Streak("b" * 40, BRANCH, SHARDS, original)
        self.assertEqual(original["rounds"][1]["status"], "failed")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            self.streak.save(path)
            self.assertTrue(path.exists())
            self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_registry_includes_every_existing_shard_and_both_stages(self):
        root = Path(__file__).resolve().parents[3]
        shards = Command.shards(root)
        self.assertEqual(len(shards), 16)
        self.assertTrue({"chaos-integrity", "chaos-carry", "load", "semantics"} <= shards)


if __name__ == "__main__":
    unittest.main()
