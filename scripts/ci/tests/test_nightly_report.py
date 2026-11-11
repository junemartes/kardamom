"""The jobs of a nightly run that get an issue (scripts/ci/nightly_report.py)."""

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nightly_report import STAGES, NightlyReport  # noqa: E402

try:
    import yaml
except ImportError:
    yaml = None


def job(name, conclusion, number=1):
    return {"id": number, "name": name, "conclusion": conclusion,
            "html_url": f"https://example/job/{number}"}


STAGES_OK = [job("build cluster artifact", "success", 1), job("prepare cluster node image", "success", 2),
             job("prepare the chaos cluster", "success", 3)]


class NightlyReportTest(unittest.TestCase):
    def titles(self, jobs):
        return [title for _, _, title in NightlyReport(jobs).findings()]

    def test_a_green_run_opens_no_issue(self):
        jobs = STAGES_OK + [job("container cluster e2e (load)", "success", 4)]
        self.assertEqual(self.titles(jobs), [])

    def test_every_shard_that_does_not_succeed_gets_an_issue(self):
        jobs = STAGES_OK + [
            job("container cluster e2e (load)", "failure", 4),
            job("container cluster e2e (chaos-carry)", "cancelled", 5),
            job("container cluster e2e (chaos-executor)", "timed_out", 6),
            job("container cluster e2e (semantics)", "success", 7),
        ]
        self.assertEqual(self.titles(jobs), [
            "nightly: the load shard failed",
            "nightly: the chaos-carry shard failed",
            "nightly: the chaos-executor shard failed",
        ])

    def test_a_failed_stage_gets_the_issue_and_its_skipped_shards_get_none(self):
        jobs = [job("build cluster artifact", "failure", 1), job("prepare cluster node image", "success", 2),
                job("prepare the chaos cluster", "skipped", 3),
                job("container cluster e2e (load)", "skipped", 4)]
        self.assertEqual(self.titles(jobs), [
            "nightly: the build cluster artifact job failed",
            "nightly: the prepare the chaos cluster job failed",
        ])

    def test_a_line_carries_the_kind_the_job_and_the_title(self):
        jobs = STAGES_OK + [job("container cluster e2e (chaos-l1)", "failure", 9)]
        self.assertEqual(NightlyReport(jobs).lines(),
                         ["shard\t9\thttps://example/job/9\tfailure\tnightly: the chaos-l1 shard failed"])


@unittest.skipUnless(yaml, "PyYAML required")
class WorkflowTest(unittest.TestCase):
    """The parts of cluster-e2e.yml that the report and the label rely on."""

    def setUp(self):
        root = Path(__file__).resolve().parents[3]
        self.workflow = yaml.safe_load((root / ".github/workflows/cluster-e2e.yml").read_text())

    def test_each_label_has_its_own_concurrency_group(self):
        self.assertIn("github.event.label.name", self.workflow["concurrency"]["group"])

    def test_the_report_runs_after_every_stage_of_a_scheduled_run(self):
        report = self.workflow["jobs"]["nightly-report"]
        self.assertEqual(set(report["needs"]), {"build", "node-image", "prepare-cluster", "cluster-e2e"})
        self.assertEqual(report["if"], "always() && github.event_name == 'schedule'")

    def test_the_stage_names_of_the_report_are_the_job_names(self):
        names = {self.workflow["jobs"][job]["name"] for job in ("build", "node-image", "prepare-cluster")}
        self.assertEqual(names, set(STAGES))


if __name__ == "__main__":
    unittest.main()
