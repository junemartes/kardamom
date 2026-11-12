"""The jobs of a nightly run that get an issue (scripts/ci/nightly_report.py)."""

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nightly_report import STAGES, NightlyReport, ShardLog  # noqa: E402

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


STAMP = "2026-11-12T11:54:02.5024262Z "
SHARD_LOG = "\n".join(STAMP + line for line in [
    "==> ================= CHAOS CASE: hard-executor =================",
    "==> CHAOS CASE hard-executor: PASS in 60s",
    "==> ================= CHAOS CASE: node-failure-executor =================",
    "\x1b[2mCHAOS FAIL: node-failure-executor: the case runs past its deadline of 180s\x1b[0m",
    "CHAOS FAIL: node-failure-executor: a later line of the same case",
    "thread 'chaos_executor' (2709) panicked at crates/chaos/tests/shards.rs:163:9:",
])


class ShardLogTest(unittest.TestCase):
    def test_a_failed_case_has_the_first_reason_of_the_log_without_color_codes(self):
        self.assertEqual(ShardLog(SHARD_LOG).failed(),
                         [("node-failure-executor", "the case runs past its deadline of 180s")])

    def test_a_case_without_a_fail_line_has_the_last_line_before_the_panic(self):
        log = "\n".join(STAMP + line for line in [
            "==> ================= CHAOS CASE: cluster-two-blank-recover =================",
            "docker exec kardamom-sealer-0 sha256sum timed out after 20s: deadline has elapsed",
            "",
            "thread 'chaos_integrity' (2582) panicked at crates/chaos/tests/shards.rs:163:9:",
            "shard chaos-integrity failed; the cluster stays up for diagnostics",
        ])
        self.assertEqual(ShardLog(log).failed(), [
            ("cluster-two-blank-recover",
             "docker exec kardamom-sealer-0 sha256sum timed out after 20s: deadline has elapsed")])

    def test_a_log_that_ends_in_a_started_case_names_it_without_a_reason(self):
        log = STAMP + "==> ================= CHAOS CASE: l1-liar ================="
        self.assertEqual(ShardLog(log).failed(), [("l1-liar", ShardLog.NO_REASON)])

    def test_a_log_where_every_case_passes_names_no_case(self):
        self.assertEqual(ShardLog("\n".join(SHARD_LOG.splitlines()[:2])).failed(), [])


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

    def test_the_report_reads_a_shard_log_that_holds_color_codes(self):
        """The `gh api` of the runner prints no response that holds terminal escape sequences."""
        script = self.workflow["jobs"]["nightly-report"]["steps"][-1]["run"]
        self.assertNotIn('gh api "repos/$GH_REPO/actions/jobs/', script)
        self.assertIn("nightly_report.py cases", script)

    def test_the_stage_names_of_the_report_are_the_job_names(self):
        names = {self.workflow["jobs"][job]["name"] for job in ("build", "node-image", "prepare-cluster")}
        self.assertEqual(names, set(STAGES))


if __name__ == "__main__":
    unittest.main()
