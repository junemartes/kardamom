"""The catalog validator rejects incomplete ported cases and malformed case fields."""

import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from component_catalog import Catalog

DOCUMENT = {
    "families": {"Ingress": {}, "Cache": {}},
    "local_suites": {
        "guarded": {"ci_mode": "in-process", "tests": ["recovers", "restarts"],
                    "negative_controls": {"no_replay": {"test": "recovers"}}},
        "unguarded": {"ci_mode": "docker", "tests": ["observes"]},
        "unrun": {"ci_mode": "docker", "tests": ["drains"], "negative_control": "The old path fails."},
    },
    "cases": [
        {"case": "ported-case", "family": "Ingress", "status": "ported",
         "local_tests": [{"suite": "guarded", "test": "recovers"}],
         "full_case_locally_validated": True,
         "cluster_only_checks": ["reschedule: Nomad places the new allocation"]},
        {"case": "partial-case", "family": "Ingress", "status": "partial",
         "local_tests": [{"suite": "unguarded", "test": "observes"}],
         "full_case_locally_validated": False},
        {"case": "planned-case", "family": "Cache", "status": "planned",
         "local_tests": [], "full_case_locally_validated": False},
    ],
}


class CatalogTests(unittest.TestCase):
    def catalog(self, change=None):
        document = copy.deepcopy(DOCUMENT)
        if change:
            change(document)
        return Catalog(document)

    def assert_error(self, change, text):
        errors = self.catalog(change).errors()
        self.assertTrue(any(text in error for error in errors), errors)

    def test_a_valid_catalog_has_no_errors(self):
        self.assertEqual(self.catalog().errors(), [])

    def test_the_repository_catalog_is_valid(self):
        self.assertEqual(Catalog.load().errors(), [])

    def test_cluster_only_checks_is_optional(self):
        self.assertEqual(self.catalog(lambda d: d["cases"][0].pop("cluster_only_checks")).errors(), [])

    def test_cluster_only_checks_must_be_a_list_of_strings(self):
        for value in ["one string", [], [""], [3], {"check": "why"}]:
            with self.subTest(value=value):
                self.assert_error(lambda d, v=value: d["cases"][0].update(cluster_only_checks=v),
                                  "cluster_only_checks")

    def test_an_unknown_case_key_is_rejected(self):
        self.assert_error(lambda d: d["cases"][1].update(cluster_only=["x"]), "unknown keys ['cluster_only']")

    def test_a_ported_case_needs_a_local_test(self):
        self.assert_error(lambda d: d["cases"][0].update(local_tests=[]), "at least one local test")

    def test_a_ported_case_needs_a_negative_control(self):
        def unguard(document):
            document["cases"][0]["local_tests"] = [{"suite": "unguarded", "test": "observes"}]
        self.assert_error(unguard, "negative control")

    def test_a_control_for_another_test_of_the_suite_does_not_count(self):
        self.assert_error(lambda d: d["cases"][0].update(local_tests=[{"suite": "guarded", "test": "restarts"}]),
                          "negative control")

    def test_a_ported_case_needs_a_suite_that_ci_runs(self):
        def unrun(document):
            document["local_suites"]["unrun"].pop("ci_mode")
            document["cases"][0]["local_tests"] = [{"suite": "unrun", "test": "drains"}]
        self.assert_error(unrun, "so that CI runs it")

    def test_a_raised_test_timeout_needs_a_reason_and_a_bound(self):
        def raise_to(seconds, reason=None):
            def change(document):
                document["local_suites"]["guarded"]["test_timeout_s"] = seconds
                if reason is not None:
                    document["local_suites"]["guarded"]["test_timeout_reason"] = reason
            return change
        self.assertEqual(self.catalog(raise_to(60)).errors(), [])
        self.assertEqual(self.catalog(raise_to(120, "the first log purge pass is a fixed 30 s")).errors(), [])
        self.assert_error(raise_to(90), "needs a test_timeout_reason")
        self.assert_error(raise_to(90, " "), "needs a test_timeout_reason")
        for seconds in [59, 121, 90.5, "90", True]:
            with self.subTest(seconds=seconds):
                self.assert_error(raise_to(seconds, "reason"), "an integer from 60 to 120")

    def test_every_test_needs_a_recorded_runtime(self):
        timings = {f"{name}::{test}": 1.0 for name, suite in DOCUMENT["local_suites"].items()
                   for test in suite["tests"]}
        self.assertEqual(Catalog(copy.deepcopy(DOCUMENT), timings).errors(), [])
        timings.pop("guarded::restarts")
        self.assertEqual(Catalog(copy.deepcopy(DOCUMENT), timings).errors(),
                         ["no runtime in scripts/ci/component-timings.json for guarded::restarts"])

    def test_every_suite_needs_a_known_ci_mode(self):
        self.assert_error(lambda d: d["local_suites"]["guarded"].pop("ci_mode"), "suite guarded: ci_mode")
        self.assert_error(lambda d: d["local_suites"]["guarded"].update(ci_mode="nightly"), "suite guarded: ci_mode")

    def test_every_negative_control_format_counts(self):
        for key, value in [("clock_negative_controls", [{"test": "observes"}]),
                           ("negative_controls", [{"failed_tests": ["actor::tests::observes"]}]),
                           ("negative_controls", [{"tests": ["observes"]}]),
                           ("negative_controls", [{"mutation": "skip the replay", "failed_tests": 6}]),
                           ("negative_control", "The old path fails.")]:
            def guard(document, key=key, value=value):
                document["local_suites"]["unguarded"][key] = value
                document["cases"][0]["local_tests"] = [{"suite": "unguarded", "test": "observes"}]
            with self.subTest(key=key):
                self.assertEqual(self.catalog(guard).errors(), [])

    def test_a_ported_status_needs_the_flag(self):
        self.assert_error(lambda d: d["cases"][0].update(full_case_locally_validated=False), "must go together")

    def test_the_flag_needs_a_ported_status(self):
        self.assert_error(lambda d: d["cases"][0].update(status="partial"), "must go together")
        self.assert_error(lambda d: d["cases"][2].update(full_case_locally_validated=True), "must go together")

    def test_a_local_test_must_be_catalogued(self):
        self.assert_error(lambda d: d["cases"][1]["local_tests"].append({"suite": "guarded", "test": "x"}),
                          "not in its catalogued suite")

    def test_unknown_family_status_and_duplicate_case_are_rejected(self):
        self.assert_error(lambda d: d["cases"][2].update(family="Nope"), "unknown family")
        self.assert_error(lambda d: d["cases"][2].update(status="done"), "status must be")
        self.assert_error(lambda d: d["cases"].append(copy.deepcopy(d["cases"][2])), "duplicate case")

    def test_the_summary_counts_each_state_per_family(self):
        lines = self.catalog().summary().splitlines()
        self.assertEqual(lines[0].split(), ["family", "planned", "partial", "ported"])
        self.assertEqual(lines[1].split(), ["Cache", "1", "0", "0"])
        self.assertEqual(lines[2].split(), ["Ingress", "0", "1", "1"])
        self.assertEqual(lines[3].split(), ["total", "1", "1", "1"])


if __name__ == "__main__":
    unittest.main()
