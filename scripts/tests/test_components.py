import contextlib
import importlib.util
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "components", Path(__file__).resolve().parents[1] / "test-components.py")
components = importlib.util.module_from_spec(spec)
spec.loader.exec_module(components)


class EvidenceTests(unittest.TestCase):
    def run_fixture(self, output, code=0):
        suite = {"command": [sys.executable, "-c", f"print({output!r}); raise SystemExit({code})"],
                 "tests": ["recovers"]}
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            return components.Run({"fixture": suite}, Path(directory)).suite("fixture")

    def test_exact_or_qualified_test_name_proves_execution(self):
        for name in ["recovers", "actor::tests::recovers"]:
            self.assertTrue(self.run_fixture(f"test {name} ... ok")["success"])

    def test_zero_tests_or_wrong_name_does_not_pass(self):
        for output in ["running 0 tests", "test does_not_recovers ... ok", "test recovers ... ignored"]:
            result = self.run_fixture(output)
            self.assertFalse(result["success"])
            self.assertEqual(result["missing_expected_tests"], ["recovers"])

    def test_nonzero_exit_fails_even_if_expected_test_passed(self):
        self.assertFalse(self.run_fixture("test recovers ... ok", 1)["success"])

    def test_failed_assertion_is_named_in_the_evidence(self):
        result = self.run_fixture("test actor::recovers ... FAILED", 101)
        self.assertFalse(result["success"])
        self.assertEqual(result["failed"], ["actor::recovers"])

    def junit_fixture(self, content, stale=None):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            root = Path(directory)
            report = root / "TEST-Actor.xml"
            if stale is not None:
                report.write_text(stale)
            code = "pass" if content is None else f"from pathlib import Path; Path('TEST-Actor.xml').write_text({content!r})"
            suite = {"command": [sys.executable, "-c", code], "tests": ["recovers()"],
                     "junit_reports": [report.name]}
            with patch.object(components, "ROOT", root):
                return components.Run({"java": suite}, root).suite("java")

    def test_fresh_junit_execution_is_required(self):
        xml = '<testsuite><testcase classname="Actor" name="recovers()"/></testsuite>'
        self.assertTrue(self.junit_fixture(xml)["success"])
        stale = self.junit_fixture(None, stale=xml)
        self.assertFalse(stale["success"])
        self.assertTrue(stale["report_errors"])

    def test_junit_failures_and_skips_cannot_pass_with_exit_zero(self):
        for child in ["<failure/>", "<error/>", "<skipped/>"]:
            xml = f'<testsuite><testcase classname="Actor" name="recovers()">{child}</testcase></testsuite>'
            self.assertFalse(self.junit_fixture(xml)["success"])

    def test_malformed_or_empty_junit_cannot_pass(self):
        for xml in ["not XML", "<testsuite/>"]:
            self.assertFalse(self.junit_fixture(xml)["success"])

    def test_an_extra_junit_failure_is_not_hidden_by_the_expected_pass(self):
        xml = ('<testsuite><testcase classname="Actor" name="recovers()"/>'
               '<testcase classname="Actor" name="other()"><failure/></testcase></testsuite>')
        result = self.junit_fixture(xml)
        self.assertFalse(result["success"])
        self.assertEqual(result["failed"], ["Actor::other()"])


if __name__ == "__main__":
    unittest.main()
