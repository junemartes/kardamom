"""Prebuilt recovery tests require exact execution and enforce both deadlines."""

import json
import re
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import component_tests as component


class ComponentTests(unittest.TestCase):
    def test_missing_or_ambiguous_names_do_not_supply_coverage(self):
        self.assertEqual(component.Bundle.resolve("recovery", ["actor::recovery"]), "actor::recovery")
        for names in [[], ["not_recovery"], ["left::recovery", "right::recovery"]]:
            with self.subTest(names=names), self.assertRaises(ValueError):
                component.Bundle.resolve("recovery", names)

    def test_incomplete_or_failed_builds_cannot_supply_test_binaries(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "build.jsonl"
            for messages in [[], [{"reason": "build-finished", "success": False}],
                             [{"reason": "build-finished", "success": True}] * 2]:
                log.write_text("\n".join(json.dumps(item) for item in messages))
                with self.subTest(messages=messages), self.assertRaises(ValueError):
                    component.Bundle.build_messages(log)
            log.write_text(json.dumps({"reason": "build-finished", "success": True}))
            self.assertEqual(len(component.Bundle.build_messages(log)), 1)

    def test_catalog_builds_one_graph_without_docker_features(self):
        suites = component.Bundle.selection(json.loads(component.CATALOG.read_text()))
        command = component.Bundle.build_command(suites)
        self.assertEqual(len(suites), 14)
        self.assertEqual(command.count("cargo"), 1)
        self.assertNotIn("--features", command)
        for suite in suites.values():
            self.assertNotIn("--ignored", suite["command"])
            self.assertTrue(set(suite["tests"]))

    def fixture(self, code, **budgets):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "actor"
            executable.write_text(f"#!{sys.executable}\n" + code)
            executable.chmod(0o755)
            bundle = component.Bundle(root)
            manifest = {"revision": None, "shards": component.SHARDS,
                        "binaries": {"actor": bundle.digest(executable)},
                        "tests": [{"suite": "actor", "test": "recovers", "binary": "actor"}]}
            (root / "manifest.json").write_text(json.dumps(manifest))
            with patch.dict(component.os.environ, {}, clear=True), patch.multiple(component, **dict(TEST_SECONDS=component.TEST_SECONDS, SHARD_SECONDS=component.SHARD_SECONDS) | budgets):
                success = bundle.run(0, root / "results")
            return success, json.loads((root / "results/results.json").read_text())

    def test_an_exact_test_must_run_and_pass(self):
        success, report = self.fixture('print("test recovers ... ok\\ntest result: ok. 1 passed; 0 failed; 0 ignored;")\n')
        self.assertTrue(success)
        self.assertEqual(len(report["tests"]), 1)

    def test_zero_skipped_wrong_and_failed_tests_cannot_pass(self):
        for text in ["test result: ok. 0 passed; 0 failed; 0 ignored;",
                     "test result: ok. 0 passed; 0 failed; 1 ignored;",
                     "test another ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;",
                     "test result: FAILED. 0 passed; 1 failed; 0 ignored;"]:
            with self.subTest(text=text):
                self.assertFalse(self.fixture(f"print({text!r})\n")[0])

    def test_per_test_deadline_terminates_the_process(self):
        success, report = self.fixture("import time\ntime.sleep(20)\n", TEST_SECONDS=0.05)
        self.assertFalse(success)
        self.assertTrue(report["tests"][0]["timed_out"])
        self.assertLess(report["seconds"], 2)

    def test_a_late_success_cannot_pass_the_test_budget(self):
        def late_success(command, log, seconds, environment):
            log.write_text("test recovers ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
            return 0, False, component.TEST_SECONDS + 0.01
        with patch.object(component.Bundle, "execute", side_effect=late_success):
            self.assertFalse(self.fixture("pass\n")[0])

    def test_shard_deadline_bounds_a_test_with_remaining_budget(self):
        success, report = self.fixture("import time\ntime.sleep(20)\n", SHARD_SECONDS=0.05)
        self.assertFalse(success)
        self.assertTrue(report["tests"][0]["timed_out"])
        self.assertLess(report["seconds"], 2)

    def test_workflow_schedules_every_runner_shard(self):
        workflow = (component.ROOT / ".github/workflows/ci.yml").read_text()
        block = workflow.split("  component-recovery:\n", 1)[1]
        matrix = json.loads(re.search(r"shard: (\[[^\n]+\])", block).group(1))
        self.assertEqual(matrix, list(range(component.SHARDS)))
        self.assertIn("    needs: build\n", block)
        self.assertIn("    timeout-minutes: 5\n", block)

    def test_bundle_rejects_a_changed_binary_or_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "actor"
            executable.write_text("original")
            bundle = component.Bundle(root)
            manifest = {"revision": "a" * 40, "shards": component.SHARDS,
                        "binaries": {"actor": bundle.digest(executable)}, "tests": ["recovers"]}
            (root / "manifest.json").write_text(json.dumps(manifest))
            with patch.dict(component.os.environ, GITHUB_SHA="b" * 40), self.assertRaises(ValueError):
                bundle.load()
            executable.write_text("changed")
            with patch.dict(component.os.environ, GITHUB_SHA="a" * 40), self.assertRaises(ValueError):
                bundle.load()


if __name__ == "__main__":
    unittest.main()
