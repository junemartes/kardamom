"""The shared dependency build retains the selected Docker recovery coverage."""

import json
from pathlib import Path
import re
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import component_tests as component
from dependency_tests import DependencyBundle, SHARDS


class DependencyTests(unittest.TestCase):
    def setUp(self):
        self.suites = DependencyBundle.selection(json.loads(component.CATALOG.read_text()))

    def test_one_build_contains_every_package_target_and_qualified_feature(self):
        command = DependencyBundle.build_command(self.suites)
        features = set(command[command.index("--features") + 1].split(","))
        self.assertEqual(command.count("cargo"), 1)
        self.assertIn("--no-run", command)
        self.assertIn("--locked", command)
        for suite in self.suites.values():
            original = suite["command"]
            package, target = DependencyBundle.target(suite)
            self.assertIn(package, command)
            if "--test" in original:
                self.assertIn(target, command)
            if "--features" in original:
                expected = {package + "/" + name for name in original[original.index("--features") + 1].split(",")}
                self.assertTrue(expected <= features)

    def test_existing_docker_groups_are_all_selected_without_child_helpers(self):
        self.assertEqual(set(self.suites), {
            "aeron-round-trip", "aeron-client-timeout", "batcher-null-receipts",
            "archive-recovery", "transport-pressure", "cache-redis", "cache-sentinel",
            "state-mirror", "state-mirror-coverage-gap", "checkpoint-copy",
            "checkpoint-fault", "state-aux-enospc", "state-enospc", "ingress-redis",
        })
        tests = [test for suite in self.suites.values() for test in suite["tests"]]
        self.assertFalse(any(test.endswith("_child") or test == "child_holds_one_client" for test in tests))
        self.assertEqual(len(self.suites["cache-redis"]["tests"]), 7)
        self.assertEqual(len(self.suites["ingress-redis"]["tests"]), 7)
        self.assertTrue(DependencyBundle.include_ignored)

    def test_real_dependency_budgets_do_not_relax_in_process_budgets(self):
        actor = component.Bundle(Path("unused"))
        dependency = DependencyBundle(Path("unused"))
        self.assertEqual((actor.test_seconds, actor.shard_seconds), (60, 240))
        self.assertEqual((dependency.test_seconds, dependency.shard_seconds), (180, 720))
        self.assertFalse(actor.include_ignored)

    def test_all_execution_jobs_consume_the_build_without_cargo(self):
        workflow = (component.ROOT / ".github/workflows/docker-e2e.yml").read_text()
        block = workflow.split("  dependency-recovery:\n", 1)[1]
        matrix = json.loads(re.search(r"shard: (\[[^\n]+\])", block).group(1))
        self.assertEqual(matrix, list(range(SHARDS)))
        self.assertIn("    needs: build\n", block)
        self.assertIn("    timeout-minutes: 15\n", block)
        self.assertNotIn("cargo ", block)
        self.assertNotIn("rust-toolchain", block)
        self.assertIn("/tmp/kardamom-ingress-account-profile.jsonl", block)


if __name__ == "__main__":
    unittest.main()
