"""A shard requires the complete catalog plan before it executes a test."""

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import component_tests as component


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.executable = self.root / "actor"
        self.executable.write_text(
            f"#!{sys.executable}\nimport sys\n"
            'print("test " + sys.argv[2] + " ... ok")\n'
            'print("test result: ok. 1 passed; 0 failed; 0 ignored;")\n'
        )
        self.executable.chmod(0o755)
        self.bundle = component.Bundle(self.root)
        self.manifest = {
            "revision": None, "libraries": {},
            "binaries": {"actor": self.bundle.digest(self.executable)},
            "tests": [{"suite": "actor", "test": f"case_{i}", "binary": "actor", "name": f"recovery::case_{i}"}
                      for i in range(8)],
        }
        self.catalog_file = self.root / "catalog.json"
        self.catalog = {"local_suites": {
            "actor": {"ci_mode": "in-process", "command": ["cargo", "test", "-p", "actor"],
                      "tests": [f"case_{i}" for i in range(8)]}
        }}
        self.catalog_file.write_text(json.dumps(self.catalog))
        timings = self.root / "timings.json"
        timings.write_text("{}")
        for name, value in [("CATALOG", self.catalog_file), ("TIMINGS", timings)]:
            patcher = patch.object(component, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.environment = patch.dict(component.os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def write_manifest(self, manifest):
        (self.root / "manifest.json").write_text(json.dumps(manifest))

    def shard(self, name):
        return component.Shard(self.bundle, component.SealerRuntime(self.root / "sealer"), self.root / name)

    def test_a_complete_plan_runs_every_test_exactly_once_across_shards(self):
        self.write_manifest(self.manifest)
        executed = []
        for index, tests in enumerate(component.Plan.load().shards()):
            output = self.root / f"results-{index}"
            self.assertTrue(self.shard(output.name).run(index, tests))
            report = json.loads((output / "results.json").read_text())
            executed.extend(test["test"] for test in report["tests"])
        self.assertCountEqual(executed, [test["test"] for test in self.manifest["tests"]])
        self.assertEqual(len(executed), len(set(executed)))

    def test_partial_duplicate_extra_reordered_and_unhashed_plans_cannot_run(self):
        partial = copy.deepcopy(self.manifest)
        partial["tests"] = partial["tests"][:4]
        duplicate = copy.deepcopy(self.manifest)
        duplicate["tests"].append(duplicate["tests"][0].copy())
        extra = copy.deepcopy(self.manifest)
        extra["tests"].append({"suite": "actor", "test": "unknown", "binary": "actor", "name": "unknown"})
        reordered = copy.deepcopy(self.manifest)
        reordered["tests"].reverse()
        unhashed = copy.deepcopy(self.manifest)
        for test in unhashed["tests"]:
            test["binary"] = "unlisted"
        foreign_suite = copy.deepcopy(self.manifest)
        foreign_suite["tests"][0]["suite"] = "unlisted"
        tests = component.Plan.load().shards()[0]
        for name, manifest in [("partial", partial), ("duplicate", duplicate), ("extra", extra),
                               ("reordered", reordered), ("unhashed", unhashed), ("suite", foreign_suite)]:
            with self.subTest(name=name), patch.object(component.Shard, "execute") as execute:
                self.write_manifest(manifest)
                with self.assertRaises(ValueError):
                    self.shard(name).run(0, tests)
                execute.assert_not_called()

    def test_a_shard_uses_the_current_catalog_even_if_binary_hashes_match(self):
        self.write_manifest(self.manifest)
        self.catalog["local_suites"]["actor"]["tests"].append("new_recovery")
        self.catalog_file.write_text(json.dumps(self.catalog))
        with self.assertRaises(ValueError):
            self.bundle.load()

    def test_one_executable_test_cannot_supply_two_catalog_suites(self):
        self.catalog["local_suites"]["second"] = {"ci_mode": "docker", "command": ["cargo", "test", "-p", "actor"],
                                                  "tests": ["case_0"]}
        self.catalog_file.write_text(json.dumps(self.catalog))
        self.manifest["tests"].append({"suite": "second", "test": "case_0", "binary": "actor",
                                       "name": "recovery::case_0"})
        self.write_manifest(self.manifest)
        with self.assertRaises(ValueError):
            self.bundle.load()


if __name__ == "__main__":
    unittest.main()
