"""Each shard requires the complete catalog plan before it executes a test."""

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
            "revision": None, "shards": component.SHARDS,
            "binaries": {"actor": self.bundle.digest(self.executable)},
            "tests": [{"suite": "actor", "test": f"recovery::case_{i}", "binary": "actor"}
                      for i in range(8)],
        }
        catalog = self.root / "catalog.json"
        catalog.write_text(json.dumps({"local_suites": {
            "actor": {"ci_mode": "in-process", "tests": [f"case_{i}" for i in range(8)]}
        }}))
        self.catalog = patch.object(component, "CATALOG", catalog)
        self.catalog.start()
        self.addCleanup(self.catalog.stop)
        self.environment = patch.dict(component.os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def write_manifest(self, manifest):
        (self.root / "manifest.json").write_text(json.dumps(manifest))

    def test_a_complete_plan_runs_every_test_exactly_once_across_shards(self):
        self.write_manifest(self.manifest)
        executed = []
        for shard in range(component.SHARDS):
            output = self.root / f"results-{shard}"
            self.assertTrue(self.bundle.run(shard, output))
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
        extra["tests"].append({"suite": "actor", "test": "recovery::unknown", "binary": "actor"})
        reordered = copy.deepcopy(self.manifest)
        reordered["tests"].reverse()
        unhashed = copy.deepcopy(self.manifest)
        for test in unhashed["tests"]:
            test["binary"] = "unlisted"
        foreign_suite = copy.deepcopy(self.manifest)
        foreign_suite["tests"][0]["suite"] = "unlisted"
        for name, manifest in [("partial", partial), ("duplicate", duplicate), ("extra", extra),
                               ("reordered", reordered), ("unhashed", unhashed), ("suite", foreign_suite)]:
            with self.subTest(name=name), patch.object(self.bundle, "execute") as execute:
                self.write_manifest(manifest)
                with self.assertRaises(ValueError):
                    self.bundle.run(0, self.root / name)
                execute.assert_not_called()

    def test_a_shard_uses_the_current_catalog_even_if_binary_hashes_match(self):
        self.write_manifest(self.manifest)
        catalog = json.loads(component.CATALOG.read_text())
        catalog["local_suites"]["actor"]["tests"].append("new_recovery")
        component.CATALOG.write_text(json.dumps(catalog))
        with self.assertRaises(ValueError):
            self.bundle.load()

    def test_one_executable_test_cannot_supply_two_catalog_suites(self):
        catalog = json.loads(component.CATALOG.read_text())
        catalog["local_suites"]["second"] = {"ci_mode": "in-process", "tests": ["case_0"]}
        component.CATALOG.write_text(json.dumps(catalog))
        self.manifest["tests"].append({
            "suite": "second", "test": "recovery::case_0", "binary": "actor"
        })
        self.write_manifest(self.manifest)
        with self.assertRaises(ValueError):
            self.bundle.load()
