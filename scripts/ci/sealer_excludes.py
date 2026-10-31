#!/usr/bin/env python3
"""Write a Gradle init script that leaves out the catalogued sealer tests.

The component recovery shards run every catalogued JUnit method of the
sealer (ci_mode gradle in scripts/ci/component-recovery.json). The sealer
tests job runs the rest of the JUnit suite with this init script, so that
each test runs once for each pull request.

A catalogued method that no test source declares fails the script. Then a
rename cannot drop a test from both the shards and the sealer tests job.

Usage: python3 scripts/ci/sealer_excludes.py <init script>
"""

import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "scripts/ci/component-recovery.json"
# The test source roots of the sealer modules.
SOURCES = sorted((ROOT / "cluster/sealer-service").glob("*/src/test/java"))


class Excludes:
    """The catalogued JUnit methods, as Gradle test filter patterns."""

    def __init__(self, catalog):
        suites = json.loads(catalog.read_text())["local_suites"].values()
        # A catalogued test is `Class::method()`; a Gradle filter names it `Class.method`.
        self.patterns = sorted({test.removesuffix("()").replace("::", ".")
                                for suite in suites if suite.get("ci_mode") == "gradle"
                                for test in suite["tests"]})

    @staticmethod
    def declared(pattern):
        """Whether a test source declares the method that `pattern` names."""
        name, _, method = pattern.rpartition(".")
        relative = Path(*name.split(".")).with_suffix(".java")
        declaration = re.compile(rf"\bvoid\s+{re.escape(method)}\s*\(")
        return any(declaration.search((root / relative).read_text())
                   for root in SOURCES if (root / relative).is_file())

    def missing(self):
        """The patterns whose method no test source declares."""
        return [pattern for pattern in self.patterns if not self.declared(pattern)]

    def script(self):
        """The init script: every Test task of the build leaves out each pattern."""
        lines = "".join(f"            excludeTestsMatching '{pattern}'\n" for pattern in self.patterns)
        return ("allprojects {\n"
                "    tasks.withType(Test).configureEach {\n"
                "        filter {\n"
                f"{lines}"
                "        }\n"
                "    }\n"
                "}\n")


if __name__ == "__main__":
    excludes = Excludes(CATALOG)
    missing = excludes.missing()
    if missing:
        sys.exit("catalogued sealer tests that no test source declares:\n" + "\n".join(missing))
    Path(sys.argv[1]).write_text(excludes.script())
    print(f"left out {len(excludes.patterns)} catalogued sealer tests")
