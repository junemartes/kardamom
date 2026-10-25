"""Check that every test class of test_deploy.py has a part in the matrix
of the deploy tests. A class with no part never runs in CI.

Usage: python3 check_parts.py <workflow file>
"""
import re
import sys
from pathlib import Path

import yaml

MODULE = Path(__file__).with_name('test_deploy.py')
WORKFLOW = Path(sys.argv[1])

source = MODULE.read_text()
classes = re.findall(r'^class (\w+)\([^)]*\):\n(?:(?!^class ).*\n)*?    def test_', source, re.M)
matrix = yaml.safe_load(WORKFLOW.read_text())['jobs']['deploy-tests']['strategy']['matrix']['include']
parts = {entry['part'] for entry in matrix}
missing = sorted(set(classes) - parts)
if missing:
    sys.exit(f'test classes with no part in {WORKFLOW}: {", ".join(missing)}')
print(f'every test class has a part: {", ".join(sorted(classes))}')
