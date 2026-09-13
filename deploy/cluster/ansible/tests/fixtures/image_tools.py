#!/usr/bin/env python3
"""Isolated Docker/cosign protocol stand-in; only used by test_images.py."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

args = sys.argv[1:]
state_dir = Path(os.environ['KARDAMOM_IMAGE_TEST_DIR'])
state_file = state_dir / 'engine.json'
state = json.loads(state_file.read_text()) if state_file.exists() else {}
with (state_dir / 'calls.jsonl').open('a') as stream:
    stream.write(json.dumps([Path(sys.argv[0]).name, args]) + '\n')


def save():
    state_file.write_text(json.dumps(state))


def fail_at(stage):
    if os.environ.get('KARDAMOM_IMAGE_TEST_FAIL') == stage:
        print('injected failure: ' + stage, file=sys.stderr)
        sys.exit(1)


if Path(sys.argv[0]).name == 'cosign':
    fail_at(args[0])
    if args[0] == 'sign-blob':
        manifest = Path(args[-1]).read_text()
        assert len(manifest.splitlines()) == 8, manifest
        Path(args[args.index('--bundle') + 1]).write_text(manifest)
    sys.exit(0)

while args[0] in ('--host', '--context'):
    args = args[2:]
if args[0] == 'context':
    print('test-context')
elif args[0] == 'version':
    print(json.dumps({'Client': {'Version': '28.0.0'}, 'Server': {'ApiVersion': '1.48'}}))
elif args[0] == 'info':
    print(json.dumps({'ClientInfo': {'Plugins': [{'Name': 'buildx', 'Version': 'v0.21.0'}]}}))
elif args[:2] == ['image', 'ls']:
    repo = args[-1].removeprefix('reference=')
    for ref, image_id in state.items():
        if ref.rsplit(':', 1)[0] == repo:
            print(json.dumps({'Repository': repo, 'Tag': ref.rsplit(':', 1)[1], 'ID': image_id}))
elif args[:2] == ['image', 'inspect']:
    print(json.dumps([{'Id': args[-1]}]))
elif args[:2] == ['buildx', 'build']:
    fail_at('build')
    ref = args[args.index('--tag') + 1]
    context = Path(args[-1])
    dockerfile = Path(args[args.index('--file') + 1])
    assert dockerfile.is_file()
    if dockerfile.name == 'ci-service.Dockerfile':
        binary = args[args.index('--build-arg') + 1].removeprefix('BIN=')
        assert (context / binary).is_file()
        assert (context / '_aeronlibs/libaeron.so').is_file()
        assert (context / '_aeronlibs/libaeron_archive_c_client.so').is_file()
    if dockerfile.name == 'cluster.Dockerfile':
        assert (context / 'kardamom-cluster-node.jar').is_file()
    state[ref] = 'sha256:' + hashlib.sha256(ref.encode()).hexdigest()
    save()
elif args[:2] == ['image', 'save']:
    Path(args[args.index('--output') + 1]).write_text(args[-1])
elif args[0] == 'cp':
    fail_at('copy')
    shutil.copyfile(args[1], state_dir / Path(args[2].split(':', 1)[1]).name)
elif args[0] == 'exec' and args[2:5] == ['docker', 'image', 'load']:
    fail_at('load')
    archive = state_dir / Path(args[-1]).name
    ref = archive.read_text()
    state['node:' + ref] = state[ref]
    save()
elif args[0] == 'exec' and args[2] == 'rm':
    (state_dir / Path(args[-1]).name).unlink(missing_ok=True)
elif args[0] == 'push' or (args[0] == 'exec' and args[2:4] == ['docker', 'push']):
    fail_at('push')
    ref = args[-1]
    key = 'node:' + ref if args[0] == 'exec' else ref
    assert key in state, ('pushing image missing from engine', key)
    digest = state[key]
    if os.environ.get('KARDAMOM_IMAGE_TEST_FAIL') == 'digest':
        digest = 'sha256:invalid'
    # Exercise CRLF output and progress lines; the pushed digest is authoritative.
    print('layer: Pushed\r')
    print(f'test: digest: {digest} size: 1234\r')
else:
    raise AssertionError(args)
