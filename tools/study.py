#!/usr/bin/env python3
"""Frozen-binary development screen; never a final speed acceptance gate."""
import argparse
import concurrent.futures
import hashlib
import itertools
import json
import os
import pathlib
import random
import shutil
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def bindings():
    paths = [ROOT / 'Cargo.toml', ROOT / 'Cargo.lock']
    for directory in ['src', 'tests', 'examples', 'tools', 'research']:
        paths += sorted((ROOT / directory).rglob('*.rs' if directory in ['src', 'tests', 'examples'] else '*'))
    generated={'target','build','evidence','__pycache__','.git'}
    return {str(p.relative_to(ROOT)): digest(p) for p in paths if p.is_file() and not generated.intersection(p.relative_to(ROOT).parts)}


def freeze(label, example='fanout', features=None, manifest=None, binary_name=None, toolchain='stable'):
    binary = ROOT / 'build' / label
    if binary.exists():
        raise RuntimeError('Frozen binary already exists')
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT).strip():
        raise RuntimeError('Commit source first')
    inputs = bindings()
    env = dict(os.environ, RUSTUP_TOOLCHAIN=toolchain, CARGO_BUILD_JOBS='1')
    project = (ROOT/manifest).parent if manifest else ROOT
    preflight = subprocess.check_output(['ink-cache', 'prepare-sources', str(project), '--offline'], cwd=ROOT, env=env, text=True)
    args = ['cargo', 'build', '--release', '--locked']
    args += ['--bin',binary_name] if binary_name else ['--example',example]
    if manifest:
        args += ['--manifest-path',str(ROOT/manifest)]
    if features:
        args += ['--features', features]
    log = ROOT / 'evidence' / f'{label}-build.log'
    with log.open('w') as output:
        subprocess.run(args, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=True)
    binary.parent.mkdir(exist_ok=True)
    artifact = project/'target/release'/binary_name if binary_name else project/'target/release/examples'/example
    shutil.copy2(artifact, binary)
    assert inputs == bindings(), 'Inputs changed during build'
    receipt = {
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'inputs': inputs, 'compiler': subprocess.check_output(['rustc', f'+{toolchain}', '-Vv'], text=True),
        'command': args, 'environment': {k: env.get(k) for k in ['RUSTUP_TOOLCHAIN', 'CARGO_BUILD_JOBS', 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RAYON_NUM_THREADS']},
        'preflight': json.loads(preflight), 'binary': str(binary.relative_to(ROOT)),
        'binary_sha256': digest(binary), 'build_log_sha256': digest(log),
        'machine': subprocess.check_output(['uname', '-a'], text=True).strip(),
    }
    (ROOT / 'evidence' / f'{label}-build.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print(json.dumps(receipt, indent=2))


def study(args):
    binary = ROOT / 'build' / args.binary
    build_receipt = ROOT / 'evidence' / f'{args.binary}-build.json'
    assert digest(binary) == json.loads(build_receipt.read_text())['binary_sha256']
    destination = ROOT / 'evidence' / args.label
    destination.mkdir(exist_ok=False)
    tasks = list(itertools.product(['full','shared-full','individual','shared','adaptive'], ['index','mask'], [4,16], [0,64], args.cases))
    random.Random(20261022).shuffle(tasks)
    receipt = {'kind': 'development-only', 'binary_sha256': digest(binary), 'build_receipt_sha256': digest(build_receipt),
               'workers': args.workers, 'peers': args.peers, 'frames': args.frames, 'tasks': len(tasks),
               'cases': args.cases, 'complete': False, 'failures': [], 'started_at': time.time()}
    receipt_path = destination / 'receipt.json'
    receipt_path.write_text(json.dumps(receipt, indent=2)+'\n')

    def execute(task):
        strategy, codec, history, cache, case = task
        command = [str(binary), strategy, codec, str(history), str(cache), str(args.peers), str(args.frames), case]
        start = time.time()
        process = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if process.returncode:
            return {'task':task,'error':process.stderr,'returncode':process.returncode}
        result = json.loads(process.stdout)
        assert result['verified'] is True and result['peers'] == args.peers and result['frames'] == args.frames
        assert (result['strategy'], result['codec'], result['history'], result['cache'], result['case']) == task
        result['started_at'] = start
        return result

    with (destination/'runs.jsonl').open('w') as out, concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(execute, task): task for task in tasks}
        for completed, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                row = future.result()
            except Exception as error:
                row = {'task': futures[future], 'error':repr(error)}
            if 'error' in row: receipt['failures'].append(row)
            out.write(json.dumps(row)+'\n'); out.flush()
            if completed % 8 == 0: print(f'{completed}/{len(tasks)} completed; failures={len(receipt["failures"])}', flush=True)
    receipt['finished_at'] = time.time()
    receipt['complete'] = not receipt['failures']
    receipt['runs_sha256'] = digest(destination/'runs.jsonl')
    receipt_path.write_text(json.dumps(receipt, indent=2)+'\n')
    if receipt['failures']: raise SystemExit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='action', required=True)
    f = sub.add_parser('freeze'); f.add_argument('label')
    f.add_argument('--example',default='fanout'); f.add_argument('--features')
    f.add_argument('--manifest'); f.add_argument('--bin'); f.add_argument('--toolchain',default='stable')
    s = sub.add_parser('run')
    s.add_argument('--binary', required=True); s.add_argument('--label', required=True)
    s.add_argument('--peers', type=int, default=20_000); s.add_argument('--frames', type=int, default=32)
    s.add_argument('--workers', type=int, default=2)
    s.add_argument('--cases', nargs='+', choices=['sync','staggered','loss','dense'], default=['sync','staggered','loss','dense'])
    a = parser.parse_args()
    if a.action == 'freeze': freeze(a.label,a.example,a.features,a.manifest,a.bin,a.toolchain)
    else:
        assert a.workers>0 and a.peers>0 and a.frames>=32 and len(set(a.cases))==len(a.cases)
        study(a)
