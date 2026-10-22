#!/usr/bin/env python3
"""Source-bound parallel exploratory cohorts; no final speed acceptance."""
import argparse
import concurrent.futures
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
COHORTS = 'research/matched-snapshot-v6-cohorts.json'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tasks_for(phase):
    return [(engine, strategy, codec, phase['peers'], case, size, trace, delay)
            for (engine, strategy, codec), size, (case, trace, delay) in
            itertools.product(phase['configurations'], phase['sizes'], phase['profiles'])]


def validate(row, task):
    keys = ['engine', 'strategy', 'codec', 'peers', 'case', 'size', 'trace', 'ack_delay']
    assert tuple(row[k] for k in keys) == tuple(task)
    assert row['verified'] is True and row['timing_acceptance'] is False
    assert row['frames'] == 32 and row['fixed_clock_ns'] == 16666667
    assert row['decoder_replays'] == row['peers']
    assert row['full_value_checks'] == row['exact_ack_frames'] == row['peers'] * (41 + row['ack_delay'])
    assert row['peak_rss_bytes'] > 0
    if row['trace'] == 'loss' and row['peers'] >= 8:
        assert all(row[k] > 0 for k in ['dropped', 'delayed', 'duplicated'])
    for key in ['publication_ns', 'server_step_ns', 'ack_ingest_ns', 'receiver_setup_ns',
                'receiver_ingest_ns', 'receiver_step_ns', 'ack_wire_bytes', 'server_wire_bytes']:
        assert row[key] >= 0


def run(args):
    build_path = ROOT / 'evidence' / f'{args.binary}-build.json'
    build = json.loads(build_path.read_text())
    binary = ROOT / build['binary']
    assert sha(binary) == build['binary_sha256']
    cohorts = json.loads((ROOT / COHORTS).read_text())
    phases = cohorts['phases']
    assert args.workers in [1, 2]
    root = ROOT / 'evidence' / args.label
    root.mkdir(exist_ok=False)
    for phase in phases:
        folder = root / phase['name']
        folder.mkdir()
        tasks = tasks_for(phase)
        assert len(set(tasks)) == len(tasks)
        random.Random(cohorts['shuffle_seed']).shuffle(tasks)
        record = {'binary': args.binary, 'binary_sha256': sha(binary),
                  'build_receipt_sha256': sha(build_path), 'cohorts': COHORTS,
                  'cohorts_sha256': sha(ROOT / COHORTS), 'phase': phase['name'],
                  'plan_sha256': sha(ROOT / cohorts['plan']), 'workers': args.workers,
                  'tasks': len(tasks), 'started_at': time.time(), 'complete': False,
                  'failures': [], 'claim': 'Parallel development screen; no engine speed acceptance; RSS includes traces/oracle'}
        receipt = folder / 'receipt.json'
        receipt.write_text(json.dumps(record, indent=2) + '\n')

        def execute(index, task):
            stdout = folder / f'current-{index}.stdout'
            stderr = folder / f'current-{index}.stderr'
            with stdout.open('w') as out, stderr.open('w') as err:
                start = time.time()
                process = subprocess.Popen([str(binary), *map(str, task)], stdout=out, stderr=err)
                _, status, usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)
            if process.returncode:
                row = {'task': task, 'returncode': process.returncode, 'error': stderr.read_text()}
            else:
                row = json.loads(stdout.read_text())
                row.update({'peak_rss_bytes': usage.ru_maxrss * (1 if platform.system() == 'Darwin' else 1024),
                            'process_user_seconds': usage.ru_utime, 'process_system_seconds': usage.ru_stime,
                            'started_at': start, 'process_wall_seconds': time.time() - start})
                validate(row, task)
            stdout.unlink()
            stderr.unlink()
            return row

        with (folder / 'runs.jsonl').open('w') as output, concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(execute, index, task): task for index, task in enumerate(tasks)}
            for completed, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    row = future.result()
                except Exception as error:
                    row = {'task': futures[future], 'error': repr(error)}
                if 'error' in row:
                    record['failures'].append(row)
                output.write(json.dumps(row) + '\n')
                output.flush()
                if completed % 16 == 0 or completed == len(tasks):
                    print(f"{phase['name']} {completed}/{len(tasks)} complete; failures={len(record['failures'])}", flush=True)
        record.update({'complete': not record['failures'], 'finished_at': time.time(),
                       'runs_sha256': sha(folder / 'runs.jsonl')})
        receipt.write_text(json.dumps(record, indent=2) + '\n')
        if not record['complete']:
            raise SystemExit('Correctness failure: later phase not admitted')
        audit_phase(folder)


def historical(build, path):
    return subprocess.check_output(['git', 'show', f"{build['source_commit']}:{path}"], cwd=ROOT)


def audit_phase(folder):
    record = json.loads((folder / 'receipt.json').read_text())
    assert record['complete'] and not record['failures']
    assert sha(folder / 'runs.jsonl') == record['runs_sha256']
    build_path = ROOT / 'evidence' / f"{record['binary']}-build.json"
    assert sha(build_path) == record['build_receipt_sha256']
    build = json.loads(build_path.read_text())
    assert sha(ROOT / build['binary']) == build['binary_sha256'] == record['binary_sha256']
    for path, digest in build['inputs'].items():
        assert hashlib.sha256(historical(build, path)).hexdigest() == digest
    data = historical(build, record['cohorts'])
    assert hashlib.sha256(data).hexdigest() == record['cohorts_sha256']
    cohorts = json.loads(data)
    assert hashlib.sha256(historical(build, cohorts['plan'])).hexdigest() == record['plan_sha256']
    phase, = [p for p in cohorts['phases'] if p['name'] == record['phase']]
    expected = set(tasks_for(phase))
    rows = [json.loads(line) for line in (folder / 'runs.jsonl').read_text().splitlines()]
    keys = ['engine', 'strategy', 'codec', 'peers', 'case', 'size', 'trace', 'ack_delay']
    assert {tuple(r[k] for k in keys) for r in rows} == expected
    assert len(rows) == len(expected) == record['tasks']
    for row in rows:
        validate(row, tuple(row[k] for k in keys))
    for case, size in {(r['case'], r['size']) for r in rows}:
        assert len({r['model_digest'] for r in rows if (r['case'], r['size']) == (case, size)}) == 1
    result = {'complete': True, 'processes': len(rows), 'phase': record['phase'],
              'receipt_sha256': sha(folder / 'receipt.json'), 'runs_sha256': record['runs_sha256'],
              'decoder_replays': sum(r['decoder_replays'] for r in rows),
              'full_value_checks': sum(r['full_value_checks'] for r in rows),
              'peak_process_rss_bytes': max(r['peak_rss_bytes'] for r in rows),
              'claim': 'Complete framed snapshot proof and exploratory timing. One resident decoder at a time; server peers coexist. No sockets, p99, or all-engine winner.'}
    (folder / 'audit.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='action', required=True)
    run_parser = sub.add_parser('run')
    run_parser.add_argument('--binary', default='matched-snapshot-v6')
    run_parser.add_argument('--label', required=True)
    run_parser.add_argument('--workers', type=int, default=2)
    audit_parser = sub.add_parser('audit')
    audit_parser.add_argument('folder')
    args = parser.parse_args()
    if args.action == 'run':
        run(args)
    else:
        audit_phase(ROOT / args.folder)
