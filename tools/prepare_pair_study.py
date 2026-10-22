#!/usr/bin/env python3
"""Counterbalanced fresh-process v6/v7 blocks, retaining every observation."""
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

import matched_snapshot_study as study

ROOT = Path(__file__).resolve().parents[1]
PLAN = 'research/prepare-hot-path-v7-plan.json'
SAME = ['engine', 'strategy', 'codec', 'peers', 'case', 'size', 'trace', 'ack_delay',
        'model_digest', 'server_wire_bytes', 'ack_wire_bytes', 'trace_messages',
        'dropped', 'delayed', 'duplicated', 'decoder_replays', 'full_value_checks', 'exact_ack_frames']
METRICS = {'sender_work_ns': ['publication_ns', 'server_step_ns', 'ack_ingest_ns'],
           'receiver_work_ns': ['receiver_ingest_ns', 'receiver_step_ns']}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args):
    plan = json.loads((ROOT / PLAN).read_text())
    assert args.workers in [1, 2]
    study.audit_phase(ROOT / args.baseline_proof)
    study.audit_phase(ROOT / args.candidate_proof)
    builds = {}
    for version in ['baseline', 'candidate']:
        label = getattr(args, version)
        path = ROOT / 'evidence' / f'{label}-build.json'
        build = json.loads(path.read_text())
        assert sha(ROOT / build['binary']) == build['binary_sha256']
        proof = json.loads((ROOT / getattr(args, f'{version}_proof') / 'receipt.json').read_text())
        assert proof['binary'] == label
        builds[version] = {'label': label, 'receipt_sha256': sha(path), 'receipt': build,
                           'proof': getattr(args, f'{version}_proof'),
                           'proof_audit_sha256': sha(ROOT / getattr(args, f'{version}_proof') / 'audit.json')}
    assert builds['baseline']['receipt']['source_commit'] == plan['baseline_source']
    blocks = [(config, profile, repeat) for config, profile, repeat in
              itertools.product(plan['paired_candidates'], plan['paired_profiles'], range(plan['paired_repetitions']))]
    rng = random.Random(plan['random_seed'])
    rng.shuffle(blocks)
    tasks = []
    for index, (config, profile, repeat) in enumerate(blocks):
        versions = ['baseline', 'candidate']
        rng.shuffle(versions)
        tasks.append((index, config, profile, repeat, versions))
    folder = ROOT / 'evidence' / args.label
    folder.mkdir(exist_ok=False)
    receipt = {'kind': 'parallel-development-paired-blocks', 'plan': PLAN,
               'plan_sha256': sha(ROOT / PLAN), 'analysis_source_sha256': sha(Path(__file__)),
               'blocks': len(blocks), 'processes': len(blocks) * 2, 'workers': args.workers,
               'builds': builds, 'complete': False, 'failures': [], 'started_at': time.time(),
               'claim': 'Three repeated exploratory observations per cell; other host workers active. No confidence interval or final speed acceptance.'}
    receipt_path = folder / 'receipt.json'
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')

    def execute(task):
        index, config, (case, trace, delay), repeat, versions = task
        command_args = [*config, plan['paired_peers'], case, plan['paired_size'], trace, delay]
        results = []
        for version in versions:
            stdout = folder / f'current-{index}-{version}.stdout'
            stderr = folder / f'current-{index}-{version}.stderr'
            binary = ROOT / builds[version]['receipt']['binary']
            with stdout.open('w') as out, stderr.open('w') as err:
                start = time.time()
                process = subprocess.Popen([str(binary), *map(str, command_args)], stdout=out, stderr=err)
                _, status, usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)
            if process.returncode:
                row = {'error': stderr.read_text(), 'returncode': process.returncode, 'task': command_args}
            else:
                row = json.loads(stdout.read_text())
                row.update({'peak_rss_bytes': usage.ru_maxrss * (1 if platform.system() == 'Darwin' else 1024),
                            'process_user_seconds': usage.ru_utime, 'process_system_seconds': usage.ru_stime,
                            'process_wall_seconds': time.time() - start, 'started_at': start})
                study.validate(row, command_args)
            row.update({'version': version, 'block': index, 'repeat': repeat, 'version_order': versions})
            results.append(row)
            stdout.unlink()
            stderr.unlink()
        if not any('error' in r for r in results):
            differences = {k: [results[0][k], results[1][k]] for k in SAME if results[0][k] != results[1][k]}
            if differences:
                results[0].update({'error': 'Protocol/model counts changed between versions', 'differences': differences})
        return results

    with (folder / 'runs.jsonl').open('w') as output, concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(execute, t): t for t in tasks}
        for completed, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                rows = future.result()
            except Exception as error:
                rows = [{'task': futures[future], 'error': repr(error)}]
            for row in rows:
                if 'error' in row:
                    receipt['failures'].append(row)
                output.write(json.dumps(row) + '\n')
            output.flush()
            if completed % 4 == 0 or completed == len(tasks):
                print(f"{completed}/{len(tasks)} paired blocks; failures={len(receipt['failures'])}", flush=True)
    receipt.update({'complete': not receipt['failures'], 'finished_at': time.time(),
                    'runs_sha256': sha(folder / 'runs.jsonl')})
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    if not receipt['complete']:
        raise SystemExit(1)
    audit(folder)


def audit(folder):
    receipt = json.loads((folder / 'receipt.json').read_text())
    assert receipt['complete'] and not receipt['failures']
    assert sha(folder / 'runs.jsonl') == receipt['runs_sha256']
    assert sha(ROOT / receipt['plan']) == receipt['plan_sha256']
    plan = json.loads((ROOT / receipt['plan']).read_text())
    for version, binding in receipt['builds'].items():
        build_path = ROOT / 'evidence' / f"{binding['label']}-build.json"
        assert sha(build_path) == binding['receipt_sha256']
        build = json.loads(build_path.read_text())
        assert sha(ROOT / build['binary']) == build['binary_sha256']
        for path, digest in build['inputs'].items():
            assert hashlib.sha256(study.historical(build, path)).hexdigest() == digest
        assert sha(ROOT / binding['proof'] / 'audit.json') == binding['proof_audit_sha256']
    rows = [json.loads(line) for line in (folder / 'runs.jsonl').read_text().splitlines()]
    assert len(rows) == receipt['processes']
    expected = {(tuple(c), tuple(p), r, v) for c, p, r, v in itertools.product(
        plan['paired_candidates'], plan['paired_profiles'], range(plan['paired_repetitions']), ['baseline', 'candidate'])}
    actual = {(tuple(r[k] for k in ['engine', 'strategy', 'codec']), tuple(r[k] for k in ['case', 'trace', 'ack_delay']),
               r['repeat'], r['version']) for r in rows}
    assert actual == expected
    assert all(r['peers'] == plan['paired_peers'] and r['size'] == plan['paired_size'] for r in rows)
    contrasts = []
    for block in range(receipt['blocks']):
        pair = {r['version']: r for r in rows if r['block'] == block}
        assert set(pair) == {'baseline', 'candidate'}
        base, candidate = pair['baseline'], pair['candidate']
        assert all(base[k] == candidate[k] for k in SAME)
        for row in pair.values():
            study.validate(row, [row[k] for k in ['engine', 'strategy', 'codec', 'peers', 'case', 'size', 'trace', 'ack_delay']])
        contrasts.append({'block': block, 'strategy': base['strategy'], 'codec': base['codec'],
                          'case': base['case'], 'trace': base['trace'], 'ack_delay': base['ack_delay'],
                          'repeat': base['repeat'], 'metrics': {
                              metric: {'baseline': sum(base[k] for k in columns), 'candidate': sum(candidate[k] for k in columns),
                                       'candidate_div_baseline': sum(candidate[k] for k in columns) / sum(base[k] for k in columns)}
                              for metric, columns in METRICS.items()}})
    result = {'complete': True, 'processes': len(rows), 'receipt_sha256': sha(folder / 'receipt.json'),
              'runs_sha256': receipt['runs_sha256'], 'analysis_source_sha256': sha(Path(__file__)),
              'all_fixed_contrasts': contrasts, 'claim': receipt['claim']}
    (folder / 'audit.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'complete': True, 'processes': len(rows), 'paired_blocks': len(contrasts)}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='action', required=True)
    run_parser = sub.add_parser('run')
    run_parser.add_argument('--baseline', default='matched-snapshot-v6')
    run_parser.add_argument('--candidate', default='matched-snapshot-v7')
    run_parser.add_argument('--baseline-proof', required=True)
    run_parser.add_argument('--candidate-proof', required=True)
    run_parser.add_argument('--label', required=True)
    run_parser.add_argument('--workers', type=int, default=2)
    audit_parser = sub.add_parser('audit')
    audit_parser.add_argument('folder')
    args = parser.parse_args()
    if args.action == 'run':
        run(args)
    else:
        audit(ROOT / args.folder)
