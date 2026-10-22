#!/usr/bin/env python3
"""Finish the existing native cohort, then admit the memory-heavy paired study.

This is an immediate dependent benchmark pipeline, not a recurring scheduler.
It observes one exact live process identity and never stops another worker.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def identity(pid):
    process = subprocess.run(['ps', '-p', str(pid), '-o', 'lstart=', '-o', 'args='], capture_output=True, text=True)
    return process.stdout.strip() if process.returncode == 0 else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--wait-pid', type=int, required=True)
    parser.add_argument('--label', required=True)
    args = parser.parse_args()
    expected = identity(args.wait_pid)
    assert expected and 'tools/matched_snapshot_study.py run --label matched-snapshot-v6-development --workers 2' in expected
    record = {'kind': 'immediate-dependent-benchmark-pipeline', 'observed_pid': args.wait_pid,
              'observed_identity': expected, 'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'started_at': time.time(), 'complete': False,
              'reason': 'Do not overlap another pair of large retained-trace fixtures with the active 20k native cohort.'}
    destination = ROOT / 'evidence' / f'{args.label}-pipeline.json'
    assert not destination.exists()
    destination.write_text(json.dumps(record, indent=2) + '\n')
    while identity(args.wait_pid) == expected:
        time.sleep(10)
    print('Observed native controller terminal; checking source-bound complete target proof.', flush=True)
    baseline = 'evidence/matched-snapshot-v6-development/target-20k'
    candidate = 'evidence/matched-snapshot-v7-admission/all-configs-8'
    for proof in [baseline, candidate]:
        receipt = json.loads((ROOT / proof / 'receipt.json').read_text())
        assert receipt['complete'] and not receipt['failures'], f'Unfinished or failed prerequisite: {proof}'
    command = ['python3', 'tools/prepare_pair_study.py', 'run', '--label', args.label,
               '--baseline-proof', baseline, '--candidate-proof', candidate, '--workers', '2']
    record.update({'wait_finished_at': time.time(), 'command': command})
    destination.write_text(json.dumps(record, indent=2) + '\n')
    process = subprocess.run(command, cwd=ROOT)
    record.update({'complete': process.returncode == 0, 'returncode': process.returncode, 'finished_at': time.time()})
    destination.write_text(json.dumps(record, indent=2) + '\n')
    raise SystemExit(process.returncode)


if __name__ == '__main__':
    main()
