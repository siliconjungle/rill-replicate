#!/usr/bin/env python3
"""Require complete development coverage and retain every configuration's result."""
import argparse
import hashlib
import itertools
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def review(label):
    folder = ROOT/'evidence'/label
    receipt_path = folder/'receipt.json'
    receipt = json.loads(receipt_path.read_text())
    assert receipt['complete'] and not receipt['failures']
    assert sha(folder/'runs.jsonl') == receipt['runs_sha256']
    build_path = ROOT/'evidence/fanout-v1-build.json'
    assert sha(build_path) == receipt['build_receipt_sha256']
    build = json.loads(build_path.read_text())
    assert build['binary_sha256'] == receipt['binary_sha256']
    assert sha(ROOT/build['binary']) == build['binary_sha256']
    # Historical inputs are verified against the recorded source commit. Later
    # reports may exist in the current checkout without rewriting frozen inputs.
    import subprocess
    for name, expected in build['inputs'].items():
        contents = subprocess.check_output(['git','show',f'{build["source_commit"]}:{name}'],cwd=ROOT)
        assert hashlib.sha256(contents).hexdigest() == expected, name
    rows = [json.loads(line) for line in (folder/'runs.jsonl').read_text().splitlines()]
    expected = set(itertools.product(['full','shared-full','individual','shared','adaptive'], ['index','mask'], [4,16], [0,64], receipt['cases']))
    assert len(rows) == receipt['tasks'] == len(expected)
    by_key = {}
    for row in rows:
        key = tuple(row[k] for k in ['strategy','codec','history','cache','case'])
        assert key in expected and key not in by_key
        assert row['verified'] is True and row['peers'] == receipt['peers'] and row['frames'] == receipt['frames']
        assert row['retained_revisions'] <= row['history']
        assert row['cached_bytes'] <= 1024*1024 and row['pending_attempts'] <= row['peers']*8
        assert row['applied'] >= row['peers'] and row['sender_ns'] > 0 and row['wall_ns'] > row['sender_ns']
        by_key[key] = row
    assert set(by_key) == expected
    contrasts = []
    for codec, history, cache, case in itertools.product(['index','mask'],[4,16],[0,64],receipt['cases']):
        baseline = by_key[('shared-full',codec,history,cache,case)]
        for strategy in ['full','individual','shared','adaptive']:
            row = by_key[(strategy,codec,history,cache,case)]
            contrasts.append({'strategy':strategy,'codec':codec,'history':history,'cache':cache,'case':case,
                              'sender_time_ratio_to_shared_full':row['sender_ns']/baseline['sender_ns'],
                              'body_bytes_ratio_to_shared_full':row['logical_body_bytes']/baseline['logical_body_bytes']})
    return {'kind':'development-only-no-winner','complete_processes':len(rows),'peers':receipt['peers'],
            'receipt_sha256':sha(receipt_path),'runs_sha256':receipt['runs_sha256'],
            'limitations':['one observation per cell','parallel with other workers','body bytes omit transport framing',
                           'setup excluded','forced final refresh included','no upstream adapter yet','not live sockets'],
            'all_results': sorted(rows,key=lambda r:(r['case'],r['codec'],r['history'],r['cache'],r['strategy'])),
            'all_matched_contrasts':contrasts}


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('label'); parser.add_argument('--output',required=True)
    args=parser.parse_args()
    result=review(args.label)
    (ROOT/args.output).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ['all_results','all_matched_contrasts']},indent=2))
