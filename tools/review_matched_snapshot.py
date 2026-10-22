#!/usr/bin/env python3
"""Retain every fixed native/Rill development contrast, including losses."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def review(folder):
    audit = json.loads((folder / 'audit.json').read_text())
    assert audit['complete'] and audit['runs_sha256'] == sha(folder / 'runs.jsonl')
    rows = [json.loads(line) for line in (folder / 'runs.jsonl').read_text().splitlines()]
    key = lambda r: (r['peers'], r['case'], r['size'], r['trace'], r['ack_delay'])
    native = {key(r): r for r in rows if r['engine'] == 'native'}
    comparisons = []
    for row in rows:
        if row['engine'] == 'native':
            continue
        reference = native[key(row)]
        assert row['model_digest'] == reference['model_digest']
        metrics = {}
        for name, columns in {
            'sender_work_ns': ['publication_ns', 'server_step_ns', 'ack_ingest_ns'],
            'receiver_work_ns': ['receiver_ingest_ns', 'receiver_step_ns'],
            'complete_message_bytes': ['server_wire_bytes', 'ack_wire_bytes'],
        }.items():
            ours = sum(row[k] for k in columns)
            theirs = sum(reference[k] for k in columns)
            metrics[name] = {'rill': ours, 'native': theirs, 'rill_div_native': ours / theirs}
        comparisons.append({'strategy': row['strategy'], 'codec': row['codec'],
                            'cell': key(row), 'metrics': metrics})
    summaries = []
    for strategy, codec in sorted({(r['strategy'], r['codec']) for r in comparisons}):
        cells = [r for r in comparisons if (r['strategy'], r['codec']) == (strategy, codec)]
        summaries.append({'strategy': strategy, 'codec': codec, 'cells': len(cells), 'metrics': {
            name: {'observed_ratio_median_across_different_cells': statistics.median(r['metrics'][name]['rill_div_native'] for r in cells),
                   'worst_observed_ratio': max(r['metrics'][name]['rill_div_native'] for r in cells),
                   'observed_lower_cells': sum(r['metrics'][name]['rill'] < r['metrics'][name]['native'] for r in cells),
                   'observed_equal_cells': sum(r['metrics'][name]['rill'] == r['metrics'][name]['native'] for r in cells),
                   'observed_higher_cells': sum(r['metrics'][name]['rill'] > r['metrics'][name]['native'] for r in cells)}
            for name in cells[0]['metrics']}})
    result = {'audit_sha256': sha(folder / 'audit.json'), 'runs_sha256': sha(folder / 'runs.jsonl'),
              'analysis_source_sha256': sha(Path(__file__)), 'processes': len(rows),
              'claim': 'Exploratory observed ratios, one observation per cell; no confidence intervals or performance acceptance. All fixed contrasts retained. Native provides additional ECS capabilities. Process RSS is not library-only memory.',
              'all_fixed_contrasts': comparisons, 'fixed_candidate_summaries': summaries}
    (folder / 'review.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'processes': len(rows), 'fixed_candidate_summaries': summaries}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('folder')
    args = parser.parse_args()
    review(ROOT / args.folder)
