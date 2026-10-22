#!/usr/bin/env python3
"""All fixed profiles, not a per-case oracle or final performance acceptance."""
import hashlib
import itertools
import json
import pathlib
import statistics

ROOT=pathlib.Path(__file__).resolve().parents[1]


def main():
    folder=ROOT/'evidence/codecs-v2-full'
    audit=json.loads((ROOT/'evidence/codecs-v2-full-audit.json').read_text())
    assert audit['complete'] and audit['processes']==6240
    raw=(folder/'runs.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()==audit['runs_sha256']
    rows=[json.loads(line) for line in raw.splitlines()]
    plan=json.loads((ROOT/'research/byte-codec-plan.json').read_text())
    keys=['encode_ns','decode_ns','delta_bytes','full_bytes','full_encode_ns']
    cells={}
    for codec,case,size in itertools.product(plan['codecs'],plan['component']['cases'],plan['component']['sizes']):
        observations=[r for r in rows if (r['codec'],r['case'],r['size'])==(codec,case,size)]
        assert len(observations)==6
        assert {(r['seed'],r['repetition']) for r in observations}==set(itertools.product(plan['component']['seeds'],range(3)))
        cells[(codec,case,size)]={metric:statistics.median(r[metric] for r in observations) for metric in keys}
    own=[c for c in plan['codecs'] if 'runs-b' in c]
    external=[c for c in plan['codecs'] if any(n in c for n in ['bsdiff','bidiff'])]
    summaries=[]
    for candidate,competitor in itertools.product(own,external):
        comparison={'candidate':candidate,'competitor':competitor,'cells':20,'metrics':{}}
        for metric in ['encode_ns','decode_ns','delta_bytes']:
            pairs=[(cells[(candidate,case,size)][metric],cells[(competitor,case,size)][metric])
                   for case,size in itertools.product(plan['component']['cases'],plan['component']['sizes'])]
            comparison['metrics'][metric]={'lower_cells':sum(a<b for a,b in pairs),'equal_cells':sum(a==b for a,b in pairs),
                                           'worst_ratio':max(a/b for a,b in pairs)}
        summaries.append(comparison)
    all_medians=[{'codec':codec,'case':case,'size':size,**metrics} for (codec,case,size),metrics in cells.items()]
    result={'claim':'exploratory fixed profiles only; no CI, no final or upstream-engine performance acceptance',
            'component_processes':6240,'own_configurations':40,'external_codec_configurations':10,'full_controls':2,
            'all_cell_medians':all_medians,'all_fixed_comparisons':summaries,'source_audit':audit}
    (ROOT/'research/byte-codec-v2-review.json').write_text(json.dumps(result,indent=2)+'\n')
    all_lower=[]
    for candidate in own:
        cs=[s for s in summaries if s['candidate']==candidate]
        if all(s['metrics']['encode_ns']['lower_cells']==20 for s in cs):all_lower.append(candidate)
    print(json.dumps({'fixed_candidates_lower_encode_medians_in_every_external_cell':all_lower,
                      'claim':'Median observations only; not acceptance. All decode/size losses retained.'},indent=2))


if __name__=='__main__':main()
