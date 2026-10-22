#!/usr/bin/env python3
"""Planned codec-layer studies and actual 20k-recipient adapter admissions."""
import argparse
import concurrent.futures
import hashlib
import itertools
import json
import pathlib
import random
import subprocess
import time

ROOT=pathlib.Path(__file__).resolve().parents[1]


def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()


def run(args):
    plan_path=ROOT/'research/byte-codec-plan.json'
    plan=json.loads(plan_path.read_text())
    if args.kind=='component':
        config=plan['component']; binary='codec-v2'
        sizes=[0,64] if args.smoke else config['sizes']
        seeds=config['seeds'][:1] if args.smoke else config['seeds']
        reps=1 if args.smoke else config['repetitions']
        tasks=[{'codec':c,'case':case,'size':size,'seed':seed,'repetition':rep}
               for c,case,size,seed,rep in itertools.product(plan['codecs'],config['cases'],sizes,seeds,range(reps))]
    else:
        config=plan['replication_initial']; binary='byte-fanout-v2'
        tasks=[{'codec':c,'policy':policy,'trace':trace,'peers':128 if args.smoke else config['peers']}
               for c,policy,trace in itertools.product(config['engines'],config['policies'],config['traces'])]
    folder=ROOT/'evidence'/args.label; folder.mkdir(exist_ok=False)
    receipt_path=ROOT/'evidence'/f'{binary}-build.json';receipt=json.loads(receipt_path.read_text())
    executable=ROOT/receipt['binary'];assert sha(executable)==receipt['binary_sha256']
    record={'kind':args.kind,'claim':'development-only; upstream codec adapters do not count as independent replication engines',
            'plan_sha256':sha(plan_path),'build_receipt_sha256':sha(receipt_path),'binary':binary,'binary_sha256':sha(executable),
            'smoke':args.smoke,'tasks':len(tasks),'workers':args.workers,'complete':False,'failures':[],'started_at':time.time()}
    status=folder/'receipt.json';status.write_text(json.dumps(record,indent=2)+'\n')
    random.Random(20261023).shuffle(tasks)

    def execute(task):
        if args.kind=='component':command=[str(executable),task['codec'],task['case'],str(task['size']),str(task['seed'])]
        else:command=[str(executable),task['codec'],task['policy'],str(task['peers']),task['trace']]
        start=time.time();process=subprocess.run(command,capture_output=True,text=True,timeout=300)
        if process.returncode:return {'task':task,'error':process.stderr,'returncode':process.returncode,'started_at':start}
        row=json.loads(process.stdout)
        for k,v in task.items():
            if k!='repetition':assert row[k]==v,(k,v,row)
        assert row['verified'] is True
        row['started_at']=start;row['repetition']=task.get('repetition',0)
        return row

    with (folder/'runs.jsonl').open('w') as output,concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures={pool.submit(execute,task):task for task in tasks}
        for count,future in enumerate(concurrent.futures.as_completed(futures),1):
            try:row=future.result()
            except Exception as error:row={'task':futures[future],'error':repr(error)}
            if 'error' in row:record['failures'].append(row)
            output.write(json.dumps(row)+'\n');output.flush()
            if count%24==0:print(f'{count}/{len(tasks)} completed; failures={len(record["failures"])}',flush=True)
    record['complete']=not record['failures'];record['finished_at']=time.time();record['runs_sha256']=sha(folder/'runs.jsonl')
    status.write_text(json.dumps(record,indent=2)+'\n')
    if record['failures']:raise SystemExit(1)


def audit(label):
    folder=ROOT/'evidence'/label;record=json.loads((folder/'receipt.json').read_text())
    assert record['complete'] and not record['failures'];assert sha(folder/'runs.jsonl')==record['runs_sha256']
    build_path=ROOT/'evidence'/f'{record["binary"]}-build.json';assert sha(build_path)==record['build_receipt_sha256']
    build=json.loads(build_path.read_text());assert build['binary_sha256']==record['binary_sha256']==sha(ROOT/build['binary'])
    for path,digest in build['inputs'].items():
        contents=subprocess.check_output(['git','show',f'{build["source_commit"]}:{path}'],cwd=ROOT)
        assert hashlib.sha256(contents).hexdigest()==digest,path
    plan_bytes=subprocess.check_output(['git','show',f'{build["source_commit"]}:research/byte-codec-plan.json'],cwd=ROOT)
    assert hashlib.sha256(plan_bytes).hexdigest()==record['plan_sha256'];plan=json.loads(plan_bytes)
    rows=[json.loads(line) for line in (folder/'runs.jsonl').read_text().splitlines()]
    assert len(rows)==record['tasks'];seen=set();models={}
    if record['kind']=='component':
        cfg=plan['component'];sizes=[0,64] if record['smoke'] else cfg['sizes'];seeds=cfg['seeds'][:1] if record['smoke'] else cfg['seeds'];reps=1 if record['smoke'] else cfg['repetitions']
        expected=set(itertools.product(plan['codecs'],cfg['cases'],sizes,seeds,range(reps)))
    else:
        cfg=plan['replication_initial'];expected=set(itertools.product(cfg['engines'],cfg['policies'],cfg['traces'],[128 if record['smoke'] else cfg['peers']]))
    for row in rows:
        assert row['verified'] is True
        if record['kind']=='component':
            key=tuple(row[k] for k in ['codec','case','size','seed','repetition']);assert row['revisions']==32
            model=tuple(row[k] for k in ['case','size','seed']);models.setdefault(model,row['checksum']);assert row['checksum']==models[model]
        else:
            key=tuple(row[k] for k in ['codec','policy','trace','peers']);assert row['applied']>=row['peers']
        assert key in expected and key not in seen;seen.add(key)
    assert seen==expected
    result={'complete':True,'processes':len(rows),'kind':record['kind'],'receipt_sha256':sha(folder/'receipt.json'),
            'runs_sha256':record['runs_sha256'],'source_commit':build['source_commit'],'claim':'proof coverage only; no final performance acceptance'}
    (ROOT/'evidence'/f'{label}-audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest='action',required=True)
    r=sub.add_parser('run');r.add_argument('--kind',choices=['component','replication'],required=True);r.add_argument('--label',required=True);r.add_argument('--workers',type=int,default=2);r.add_argument('--smoke',action='store_true')
    a=sub.add_parser('audit');a.add_argument('label')
    args=parser.parse_args()
    if args.action=='run':assert args.workers>0;run(args)
    else:audit(args.label)
