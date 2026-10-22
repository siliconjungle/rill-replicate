#!/usr/bin/env python3
"""Fresh-process staged native client proof with actual per-child CPU/RSS."""
import argparse
import concurrent.futures
import hashlib
import itertools
import json
import os
import pathlib
import platform
import subprocess
import time

ROOT=pathlib.Path(__file__).resolve().parents[1]


def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def run(args):
    build_path=ROOT/'evidence'/f'{args.binary}-build.json';build=json.loads(build_path.read_text())
    binary=ROOT/build['binary'];assert sha(binary)==build['binary_sha256']
    plan_name='research/native-replay-v4-plan.json' if args.kind=='replay' else 'research/native-scale-v3-plan.json'
    plan_path=ROOT/plan_name;plan=json.loads(plan_path.read_text())
    clients=args.clients or [n for n in plan['stages'] if n<=512]
    modes=plan['modes'] if args.kind=='replay' else plan['initial_modes']
    tasks=list(itertools.product(clients,modes,plan['traces'],plan['ack_delays']))
    folder=ROOT/'evidence'/args.label;folder.mkdir(exist_ok=False)
    record={'binary':args.binary,'binary_sha256':sha(binary),'build_receipt_sha256':sha(build_path),
            'plan_sha256':sha(plan_path),'plan':plan_name,'tasks':len(tasks),'clients':clients,'kind':args.kind,
            'complete':False,'failures':[],'started_at':time.time(),'workers':args.workers,
            'rss_scope':'Peak entire fixture; replay includes retained traces and oracle work, not library-only memory',
            'claim':'development/admission only; no cross-engine performance acceptance'}
    path=folder/'receipt.json';path.write_text(json.dumps(record,indent=2)+'\n')
    # Large-capacity studies are launched separately only after the staged
    # proof/resource cohort has completed. Each row is an independent process.
    def execute(index,task):
            clients,mode,trace,delay=task
            stdout=folder/f'current-{index}.stdout';stderr=folder/f'current-{index}.stderr'
            with stdout.open('w') as out,stderr.open('w') as err:
                command=[str(binary),mode,str(clients),trace,str(delay)]
                process=subprocess.Popen(command,stdout=out,stderr=err)
                _,status,usage=os.wait4(process.pid,0)
                process.returncode=os.waitstatus_to_exitcode(status)
            if process.returncode:
                row={'clients':clients,'mode':mode,'trace':trace,'ack_delay':delay,'error':stderr.read_text(),'returncode':process.returncode}
            else:
                row=json.loads(stdout.read_text());assert row['verified'] is True
                peer_key='server_peers' if args.kind=='replay' else 'clients'
                assert (row[peer_key],row['mode'],row['trace'],row['ack_delay'])==(clients,mode,trace,delay)
            row['peak_rss_bytes']=int(usage.ru_maxrss)*(1 if platform.system()=='Darwin' else 1024)
            row['process_user_seconds']=usage.ru_utime;row['process_system_seconds']=usage.ru_stime
            stdout.unlink();stderr.unlink()
            return row
    with (folder/'runs.jsonl').open('w') as output,concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures={pool.submit(execute,i,t):t for i,t in enumerate(tasks)}
        for completed,future in enumerate(concurrent.futures.as_completed(futures),1):
            task=futures[future]
            try:row=future.result()
            except Exception as error:row={'task':task,'error':repr(error)}
            if 'error' in row:record['failures'].append(row)
            output.write(json.dumps(row)+'\n');output.flush()
            print(f'{completed}/{len(tasks)} complete; clients={task[0]}; peak_MiB={row.get("peak_rss_bytes",0)/1048576:.1f}; failures={len(record["failures"])}',flush=True)
    record['complete']=not record['failures'];record['finished_at']=time.time();record['runs_sha256']=sha(folder/'runs.jsonl')
    path.write_text(json.dumps(record,indent=2)+'\n')
    if not record['complete']:raise SystemExit(1)


def audit(label):
    folder=ROOT/'evidence'/label;r=json.loads((folder/'receipt.json').read_text());assert r['complete'] and not r['failures']
    assert sha(folder/'runs.jsonl')==r['runs_sha256'];bpath=ROOT/'evidence'/f'{r["binary"]}-build.json';assert sha(bpath)==r['build_receipt_sha256']
    b=json.loads(bpath.read_text());assert sha(ROOT/b['binary'])==b['binary_sha256']==r['binary_sha256']
    for file,digest in b['inputs'].items():assert hashlib.sha256(subprocess.check_output(['git','show',f'{b["source_commit"]}:{file}'],cwd=ROOT)).hexdigest()==digest
    plan_name=r.get('plan','research/native-scale-v3-plan.json')
    pbytes=subprocess.check_output(['git','show',f'{b["source_commit"]}:{plan_name}'],cwd=ROOT)
    assert hashlib.sha256(pbytes).hexdigest()==r['plan_sha256'];p=json.loads(pbytes)
    rows=[json.loads(l)for l in(folder/'runs.jsonl').read_text().splitlines()]
    replay=r.get('kind')=='replay';peer_key='server_peers' if replay else 'clients'
    expected=set(itertools.product(r['clients'],p['modes'] if replay else p['initial_modes'],p['traces'],p['ack_delays']))
    actual={(v[peer_key],v['mode'],v['trace'],v['ack_delay'])for v in rows};assert actual==expected and len(rows)==len(expected)==r['tasks']
    for v in rows:
        assert v['verified'] is True and v['frames']==32 and v['full_value_checks']==v[peer_key]*(33+v['ack_delay']+8)
        assert v['peak_rss_bytes']>0 and v['timing_acceptance'] is False
        if replay:assert v['native_decoder_replays']==v[peer_key] and v['exact_ack_frames']==v['full_value_checks'] and v['fixed_clock_ns']==16666667
        if v['trace']=='loss' and v[peer_key]>=8:assert v['dropped']>0 and v['delayed']>0 and v['duplicated']>0
    result={'complete':True,'processes':len(rows),'clients':r['clients'],'receipt_sha256':sha(folder/'receipt.json'),'runs_sha256':r['runs_sha256'],
            'claim':'native full/diff bulk routing, complete values, delayed ordered ACKs, unreliable message faults and process resource evidence; no speed acceptance'}
    if replay:
        result.update({'kind':'exhaustive-native-decoder-replay','simultaneous_server_peer_counts':r['clients'],
                       'max_simultaneous_native_decoder_apps':1,'total_native_decoder_replays':sum(v['native_decoder_replays']for v in rows),
                       'full_value_checks':sum(v['full_value_checks']for v in rows),'exact_ack_frames':sum(v['exact_ack_frames']for v in rows),
                       'scope':'Fixed-clock deterministic protocol simulation; all native server peers coexist, client Apps are replayed sequentially. ACK byte predictions and application-before-server-ACK delivery are checked exhaustively. Not live sockets or client latency.'})
    (ROOT/'evidence'/f'{label}-audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser();subs=parser.add_subparsers(dest='action',required=True)
    run_parser=subs.add_parser('run');run_parser.add_argument('--binary',default='native-scale-v3');run_parser.add_argument('--label',required=True);run_parser.add_argument('--clients',type=int,nargs='+')
    run_parser.add_argument('--kind',choices=['live','replay'],default='live');run_parser.add_argument('--workers',type=int,default=1)
    a=subs.add_parser('audit');a.add_argument('label')
    args=parser.parse_args()
    if args.action=='run':run(args)
    else:audit(args.label)
