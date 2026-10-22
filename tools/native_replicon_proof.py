#!/usr/bin/env python3
"""Freeze and execute actual upstream engine capability proof, never timings."""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess

ROOT=pathlib.Path(__file__).resolve().parents[1]
CHILD=ROOT/'research/replicon-admission'


def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def main(label):
    binary=ROOT/'build'/label;assert not binary.exists(),'Never replace frozen proof'
    paths=[CHILD/'Cargo.toml',CHILD/'Cargo.lock',CHILD/'protocol.md',*sorted((CHILD/'src').rglob('*.rs'))]
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    inputs={str(p.relative_to(ROOT)):sha(p) for p in paths}
    for path,digest in inputs.items():
        assert hashlib.sha256(subprocess.check_output(['git','show',f'{commit}:{path}'],cwd=ROOT)).hexdigest()==digest,path
    env=dict(os.environ,RUSTUP_TOOLCHAIN='1.97.1',CARGO_BUILD_JOBS='2')
    preflight=json.loads(subprocess.check_output(['ink-cache','prepare-sources',str(CHILD),'--offline'],env=env,text=True))
    args=['cargo','build','--release','--locked','--manifest-path',str(CHILD/'Cargo.toml'),'--message-format=json']
    output=ROOT/'evidence'/f'{label}-build.jsonl';log=ROOT/'evidence'/f'{label}-build.log'
    with output.open('w') as stdout,log.open('w') as stderr:
        subprocess.run(args,env=env,cwd=ROOT,stdout=stdout,stderr=stderr,check=True)
    artifacts=[json.loads(line) for line in output.read_text().splitlines() if line.startswith('{')]
    artifact=next(row for row in artifacts if row.get('reason')=='compiler-artifact' and row.get('executable') and row['target']['name']=='rill-replicate-replicon-admission')
    for path,digest in inputs.items():assert sha(ROOT/path)==digest,'Inputs changed during build'
    binary.parent.mkdir(exist_ok=True);shutil.copy2(artifact['executable'],binary)
    process=subprocess.run([str(binary)],capture_output=True,text=True,timeout=60)
    proof_log=ROOT/'evidence'/f'{label}.log';proof_log.write_text(process.stdout+process.stderr)
    rows=[json.loads(line) for line in process.stdout.splitlines() if line.startswith('{')]
    complete=process.returncode==0 and len(rows)==4
    if complete:
        assert {(r['native_diff'],r['loss']) for r in rows}=={(False,False),(False,True),(True,False),(True,True)}
        assert all(r['complete_values'] and r['despawn_reinsert'] and r['reconnect'] and r['clients']==4 and r['revisions']==32 and r['timing_eligible'] is False for r in rows)
    receipt={'complete':complete,'source_commit':commit,'inputs':inputs,'command':args,'toolchain':'1.97.1',
             'compiler':subprocess.check_output(['rustc','+1.97.1','-Vv'],text=True),'preflight':preflight,
             'environment':{k:env.get(k) for k in ['RUSTUP_TOOLCHAIN','CARGO_BUILD_JOBS','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS']},
             'binary':str(binary.relative_to(ROOT)),'binary_sha256':sha(binary),'build_output_sha256':sha(output),
             'build_log_sha256':sha(log),'proof_log_sha256':sha(proof_log),'returncode':process.returncode,'rows':rows,
             'claim':'Four-client native engine capability proof only; no 20k scaling or speed comparison'}
    (ROOT/'evidence'/f'{label}-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps({'complete':complete,'rows':rows,'returncode':process.returncode},indent=2))
    if not complete:raise SystemExit(1)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('label');main(parser.parse_args().label)
