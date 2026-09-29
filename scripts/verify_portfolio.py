"""One entry: portable code tests or full existing-cluster validation. Never downloads data."""
import argparse,ast,datetime as dt,gzip,hashlib,json,os,pathlib,subprocess,sys,time,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT),str(ROOT/'scripts')]
from candidate_files import scan
p=argparse.ArgumentParser();p.add_argument('--mode',choices=['portable','full'],default='portable');a=p.parse_args()
(ROOT/'build').mkdir(exist_ok=True)
out=ROOT/'build/validation'/('v'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+uuid.uuid4().hex[:8]);out.mkdir(parents=True)
r={'mode':a.mode,'passed':False,'checks':{},'remote_ci_executed':os.environ.get('GITHUB_ACTIONS')=='true','output':str(out),'started_at':dt.datetime.now(dt.timezone.utc).isoformat()}
def save():
    temp=out/'result.next.json'
    temp.write_text(json.dumps(r,indent=2))
    temp.replace(out/'result.json')
def check(name,ok):
    r['checks'][name]=bool(ok);save()
    if not ok:raise RuntimeError(name)
try:
    check('python_3_11',sys.version_info[:2]==(3,11))
    with (out/'unit-tests.log').open('wb') as log:
        code=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-v'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT).returncode
    check('unit_tests',code==0)
    audit=scan(ROOT);(out/'candidate-scan.json').write_text(json.dumps(audit,indent=2));check('candidate_scan',audit['passed'])
    for f in audit['files']:
        path=ROOT/f['path']
        if path.suffix=='.py':ast.parse(path.read_text(encoding='utf-8-sig'),filename=f['path'])
    check('python_syntax',True)
    required=['README.md','LICENSE','.github/workflows/ci.yml','docs/DESIGN_NOTES.md','docs/PERFORMANCE.md','docs/DEMO.md','docs/LINEAGE.md','docs/REPRODUCIBILITY.md','evidence/public/portfolio.json']
    check('required_artifacts',all((ROOT/x).is_file() for x in required))
    with (out/'doc-links.json').open('wb') as log:
        code=subprocess.run([sys.executable,str(ROOT/'scripts/check_doc_links.py'),str(ROOT)],stdout=log,stderr=subprocess.STDOUT).returncode
    check('document_links',code==0)
    if a.mode=='full':
        from warehouse.common import read,atomic
        from warehouse.hdfs import HDFS
        from warehouse.operations import disk_usage
        fs=HDFS();model=read(ROOT/'build/control-m3/published.json');days=[read(ROOT/'build/control-v2/published'/f'2024-01-{i:02d}.json') for i in range(1,8)]
        for n in (1,2,3,4):check('M'+str(n)+'_evidence',read(ROOT/'evidence'/f'M{n}_SUMMARY.json')['passed'])
        for day in days:
            print('HASH_DATE',day['date'],flush=True)
            check(day['date']+'_release_sha',fs.inventory(day['hdfs_path'])==day['inventory'])
            receipts=[read(x) for x in (ROOT/'build/control-v2/sources'/day['date']).glob('*.json')]
            check(day['date']+'_24_raw_hours',sorted(x['manifest']['source_hour'] for x in receipts)==list(range(24)))
            for receipt in receipts:
                m=receipt['manifest'];path=f"/osp-offline/raw/source_date={m['source_date']}/source_hour={m['source_hour']:02d}/sha256={m['sha256']}/source.json.gz"
                check(day['date']+'_raw_'+str(m['source_hour']),fs.sha(path)=={'bytes':m['compressed_bytes'],'sha256':m['sha256']})
        print('HASH_MODEL',flush=True);check('model_file_sha',fs.inventory(model['hdfs_path'])==model['inventory'])
        check('live_storage_headroom',disk_usage()+(4<<30)<read(ROOT/'config/m2-budget.json')['disk_limit_bytes'])
        with (out/'spark.log').open('wb') as log:
            proc=subprocess.Popen(['spark-submit','--master','yarn',str(ROOT/'spark/verify_live.py'),str(out)],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            samples=[]
            while proc.poll() is None:
                used=disk_usage();samples.append({'at':time.time(),'bytes':used})
                if used>read(ROOT/'config/m2-budget.json')['disk_limit_bytes']-(2<<30):
                    proc.terminate();proc.wait(timeout=60);raise RuntimeError('validation disk watchdog')
                time.sleep(15)
        atomic(out/'budget.json',{'samples':samples});check('live_spark_exit',proc.returncode==0)
        check('live_checks',read(out/'live-result.json')['passed'])
        lineage=read(out/'lineage.json');receipt=read(ROOT/'build/control-v2/sources'/lineage['date']/(str(lineage['source_hour'])+'.json'))
        with gzip.open(ROOT/'build/acquisition/objects'/receipt['object_id']/'source.json.gz','rt',encoding='utf-8') as stream:
            for i,line in enumerate(stream,1):
                if i==lineage['line_number']:
                    check('lineage_raw_bytes',hashlib.sha256(line.rstrip('\r\n').encode()).hexdigest()==lineage['raw_line_sha256']);break
            else:raise RuntimeError('lineage line absent')
        check('model_pointer_unchanged',model==read(ROOT/'build/control-m3/published.json'))
        check('daily_pointers_unchanged',all(d==read(ROOT/'build/control-v2/published'/(d['date']+'.json')) for d in days))
    r['passed']=all(r['checks'].values())
except Exception as e:
    import traceback
    r['error']=traceback.format_exc()
finally:
    r['ended_at']=dt.datetime.now(dt.timezone.utc).isoformat();save()
print(json.dumps(r,indent=2));raise SystemExit(0 if r['passed'] else 1)
