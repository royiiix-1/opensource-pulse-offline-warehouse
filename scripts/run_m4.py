"""Run isolated benchmarks under the already approved local resource budget."""
import datetime as dt,hashlib,json,pathlib,subprocess,sys,time,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import read,atomic
from warehouse.operations import disk_usage
run=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+uuid.uuid4().hex[:8]
out=ROOT/'evidence'/('m4-'+run);out.mkdir()
policy=read(ROOT/'config/m2-budget.json');before=disk_usage()
if before+(12<<30)>policy['disk_limit_bytes']:raise RuntimeError('insufficient benchmark storage headroom')
source=read(ROOT/'build/control-m3/published.json')
ctx={'run':run,'evidence':str(out),'hdfs_path':'/osp-offline/experiments/m4/'+run,'source':source,
 'day_rows':read(ROOT/'build/control-v2/published/2024-01-01.json')['tables']['dwd_event']['rows'],
 'code_sha256':hashlib.sha256((ROOT/'spark/m4_benchmarks.py').read_bytes()).hexdigest(),'disk_before':before}
atomic(out/'context.json',ctx);print('EVIDENCE',str(out),flush=True)
with (out/'spark.log').open('wb') as log:
    proc=subprocess.Popen(['spark-submit','--master','yarn',str(ROOT/'spark/m4_benchmarks.py'),str(out/'context.json')],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
    samples=[];exceeded=False
    while proc.poll() is None:
        used=disk_usage();samples.append({'at':time.time(),'bytes':used})
        if used>policy['disk_limit_bytes']-(2<<30):
            exceeded=True;proc.terminate();proc.wait(timeout=60);break
        time.sleep(15)
atomic(out/'execution.json',{'exit_code':proc.returncode,'budget_exceeded':exceeded,'samples':samples,
 'published_pointer_unchanged':source==read(ROOT/'build/control-m3/published.json'),
 'code_unchanged':ctx['code_sha256']==hashlib.sha256((ROOT/'spark/m4_benchmarks.py').read_bytes()).hexdigest()})
raise SystemExit(proc.returncode)
