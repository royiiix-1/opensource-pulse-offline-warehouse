"""Bounded six-day expansion using the existing real Airflow DAG, one date at a time."""
import argparse,datetime as dt,json,pathlib,subprocess,sys,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import atomic,read
parser=argparse.ArgumentParser();parser.add_argument('--start-date',default='2024-01-02');parser.add_argument('--end-date',default='2024-01-07');args=parser.parse_args()
from warehouse.common import dates
selected=dates(args.start_date,args.end_date)
if any(d not in read(ROOT/'config/m2-budget.json')['allowed_dates'] for d in selected):raise ValueError('unapproved date')
run=ROOT/'evidence'/('m3-expansion-'+uuid.uuid4().hex);run.mkdir()
report={'status':'RUNNING','dates':[],'started_at':dt.datetime.now(dt.timezone.utc).isoformat()}
atomic(run/'progress.json',report)
for date in selected:
    request=run/(date+'-request.json');atomic(request,{'process_date':date,'acquisition_policy':'download_missing'})
    print('PROCESSING',date,str(run),flush=True)
    with (run/(date+'.log')).open('wb') as out:
        code=subprocess.run(['bash','scripts/trigger_day.sh',str(request)],cwd=ROOT,stdout=out,stderr=subprocess.STDOUT).returncode
    item={'date':date,'exit_code':code}
    pointer=ROOT/'build/control-v2/published'/(date+'.json')
    if pointer.exists():
        p=read(pointer);item.update(release=p['release'],events=p['tables']['dwd_event']['rows'],coverage=p['coverage'])
    report['dates'].append(item)
    if code:report['status']='FAILED'
    atomic(run/'progress.json',report)
    if code:raise SystemExit(code)
report['status']='COMPLETE';report['finished_at']=dt.datetime.now(dt.timezone.utc).isoformat();atomic(run/'progress.json',report)
print(json.dumps(report,indent=2))
