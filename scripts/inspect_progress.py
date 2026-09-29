"""Read-only local progress snapshot; no jobs submitted and no private credentials read."""
import datetime as dt,json,pathlib,sqlite3,urllib.request,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1]
report={'at':dt.datetime.now(dt.timezone.utc).isoformat()}
with sqlite3.connect('file:'+str(ROOT/'build/airflow/airflow.db')+'?mode=ro',uri=True) as conn:
    report['active_tasks']=conn.execute("SELECT dag_id,run_id,task_id,state,try_number FROM task_instance WHERE state IN ('running','queued','up_for_retry')").fetchall()
for label,url in [('yarn','http://127.0.0.1:18088/ws/v1/cluster/apps?states=RUNNING,ACCEPTED'),('spark','http://127.0.0.1:18090/api/v1/applications')]:
    try:
        with urllib.request.urlopen(url,timeout=5) as response:report[label]=json.load(response)
    except Exception as e:report[label]={'unavailable':str(e)}
if isinstance(report.get('spark'),list) and report['spark']:
    app=report['spark'][0]['id']
    try:
        with urllib.request.urlopen('http://127.0.0.1:18090/api/v1/applications/'+app+'/stages?status=active',timeout=5) as response:
            report['active_stages']=[{k:s[k] for k in ('stageId','name','numTasks','numActiveTasks','numCompleteTasks','numFailedTasks') if k in s} for s in json.load(response)]
    except Exception as e:report['stage_error']=str(e)
report['accepted_hours']={p.name:len(list(p.glob('*.json'))) for p in (ROOT/'build/control-v2/sources').iterdir() if p.is_dir()}
report['published_dates']=[p.stem for p in (ROOT/'build/control-v2/published').glob('*.json')]
out=ROOT/'evidence/progress-snapshots';out.mkdir(exist_ok=True)
(out/(uuid.uuid4().hex+'.json')).write_text(json.dumps(report,indent=2))
print(json.dumps({k:v for k,v in report.items() if k not in ('spark','yarn')},indent=2))
