"""Bounded continuation of the authorized expansion; stop on any failed gate."""
import datetime as dt,json,pathlib,subprocess,sys,time,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import read,atomic
progress=pathlib.Path(sys.argv[1]).resolve()
if not progress.is_relative_to((ROOT/'evidence').resolve()):raise ValueError('invalid batch evidence')
out=ROOT/'evidence'/('m3-continuation-'+uuid.uuid4().hex);out.mkdir()
state={'status':'WAITING_FOR_DAILY_RELEASES','batch_progress':str(progress),'started_at':dt.datetime.now(dt.timezone.utc).isoformat()}
atomic(out/'progress.json',state)
try:
    deadline=time.time()+20*3600
    while time.time()<deadline:
        batch=read(progress)
        if batch['status']!='RUNNING':break
        time.sleep(15)
    if batch['status']!='COMPLETE':raise RuntimeError('daily expansion incomplete or failed')
    state['status']='MODEL_DAG_RUNNING';atomic(out/'progress.json',state)
    for attempt in (1,2):
        before=read(ROOT/'build/control-m3/published.json') if attempt==2 else None
        existing=set((ROOT/'evidence').glob('m3-model-*/publication.json'))
        with (out/('model-dag-'+str(attempt)+'.log')).open('wb') as log:
            code=subprocess.run(['bash','scripts/run_m3_model.sh'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT).returncode
        if code:raise RuntimeError('model DAG failed on attempt '+str(attempt))
        active=read(ROOT/'build/control-m3/published.json')
        if attempt==2:
            publications=set((ROOT/'evidence').glob('m3-model-*/publication.json'))-existing
            if before!=active or not any(read(p)['status']=='NO_OP' for p in publications):raise RuntimeError('model rerun is not NO_OP')
    days=[read(ROOT/'build/control-v2/published'/('2024-01-'+str(i).zfill(2)+'.json')) for i in range(1,8)]
    events=sum(p['tables']['dwd_event']['rows'] for p in days)
    checks={'seven_complete_dates':all(p['coverage']=='COMPLETE' and p['hours']==list(range(24)) for p in days),
            'at_least_ten_million_distinct_events':events>=10000000,
            'model_event_count_matches':active['tables']['dwd_event']['rows']==events,
            'model_rerun_noop':True}
    if not all(checks.values()):raise RuntimeError('scale acceptance failed: '+str(checks))
    policy=read(ROOT/'config/m2-budget.json')
    reserved=policy['prior_runtime_reservations']+policy['prior_source_allowance']+sum(json.loads(x)['reserved_bytes'] for x in (ROOT/'evidence/network-m2.jsonl').read_text().splitlines())
    from warehouse.operations import disk_usage
    used=disk_usage();checks['network_within_budget']=reserved<=policy['network_limit_bytes'];checks['disk_within_budget']=used<=policy['disk_limit_bytes']
    summary={'milestone':'M3','passed':all(checks.values()),'checks':checks,'distinct_events':events,'dates':[p['date'] for p in days],
             'model_release':active['release'],'database':active['database'],'tables':{k:v['rows'] for k,v in active['tables'].items()},
             'network_reserved_bytes':reserved,'conservative_disk_bytes':used,'continuation_evidence':str(out.relative_to(ROOT)),
             'limitations':['Single WSL machine; SQLite Airflow development executor','Seven days cannot mature next-week retention','Observed names are not complete repository metadata'],
             'completed_at':dt.datetime.now(dt.timezone.utc).isoformat()}
    atomic(ROOT/'evidence/M3_SUMMARY.json',summary)
    if not summary['passed']:raise RuntimeError('budget acceptance failed')
    dictionary=['# M3 实际数据字典','', '来自已发布模型 '+active['release']+'；nullable 是物理 schema 属性。','']
    for name,table in active['tables'].items():
        dictionary+=['## '+name,'','行数：'+str(table['rows']),'','| 字段 | 类型 | Nullable |','|---|---|---|']
        dictionary += ['| '+f['name']+' | '+str(f['type'])+' | '+str(f['nullable'])+' |' for f in table['schema']['fields']]
        dictionary.append('')
    (ROOT/'docs/DATA_DICTIONARY_M3.md').write_text('\n'.join(dictionary))
    with (ROOT/'STATUS.md').open('a') as f:
        f.write('\n\n## M3 自动验收完成\n\n七个完整日期，去重事件 '+str(events)+'；多日模型和重跑 NO_OP 门禁全部通过。最终证据见 evidence/M3_SUMMARY.json。旧进度段落保留为执行历史，M4/M5 尚未完成。\n')
    state['status']='COMPLETE';state['summary']='evidence/M3_SUMMARY.json'
except Exception as e:
    state['status']='FAILED';state['error']=repr(e)
finally:
    state['ended_at']=dt.datetime.now(dt.timezone.utc).isoformat();atomic(out/'progress.json',state)
print(json.dumps(state,indent=2));raise SystemExit(0 if state['status']=='COMPLETE' else 1)
