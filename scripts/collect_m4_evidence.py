"""Build reproducible M4 acceptance from saved SQL plans, exact results and native event metrics."""
import collections,http.client,json,pathlib,statistics,sys
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import read,atomic
from warehouse.hdfs import HDFS
out=pathlib.Path(sys.argv[1]).resolve()
if not out.is_relative_to((ROOT/'evidence').resolve()):raise ValueError('invalid evidence directory')
r=read(out/'result.json');execution=read(out/'execution.json')
if r['status']!='VALIDATED' or execution['exit_code']!=0:raise RuntimeError('benchmark not successfully completed')
environment={}
wanted={t['group'] for t in r['trials']};jobs=collections.defaultdict(list);sql={};stages=collections.defaultdict(lambda:collections.Counter())
fs=HDFS();source='/osp-offline/spark-events/'+r['application_id']
if not fs.exists(source):raise RuntimeError('completed Spark event log required')
conn=http.client.HTTPConnection('127.0.0.1',19864,timeout=60)
try:
    conn.request('GET',fs.redirect(source,'OPEN'));resp=conn.getresponse()
    if resp.status!=200:raise RuntimeError('cannot read Spark event log')
    for line in resp:
        e=json.loads(line);kind=e.get('Event','')
        if kind=='SparkListenerEnvironmentUpdate':
            props=e.get('Spark Properties',{})
            environment={k:props.get(k) for k in ('spark.master','spark.executor.memory','spark.executor.cores','spark.executor.instances','spark.driver.memory','spark.sql.adaptive.enabled','spark.sql.autoBroadcastJoinThreshold','spark.sql.shuffle.partitions')}
        if kind.endswith('SparkListenerSQLExecutionStart'):
            sql[str(e['executionId'])]={'description':e.get('description',''),'start':e['time']}
        elif kind.endswith('SparkListenerSQLExecutionEnd'):
            sql.setdefault(str(e['executionId']),{})['end']=e['time']
        elif kind=='SparkListenerJobStart':
            p=e.get('Properties') or {};group=p.get('spark.jobGroup.id');sid=p.get('spark.sql.execution.id')
            if group in wanted and sid is not None:jobs[group].append({'sql_id':str(sid),'job_id':e['Job ID'],'stages':e['Stage IDs']})
        elif kind=='SparkListenerTaskEnd':
            m=e.get('Task Metrics',{});v=stages[e['Stage ID']]
            v['tasks']+=1;v['failed_tasks']+=e.get('Task End Reason',{}).get('Reason')!='Success'
            v['input_bytes']+=m.get('Input Metrics',{}).get('Bytes Read',0)
            sh=m.get('Shuffle Read Metrics',{})
            v['shuffle_read_bytes']+=sh.get('Remote Bytes Read',0)+sh.get('Local Bytes Read',0)
            v['shuffle_write_bytes']+=m.get('Shuffle Write Metrics',{}).get('Shuffle Bytes Written',0)
            v['disk_bytes_spilled']+=m.get('Disk Bytes Spilled',0)
            v['memory_bytes_spilled']+=m.get('Memory Bytes Spilled',0)
finally:conn.close()
rows=[];selected_stages={}
for trial in r['trials']:
    group=trial['group'];candidates={j['sql_id'] for j in jobs[group]}
    collects=[s for s in candidates if sql.get(s,{}).get('description','')==group]
    if not collects:raise RuntimeError('no collect SQL execution linked to trial '+group)
    # A trial's first collect is the measured action. Subsequent preparation may inherit its job tag.
    sid=min(collects,key=lambda s:sql[s]['start']);chosen=[j for j in jobs[group] if j['sql_id']==sid]
    stage_ids=set(x for j in chosen for x in j['stages']);total=collections.Counter()
    for stage in stage_ids:
        if stage in selected_stages and selected_stages[stage]!=group:raise RuntimeError('shared stage attribution ambiguous')
        selected_stages[stage]=group;total.update(stages[stage])
    scans=[n['metrics'] for n in trial['metrics'] if 'numFiles' in n['metrics']]
    row={**trial,'sql_execution_id':sid,'sql_description':sql[sid]['description'],'stage_ids':sorted(stage_ids),
         'excluded_followup_sql_ids':sorted(candidates-{sid}),'task_metrics':dict(total),
         'scan_files':sum(s['numFiles'] for s in scans),'selected_file_bytes':sum(s['filesSize'] for s in scans),
         'scan_partitions':sum(s.get('numPartitions',0) for s in scans)}
    rows.append(row)
atomic(out/'trials-with-native-metrics.json',rows)
groups={}
for t in rows:
    if t['phase']=='measured':groups.setdefault((t['experiment'],t['variant']),[]).append(t)
summaries=[]
for (experiment,variant),items in groups.items():
    times=[t['elapsed_seconds'] for t in items]
    summary={'experiment':experiment,'variant':variant,'n':len(items),'median_seconds':statistics.median(times),'min_seconds':min(times),'max_seconds':max(times),
             'scan_files':items[0]['scan_files'],'selected_file_bytes':items[0]['selected_file_bytes'],'result_sha256':items[0]['result_sha256']}
    for metric in ('input_bytes','shuffle_read_bytes','shuffle_write_bytes','disk_bytes_spilled','memory_bytes_spilled'):
        summary['median_'+metric]=statistics.median([t['task_metrics'].get(metric,0) for t in items])
    summaries.append(summary)
def result(experiment,variant):return next(s for s in summaries if s['experiment']==experiment and s['variant']==variant)
checks={'spark_checks':all(c['passed'] for c in r['checks'].values()),'budget_guard':not execution['budget_exceeded'],
 'published_pointer_unchanged':execution['published_pointer_unchanged'],'code_unchanged':execution['code_unchanged'],
 'three_measured_repeats_each':len(summaries)==8 and all(s['n']==3 for s in summaries),
 'native_metrics_present':all(t['task_metrics'].get('tasks',0)>0 and t['scan_files']>0 for t in rows),
 'no_measured_task_failures':all(t['task_metrics'].get('failed_tasks',0)==0 for t in rows if t['phase']=='measured'),
 'date_partition_pruning':result('A_date','one_date')['scan_files']<result('A_date','all_dates')['scan_files'],
 'hour_partition_pruning':result('A_granularity','hourly')['scan_files']<result('A_granularity','daily_compact')['scan_files']}
for ex in ('A_granularity','B_join','C_compaction'):
    trials=[t for t in rows if t['experiment']==ex]
    # Compare saved result bodies exactly, independent of the checksum.
    bodies=[(out/(t['group']+'-rows.json')).read_bytes() for t in trials]
    checks[ex+'_exact_saved_results_equal']=all(b==bodies[0] for b in bodies)
old=read(ROOT/'build/control-v2/runs/e33753a44409d0612d40/2024-01-04.json')
new=read(ROOT/'build/control-v2/runs/e0fa7b1cd2b255c519ea/2024-01-04.json')
active=read(ROOT/'build/control-v2/published/2024-01-04.json')
repair=read(ROOT/'evidence/m3-ods-memory-repair/acceptance.json')
failed_reports=[read(p) for p in (ROOT/old['evidence']).glob('ods-*.json') if not p.name.endswith('.budget.json')]
recovery={'failed_reports':len(failed_reports),'old_release':old['release'],'recovered_release':active['release'],
          'same_inputs':[s['object_id'] for s in old['sources']]==[s['object_id'] for s in new['sources']],
          'failed_release_not_published':old.get('active') is None and old['release']!=active['release'],
          'recovered_complete':active['coverage']=='COMPLETE' and active['release']==new['release'],
          'ods_memory_repair':repair['memory_repair_passed'],'sort_elimination':repair['sort_elimination_passed']}
checks['failure_recovery']=len(failed_reports)==2 and all(x['status']=='FAILED' for x in failed_reports) and all(recovery[k] for k in ('same_inputs','failed_release_not_published','recovered_complete','ods_memory_repair'))
summary={'milestone':'M4','passed':all(checks.values()),'checks':checks,'evidence':str(out.relative_to(ROOT)),
 'application_id':r['application_id'],'environment':environment,'comparisons':summaries,'layouts':r['layouts'],'failure_recovery':recovery,
 'limitations':['Single host, 2 one-core 1 GiB executors; no cold-cache or production SLA claim','Three measured trials per variant, all retained; no statistical significance claim','A_date changes input scope; its timing ratio is not equivalent-query speedup','Layout comparisons use one complete real day projected to five narrow columns','Task input bytes differ from selected whole-file sizes; metrics scoped to the first collect SQL execution per trial tag'],
 'max_conservative_disk_bytes':max(s['bytes'] for s in execution['samples'])}
atomic(out/'acceptance.json',summary)
if summary['passed']:atomic(ROOT/'evidence/M4_SUMMARY.json',summary)
print(json.dumps({k:v for k,v in summary.items() if k!='layouts'},indent=2))
raise SystemExit(0 if summary['passed'] else 1)
