"""Read-only final checks against current Hive/HDFS releases; runs on YARN."""
import hashlib,json,pathlib,sys,time,traceback
from pyspark.sql import SparkSession,functions as F
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import read,atomic
out=pathlib.Path(sys.argv[1]);model=read(ROOT/'build/control-m3/published.json')
spark=SparkSession.builder.appName('osp-m5-readonly-validation').config('spark.sql.shuffle.partitions','48').enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('WARN');r={'passed':False,'application_id':spark.sparkContext.applicationId,'checks':{}}
def check(name,ok,actual=None):
    r['checks'][name]={'passed':bool(ok),'actual':actual};atomic(out/'live-result.json',r)
    if not ok:raise RuntimeError(name)
def table_check(db,name,meta):
    table=spark.table(db+'.'+name)
    check(db+'.'+name+'.rows',table.count()==meta['rows'])
    location=[x.data_type for x in spark.sql(f'DESCRIBE FORMATTED {db}.{name}').collect() if x.col_name.strip()=='Location']
    check(db+'.'+name+'.location',location==[meta['path']])
    for part in meta.get('partitions',[]):
        if isinstance(part,str):values=dict(v.split('=',1) for v in part.split('/'));path=meta['path']+'/'+part
        else:values=part['values'];path=part['path']
        cat=spark._jsparkSession.sessionState().catalog().externalCatalog()
        actual=cat.getPartition(db,name,spark._jvm.PythonUtils.toScalaMap(values)).storage().locationUri().get().toString()
        check(db+'.'+name+'.partition.'+str(values),actual==path)
try:
    check('yarn',spark.sparkContext.master=='yarn')
    days=[read(p) for p in sorted((ROOT/'build/control-v2/published').glob('2024-01-0[1-7].json'))]
    check('seven_dates',len(days)==7)
    for p in days:
        check(p['date']+'.complete',p['coverage']=='COMPLETE' and p['hours']==list(range(24)))
        for name,meta in p['tables'].items():table_check(p['database'],name,meta)
    for name,meta in model['tables'].items():table_check(model['database'],name,meta)
    for name in model['tables']:spark.table(model['database']+'.'+name).createOrReplaceTempView(name)
    check('global_event_count',spark.table('dwd_event').count()==sum(p['tables']['dwd_event']['rows'] for p in days))
    check('scd_intervals',spark.sql('''WITH x AS (SELECT *, lead(valid_from) OVER(PARTITION BY repo_id ORDER BY valid_from) nxt FROM dim_repository_scd2)
      SELECT * FROM x WHERE NOT(valid_to <=> nxt) OR (valid_to IS NOT NULL AND valid_to<=valid_from) OR is_current<>(valid_to IS NULL)''').limit(1).count()==0)
    check('asof_links',spark.sql('''SELECT f.event_id FROM dwd_event f LEFT JOIN dim_repository_scd2 d ON f.repo_sk=d.repo_sk
      WHERE d.repo_sk IS NULL OR f.repo_id<>d.repo_id OR f.repo_name<>d.repo_name OR f.event_time<d.valid_from OR (d.valid_to IS NOT NULL AND f.event_time>=d.valid_to)''').limit(1).count()==0)
    for direction in (0,1):
        left="SELECT event_date,split(repo_name,'/')[0] owner_name,count(*) event_count,count(DISTINCT actor_id) active_actor_count FROM dwd_event GROUP BY event_date,split(repo_name,'/')[0]"
        right='SELECT event_date,owner_name,event_count,active_actor_count FROM dws_organisation_daily'
        if direction:left,right=right,left
        check('owner_independent_'+str(direction),spark.sql(left+' EXCEPT ALL '+right).limit(1).count()==0)
    # Independent commit reconciliation uses persisted typed facts instead of reparsing payload JSON.
    from functools import reduce
    pushes=reduce(lambda a,b:a.unionByName(b),[spark.table(d['database']+'.dwd_push_event') for d in days])
    pushes.createOrReplaceTempView('typed_pushes')
    check('owner_commit_independent',spark.sql('''WITH p AS (
      SELECT f.event_date,split(f.repo_name,'/')[0] owner_name,count(*) n,sum(p.declared_commit_count) declared,
        sum(CASE WHEN p.declared_commit_count IS NULL THEN 1 ELSE 0 END) missing
      FROM typed_pushes p JOIN dwd_event f ON p.event_id=f.event_id GROUP BY f.event_date,split(f.repo_name,'/')[0])
      SELECT s.owner_name FROM dws_organisation_daily s FULL OUTER JOIN p ON s.event_date=p.event_date AND s.owner_name=p.owner_name
      WHERE s.owner_name IS NULL OR s.push_events<>coalesce(p.n,0) OR NOT(s.declared_commit_count <=> p.declared)
        OR s.missing_declared_commit_events<>coalesce(p.missing,0)''').limit(1).count()==0)
    weekly="""SELECT date_sub(event_date,pmod(dayofweek(event_date)+5,7)) week_start,actor_id,count(*) event_count,
       count(DISTINCT event_date) active_days,count(DISTINCT repo_id) repository_count
       FROM dwd_event WHERE actor_id IS NOT NULL GROUP BY date_sub(event_date,pmod(dayofweek(event_date)+5,7)),actor_id"""
    actual='SELECT week_start,actor_id,event_count,active_days,repository_count FROM dws_contributor_weekly'
    check('weekly_independent_forward',spark.sql(weekly+' EXCEPT ALL '+actual).limit(1).count()==0)
    check('weekly_independent_reverse',spark.sql(actual+' EXCEPT ALL '+weekly).limit(1).count()==0)
    check('weekly_full_coverage',spark.table('dws_contributor_weekly').filter("covered_days<>7 OR coverage<>'COMPLETE'").count()==0)
    cohort=spark.table('ads_contributor_retention').collect()
    actors=spark.table('dwd_event').where('actor_id IS NOT NULL').select('actor_id').distinct().count()
    check('cohort_denominator',len(cohort)==1 and cohort[0].cohort_size==actors)
    check('retention_immature',spark.table('ads_contributor_retention').filter("maturity<>'IMMATURE' OR retained_count IS NOT NULL OR retention_rate IS NOT NULL OR observation_covered_days<>0").count()==0)
    sample=spark.table('dwd_event').where("event_date=DATE '2024-01-01'").select('event_id','repo_sk','source_sha256','line_number','source_hour','source_file').first()
    ods=spark.table(days[0]['database']+'.ods_github_event_raw').where((F.col('source_hour')==sample.source_hour)&(F.col('line_number')==sample.line_number)&(F.col('source_sha256')==sample.source_sha256))
    records=ods.select('event_id',F.sha2('raw_json',256).alias('raw_line_sha256')).collect()
    check('lineage_ods',len(records)==1 and records[0].event_id==sample.event_id)
    atomic(out/'lineage.json',{**sample.asDict(),**records[0].asDict(),'date':'2024-01-01'})
    r['passed']=True
except Exception:r['error']=traceback.format_exc()
finally:atomic(out/'live-result.json',r);spark.stop()
raise SystemExit(0 if r['passed'] else 1)
