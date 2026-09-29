"""Bounded real-data comparisons. Experimental copies never replace published tables."""
import hashlib,json,pathlib,sys,time,traceback
from pyspark.sql import SparkSession,functions as F
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import read,atomic
ctx=read(sys.argv[1]);out=pathlib.Path(ctx['evidence']);base='hdfs://127.0.0.1:19000'+ctx['hdfs_path']
spark=(SparkSession.builder.appName('osp-m4-'+ctx['run']).config('spark.sql.adaptive.enabled','false')
       .config('spark.sql.autoBroadcastJoinThreshold','-1').config('spark.sql.shuffle.partitions','48')
       .config('spark.sql.parquet.aggregatePushdown','false').enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel('WARN')
report={'status':'RUNNING','application_id':spark.sparkContext.applicationId,'source_identity':ctx['source']['identity'],
 'checks':{},'trials':[],'layouts':{},'settings':{'AQE':False,'autoBroadcastJoinThreshold':-1,'shuffle_partitions':48,
 'cache_policy':'No DataFrame cache. OS/HDFS caches not cleared. One warmup per variant; alternating measured order ABBAAB.'}}
def save():atomic(out/'result.json',report)
def check(name,ok,actual=None):
    report['checks'][name]={'passed':bool(ok),'actual':actual};save()
    if not ok:raise RuntimeError('M4 check failed: '+name)
def plan_metrics(node):
    result=[];it=node.metrics().iterator();m={}
    while it.hasNext():
        pair=it.next();m[str(pair._1())]=pair._2().value()
    result.append({'node':node.nodeName(),'metrics':m})
    children=node.children().iterator()
    while children.hasNext():result.extend(plan_metrics(children.next()))
    return result
def measure(experiment,variant,sql,phase,index):
    group=f'{experiment}-{variant}-{phase}-{index}'
    spark.sparkContext.setJobGroup(group,group)
    t=time.perf_counter();df=spark.sql(sql);rows=df.collect();elapsed=time.perf_counter()-t
    values=sorted([r.asDict() for r in rows],key=lambda r:json.dumps(r,sort_keys=True,default=str))
    data=json.dumps(values,sort_keys=True,default=str,separators=(',',':'))
    plan=spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(),'formatted')
    (out/(group+'.sql')).write_text(sql);(out/(group+'-plan.txt')).write_text(plan)
    (out/(group+'-rows.json')).write_text(data)
    metrics=plan_metrics(df._jdf.queryExecution().executedPlan())
    trial={'group':group,'experiment':experiment,'variant':variant,'phase':phase,'elapsed_seconds':elapsed,
           'result_rows':len(values),'result_sha256':hashlib.sha256(data.encode()).hexdigest(),'plan_file':group+'-plan.txt','metrics':metrics}
    report['trials'].append(trial);save();print('TRIAL',group,round(elapsed,3),flush=True)
    return trial,values,plan
def pair(name,queries,equivalent=True,join_types=None):
    reference={}
    for i,(variant,phase) in enumerate([(0,'warmup'),(1,'warmup')]+[(v,'measured') for v in (0,1,1,0,0,1)]):
        label,sql=queries[variant];trial,values,plan=measure(name,label,sql,phase,i)
        if label not in reference:reference[label]=trial['result_sha256']
        check(trial['group']+'_stable',trial['result_sha256']==reference[label])
        if join_types:check(trial['group']+'_join_operator',join_types[variant] in plan)
        if name=='A_date':
            expected=ctx['source']['tables']['dwd_event']['rows'] if variant==0 else ctx['day_rows']
            check(trial['group']+'_expected_events',sum(r['events'] for r in values)==expected)
    if equivalent:check(name+'_exact_query_results_equal',len(set(reference.values()))==1)
def inventory(path):
    fs=spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(path),spark.sparkContext._jsc.hadoopConfiguration())
    iterator=fs.listFiles(spark._jvm.org.apache.hadoop.fs.Path(path),True);files=[]
    while iterator.hasNext():
        f=iterator.next()
        if f.getPath().getName().endswith('.parquet'):files.append({'path':f.getPath().toString(),'bytes':f.getLen()})
    return {'files':files,'count':len(files),'total_bytes':sum(f['bytes'] for f in files),'average_bytes':sum(f['bytes'] for f in files)/len(files)}
try:
    check('master_yarn',spark.sparkContext.master=='yarn')
    spark.table(ctx['source']['database']+'.dwd_event').createOrReplaceTempView('facts')
    agg='SELECT event_type,count(*) AS events,sum(cast(repo_id AS DECIMAL(38,0))) AS repo_sum,count(actor_id) AS known_actor_events FROM '
    pair('A_date',[('all_dates',agg+'facts GROUP BY event_type'),('one_date',agg+"facts WHERE event_date=DATE '2024-01-01' GROUP BY event_type")],equivalent=False)
    # Two narrow layouts, same real day and exact rows; this is a layout experiment, not another dataset scale claim.
    cols=['event_id','event_type','repo_id','actor_id','source_hour']
    day=spark.table('facts').where("event_date=DATE '2024-01-01'").select(*cols)
    for label,frame,partition in [('hourly',day.repartition(24,'source_hour'),'source_hour'),('daily_compact',day.repartition(4),None)]:
        t=time.perf_counter();writer=frame.write.mode('errorifexists').option('compression','snappy').option('parquet.block.size',32<<20)
        if partition:writer=writer.partitionBy(partition)
        writer.parquet(base+'/'+label)
        report['layouts'][label]=inventory(base+'/'+label);report['layouts'][label]['build_seconds']=time.perf_counter()-t
        spark.read.parquet(base+'/'+label).createOrReplaceTempView(label);save()
    hourly=spark.table('hourly').select(*cols);compact=spark.table('daily_compact').select(*cols)
    check('layout_count',hourly.count()==compact.count()==ctx['day_rows'])
    check('layout_exact_multiset_forward',hourly.exceptAll(compact).limit(1).count()==0)
    check('layout_exact_multiset_reverse',compact.exceptAll(hourly).limit(1).count()==0)
    check('compaction_reduces_files',report['layouts']['daily_compact']['count']<report['layouts']['hourly']['count'])
    pair('A_granularity',[('daily_compact',agg+'daily_compact WHERE source_hour=12 GROUP BY event_type'),('hourly',agg+'hourly WHERE source_hour=12 GROUP BY event_type')])
    pair('C_compaction',[('hourly',agg+'hourly GROUP BY event_type'),('daily_compact',agg+'daily_compact GROUP BY event_type')])
    # A deterministic small subset of the real SCD2 dimension; joins use historical repo_sk, never current-name substitution.
    dim=spark.table(ctx['source']['database']+'.dim_repository_scd2').where('pmod(xxhash64(repo_sk),256)=0').select('repo_sk','owner_name')
    small=dim.limit(50001).collect()
    estimated=len(json.dumps([r.asDict() for r in small]).encode())
    check('small_dimension_bounded',0<len(small)<=50000 and estimated<=8*1024**2,{'rows':len(small),'json_bytes':estimated})
    dim.coalesce(1).write.mode('errorifexists').option('compression','snappy').parquet(base+'/small_dimension')
    report['layouts']['small_dimension']=inventory(base+'/small_dimension');report['small_dimension_rows']=len(small);report['small_dimension_json_bytes']=estimated
    spark.read.parquet(base+'/small_dimension').createOrReplaceTempView('small_dimension')
    joins=[]
    for label,hint in [('sort_merge','MERGE(f,d)'),('broadcast','BROADCAST(d)')]:
        joins.append((label,'SELECT /*+ '+hint+' */ d.owner_name,count(*) AS events,sum(cast(f.repo_id AS DECIMAL(38,0))) AS repo_sum FROM facts f JOIN small_dimension d ON f.repo_sk=d.repo_sk GROUP BY d.owner_name'))
    pair('B_join',joins,join_types=['SortMergeJoin','BroadcastHashJoin'])
    report['status']='VALIDATED'
except Exception:
    report['status']='FAILED';report['error']=traceback.format_exc()
finally:
    save();spark.stop()
raise SystemExit(0 if report['status']=='VALIDATED' else 1)
