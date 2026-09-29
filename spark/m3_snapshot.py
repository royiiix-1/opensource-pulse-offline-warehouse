"""Versioned multi-day model; all data jobs must execute on YARN."""
import json,pathlib,sys,time,traceback
from functools import reduce
from pyspark.sql import SparkSession,functions as F
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT),str(ROOT/'spark')]
from repository_scd2 import build,require_publishable
from m3_aggregates import build as aggregates
from warehouse.common import read,atomic
ctx=read(sys.argv[1]);out=pathlib.Path(ctx['evidence']);db=ctx['database'];base='hdfs://127.0.0.1:19000'+ctx['staging_path']
spark=SparkSession.builder.appName('osp-m3-'+ctx['release']).config('spark.sql.shuffle.partitions','48').enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('WARN')
report={'status':'RUNNING','application_id':spark.sparkContext.applicationId,'tables':{},'checks':{},'started_at':time.time()}
def check(name,passed,actual=None):
    report['checks'][name]={'passed':bool(passed),'actual':actual}
    if not passed:raise RuntimeError('M3 gate: '+name)
def write(name,df,partition=None):
    path=base+'/'+name
    (out/(name+'-plan.txt')).write_text(spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(),'formatted'))
    writer=df.write.mode('errorifexists').option('compression','snappy')
    if partition:writer=writer.partitionBy(partition)
    writer.parquet(path)
    columns=', '.join('`'+f.name+'` '+f.dataType.simpleString() for f in df.schema.fields if f.name!=partition)
    parts=(' PARTITIONED BY ('+partition+' '+df.schema[partition].dataType.simpleString()+')') if partition else ''
    spark.sql(f"CREATE EXTERNAL TABLE {db}.{name} ({columns}){parts} STORED AS PARQUET LOCATION '{path}'")
    if partition:spark.sql(f'MSCK REPAIR TABLE {db}.{name}')
    table=spark.table(db+'.'+name)
    n=table.count();report['tables'][name]={'rows':n,'path':path,'schema':table.schema.jsonValue()}
    if partition:
        report['tables'][name]['partitions']=[r[0] for r in spark.sql(f'SHOW PARTITIONS {db}.{name}').collect()]
        for value in report['tables'][name]['partitions']:
            k,v=value.split('=',1)
            catalog=spark._jsparkSession.sessionState().catalog().externalCatalog()
            actual=catalog.getPartition(db,name,spark._jvm.PythonUtils.toScalaMap({k:v})).storage().locationUri().get().toString()
            check(name+'_'+value+'_location',actual==path+'/'+value,actual)
    print('TABLE',name,n,flush=True)
    return table
try:
    check('yarn',spark.sparkContext.master=='yarn')
    spark.sql('CREATE DATABASE '+db)
    inputs=[spark.table(p['database']+'.dwd_event') for p in ctx['inputs']]
    facts=reduce(lambda a,b:a.unionByName(b),inputs)
    expected=sum(p['tables']['dwd_event']['rows'] for p in ctx['inputs'])
    check('input_row_counts',facts.count()==expected,expected)
    duplicates=facts.groupBy('event_id').count().filter('count>1')
    duplicate_count=duplicates.count();check('cross_date_event_id_unique',duplicate_count==0,duplicate_count)
    dim,conflicts=build(spark,facts.selectExpr('repo_id','repo_name','event_time AS observed_at'))
    report['conflicting_keys']=conflicts.count()
    require_publishable(conflicts,base+'/quarantine_repository_conflicts')
    dim=write('dim_repository_scd2',dim);dim.createOrReplaceTempView('repository_dimension')
    check('surrogate_unique',dim.select('repo_sk').distinct().count()==report['tables']['dim_repository_scd2']['rows'])
    check('one_current',dim.groupBy('repo_id').agg(F.sum(F.col('is_current').cast('int')).alias('n')).filter('n<>1').count()==0)
    facts.createOrReplaceTempView('source_events')
    enriched=spark.sql('''SELECT f.*,d.repo_sk FROM source_events f LEFT JOIN repository_dimension d
      ON f.repo_id=d.repo_id AND f.event_time>=d.valid_from AND (d.valid_to IS NULL OR f.event_time<d.valid_to)''')
    enriched=write('dwd_event',enriched,'event_date');enriched.createOrReplaceTempView('enriched_events')
    check('asof_no_row_multiplication',report['tables']['dwd_event']['rows']==expected)
    check('asof_no_missing_keys',enriched.filter('repo_sk IS NULL').count()==0)
    import datetime as dt
    spark.createDataFrame([(dt.date.fromisoformat(p['date']),) for p in ctx['inputs']],'covered_date date').createOrReplaceTempView('covered_dates')
    models=aggregates(spark)
    for name,df in models.items():
        partition='event_date' if name=='dws_organisation_daily' else 'week_start' if name=='dws_contributor_weekly' else 'cohort_week'
        write(name,df,partition)
    owner=spark.table(db+'.dws_organisation_daily')
    check('owner_total_reconciles',owner.agg(F.sum('event_count')).first()[0]==expected)
    weekly=spark.table(db+'.dws_contributor_weekly')
    valid_actor=enriched.filter('actor_id IS NOT NULL').count()
    check('weekly_total_reconciles',weekly.agg(F.sum('event_count')).first()[0]==valid_actor)
    retention=spark.table(db+'.ads_contributor_retention')
    check('immature_retention_null',retention.filter("maturity='IMMATURE' AND (retained_count IS NOT NULL OR retention_rate IS NOT NULL)").count()==0)
    report['retention']=[r.asDict() for r in retention.collect()]
    # Files become a candidate final release before catalog relocation; the pointer is still unchanged.
    from warehouse.hdfs import HDFS
    fs=HDFS();fs.mkdir(str(pathlib.PurePosixPath(ctx['hdfs_path']).parent));fs.rename(ctx['staging_path'],ctx['hdfs_path'])
    final_base='hdfs://127.0.0.1:19000'+ctx['hdfs_path']
    for name,table in report['tables'].items():
        target=final_base+'/'+name
        spark.sql(f"ALTER TABLE {db}.{name} SET LOCATION '{target}'")
        for part in table.get('partitions',[]):
            k,v=part.split('=',1)
            spark.sql(f"ALTER TABLE {db}.{name} PARTITION ({k}='{v}') SET LOCATION '{target}/{part}'")
            catalog=spark._jsparkSession.sessionState().catalog().externalCatalog()
            actual=catalog.getPartition(db,name,spark._jvm.PythonUtils.toScalaMap({k:v})).storage().locationUri().get().toString()
            check('relocated_'+name+'_'+part,actual==target+'/'+part,actual)
        spark.catalog.refreshTable(db+'.'+name)
        location=[r.data_type for r in spark.sql(f'DESCRIBE FORMATTED {db}.{name}').collect() if r.col_name.strip()=='Location']
        check('relocated_'+name+'_base',location==[target],location)
        check('relocated_'+name+'_rows',spark.table(db+'.'+name).count()==table['rows'])
        table['path']=target
    report['status']='VALIDATED'
except Exception:
    report['status']='FAILED';report['error']=traceback.format_exc()
finally:
    report['ended_at']=time.time()
    (out/'spark-result.json').write_text(json.dumps(report,indent=2,default=str));spark.stop()
raise SystemExit(0 if report['status']=='VALIDATED' else 1)
