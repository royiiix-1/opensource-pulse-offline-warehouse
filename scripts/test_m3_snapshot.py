"""Synthetic seven-date end-to-end model test; never updates official pointers."""
import datetime as dt,json,pathlib,runpy,sys,uuid
from pyspark.sql import SparkSession
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import atomic
run='fixture'+uuid.uuid4().hex[:12];out=ROOT/'evidence'/('m3-snapshot-'+run);out.mkdir()
spark=SparkSession.builder.appName('osp-m3-seven-date-fixture').enableHiveSupport().getOrCreate()
inputs=[]
for offset in range(7):
    date=dt.date(2024,1,1)+dt.timedelta(days=offset);db='osp3_'+run+'_'+str(offset)
    spark.sql('CREATE DATABASE '+db)
    events=[(str(offset*2+i),i,'owner/repo'+str(i) if offset<3 else 'newowner/repo'+str(i),i,
       dt.datetime.combine(date,dt.time(1)),date,'PushEvent','{"size":1}') for i in (1,2)]
    df=spark.createDataFrame(events,'event_id string,repo_id long,repo_name string,actor_id long,event_time timestamp,event_date date,event_type string,payload_json string')
    path='hdfs://127.0.0.1:19000/osp-offline/m3-fixtures/'+run+'/input/'+str(offset)
    df.write.mode('errorifexists').parquet(path)
    columns=', '.join(f.name+' '+f.dataType.simpleString() for f in df.schema.fields)
    spark.sql(f"CREATE EXTERNAL TABLE {db}.dwd_event ({columns}) STORED AS PARQUET LOCATION '{path}'")
    inputs.append({'date':date.isoformat(),'database':db,'tables':{'dwd_event':{'rows':2}}})
ctx={'inputs':inputs,'release':run,'database':'osp3_'+run+'_model','hdfs_path':'/osp-offline/m3-fixtures/'+run+'/model','staging_path':'/osp-offline/m3-fixtures/'+run+'/staging','evidence':str(out)}
context=out/'context.json';atomic(context,ctx)
# Same production runner, same YARN session. Fixture artifacts remain isolated.
sys.argv=['m3_snapshot.py',str(context)]
try:
    runpy.run_path(str(ROOT/'spark/m3_snapshot.py'),run_name='__main__')
except SystemExit as e:
    if e.code:raise
result=json.loads((out/'spark-result.json').read_text())
checks={'production_checks':all(c['passed'] for c in result['checks'].values()),
        'four_name_versions':result['tables']['dim_repository_scd2']['rows']==4,
        'fourteen_facts':result['tables']['dwd_event']['rows']==14,
        'two_contributors':result['tables']['dws_contributor_weekly']['rows']==2,
        'one_immature_cohort':len(result['retention'])==1 and result['retention'][0]['maturity']=='IMMATURE'}
atomic(out/'fixture-acceptance.json',{'fixture':True,'checks':checks,'passed':all(checks.values()),'application_id':result['application_id']})
print(str(out),json.dumps(checks),flush=True)
raise SystemExit(0 if all(checks.values()) else 1)
