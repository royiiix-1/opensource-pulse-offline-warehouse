"""Real YARN execution of synthetic edge cases; not scale evidence."""
import datetime as dt, json, pathlib, sys, traceback
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'spark'))
from repository_scd2 import build,as_of,require_publishable
from pyspark.sql import SparkSession
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
spark=SparkSession.builder.appName('osp-m3-scd2-fixture').getOrCreate()
report={'fixture':True,'application_id':spark.sparkContext.applicationId,'checks':{}}
def frame(rows):
    return spark.createDataFrame([(i,n,dt.datetime.fromisoformat(t)) for i,n,t in rows],
        'repo_id long, repo_name string, observed_at timestamp')
def rows(df):
    return [r.asDict() for r in df.orderBy('repo_id','valid_from').collect()]
try:
    assert spark.sparkContext.master=='yarn'
    base=[(1,'a/repo','2024-01-01'),(1,'a/repo','2024-01-02'),(1,'b/repo','2024-01-04'),(1,'a/repo','2024-01-06')]
    dim,q=build(spark,frame(base+base[:1])); result=rows(dim)
    report['checks']['duplicate_and_consecutive_observations_collapse']=len(result)==3 and q.count()==0
    report['checks']['rename_back_new_version']=result[0]['repo_sk']!=result[2]['repo_sk']
    report['checks']['half_open_intervals']=result[0]['valid_to']==dt.datetime(2024,1,4) and result[1]['valid_to']==dt.datetime(2024,1,6) and result[2]['valid_to'] is None
    facts=frame([(1,'unused','2024-01-04'),(1,'unused','2024-01-06'),(1,'unused','2023-12-31')])
    linked=as_of(spark,facts,dim).orderBy('observed_at').collect()
    report['checks']['asof_boundary_and_unobserved']=len(linked)==3 and linked[0].repo_sk is None and linked[1].repo_sk==result[1]['repo_sk'] and linked[2].repo_sk==result[2]['repo_sk']
    reversed_dim,_=build(spark,frame(list(reversed(base))))
    report['checks']['deterministic_rerun']=rows(reversed_dim)==result
    late,_=build(spark,frame(base+[(1,'c/repo','2024-01-03')]))
    changed=rows(late)
    report['checks']['late_rebuild_splits_interval']=len(changed)==4 and changed[0]['valid_to']==dt.datetime(2024,1,3) and changed[1]['valid_to']==dt.datetime(2024,1,4)
    bad,conflict=build(spark,frame(base+[(1,'x/repo','2024-01-04'),(2,'z/repo','2024-01-02')]))
    conflicts=[r.asDict() for r in conflict.collect()]
    (out/'conflicts.json').write_text(json.dumps(conflicts,default=str,indent=2))
    report['checks']['conflicting_repository_excluded']=bad.count()==1 and rows(bad)[0]['repo_id']==2
    blocked=False
    try:
        require_publishable(conflict,'hdfs://127.0.0.1:19000/osp-offline/m3-fixtures/'+out.name+'/quarantine')
    except ValueError:
        blocked=True
    report['checks']['conflict_blocks_publication']=blocked
    report['publication_allowed']=not bool(conflicts)
    (out/'intervals.json').write_text(json.dumps(result,default=str,indent=2))
    (out/'plan.txt').write_text(dim._jdf.queryExecution().toString())
    report['passed']=all(report['checks'].values())
except Exception:
    report['passed']=False;report['error']=traceback.format_exc()
finally:
    (out/'result.json').write_text(json.dumps(report,indent=2));spark.stop()
print(json.dumps(report,indent=2))
raise SystemExit(0 if report['passed'] else 1)
