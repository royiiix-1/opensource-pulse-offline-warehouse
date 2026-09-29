"""Synthetic YARN metric semantics, distinct from real scale evidence."""
import datetime as dt,json,pathlib,sys,traceback
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'spark'))
from pyspark.sql import SparkSession
from m3_aggregates import build
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
spark=SparkSession.builder.appName('osp-m3-metric-fixture').getOrCreate()
report={'fixture':True,'application_id':spark.sparkContext.applicationId,'checks':{}}
def coverage(n):
    spark.createDataFrame([(dt.date(2024,1,1)+dt.timedelta(days=i),) for i in range(n)],'covered_date date').createOrReplaceTempView('covered_dates')
try:
    assert spark.sparkContext.master=='yarn'
    events=[(1,1,'owner/a','2024-01-01','{}'),(1,2,'owner/b','2024-01-01','{"size":0}'),
            (2,1,'owner/a','2024-01-01','{"size":2}'),(1,1,'owner/a','2024-01-08','{}')]
    spark.createDataFrame([(a,r,n,dt.datetime.fromisoformat(d),dt.date.fromisoformat(d),'PushEvent',p) for a,r,n,d,p in events],
       'actor_id long,repo_id long,repo_name string,event_time timestamp,event_date date,event_type string,payload_json string').createOrReplaceTempView('enriched_events')
    coverage(14);models=build(spark)
    owner=models['dws_organisation_daily'].orderBy('event_date').collect()
    report['checks']['owner_distinct_not_repo_sum']=owner[0].active_actor_count==2 and owner[0].repository_count==2
    report['checks']['unknown_commit_not_zero']=owner[1].declared_commit_count is None and owner[1].missing_declared_commit_events==1
    report['checks']['known_zero_and_known_two']=owner[0].declared_commit_count==2 and owner[0].missing_declared_commit_events==1
    retention=models['ads_contributor_retention'].first()
    report['checks']['mature_half_retained']=retention.retained_count==1 and retention.retention_rate==0.5 and retention.maturity=='MATURE'
    coverage(7);immature=build(spark)['ads_contributor_retention'].first()
    report['checks']['immature_null']=immature.retained_count is None and immature.retention_rate is None and immature.maturity=='IMMATURE'
    spark.sql("SELECT * FROM enriched_events WHERE event_date<'2024-01-08'").createOrReplaceTempView('enriched_events')
    coverage(14);zero=build(spark)['ads_contributor_retention'].first()
    report['checks']['mature_zero_is_zero']=zero.retained_count==0 and zero.retention_rate==0.0
    report['passed']=all(report['checks'].values())
except Exception:
    report['passed']=False;report['error']=traceback.format_exc()
finally:
    (out/'result.json').write_text(json.dumps(report,indent=2));spark.stop()
print(json.dumps(report,indent=2));raise SystemExit(0 if report['passed'] else 1)
