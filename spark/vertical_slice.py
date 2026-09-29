"""M1 Spark SQL transformations, immutable release tables, independent reconciliation."""
import argparse, json, pathlib, time
from pyspark.sql import SparkSession, functions as F, types as T
p=argparse.ArgumentParser()
for n in ["input","date","hour","release","sha","report","expected-lines"]: p.add_argument("--"+n,required=True)
p.add_argument("--fail-after",choices=["ods","dwd"])
a=p.parse_args()
spark=SparkSession.builder.appName("osp-offline-"+a.release).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")
if spark.sparkContext.master!="yarn": raise RuntimeError("M1 must run on YARN")
db="osp_"+a.release
base="hdfs://127.0.0.1:19000/osp-offline/staging/"+a.release
report={"application_id":spark.sparkContext.applicationId,"master":spark.sparkContext.master,
        "release":a.release,"database":db,"coverage":"PARTIAL","source_sha256":a.sha,
        "tables":{},"checks":{},"started_at":time.time()}
out=pathlib.Path(a.report)
out.mkdir(parents=True,exist_ok=True)
spark.sql(f"CREATE DATABASE {db} LOCATION '{base}'")
schema=T.StructType([
 T.StructField("id",T.StringType()),T.StructField("type",T.StringType()),
 T.StructField("actor",T.StructType([T.StructField("id",T.LongType()),T.StructField("login",T.StringType())])),
 T.StructField("repo",T.StructType([T.StructField("id",T.LongType()),T.StructField("name",T.StringType())])),
 T.StructField("public",T.BooleanType()),T.StructField("created_at",T.StringType()),
 T.StructField("_corrupt_record",T.StringType())])
known=["PushEvent","PullRequestEvent","IssuesEvent","IssueCommentEvent","ReleaseEvent","WatchEvent","ForkEvent",
       "CreateEvent","DeleteEvent","PublicEvent","PullRequestReviewEvent","CommitCommentEvent","GollumEvent",
       "MemberEvent","PullRequestReviewCommentEvent"]
def write(name, frame):
    # Explicit Hive external table DDL with Parquet serde (no accidental local catalog).
    path=base+"/"+name
    frame.coalesce(2).write.mode("errorifexists").option("compression","snappy").parquet(path)
    columns=", ".join("`"+f.name+"` "+f.dataType.simpleString() for f in frame.schema.fields)
    spark.sql(f"CREATE EXTERNAL TABLE {db}.{name} ({columns}) STORED AS PARQUET LOCATION '{path}'")
    rows=spark.table(db+"."+name).count()
    report["tables"][name]={"rows":rows,"path":path,"schema":frame.schema.jsonValue()}
    print("TABLE_VALIDATED",name,rows,flush=True)
    spark.table(db+"."+name).createOrReplaceTempView(name)
    return rows
def query(text): return spark.sql(text)
def check(name,ok,actual=None):
    report["checks"][name]={"passed":bool(ok),"actual":actual}
    if not ok: raise RuntimeError("quality gate: "+name)
try:
    # Spark Core supplies stable 1-based line position for this single immutable gzip.
    lines=spark.sparkContext.textFile(a.input).zipWithIndex().map(lambda x:(x[1]+1,x[0]))
    raw=spark.createDataFrame(lines,"line_number long, raw_json string")
    parsed=raw.withColumn("e",F.from_json("raw_json",schema,{"mode":"PERMISSIVE"})).select(
      "line_number","raw_json",F.col("e.id").alias("event_id"),F.col("e.type").alias("event_type"),
      F.col("e.repo.id").alias("repo_id"),F.col("e.repo.name").alias("repo_name"),
      F.col("e.actor.id").alias("actor_id"),F.col("e.actor.login").alias("actor_login"),
      F.col("e.public").alias("public"),F.to_timestamp("e.created_at").alias("event_time"),
      F.col("e._corrupt_record").alias("corrupt"),F.get_json_object("raw_json","$.payload").alias("payload_json"))
    reason=(F.when(F.col("corrupt").isNotNull()|F.col("event_id").isNull()|~F.col("event_id").rlike("^[0-9]+$"),"invalid_json_or_event_id")
      .when(F.col("event_type").isNull()|(F.length("event_type")==0),"missing_event_type")
      .when(F.col("repo_id").isNull()|(F.col("repo_id")<=0)|F.col("repo_name").isNull()|~F.col("repo_name").rlike("^[^/]+/[^/]+$"),"invalid_repository")
      .when(F.col("event_time").isNull(),"invalid_event_time")
      .when(F.coalesce(F.col("public"),F.lit(False))!=True,"not_public")
      .when(F.date_format("event_time","yyyy-MM-dd-HH")!=a.date+"-"+a.hour.zfill(2),"source_hour_mismatch"))
    parsed=(parsed.withColumn("reason",reason).drop("corrupt")
      .withColumn("source_date",F.lit(a.date)).withColumn("source_hour",F.lit(int(a.hour)))
      .withColumn("source_file",F.lit(a.input)).withColumn("source_sha256",F.lit(a.sha))
      .withColumn("run_id",F.lit(a.release)).withColumn("ingested_at",F.current_timestamp())
      .persist())
    raw_count=parsed.count()
    rejected=write("quarantine",parsed.where("reason is not null"))
    ods_count=write("ods_github_event_raw",parsed.where("reason is null").drop("reason"))
    check("raw_line_reconciliation",raw_count==int(a.expected_lines) and raw_count==ods_count+rejected,
          {"raw":raw_count,"ods":ods_count,"quarantine":rejected})
    if a.fail_after=="ods": raise RuntimeError("INJECTED_FAILURE_AFTER_ODS")
    conflict=query("SELECT event_id FROM ods_github_event_raw GROUP BY event_id HAVING count(distinct raw_json)>1").count()
    check("conflicting_duplicate_ids",conflict==0,conflict)
    duplicate=query("SELECT * FROM (SELECT *, row_number() OVER(PARTITION BY event_id ORDER BY line_number) rn FROM ods_github_event_raw) WHERE rn>1")
    dup_count=write("excluded_duplicates",duplicate)
    unique=query("SELECT * FROM (SELECT *, row_number() OVER(PARTITION BY event_id ORDER BY line_number) rn FROM ods_github_event_raw) WHERE rn=1").drop("rn")
    unknown=write("unknown_events",unique.where(~F.col("event_type").isin(known)))
    valid=unique.where(F.col("event_type").isin(known)).withColumn("event_date",F.to_date("event_time"))
    dwd_count=write("dwd_event",valid)
    write("dwd_push_event",query("""SELECT event_id,event_date,event_time,repo_id,actor_id,
      get_json_object(payload_json,'$.ref') ref,
      cast(get_json_object(payload_json,'$.size') as bigint) declared_commit_count,
      json_array_length(get_json_object(payload_json,'$.commits')) observed_commit_array_count
      FROM dwd_event WHERE event_type='PushEvent'"""))
    write("dwd_pull_request_event",query("""SELECT event_id,event_date,event_time,repo_id,actor_id,
      get_json_object(payload_json,'$.action') action,
      cast(get_json_object(payload_json,'$.number') as bigint) pr_number,
      cast(get_json_object(payload_json,'$.pull_request.user.id') as bigint) author_id,
      get_json_object(payload_json,'$.pull_request.state') state,
      cast(get_json_object(payload_json,'$.pull_request.merged') as boolean) merged,
      cast(get_json_object(payload_json,'$.pull_request.created_at') as timestamp) created_at,
      cast(get_json_object(payload_json,'$.pull_request.closed_at') as timestamp) closed_at,
      cast(get_json_object(payload_json,'$.pull_request.merged_at') as timestamp) merged_at
      FROM dwd_event WHERE event_type='PullRequestEvent'"""))
    write("dwd_issue_event",query("""SELECT event_id,event_date,event_time,repo_id,actor_id,
      get_json_object(payload_json,'$.action') action,
      cast(get_json_object(payload_json,'$.issue.number') as bigint) issue_number,
      get_json_object(payload_json,'$.issue.state') state,
      cast(get_json_object(payload_json,'$.issue.created_at') as timestamp) created_at,
      cast(get_json_object(payload_json,'$.issue.closed_at') as timestamp) closed_at
      FROM dwd_event WHERE event_type='IssuesEvent'"""))
    write("dwd_issue_comment_event",query("""SELECT event_id,event_date,event_time,repo_id,actor_id,
      get_json_object(payload_json,'$.action') action,
      cast(get_json_object(payload_json,'$.issue.number') as bigint) issue_number,
      cast(get_json_object(payload_json,'$.comment.id') as bigint) comment_id
      FROM dwd_event WHERE event_type='IssueCommentEvent'"""))
    write("dwd_release_event",query("""SELECT event_id,event_date,event_time,repo_id,actor_id,
      get_json_object(payload_json,'$.release.tag_name') tag,
      cast(get_json_object(payload_json,'$.release.draft') as boolean) draft,
      cast(get_json_object(payload_json,'$.release.prerelease') as boolean) prerelease,
      cast(get_json_object(payload_json,'$.release.published_at') as timestamp) published_at
      FROM dwd_event WHERE event_type='ReleaseEvent'"""))
    check("ods_dwd_reconciliation",ods_count==dwd_count+dup_count+unknown,
          {"ods":ods_count,"dwd":dwd_count,"duplicates":dup_count,"unknown":unknown})
    check("quarantine_zero",rejected==0,rejected)
    check("unknown_zero",unknown==0,unknown)
    check("event_id_unique",query("SELECT event_id FROM dwd_event GROUP BY event_id HAVING count(*)>1").count()==0)
    if a.fail_after=="dwd": raise RuntimeError("INJECTED_FAILURE_AFTER_DWD")
    # Canonical dimension observation snapshot for M1; full SCD2 is a later milestone.
    write("dim_repository_observed",query("""SELECT repo_id,repo_name,split(repo_name,'/')[0] owner_name,
      min(event_time) first_observed_at,max(event_time) last_observed_at
      FROM dwd_event GROUP BY repo_id,repo_name"""))
    write("dws_repository_daily",query("""SELECT event_date,repo_id,count(*) event_count,
      sum(CASE WHEN event_type='PushEvent' THEN 1 ELSE 0 END) push_events,
      sum(CASE WHEN event_type='PullRequestEvent' AND get_json_object(payload_json,'$.action')='opened' THEN 1 ELSE 0 END) pr_opened,
      sum(CASE WHEN event_type='PullRequestEvent' AND get_json_object(payload_json,'$.action')='closed' THEN 1 ELSE 0 END) pr_closed,
      sum(CASE WHEN event_type='PullRequestEvent' AND get_json_object(payload_json,'$.action')='closed'
        AND get_json_object(payload_json,'$.pull_request.merged')='true' THEN 1 ELSE 0 END) pr_merged,
      sum(CASE WHEN event_type='IssuesEvent' AND get_json_object(payload_json,'$.action')='opened' THEN 1 ELSE 0 END) issue_opened,
      sum(CASE WHEN event_type='IssuesEvent' AND get_json_object(payload_json,'$.action')='closed' THEN 1 ELSE 0 END) issue_closed,
      count(distinct actor_id) active_actors,
      sum(CASE WHEN event_type='ReleaseEvent' THEN 1 ELSE 0 END) release_events,
      sum(CASE WHEN event_type='WatchEvent' THEN 1 ELSE 0 END) watch_events,
      sum(CASE WHEN event_type='ForkEvent' THEN 1 ELSE 0 END) fork_events,
      'PARTIAL' coverage, 1 observed_hours
      FROM dwd_event GROUP BY event_date,repo_id"""))
    write("ads_repository_activity_trend",query("""SELECT event_date,repo_id,event_count,push_events,
       active_actors,release_events,watch_events,fork_events,coverage,observed_hours FROM dws_repository_daily"""))
    write("ads_pr_issue_throughput",query("""SELECT event_date,repo_id,pr_opened,pr_closed,pr_merged,
       issue_opened,issue_closed,coverage,observed_hours FROM dws_repository_daily"""))
    independent=(pathlib.Path(__file__).resolve().parents[1]/"hive/quality/m1_reconcile.sql").read_text()
    for statement in independent.split(";"):
        if statement.strip():
            failures=query(statement).count()
            check("independent_sql_"+str(len(report["checks"])),failures==0,failures)
    for name in report["tables"]:
        formatted=query(f"EXPLAIN FORMATTED SELECT * FROM {db}.{name}").collect()
        (out/(name+"-explain.txt")).write_text("\n".join(row[0] for row in formatted))
    report["status"]="VALIDATED"
except Exception as error:
    report["status"]="FAILED"
    report["error"]=str(error)
    raise
finally:
    report["ended_at"]=time.time()
    (out/"quality.json").write_text(json.dumps(report,indent=2))
    spark.stop()
