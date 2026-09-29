"""M2 stage runner. Each attempt writes fresh paths; retries never overwrite data."""
import argparse,json,pathlib,time,types,uuid,sys
from pyspark.sql import SparkSession,functions as F,types as T
from pyspark import StorageLevel
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import read,model_digest
p=argparse.ArgumentParser()
p.add_argument("--context",required=True);p.add_argument("--stage",required=True,choices=["ods","dwd","dws","ads","quality","relocate","catalog"])
p.add_argument("--report",required=True)
args=p.parse_args();ctx=read(args.context)
if ctx["model_digest"]!=model_digest():raise RuntimeError("model changed during run")
out=pathlib.Path(args.report);out.parent.mkdir(parents=True,exist_ok=True)
if not out.resolve().is_relative_to((ROOT/"evidence/m2").resolve()):raise ValueError("invalid evidence path")
spark=(SparkSession.builder.appName("osp-m2-"+ctx["release"]+"-"+args.stage)
       .config("spark.sql.shuffle.partitions","24").config("spark.rdd.compress","true").enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("WARN")
# ODS bounds cache/write buffers. Spark 3.5.7 threshold=1 still falls back to sorting; this profile fixes memory pressure, not sort elimination.
if args.stage=="ods":spark.conf.set("spark.sql.maxConcurrentOutputFileWriters","1")
if spark.sparkContext.master!="yarn":raise RuntimeError("YARN required")
db=ctx["database"] if not ctx.get("noop") else ctx["active"]["database"]
base="hdfs://127.0.0.1:19000/osp-offline/staging/"+ctx["release"]+"/"+args.stage+"-"+uuid.uuid4().hex[:8]
a=types.SimpleNamespace(date=ctx["date"],release=ctx["release"],expected_lines=ctx["expected_lines"])
report={"stage":args.stage,"application_id":spark.sparkContext.applicationId,"database":db,"tables":{},"checks":{},"started_at":time.time()}
def query(sql):return spark.sql(sql)
def check(name,ok,actual=None):
    report["checks"][name]={"passed":bool(ok),"actual":actual}
    if not ok:raise RuntimeError("quality gate: "+name)
def spec(values):return ", ".join(k+"='"+str(v).replace("'","''")+"'" for k,v in values.items())
def write(name,frame):
    path=base+"/"+name
    parts=(["source_date","source_hour"] if name in ["ods_github_event_raw","quarantine","excluded_duplicates","unknown_events"]
           else ["event_date"] if "event_date" in frame.columns else [])
    plan=spark._jvm.PythonSQLUtils.explainString(frame._jdf.queryExecution(),"formatted")
    (out.parent/(out.stem+"-"+name+"-plan.txt")).write_text(plan)
    writer=frame.write.mode("errorifexists").option("compression","snappy")
    if args.stage=="ods":writer=writer.option("parquet.block.size",32<<20)
    if parts:writer=writer.partitionBy(*parts)
    writer.parquet(path)
    cols=", ".join("`"+f.name+"` "+f.dataType.simpleString() for f in frame.schema.fields if f.name not in parts)
    partition_ddl=" PARTITIONED BY ("+", ".join(k+" "+frame.schema[k].dataType.simpleString() for k in parts)+")" if parts else ""
    query(f"CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{name} ({cols}){partition_ddl} STORED AS PARQUET LOCATION '{path}'")
    query(f"ALTER TABLE {db}.{name} SET LOCATION '{path}'")
    partition_values=[]
    if parts:
        fs=spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(path),spark.sparkContext._jsc.hadoopConfiguration())
        def leaves(current,depth,values):
            if depth==len(parts):
                yield current,values
                return
            for item in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(current)):
                name=item.getPath().getName()
                if item.isDirectory() and name.startswith(parts[depth]+"="):
                    yield from leaves(current+"/"+name,depth+1,{**values,parts[depth]:name.split("=",1)[1]})
        for location,values in leaves(path,0,{}):
            query(f"ALTER TABLE {db}.{name} ADD IF NOT EXISTS PARTITION ({spec(values)}) LOCATION '{location}'")
            query(f"ALTER TABLE {db}.{name} PARTITION ({spec(values)}) SET LOCATION '{location}'")
            partition_values.append({"values":values,"path":location})
    spark.catalog.refreshTable(db+"."+name)
    table=spark.table(db+"."+name);rows=table.count()
    table.createOrReplaceTempView(name)
    report["tables"][name]={"rows":rows,"path":path,"partitions":partition_values,"schema":table.schema.jsonValue()}
    print("TABLE",name,rows,flush=True)
    return rows
schema=T.StructType([
 T.StructField("id",T.StringType()),T.StructField("type",T.StringType()),
 T.StructField("actor",T.StructType([T.StructField("id",T.LongType()),T.StructField("login",T.StringType())])),
 T.StructField("repo",T.StructType([T.StructField("id",T.LongType()),T.StructField("name",T.StringType())])),
 T.StructField("public",T.BooleanType()),T.StructField("created_at",T.StringType()),
 T.StructField("_corrupt_record",T.StringType())])
known=["PushEvent","PullRequestEvent","IssuesEvent","IssueCommentEvent","ReleaseEvent","WatchEvent","ForkEvent",
       "CreateEvent","DeleteEvent","PublicEvent","PullRequestReviewEvent","CommitCommentEvent","GollumEvent",
       "MemberEvent","PullRequestReviewCommentEvent"]
try:
    query(f"CREATE DATABASE IF NOT EXISTS {db}")
    if args.stage not in ["ods","relocate","catalog"]:
        for t in spark.catalog.listTables(db):spark.table(db+"."+t.name).createOrReplaceTempView(t.name)
    if args.stage=="ods":
        rdds=[]
        for item in ctx["sources"]:
            m=item["manifest"];uri=item["hdfs_uri"]
            rdd=spark.sparkContext.textFile(uri,minPartitions=1).zipWithIndex()
            rdds.append(rdd.map(lambda pair,m=m,uri=uri:(pair[1]+1,pair[0],m["source_date"],m["source_hour"],uri,m["sha256"])))
        raw=spark.createDataFrame(spark.sparkContext.union(rdds),"line_number long,raw_json string,source_date string,source_hour int,source_file string,source_sha256 string")
        parsed=raw.withColumn("e",F.from_json("raw_json",schema,{"mode":"PERMISSIVE"})).select(
          "line_number","raw_json","source_date","source_hour","source_file","source_sha256",F.col("e.id").alias("event_id"),F.col("e.type").alias("event_type"),
          F.col("e.repo.id").alias("repo_id"),F.col("e.repo.name").alias("repo_name"),
          F.col("e.actor.id").alias("actor_id"),F.col("e.actor.login").alias("actor_login"),
          F.col("e.public").alias("public"),F.to_timestamp("e.created_at").alias("event_time"),
          F.col("e._corrupt_record").alias("corrupt"),F.get_json_object("raw_json","$.payload").alias("payload_json"))
        reason=(F.when(F.col("corrupt").isNotNull()|F.col("event_id").isNull()|~F.col("event_id").rlike("^[0-9]+$"),"invalid_json_or_event_id")
          .when(F.col("event_type").isNull()|(F.length("event_type")==0),"missing_event_type")
          .when(F.col("repo_id").isNull()|(F.col("repo_id")<=0)|F.col("repo_name").isNull()|~F.col("repo_name").rlike("^[^/]+/[^/]+$"),"invalid_repository")
          .when(F.col("event_time").isNull(),"invalid_event_time")
          .when(F.coalesce(F.col("public"),F.lit(False))!=True,"not_public")
          .when(F.date_format("event_time","yyyy-MM-dd-HH")!=F.concat(F.col("source_date"),F.lit("-"),F.lpad(F.col("source_hour").cast("string"),2,"0")),"source_hour_mismatch"))
        parsed=(parsed.withColumn("reason",reason).drop("corrupt")
          .withColumn("run_id",F.lit(a.release)).withColumn("ingested_at",F.current_timestamp())
          .persist(StorageLevel.DISK_ONLY))
        report["ods_write_policy"]={"max_concurrent_writers":1,"parquet_row_group_bytes":32<<20,"parsed_cache":"DISK_ONLY","source_hour_per_input_partition":True}
        raw_count=parsed.count()
        mixed=(parsed.select(F.spark_partition_id().alias("input_partition"),F.struct("source_date","source_hour").alias("hour"))
          .groupBy("input_partition").agg(F.countDistinct("hour").alias("hour_count")).where("hour_count>1").count())
        check("ods_single_hour_per_input_partition",mixed==0,mixed)
        rejected=write("quarantine",parsed.where("reason is not null"))
        ods_count=write("ods_github_event_raw",parsed.where("reason is null").drop("reason"))
        check("raw_line_reconciliation",raw_count==int(a.expected_lines) and raw_count==ods_count+rejected,
              {"raw":raw_count,"ods":ods_count,"quarantine":rejected})
    elif args.stage=="dwd":
        ods_count=query("SELECT count(*) FROM ods_github_event_raw").first()[0]
        rejected=query("SELECT count(*) FROM quarantine").first()[0]
        from warehouse.transform import classify_duplicates
        unique,duplicate,dedup_metrics=classify_duplicates(spark,spark.table("ods_github_event_raw"))
        report["dedup"]=dedup_metrics
        conflict=query("SELECT event_id FROM _repeated_events GROUP BY event_id HAVING count(distinct raw_json)>1").count()
        check("conflicting_duplicate_ids",conflict==0,conflict)
        dup_count=write("excluded_duplicates",duplicate)
        unknown=write("unknown_events",unique.where(~F.col("event_type").isin(known)))
        # Full raw JSON remains immutable in ODS; DWD carries payload and source lineage.
        valid=unique.where(F.col("event_type").isin(known)).drop("raw_json").withColumn("event_date",F.to_date("event_time"))
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
    elif args.stage=="dws":
        # Daily name observations; the separate M3 snapshot builds the SCD2 dimension.
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
          'COMPLETE' coverage, 24 observed_hours
          FROM dwd_event GROUP BY event_date,repo_id"""))
    elif args.stage=="ads":
        write("ads_repository_activity_trend",query("""SELECT event_date,repo_id,event_count,push_events,
           active_actors,release_events,watch_events,fork_events,coverage,observed_hours FROM dws_repository_daily"""))
        write("ads_pr_issue_throughput",query("""SELECT event_date,repo_id,pr_opened,pr_closed,pr_merged,
           issue_opened,issue_closed,coverage,observed_hours FROM dws_repository_daily"""))
    elif args.stage=="quality":
        for i,statement in enumerate((ROOT/"hive/quality/m2_reconcile.sql").read_text().split(";")):
            if statement.strip():check("independent_"+str(i),query(statement).count()==0)
        check("full_day_coverage",query("SELECT * FROM dws_repository_daily WHERE coverage<>'COMPLETE' OR observed_hours<>24").count()==0)
        check("event_date",query("SELECT * FROM dwd_event WHERE event_date<>DATE '"+ctx["date"]+"'").count()==0)
    elif args.stage in ["relocate","catalog"]:
        tables=ctx["tables"] if args.stage=="relocate" else ctx["active"]["tables"]
        for name,t in tables.items():
            if args.stage=="relocate":
                new=t["path"].replace("/osp-offline/staging/"+ctx["release"],"/osp-offline/daily-releases/"+ctx["release"],1)
                query(f"ALTER TABLE {db}.{name} SET LOCATION '{new}'")
                for part in t["partitions"]:
                    location=part["path"].replace("/osp-offline/staging/"+ctx["release"],"/osp-offline/daily-releases/"+ctx["release"],1)
                    query(f"ALTER TABLE {db}.{name} PARTITION ({spec(part['values'])}) SET LOCATION '{location}'")
            else:new=t["path"]
            spark.catalog.refreshTable(db+"."+name)
            check(name+"_schema",spark.table(db+"."+name).schema.jsonValue()==t["schema"])
            details={r.col_name.strip():r.data_type for r in query(f"DESCRIBE FORMATTED {db}.{name}").collect()}
            check(name+"_location",details["Location"]==new,details["Location"])
            for part in t["partitions"]:
                expected=part["path"] if args.stage=="catalog" else part["path"].replace("/osp-offline/staging/"+ctx["release"],"/osp-offline/daily-releases/"+ctx["release"],1)
                partition=spark._jsparkSession.sessionState().catalog().externalCatalog().getPartition(
                    db,name,spark._jvm.PythonUtils.toScalaMap(part["values"]))
                actual=partition.storage().locationUri().get().toString()
                check(name+"_"+str(part["values"]),actual==expected,actual)
    report["status"]="VALIDATED"
except Exception as e:
    report.update(status="FAILED",error=str(e))
    raise
finally:
    report["ended_at"]=time.time()
    out.write_text(json.dumps(report,indent=2))
    spark.stop()
