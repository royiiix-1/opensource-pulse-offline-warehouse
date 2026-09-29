import json,pathlib,sys
from pyspark.sql import SparkSession
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.transform import classify_duplicates
spark=SparkSession.builder.appName("osp-m2-narrow-dedup-fixture").getOrCreate()
rows=[("a",3,2,'{"v":1}'),("a",1,9,'{"v":1}'),("b",2,1,'{}'),("c",2,2,'{}'),("c",2,3,'{}')]
df=spark.createDataFrame(rows,"event_id string,source_hour int,line_number long,raw_json string")
u,d,metrics=classify_duplicates(spark,df)
selected={r.event_id:[r.source_hour,r.line_number] for r in u.collect()}
checks={"canonical_position":selected=={"a":[1,9],"b":[2,1],"c":[2,2]},"duplicates":d.count()==2,
        "no_false_conflict":spark.sql("SELECT event_id FROM _repeated_events GROUP BY event_id HAVING count(distinct raw_json)>1").count()==0}
classify_duplicates(spark,spark.createDataFrame(rows+[("a",0,99,'{"v":2}')],df.schema))
checks["conflict_detected"]=spark.sql("SELECT event_id FROM _repeated_events GROUP BY event_id HAVING count(distinct raw_json)>1").first()[0]=="a"
result={"passed":all(checks.values()),"checks":checks,"metrics":metrics,"application_id":spark.sparkContext.applicationId}
(ROOT/"evidence/m2-dedup-fixture.json").write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
spark.stop()
raise SystemExit(0 if result["passed"] else 1)
