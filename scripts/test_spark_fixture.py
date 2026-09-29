"""Synthetic Spark quarantine fixture, kept separate from real acquisition evidence."""
import datetime as dt, gzip, hashlib, json, pathlib, subprocess, uuid
R=pathlib.Path(__file__).resolve().parents[1]
run="r"+dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")+uuid.uuid4().hex[:8]
out=R/"evidence"/("spark-fixture-"+run);out.mkdir()
work=R/"build/fixtures"/run;work.mkdir(parents=True)
valid={"id":"9001","type":"PushEvent","repo":{"id":101,"name":"fixture/repo"},
       "actor":{"id":1001,"login":"fixture"},"public":True,"created_at":"2024-01-01T01:00:00Z",
       "payload":{"unexpected_future_field":"preserved"}}
unknown=dict(valid,id="9002",type="FutureEvent")
missing=dict(valid,id="9003",repo={"id":101})
misdated=dict(valid,id="9004",created_at="2024-01-01T02:00:00Z")
lines=[json.dumps(valid),json.dumps(valid),"{broken",json.dumps(unknown),json.dumps(missing),json.dumps(misdated)]
raw=work/"fixture.json.gz";raw.write_bytes(gzip.compress(("\n".join(lines)+"\n").encode(),mtime=0))
sha=hashlib.sha256(raw.read_bytes()).hexdigest()
target="/osp-offline/fixtures/"+run+".json.gz"
subprocess.run(["hdfs","dfs","-mkdir","-p","/osp-offline/fixtures"],check=True)
subprocess.run(["hdfs","dfs","-put",str(raw),target],check=True)
with (out/"spark.log").open("wb") as f:
    code=subprocess.run(["spark-submit","--master","yarn",str(R/"spark/vertical_slice.py"),
       "--input","hdfs://127.0.0.1:19000"+target,"--date","2024-01-01","--hour","1",
       "--release",run,"--sha",sha,"--report",str(out),"--expected-lines","6"],
       stdout=f,stderr=subprocess.STDOUT).returncode
q=json.loads((out/"quality.json").read_text())
tables={k:v["rows"] for k,v in q["tables"].items()}
checks={"expected_failure":code!=0 and q["status"]=="FAILED",
        "raw_balances":q["checks"]["raw_line_reconciliation"]["passed"],
        "three_quarantined":tables["quarantine"]==3,
        "unknown_preserved":tables["unknown_events"]==1,
        "duplicate_excluded":tables["excluded_duplicates"]==1,
        "one_fact":tables["dwd_event"]==1,
        "gate_rejects":q["checks"]["quarantine_zero"]["passed"] is False}
report={"fixture":True,"not_real_source_data":True,"checks":checks,"passed":all(checks.values()),
        "application_id":q["application_id"],"sha256":sha}
(out/"fixture-acceptance.json").write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
raise SystemExit(0 if report["passed"] else 1)
