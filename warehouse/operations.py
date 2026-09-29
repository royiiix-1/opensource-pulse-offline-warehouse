"""Airflow-independent M2 orchestration functions; every state is persisted and auditable."""
import datetime as dt,fcntl,hashlib,json,os,pathlib,subprocess,time,uuid
from warehouse.common import ROOT,CONTROL,read,atomic,dates,model_digest,source_plan
from warehouse.sources import accept_hour,receipts,validate_day
from warehouse.hdfs import HDFS
STAGES=["ods","dwd","dws","ads","quality"]
def request_dates(conf,logical_date):
    if conf.get("start_date") or conf.get("end_date"):
        if conf.get("process_date") or conf.get("process_hour") is not None:raise ValueError("range and hour/date are mutually exclusive")
        return dates(conf["start_date"],conf["end_date"])
    value=conf.get("process_date") or logical_date.strftime("%Y-%m-%d")
    dates(value,value)
    hour=conf.get("process_hour")
    if hour is not None and (type(hour) is not int or not 0<=hour<=23):raise ValueError("invalid repair hour")
    return [value]
def prepare(conf,logical_date,run_id):
    selected=request_dates(conf,logical_date)
    if conf.get("acquisition_policy","download_missing") not in ["cache_only","download_missing"]:raise ValueError("invalid acquisition policy")
    key=hashlib.sha256(run_id.encode()).hexdigest()[:20]
    paths=[]
    for date in selected:
        path=CONTROL/"runs"/key/(date+".json");paths.append(str(path))
        if not path.exists():
            release="d"+date.replace("-","")+key
            atomic(path,{"key":key,"run_id":run_id,"date":date,"release":release,"database":"osp2_"+release,
                         "requested_hour":conf.get("process_hour"),"policy":conf.get("acquisition_policy","download_missing"),
                         "fault_once":conf.get("fault_once"),"model_digest":model_digest(),"reports":{},
                         "evidence":str((ROOT/"evidence/m2"/key/date).relative_to(ROOT))})
    return paths
def lease(ctx,status="ACTIVE"):
    folder=CONTROL/"leases";folder.mkdir(parents=True,exist_ok=True)
    path=folder/(ctx["date"]+".json")
    with (folder/(ctx["date"]+".lock")).open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        old=read(path) if path.exists() else {}
        if status=="ACTIVE" and old.get("status")=="ACTIVE" and old.get("key")!=ctx["key"]:raise RuntimeError("date already leased")
        if status!="ACTIVE" and old.get("key")!=ctx["key"]:return
        atomic(path,{"key":ctx["key"],"status":status,"at":time.time()})
def check_sources(path):
    ctx=read(path)
    if ctx["date"] not in read(ROOT/"config/m2-budget.json")["allowed_dates"]:raise RuntimeError("date has no approved resource plan")
    if ctx["policy"]=="download_missing":
        planned=source_plan(ctx["date"])
        if planned["date"]!=ctx["date"] or not planned["all_sizes_known"]:raise RuntimeError("source HEAD plan unavailable")
    atomic(ROOT/ctx["evidence"]/"source-check.json",{"expected_hours":24,"currently_accepted":[r["manifest"]["source_hour"] for r in receipts(ctx["date"])]})
def acquire_sources(path):
    ctx=read(path)
    if ctx["policy"]=="cache_only":return
    wanted=[ctx["requested_hour"]] if ctx["requested_hour"] is not None else range(24)
    actions=[]
    for hour in wanted:
        receipt=accept_hour(ctx["date"],hour,download=True)
        actions.append({"hour":hour,"identity":receipt["object_id"]})
    atomic(ROOT/ctx["evidence"]/"acquisition.json",{"requested_hours":list(wanted),"accepted":actions})
def validate_manifest(path):
    ctx=read(path);sources=validate_day(ctx["date"]) # Missing hour fails before lease or Spark.
    ctx["sources"]=sources;ctx["expected_lines"]=sum(r["manifest"]["line_count"] for r in sources)
    identity=hashlib.sha256((ctx["model_digest"]+json.dumps([r["object_id"] for r in sources])).encode()).hexdigest()
    ctx["identity"]=identity
    active_path=CONTROL/"published"/(ctx["date"]+".json")
    active=read(active_path) if active_path.exists() else None
    ctx["active"]=active
    ctx["noop"]=bool(active and active["identity"]==identity)
    lease(ctx)
    if ctx["noop"] and HDFS().inventory(active["hdfs_path"])!=active["inventory"]:raise RuntimeError("published data mutated")
    atomic(path,ctx)
    atomic(ROOT/ctx["evidence"]/"manifest-validation.json",{"passed":True,"hours":24,"line_count":ctx["expected_lines"],"identity":identity,"noop":ctx["noop"]})
def publish_raw(path):
    ctx=read(path)
    if ctx["noop"]:return
    fs=HDFS();audit=[]
    for item in ctx["sources"]:
        m=item["manifest"];base=f"/osp-offline/raw/source_date={m['source_date']}/source_hour={m['source_hour']:02d}/sha256={m['sha256']}"
        fs.mkdir(base);target=base+"/source.json.gz"
        local=ROOT/"build/acquisition/objects"/item["object_id"]
        if not fs.exists(target):
            temp=base+"/upload-"+uuid.uuid4().hex
            fs.put(local/"source.json.gz",temp)
            if fs.sha(temp)["sha256"]!=m["sha256"]:raise RuntimeError("raw upload mismatch")
            fs.rename(temp,target);fs.permission(target)
        actual=fs.sha(target)
        if actual["sha256"]!=m["sha256"] or actual["bytes"]!=m["compressed_bytes"]:raise RuntimeError("raw differs from manifest")
        if not fs.exists(base+"/manifest.json"):
            fs.put(local/"manifest.json",base+"/manifest.json");fs.permission(base+"/manifest.json")
        item["hdfs_uri"]="hdfs://127.0.0.1:19000"+target
        audit.append({"hour":m["source_hour"],**actual})
    fs.mkdir("/osp-offline/daily-releases");fs.mkdir("/osp-offline/staging/"+ctx["release"])
    atomic(path,ctx);atomic(ROOT/ctx["evidence"]/"raw-validation.json",{"passed":True,"files":audit})
def disk_usage():
    # Fixed 8 GiB conservatively covers binaries, install caches and non-HDFS evidence.
    local=sum(p.stat().st_size for p in (ROOT/"build/acquisition").rglob("*") if p.is_file())
    native=int(subprocess.check_output(["du","-sb",read(ROOT/"config/native-cache.json")["root"]],text=True).split()[0])
    return (8<<30)+local+native+HDFS().size("/")
def spark_stage(path,stage):
    ctx=read(path)
    if ctx["model_digest"]!=model_digest():raise RuntimeError("model changed during run")
    if ctx["noop"] and stage!="catalog":return
    if stage in ctx["reports"] and read(ctx["reports"][stage])["status"]=="VALIDATED":return
    attempt=uuid.uuid4().hex
    report=ROOT/ctx["evidence"]/(stage+"-"+attempt+".json")
    report.parent.mkdir(parents=True,exist_ok=True)
    reserve_gib={"ods":8,"dwd":8,"dws":2,"ads":2,"quality":1,"relocate":1,"catalog":1}[stage]
    if disk_usage()+(reserve_gib<<30)>read(ROOT/"config/m2-budget.json")["disk_limit_bytes"]:
        raise RuntimeError("insufficient stage storage headroom")
    args=["spark-submit","--master","yarn",str(ROOT/"spark/day_stage.py"),"--context",path,"--stage",stage,"--report",str(report)]
    with report.with_suffix(".log").open("wb") as output:
        process=subprocess.Popen(args,stdout=output,stderr=subprocess.STDOUT,cwd=ROOT)
        samples=[];last=0
        while process.poll() is None:
            if time.monotonic()-last>15:
                used=disk_usage();samples.append({"at":time.time(),"conservative_bytes":used});last=time.monotonic()
                if used>read(ROOT/"config/m2-budget.json")["disk_limit_bytes"]-(2<<30):
                    process.terminate();process.wait(timeout=60)
                    atomic(report.with_suffix(".budget.json"),{"exceeded":True,"samples":samples})
                    raise RuntimeError("storage watchdog stopped this Spark driver")
            time.sleep(2)
    atomic(report.with_suffix(".budget.json"),{"exceeded":False,"samples":samples})
    if process.returncode:raise RuntimeError(f"Spark {stage} failed: {report}")
    result=read(report)
    if result["status"]!="VALIDATED" or not all(v["passed"] for v in result["checks"].values()):raise RuntimeError("stage quality failed")
    ctx=read(path);ctx["reports"][stage]=str(report);ctx.setdefault("tables",{}).update(result["tables"]);atomic(path,ctx)
def dwd_gate(path):
    ctx=read(path)
    if ctx["noop"]:return
    checks=read(ctx["reports"]["dwd"])["checks"]
    if not checks or not all(c["passed"] for c in checks.values()):raise RuntimeError("DWD gate rejected")
    if ctx.get("fault_once")=="dwd_gate":
        marker=ROOT/ctx["evidence"]/"injected-once.json"
        if not marker.exists():
            atomic(marker,{"reason":"controlled transient gate failure","at":time.time()})
            raise RuntimeError("INJECTED_TRANSIENT_DWD_GATE_FAILURE")
    atomic(ROOT/ctx["evidence"]/"dwd-gate.json",{"passed":True})
def publish_day(path):
    ctx=read(path);fs=HDFS()
    if ctx["noop"]:
        spark_stage(path,"catalog")
        atomic(ROOT/ctx["evidence"]/"publication.json",{"status":"NO_OP","coverage":"COMPLETE","date":ctx["date"],"release":ctx["active"]["release"]})
        lease(ctx,"RELEASED");return
    if any(s not in ctx["reports"] for s in STAGES):raise RuntimeError("not all stages validated")
    if not read(ROOT/ctx["evidence"]/"dwd-gate.json")["passed"]:raise RuntimeError("DWD gate missing")
    # Recheck all inputs and coverage at the commit boundary.
    sources=validate_day(ctx["date"])
    expected=hashlib.sha256((ctx["model_digest"]+json.dumps([r["object_id"] for r in sources])).encode()).hexdigest()
    if expected!=ctx["identity"]:raise RuntimeError("source set changed before commit")
    stage="/osp-offline/staging/"+ctx["release"];dest="/osp-offline/daily-releases/"+ctx["release"]
    if not fs.exists(dest):fs.rename(stage,dest)
    spark_stage(path,"relocate")
    ctx=read(path)
    tables=json.loads(json.dumps(ctx["tables"]))
    for t in tables.values():
        t["path"]=t["path"].replace(stage,dest,1)
        for part in t["partitions"]:part["path"]=part["path"].replace(stage,dest,1)
    inv=fs.inventory(dest)
    if not inv:raise RuntimeError("empty release")
    active={"coverage":"COMPLETE","date":ctx["date"],"hours":list(range(24)),"release":ctx["release"],
            "identity":ctx["identity"],"database":ctx["database"],"hdfs_path":dest,"tables":tables,"inventory":inv}
    atomic(CONTROL/"published"/(ctx["date"]+".json"),active)
    atomic(ROOT/ctx["evidence"]/"publication.json",{"status":"PUBLISHED_COMPLETE","date":ctx["date"],"release":ctx["release"],
           "tables":{k:v["rows"] for k,v in tables.items()},"identity":ctx["identity"]})
    lease(ctx,"RELEASED")
