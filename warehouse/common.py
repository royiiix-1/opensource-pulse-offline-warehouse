import datetime as dt,fcntl,hashlib,json,os,pathlib,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1]
CONTROL=ROOT/"build/control-v2"
def read(path):return json.loads(pathlib.Path(path).read_text())
def atomic(path,value):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+"-"+uuid.uuid4().hex+".tmp")
    with tmp.open("x") as f:json.dump(value,f,indent=2);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)
def dates(start,end):
    from acquisition.manifest import source_url
    source_url(start,0);source_url(end,0)
    a,b=dt.date.fromisoformat(start),dt.date.fromisoformat(end)
    if a>b or (b-a).days>6:raise ValueError("range must contain 1..7 ascending dates")
    return [(a+dt.timedelta(days=i)).isoformat() for i in range((b-a).days+1)]
def reserve_network(size,label):
    policy=read(ROOT/"config/m2-budget.json")
    ledger=ROOT/"evidence/network-m2.jsonl"
    with ledger.open("a+") as f:
        fcntl.flock(f,fcntl.LOCK_EX);f.seek(0)
        used=policy["prior_runtime_reservations"]+policy["prior_source_allowance"]+sum(json.loads(x)["reserved_bytes"] for x in f if x.strip())
        if used+size>policy["network_limit_bytes"]:raise RuntimeError("network budget exceeded; approval required")
        f.write(json.dumps({"category":label,"reserved_bytes":size,"at":dt.datetime.now(dt.timezone.utc).isoformat()})+"\n")
        f.flush();os.fsync(f.fileno())
def model_digest():
    h=hashlib.sha256()
    files=[ROOT/"spark/day_stage.py",ROOT/"hive/quality/m2_reconcile.sql",ROOT/"config/runtime-lock.json",ROOT/"config/native-cache.json"]
    files+=sorted((ROOT/"warehouse").glob("*.py"))
    for p in files:h.update(str(p.relative_to(ROOT)).encode());h.update(p.read_bytes())
    return h.hexdigest()

def source_plan(date):
    from acquisition.manifest import source_url
    source_url(date,0)
    path=ROOT/"config/source-plans"/(date+".json")
    plan=read(path)
    if plan["date"]!=date or not plan["all_sizes_known"]:raise RuntimeError("invalid source size plan")
    return plan
