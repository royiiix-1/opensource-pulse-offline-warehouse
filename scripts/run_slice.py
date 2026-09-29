"""Single-host publication coordinator: flock + immutable HDFS + atomic active pointer."""
import argparse, datetime as dt, fcntl, hashlib, json, os, pathlib, subprocess, time, uuid
R=pathlib.Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0,str(R))
from acquisition.verify import inspect_gzip
from acquisition.manifest import source_url
p=argparse.ArgumentParser()
p.add_argument("--date",required=True); p.add_argument("--hour",type=int,required=True)
p.add_argument("--force",action="store_true"); p.add_argument("--fail-after",choices=["ods","dwd"])
a=p.parse_args()
source_url(a.date,a.hour)
control=R/"build/control"; control.mkdir(parents=True,exist_ok=True)
key=a.date+"-"+str(a.hour)
lock=(control/(key+".lock")).open("a")
try:
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError:
    conflict=R/"evidence"/("lock-conflict-"+uuid.uuid4().hex+".json")
    conflict.write_text(json.dumps({"status":"FAILED","reason":"scope_locked","scope":key}))
    raise
pointer=control/(key+".json")
before=json.loads(pointer.read_text()) if pointer.exists() else None
release="r"+dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")+uuid.uuid4().hex[:8]
evidence=R/"evidence"/("slice-"+release); evidence.mkdir()
result={"release":release,"scope":key,"coverage":"PARTIAL","active_before":before["release"] if before else None,"started_at":time.time()}
def cmd(args,log=None,ok=(0,)):
    with (evidence/(log or ("command-"+uuid.uuid4().hex+".log"))).open("wb") as output:
        proc=subprocess.run(args,stdout=output,stderr=subprocess.STDOUT,cwd=R)
    if proc.returncode not in ok: raise RuntimeError("command failed: "+args[0]+" exit="+str(proc.returncode))
    return proc.returncode
def dfs(*args,ok=(0,)): return cmd(["hdfs","dfs",*args],ok=ok)
def text(*args):
    return subprocess.check_output(args,cwd=R,text=True)
def hdfs_sha(path):
    proc=subprocess.Popen(["hdfs","dfs","-cat",path],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    h=hashlib.sha256()
    for block in iter(lambda:proc.stdout.read(1<<20),b""): h.update(block)
    if proc.wait()!=0: raise RuntimeError("HDFS read failed: "+path)
    return h.hexdigest()
def inventory(base):
    rows=text("hdfs","dfs","-ls","-R",base).splitlines()
    found={}
    for row in rows:
        parts=row.split()
        if parts and parts[0].startswith("-"):
            path=parts[-1]
            found[path]={"bytes":int(parts[4]),"sha256":hdfs_sha(path)}
    if not found: raise RuntimeError("empty release inventory")
    return found
try:
    manifests=[]
    for f in (R/"build/acquisition/objects").glob("*/manifest.json"):
        m=json.loads(f.read_text())
        if m["source_date"]==a.date and m["source_hour"]==a.hour: manifests.append((f,m))
    if len(manifests)!=1: raise RuntimeError("require exactly one accepted source version per hour")
    file,m=manifests[0]
    raw=file.parent/"source.json.gz"
    stats=inspect_gzip(raw)
    if any(stats[k]!=m[k] for k in stats): raise RuntimeError("input manifest mismatch")
    h=hashlib.sha256()
    for folder in ["spark","hive","config"]:
        for path in sorted((R/folder).rglob("*")):
            if path.is_file() and path.suffix in [".py",".sql",".json",".xml",".conf"]:
                h.update(str(path.relative_to(R)).encode()); h.update(path.read_bytes())
    h.update((R/"scripts/run_slice.py").read_bytes())
    identity=hashlib.sha256((m["input_identity"]+h.hexdigest()).encode()).hexdigest()
    result.update(input_identity=m["input_identity"],transform_digest=h.hexdigest(),identity=identity)
    if before and before["identity"]==identity and not a.force and not a.fail_after:
        check=inventory(before["hdfs_path"])
        if check!=before["inventory"]: raise RuntimeError("published release mutated; refusing no-op")
        cmd(["spark-sql","--master","yarn","-e","SHOW TABLES IN "+before["database"]],"noop-catalog.log")
        catalog=(evidence/"noop-catalog.log").read_text()
        if not all(name in catalog for name in before["tables"]): raise RuntimeError("published catalog incomplete")
        result.update(status="NO_OP",active_after=before["release"])
    else:
        raw_path=f"/osp-offline/raw/source_date={a.date}/source_hour={a.hour:02d}/sha256={m['sha256']}"
        dfs("-mkdir","-p","/osp-offline/spark-events","/osp-offline/yarn-logs","/osp-offline/staging","/osp-offline/releases",raw_path)
        target=raw_path+"/source.json.gz"
        if dfs("-test","-e",target,ok=(0,1))==1:
            temporary=raw_path+"/upload-"+release
            dfs("-put",str(raw),temporary)
            if hdfs_sha(temporary)!=m["sha256"]: raise RuntimeError("HDFS upload checksum mismatch")
            dfs("-mv",temporary,target); dfs("-chmod","444",target)
        if hdfs_sha(target)!=m["sha256"]: raise RuntimeError("immutable raw checksum mismatch")
        arguments=["spark-submit","--master","yarn","--deploy-mode","client",str(R/"spark/vertical_slice.py"),
                   "--input","hdfs://127.0.0.1:19000"+target,"--date",a.date,"--hour",str(a.hour),
                   "--release",release,"--sha",m["sha256"],"--report",str(evidence),"--expected-lines",str(m["line_count"])]
        if a.fail_after: arguments+=["--fail-after",a.fail_after]
        cmd(arguments,"spark-submit.log")
        quality=json.loads((evidence/"quality.json").read_text())
        if quality["status"]!="VALIDATED" or not all(c["passed"] for c in quality["checks"].values()):
            raise RuntimeError("quality gate not passed")
        staging="/osp-offline/staging/"+release
        published="/osp-offline/releases/"+release
        dfs("-mv",staging,published)
        sql=evidence/"register-release.sql"
        sql.write_text("\n".join(f"ALTER TABLE {quality['database']}.{name} SET LOCATION 'hdfs://127.0.0.1:19000{published}/{name}';" for name in quality["tables"]))
        cmd(["spark-sql","--master","yarn","-f",str(sql)],"register-release.log")
        inv=inventory(published)
        after=dict(release=release,identity=identity,database=quality["database"],hdfs_path=published,
                   tables=list(quality["tables"]),coverage="PARTIAL",date=a.date,hours=[a.hour],inventory=inv,application_id=quality["application_id"])
        temp=control/(key+"-"+release+".tmp")
        with temp.open("x") as f:
            json.dump(after,f,indent=2); f.flush(); os.fsync(f.fileno())
        os.replace(temp,pointer)
        result.update(status="PUBLISHED_PARTIAL",active_after=release,application_id=quality["application_id"],
                      tables={k:v["rows"] for k,v in quality["tables"].items()})
except Exception as error:
    after=json.loads(pointer.read_text()) if pointer.exists() else None
    result.update(status="FAILED",error=str(error),active_after=after["release"] if after else None,
                  prior_release_preserved=after==before)
    raise
finally:
    result["ended_at"]=time.time()
    (evidence/"publication.json").write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2),flush=True)
