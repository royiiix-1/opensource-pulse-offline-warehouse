import hashlib,json,pathlib
from acquisition.download import acquire
from acquisition.manifest import source_url,require_hours,input_identity
from acquisition.verify import inspect_gzip
from warehouse.common import ROOT,CONTROL,read,atomic,reserve_network,source_plan
def cached(date,hour):
    source_url(date,hour);found=[]
    for path in (ROOT/"build/acquisition/objects").glob("*/manifest.json"):
        m=read(path)
        if m["source_date"]==date and m["source_hour"]==hour:found.append((path,m))
    if len(found)>1:raise RuntimeError("ambiguous source versions; explicit resolution required")
    return found[0] if found else None
def accept_hour(date,hour,download=False):
    source_url(date,hour)
    policy=read(ROOT/"config/m2-budget.json")
    if date not in policy["allowed_dates"]:raise RuntimeError("date has no download/storage budget approval")
    match=cached(date,hour)
    if match is None:
        if not download:raise FileNotFoundError(f"missing source hour {date}-{hour}")
        plan=source_plan(date)
        if plan["date"]!=date or not plan["all_sizes_known"]:raise RuntimeError("missing HEAD plan")
        size=next(r["bytes"] for r in plan["hours"] if r["hour"]==hour)
        from warehouse.operations import disk_usage
        if disk_usage()+size+(8<<30)>policy["disk_limit_bytes"]:
            raise RuntimeError("download would exhaust project storage headroom")
        reserve_network(size,f"gharchive:{date}:{hour}")
        result=acquire(ROOT/"build/acquisition",date,hour,compressed_limit=size)
        match=cached(date,hour)
    path,m=match
    stats=inspect_gzip(path.parent/"source.json.gz")
    if any(m[k]!=v for k,v in stats.items()) or input_identity(m)!=path.parent.name:raise RuntimeError("source checksum/identity mismatch")
    receipt={"manifest":m,"object_id":path.parent.name}
    atomic(CONTROL/"sources"/date/(str(hour)+".json"),receipt)
    return receipt
def receipts(date):
    return [read(p) for p in sorted((CONTROL/"sources"/date).glob("*.json"))]
def validate_day(date):
    rows=receipts(date)
    require_hours([r["manifest"] for r in rows],date)
    for r in rows:
        m=r["manifest"]
        obj=ROOT/"build/acquisition/objects"/r["object_id"]
        if not obj.resolve().is_relative_to((ROOT/"build/acquisition/objects").resolve()):raise ValueError("invalid object path")
        stats=inspect_gzip(obj/"source.json.gz")
        if any(m[k]!=v for k,v in stats.items()):raise RuntimeError("accepted source changed")
    return sorted(rows,key=lambda r:r["manifest"]["source_hour"])
