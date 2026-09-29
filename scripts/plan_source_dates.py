"""HEAD-only planner. Never downloads archive bodies or grants resource approval."""
import argparse,concurrent.futures,datetime as dt,json,pathlib,sys,urllib.request,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import dates,read,atomic
from warehouse.sources import cached
from acquisition.manifest import source_url
p=argparse.ArgumentParser();p.add_argument("--start-date",required=True);p.add_argument("--end-date",required=True);p.add_argument("--save-approved",action="store_true")
a=p.parse_args();selected=dates(a.start_date,a.end_date)
def head(pair):
    date,hour=pair;url=source_url(date,hour)
    try:
        req=urllib.request.Request(url,method="HEAD",headers={"User-Agent":"OpenSourcePulse/0.2 (offline warehouse planning)"})
        with urllib.request.urlopen(req,timeout=30) as r:
            size=int(r.headers.get("Content-Length",r.headers.get("x-goog-stored-content-length",0)))
            if r.geturl()!=url or size<=0:raise RuntimeError("unverified source size")
            return {"date":date,"hour":hour,"url":url,"bytes":size,"status":r.status}
    except Exception as e:return {"date":date,"hour":hour,"url":url,"error":str(e)}
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    rows=list(pool.map(head,[(d,h) for d in selected for h in range(24)]))
policy=read(ROOT/"config/m2-budget.json")
reserved=policy["prior_runtime_reservations"]+policy["prior_source_allowance"]+sum(json.loads(x)["reserved_bytes"] for x in (ROOT/"evidence/network-m2.jsonl").read_text().splitlines())
needed=sum(r.get("bytes",0) for r in rows if cached(r["date"],r["hour"]) is None)
known=all(r.get("bytes",0)>0 for r in rows)
approved=known and all(d in policy["allowed_dates"] for d in selected) and reserved+needed<=policy["network_limit_bytes"]
report={"captured_at":dt.datetime.now(dt.timezone.utc).isoformat(),"dates":selected,"hours":rows,"all_sizes_known":known,
        "additional_compressed_bytes":needed,"already_reserved_bytes":reserved,"network_limit_bytes":policy["network_limit_bytes"],
        "within_existing_approval":approved,"archive_bodies_downloaded":False}
atomic(ROOT/"evidence"/("source-planning-"+uuid.uuid4().hex+".json"),report)
if a.save_approved:
    if not approved:raise RuntimeError("incomplete plan or resource approval needed; configuration unchanged")
    for date in selected:
        items=[r for r in rows if r["date"]==date]
        atomic(ROOT/"config/source-plans"/(date+".json"),{"date":date,"hours":items,"all_sizes_known":True,
               "full_day_compressed_bytes":sum(r["bytes"] for r in items)})
print(json.dumps({k:v for k,v in report.items() if k!="hours"},indent=2))
