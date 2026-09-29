"""Read a fixed, validated active release; never enumerate staging databases."""
import argparse,json,pathlib,re,subprocess,sys
R=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(R))
from acquisition.manifest import source_url
p=argparse.ArgumentParser();p.add_argument("--date",required=True);p.add_argument("--hour",type=int,required=True)
a=p.parse_args();source_url(a.date,a.hour)
active=json.loads((R/"build/control"/(a.date+"-"+str(a.hour)+".json")).read_text())
db=active["database"]
if not re.fullmatch(r"osp_r[0-9a-f]+",db): raise ValueError("invalid catalog identifier")
print(json.dumps({"release":active["release"],"coverage":active["coverage"],"database":db}),flush=True)
sql=f"""SELECT event_type,count(*) event_count FROM {db}.dwd_event GROUP BY event_type ORDER BY event_type;
SELECT count(*) repositories,sum(event_count) events,sum(push_events) pushes,
sum(pr_opened) pr_opened,sum(pr_closed) pr_closed,sum(pr_merged) pr_merged,
sum(issue_opened) issue_opened,sum(issue_closed) issue_closed
FROM {db}.dws_repository_daily;"""
raise SystemExit(subprocess.run(["spark-sql","--master","yarn","-e",sql]).returncode)
