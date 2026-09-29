import argparse,json,pathlib,re,subprocess,sys
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from acquisition.manifest import source_url
p=argparse.ArgumentParser();p.add_argument("--date",required=True);a=p.parse_args();source_url(a.date,0)
active=json.loads((ROOT/"build/control-v2/published"/(a.date+".json")).read_text())
if active["coverage"]!="COMPLETE" or active["hours"]!=list(range(24)):raise RuntimeError("not a complete publication")
db=active["database"]
if not re.fullmatch(r"osp2_d[0-9a-f]+",db):raise ValueError("invalid database")
print(json.dumps({"date":a.date,"coverage":"COMPLETE","release":active["release"],"database":db}),flush=True)
sql=f"""SHOW PARTITIONS {db}.ods_github_event_raw;
SELECT event_type,count(*) FROM {db}.dwd_event WHERE event_date=DATE '{a.date}' GROUP BY event_type ORDER BY event_type;
SELECT count(*) repositories,sum(event_count) events,sum(push_events) pushes,sum(pr_opened),sum(pr_closed),sum(pr_merged),sum(issue_opened),sum(issue_closed)
FROM {db}.dws_repository_daily WHERE event_date=DATE '{a.date}';"""
raise SystemExit(subprocess.run(["spark-sql","--master","yarn","-e",sql]).returncode)
