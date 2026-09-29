import json,pathlib,sys
R=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(R))
from acquisition.download import acquire
from acquisition.manifest import source_url
date,hour=sys.argv[1],int(sys.argv[2]); source_url(date,hour)
matches=[]
for p in (R/"build/acquisition/objects").glob("*/manifest.json"):
    m=json.loads(p.read_text())
    if m["source_date"]==date and m["source_hour"]==hour: matches.append(p)
if not matches:
    print(json.dumps(acquire(R/"build/acquisition",date,hour),indent=2))
elif len(matches)!=1:
    raise RuntimeError("Multiple source versions require explicit resolution")
