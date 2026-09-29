import hashlib,json,pathlib,subprocess,sys,time,uuid
from airflow.models import DagRun
from airflow.utils.session import create_session
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.operations import request_dates
import datetime as dt
request=json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8-sig"))
request_dates(request,dt.datetime.now(dt.timezone.utc)-dt.timedelta(days=1))
if request.get("start_date") and "acquisition_policy" not in request:request["acquisition_policy"]="cache_only"
run_id="manual_"+uuid.uuid4().hex
subprocess.run(["airflow","dags","unpause","opensource_pulse_daily"],check=True)
subprocess.run(["airflow","dags","trigger","opensource_pulse_daily","--run-id",run_id,"--conf",json.dumps(request)],check=True)
print("DAG_RUN",run_id,flush=True)
deadline=time.time()+10800;state=None
try:
    while time.time()<deadline:
        with create_session() as s:
            row=s.query(DagRun).filter_by(dag_id="opensource_pulse_daily",run_id=run_id).first()
            now=row.state if row else "not_created"
        if now!=state:print(now,flush=True);state=now
        if state in ("success","failed"):break
        time.sleep(5)
    if state!="success":raise RuntimeError("DAG did not succeed: "+str(state))
finally:
    subprocess.run(["airflow","dags","pause","opensource_pulse_daily"])
