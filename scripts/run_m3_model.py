"""Trigger the real model DAG and wait; preserve every run and pause on exit."""
import json,pathlib,subprocess,time,uuid
from airflow.models import DagRun
from airflow.utils.session import create_session
ROOT=pathlib.Path(__file__).resolve().parents[1]
out=ROOT/'evidence'/('m3-model-dag-'+uuid.uuid4().hex);out.mkdir()
name='opensource_pulse_model_snapshot';run_id='m3_'+uuid.uuid4().hex
result={'dag_id':name,'run_id':run_id,'status':'RUNNING'}
try:
    subprocess.run(['airflow','dags','unpause',name],check=True)
    subprocess.run(['airflow','dags','trigger',name,'--run-id',run_id,'--conf',json.dumps({'start_date':'2024-01-01','end_date':'2024-01-07'})],check=True)
    deadline=time.time()+10800
    while time.time()<deadline:
        with create_session() as s:
            row=s.query(DagRun).filter_by(dag_id=name,run_id=run_id).first()
            state=row.state if row else 'not_created'
            if row:result['tasks']={t.task_id:t.state for t in row.get_task_instances()}
        if state in ('success','failed'):break
        time.sleep(10)
    result['status']=state
finally:
    subprocess.run(['airflow','dags','pause',name])
    (out/'result.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
raise SystemExit(0 if result['status']=='success' else 1)
