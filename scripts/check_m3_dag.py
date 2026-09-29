import json,pathlib
from airflow.models import DagBag
root=pathlib.Path(__file__).resolve().parents[1]
bag=DagBag(str(root/'airflow/dags'),include_examples=False)
name='opensource_pulse_model_snapshot'
r={'passed':not bag.import_errors and name in bag.dags,'import_errors':bag.import_errors,
   'model_tasks':list(bag.dags[name].task_ids) if name in bag.dags else []}
(root/'evidence/m3-dag-parse.json').write_text(json.dumps(r,indent=2,default=str))
print(json.dumps(r,indent=2,default=str));raise SystemExit(0 if r['passed'] else 1)
