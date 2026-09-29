"""Manual model rebuild after complete daily publication; reruns are identity based."""
import datetime as dt,pathlib,sys
from airflow import DAG
from airflow.operators.python import PythonOperator
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
def snapshot(**context):
    from m3.control import run
    conf=context['dag_run'].conf or {}
    result=run(conf.get('start_date','2024-01-01'),conf.get('end_date','2024-01-07'))
    return {'release':result['release'],'identity':result['identity']}
with DAG('opensource_pulse_model_snapshot',start_date=dt.datetime(2024,1,1,tzinfo=dt.timezone.utc),
         schedule=None,catchup=False,max_active_runs=1,max_active_tasks=1,is_paused_upon_creation=True,
         dagrun_timeout=dt.timedelta(hours=3),tags=['offline','M3'],
         default_args={'retries':1,'retry_delay':dt.timedelta(seconds=30),'execution_timeout':dt.timedelta(hours=2)}) as dag:
    PythonOperator(task_id='build_validate_publish_model',python_callable=snapshot)
