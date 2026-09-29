"""UTC daily ingest, repair, and bounded date-range backfill. Local portfolio deployment."""
import datetime as dt,hashlib,pathlib,sys
from airflow import DAG
from airflow.operators.python import PythonOperator
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from warehouse import operations as ops
from warehouse.common import read,atomic
def prepare_run(**context):
    return ops.prepare(dict(context["dag_run"].conf or {}),context["data_interval_start"],context["dag_run"].run_id)
def execute(action,stage=None,**context):
    paths=context["ti"].xcom_pull(task_ids="prepare_run")
    for path in paths:
        if stage:ops.spark_stage(path,stage)
        else:getattr(ops,action)(path)
def audit(**context):
    run=context["dag_run"]
    states={t.task_id:t.state for t in run.get_task_instances() if t.task_id!="audit_run"}
    failed=any(s in ("failed","upstream_failed") for s in states.values())
    paths=context["ti"].xcom_pull(task_ids="prepare_run") or []
    for path in paths:
        if pathlib.Path(path).exists():ops.lease(read(path),"FAILED" if failed else "RELEASED")
    key=hashlib.sha256(run.run_id.encode()).hexdigest()[:20]
    atomic(ROOT/"evidence/m2/dag-runs"/(key+".json"),{"run_id":run.run_id,"states":states,"passed":not failed,"contexts":paths})
    if failed:raise RuntimeError("upstream failed; audit must not mask DAG failure")
def on_retry(context):
    ti=context["ti"];key=hashlib.sha256(context["dag_run"].run_id.encode()).hexdigest()[:20]
    atomic(ROOT/"evidence/m2/retries"/(key+"-"+ti.task_id+"-"+str(ti.try_number)+".json"),
           {"run_id":context["dag_run"].run_id,"task_id":ti.task_id,"try_number":ti.try_number,"event":"UP_FOR_RETRY"})
with DAG("opensource_pulse_daily",start_date=dt.datetime(2024,1,1,tzinfo=dt.timezone.utc),
         schedule="@daily",catchup=False,max_active_runs=1,max_active_tasks=1,
         is_paused_upon_creation=True,dagrun_timeout=dt.timedelta(hours=3),
         default_args={"retries":1,"retry_delay":dt.timedelta(seconds=15),
                       "execution_timeout":dt.timedelta(minutes=50),"on_retry_callback":on_retry},
         tags=["offline","github-archive","M2"]) as dag:
    first=PythonOperator(task_id="prepare_run",python_callable=prepare_run)
    prev=first
    specs=[("check_source_hours","check_sources",None),("acquire_hourly_files","acquire_sources",None),
           ("validate_manifest","validate_manifest",None),("publish_raw_to_hdfs","publish_raw",None),
           ("build_ods_partition",None,"ods"),("build_dwd_partition",None,"dwd"),
           ("run_dwd_quality_gate","dwd_gate",None),("build_dws_partition",None,"dws"),
           ("build_ads_partition",None,"ads"),("reconcile_layers",None,"quality"),
           ("reconcile_and_publish","publish_day",None)]
    for name,action,stage in specs:
        task=PythonOperator(task_id=name,python_callable=execute,op_kwargs={"action":action,"stage":stage})
        prev>>task;prev=task
    last=PythonOperator(task_id="audit_run",python_callable=audit,trigger_rule="all_done",retries=0)
    prev>>last
