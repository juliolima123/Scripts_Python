from datetime import timedelta, datetime
from airflow import DAG
from airflow_hop.operators import HopWorkflowOperator
import pendulum
from airflow.operators.python import PythonOperator

local_tz = pendulum.timezone('America/Fortaleza')

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 12, tzinfo=local_tz),
    'email': ['yurisantana@grupolaredo.com.br'],
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='FS_EVENTOS',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    catchup=False,
    schedule='*/30 * * * *',
    max_active_runs=1
) as dag:

    eventos = HopWorkflowOperator(
        dag=dag,
        task_id='eventos',
        workflow='/fusion/workflows/WORK_EVENTOS_ROMANEIO.hwf',
        log_level='Basic',
        project_name='default',
    )

eventos




































