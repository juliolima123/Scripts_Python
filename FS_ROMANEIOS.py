from datetime import timedelta, datetime
from airflow import DAG
from airflow_hop.operators import HopWorkflowOperator
import pendulum
from datetime import datetime
# from airflow.operators.python import PythonOperator
# import time

hora = datetime.now().hour
dias = '30' if hora == 22 else '5'


local_tz = pendulum.timezone('America/Fortaleza')

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 30, 9, 30, tzinfo=local_tz),
    'email': ['yurisantana@grupolaredo.com.br'],
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='FS_ROMANEIOS',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    catchup=False,
    max_active_runs=1,
    schedule='*/40 * * * *'
) as dag:

    romaneios = HopWorkflowOperator(
        dag=dag,
        task_id='romaneios',
        workflow='/fusion/workflows/FS_ROMANEIOS.hwf',
        log_level='Basic',
        project_name='default',
        params={
            'dias':dias
        }
    )

romaneios





































