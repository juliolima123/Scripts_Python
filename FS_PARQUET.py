import os
import boto3
from botocore.client import Config
from datetime import timedelta, datetime
from airflow import DAG
from airflow_hop.operators import HopWorkflowOperator
import pendulum
from airflow.operators.python import PythonOperator
import shutil
import time

def excluir():
    folder = '/mnt/biprivado/12 - Diversos/hop/FS_PARQUET'
    for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Exclusão falha %s. Motivo: %s' % (file_path, e))

def up_minio(arquivo, **kwargs):

    data = datetime.now()
    i = data.strftime("%Y-%m-%d:%H")
    arquivo = ['detalhe_entrega', 'lista_cargas', 'fsentrega_rmn', 'fsromaneio_detalhado']

    s3 = boto3.resource('s3',
                        endpoint_url='https://s3.grupolaredo.online',
                        aws_access_key_id='ubPXa8xMxlDTreOUju1x',
                        aws_secret_access_key='6IiurWNmkk6DPCnSjbGGVqEtItjsiF0G5VpspVOM',
                        config=Config(signature_version='s3v4')
                        )
    for x in arquivo:
        s3.Bucket('yuri').upload_file(f'/mnt/biprivado/12 - Diversos/hop/FS_PARQUET/{x}.parquet',f'fs_parquet/{x}_{i}.parquet')

#################################################

local_tz = pendulum.timezone('America/Fortaleza')

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 12, 12, tzinfo=local_tz),
    'email': ['yurisantana@grupolaredo.com.br'],
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='MINIO_FSROMANEIOS',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    catchup=False,
    schedule='@hourly',
    max_active_runs=1
) as dag:

    romaneios_pqt = HopWorkflowOperator(
        dag=dag,
        task_id='romaneios',
        workflow='/fusion/workflows/FS_ROMANEIOS_PQT.hwf',
        log_level='Basic',
        project_name='default'
    )

    romaneios_pqt





































