from sqlalchemy import create_engine, text
import pandas as pd
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

def main():

    load_dotenv()

    def accessMinio():
        s3 = boto3.client('s3',
                        endpoint_url=os.getenv('HOST_MINIO', None),
                        aws_access_key_id=os.getenv("AWS_ACESS_KEY", None),
                        aws_secret_access_key=os.getenv("AWS_SECRET_KEY", None),
                        config=Config(signature_version='s3v4')
                        )
        try:
            s3.list_buckets()
            return s3
        except Exception as e:
            print(f"Erro ao acessar MinIO: {e}")
            raise SystemExit
        
    def list_object_bucket(s3, bucket, path):
        response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
        list = []
        for obj in response['Contents']:
            list.append(obj['Key'])
        return list

    def create_filter_df(list, prefixo):
        df = pd.DataFrame(list, columns=['diretorio'])
        df[['pasta', 'setor', 'tipo', 'arquivo']] = df['diretorio'].str.split('/', expand=True)
        df['link'] = prefixo + df['diretorio']
        df.drop(columns=['diretorio', 'pasta'], inplace=True)
        df = df.fillna('N/A')
        df_filtrado = df[(df['setor'].str.contains('Thumbs.db|N/A')==False) & (df['tipo'].str.contains('Thumbs.db|N/A')==False) & (df['arquivo'].str.contains('Thumbs.db|N/A')==False)]
        return df_filtrado

    def create_engine_db():
        host = os.getenv('HOST_POSTGRES', None)
        port = os.getenv('PORT_POSTGRES', None)
        dbname = os.getenv('DBNAME_POSTGRES', None)
        user = os.getenv('USER_POSTGRES', None)
        password = os.getenv('PASSWORD_POSTGRES', None)
        conn_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(user, password, host, port, dbname)
        engine = create_engine(conn_string)

        return engine

    def load_db(df, table_name, schema, engine):
        try:
            df.to_sql(name = table_name, schema = schema, con = engine, if_exists='replace', index=False)
            print(f'Dados escritos no BD com sucesso!')
            grant_table_db(engine, schema, 'readaccess')
            return 0
        except Exception as err:
            print(f'Falha ao escrever dados no BD: {err}')
            return 1

    def remove_Thumbs(df):
        return df[(df['tipo'].str.contains('Thumbs.db')==False) & (df['arquivo'].str.contains('Thumbs.db|N/A')==False)]

    def grant_table_db(engine, schema, group):
        with engine.begin() as conn:
            conn.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO GROUP {group}  WITH GRANT OPTION'))
        return print('Grant realizado')

    bucket = 'processos'
    path = 'PROCEDIMENTOS_SGP'
    prefixo_link = f'https://s3.grupolaredo.online/{bucket}/'
    s3 = accessMinio()
    lista_arquivos = list_object_bucket(s3, bucket, path)
    df = create_filter_df(lista_arquivos, prefixo_link)
    engine = create_engine_db()
    load_db(df, 'documentos', 'processos', engine)
    
if __name__ == "__main__":
    main()
    
###################################################

local_tz = pendulum.timezone('America/Fortaleza')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 11, tzinfo=local_tz),  
    'retries': 0, 
    'retry_delay': timedelta(minutes=5),  
}

with DAG(
    'LISTAGEM_DOCUMENTOS',
    default_args=default_args,
    description='Listagem de documentos presentes no bucket - processos',
    schedule='25 8,12 * * *', 
    catchup=False,
    max_active_runs=1,
    tags=['processos']
) as dag: 

    main = PythonOperator(
        task_id='main',
        python_callable=main,
        dag=dag,
    )

    main 