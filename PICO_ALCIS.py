#IMPORTANDO BIBLIOTECAS
import requests
import pandas as pd
import datetime as dt
import json
import cx_Oracle
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

def main():

    #CRIANDO FUNÇÃO PARA GERAR TOKEN NO CASO DE NÃO AUTORIZAÇÃO
    def token():
        url_token = "https://api.alcis.com.br:4434/api/v1/Token"
        payload_token = {}
        headers_token = {
        'Alias': 'LAREDO',
        'Username': 'API',
        'Password': 'AGX54AT*'
        }
        response_token = requests.request("POST", url_token, headers=headers_token, data=payload_token)
        return 'Bearer ' + ( response_token.text)
    token_valido = token()
    print(token_valido)

    #PUXANDO DADOS DOS DEPOSITANTES PELA API
    url = "https://api.alcis.com.br:4434/api/v1/ProprietarioEstoqueBILaredo"

    payload={}
    headers = {'Authorization': token_valido
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.status_code)
    if response.status_code == 200:
        dep = response.json()
        #print(dep)
    else:
        print("Error: Failed to retrieve data from API.")
        
    dfdep = pd.DataFrame(dep)
    depositantes = dfdep['codigoProprietario']
    q_depositantes = len(depositantes.index)

    #PUXANDO DADOS DO ESTOQUE PELA API
    estoque = []
    for i in depositantes.index:
        url_estoque = "https://api.alcis.com.br:4434/api/v1/EstoqueBILaredo?proprietario={}&site=001".format(depositantes[i])
        payload={}
        headers = {
        'Authorization': token_valido
        }

        response = requests.request("GET", url_estoque, headers=headers, data=payload)

        if (response.status_code  == 401):
            token_valido = token()
            url_estoque = "https://api.alcis.com.br:4434/api/v1/EstoqueBILaredo?proprietario={}&site=001".format(depositantes[i])
            payload={}
            headers = {
            'Authorization': str(token_valido)
            }
            response = requests.request("GET", url_estoque, headers=headers, data=payload)
        estoque.extend(response.json())


    #CRIANDO DATA FRAME DOS DADOS DE ESTOQUE
    dfestoque = pd.DataFrame(estoque)
    dfestoque.rename(columns={'site': 'Site_alcis','codigoProduto':'CODPROD','codigoProprietario':'CODEPOSITANTE'}, inplace=True)
    dfestoque.columns = dfestoque.columns.str.upper()

    #CRIANDO COLUNA DATAESTOQUE COM A DATA DE HOJE

    num_linhas = len(dfestoque)

    data_referencia = dt.datetime(2023, 1, 1)
    data_atual = dt.datetime.now()
    diferenca = data_atual - data_referencia
    data_str = (data_referencia + diferenca).strftime("%Y-%m-%dT%H:%M:%S")

    data = dt.datetime.strptime(data_str, "%Y-%m-%dT%H:%M:%S")
    data = data.replace(minute=30, second=0, microsecond=0)

    dfestoque['DATAREGISTRO'] = [data] * num_linhas
    dfestoque['HORA'] = dfestoque['DATAREGISTRO'].dt.hour

    dfestoque = dfestoque[['DATAREGISTRO', 'DATAVALIDADE', 'CODEPOSITANTE', 'QUANTIDADEDISPONIVEL', 'QUANTIDADERESERVADA', 'PESODISPONIVEL', 
                            'PESORESERVADO', 'SITE_ALCIS', 'CODPROD', 'DEPOSITO', 'IDESTOQUE', 'AREA', 'ENDERECO', 'NUMEROUZ', 'LOTE', 'HORA']]

    #FAZENDO A CONEXÃO COM O BANCO

    dsn = cx_Oracle.makedsn('10.10.0.60', '1521', 'ORCL')

    conn = cx_Oracle.connect('laredo', 'laredo', '10.10.0.60:1521/ORCL')

    cur = conn.cursor()

    #INSERINDO OS DADOS DO DF AO BANCO

    for index, row in dfestoque.iterrows():
        sql = "INSERT INTO ALCIS.PICO_ESTOQUE (DATAVALIDADE,DATAREGISTRO,CODEPOSITANTE,QUANTIDADEDISPONIVEL,QUANTIDADERESERVADA,PESODISPONIVEL,PESORESERVADO,SITE_ALCIS,CODPROD,DEPOSITO,IDESTOQUE,AREA,ENDERECO,NUMEROUZ,LOTE,HORA) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)"

        data_validade = None
        if row.DATAVALIDADE is not None:
            try:
                data_validade = datetime.strptime(row.DATAVALIDADE, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                print(f"Erro de formato de data para o registro {index}: {row.DATAVALIDADE}")
                continue

        values = (
            data_validade, 
            row.DATAREGISTRO,
            row.CODEPOSITANTE,
            row.QUANTIDADEDISPONIVEL if not np.isnan(row.QUANTIDADEDISPONIVEL) else None,
            row.QUANTIDADERESERVADA if not np.isnan(row.QUANTIDADERESERVADA) else None,
            row.PESODISPONIVEL if not np.isnan(row.PESODISPONIVEL) else None,
            row.PESORESERVADO if not np.isnan(row.PESORESERVADO) else None,
            row.SITE_ALCIS, 
            row.CODPROD, 
            row.DEPOSITO, 
            row.IDESTOQUE, 
            row.AREA,
            row.ENDERECO, 
            row.NUMEROUZ, 
            row.LOTE,
            row.HORA
        )

        cur.execute(sql, values)

    conn.commit()
    cur.close()

if __name__ == "__main__":
        main()

###########################################

local_tz = pendulum.timezone('America/Fortaleza')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 23, 11, 30, 0, tzinfo=local_tz),  
    'retries': 0, 
    'retry_delay': timedelta(minutes=5),  
}


with DAG(
    'PICO_ESTOQUE',
    default_args=default_args,
    description='PICO',
    schedule='30 * * * *', 
    catchup=False,
    max_active_runs=1,
    tags=['alcis']
) as dag: 

    api = PythonOperator(
        task_id='ingestão_dados',
        python_callable=main,
        dag=dag,
    )
  
api
    

    

    
    



