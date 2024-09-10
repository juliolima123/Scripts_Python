import datetime as dt
import requests
import json
import pandas as pd
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import cycle
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

# CONFIGURANDO EMAIL DE LOG

def email_falha(log_erro):
    msg = MIMEMultipart()
    msg['From'] = 'juliocesarrilima17@gmail.com'
    emails = ['julio.lima@grupolaredo.com.br', 'yurisantana@grupolaredo.com.br']
    msg['To'] = ', '.join(emails)
    msg['Subject'] = 'Erro ao importar os pré-pedidos ao Máxima'
    message = 'Bom dia a todos, tudo bem?\n\n'
    message += 'Ocorreu um erro ao exportar a base ao sistema do Máxima,\n\n'
    message += 'Log de erro:\n\n' + log_erro
    
    msg.attach(MIMEText(message))

    mailserver = smtplib.SMTP('smtp.gmail.com', 587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.login('juliocesarrilima17@gmail.com', 'yuugqcwnzgklilbk')
    mailserver.sendmail('juliocesarrilima17@gmail.com', emails, msg.as_string())
    mailserver.quit()

# EXCLUSÃO DE PRÉ-PEDIDOS VENCIDOS

def exclusao():

    #ADICIONANDO COLUNAS FIXAS

    data = [1] * 20000
    valores = list(range(1, 20500))
    iterador = cycle(valores)

    codigos = [next(iterador) for _ in range(len(data))]

    df_base = pd.DataFrame({'N_Codigo': codigos})

    # #INFORMANDO O INTERVALO DOS PRE PEDIDOS QUE VÃO SER EXCLUÍDOS

    data_referencia = dt.datetime(2023, 1, 1)
    data_atual = dt.datetime.now()
    numeros_de_dias = []

    for i in range(1, 5): 
        data = data_atual - dt.timedelta(days=i)
        numero_dia = (data - data_referencia).days 
        numeros_de_dias.append(numero_dia)

    df_dia = pd.DataFrame([{'N_Dia': numero} for numero in numeros_de_dias])

    #Concatenação de Valores e transformar em JSON

    df_base['join_key'] = 1
    df_dia['join_key'] = 1

    df_resultado = df_dia.merge(df_base, on='join_key', how='outer').drop('join_key', axis=1)

    df_resultado['codigo'] = df_resultado['N_Dia'].astype(str) + df_resultado['N_Codigo'].astype(str)
    df_resultado['codigo'] = df_resultado['codigo'].astype(int)
    df_resultado = df_resultado.drop(columns=['N_Dia', 'N_Codigo'])

    js_resultado = df_resultado.to_json(orient='records')
    
    #GERANDO TOKEN PARA OS ENDPOINTS

    def token():

        url_token = "https://intext-hmg.solucoesmaxima.com.br:81/api/v3/Login"

        payload = json.dumps({
        "login": "WV8QKSJFPRAC2Wh/jkkKIMi5MpOJ7A89PoXL412anX4=",
        "password": "XC9D2SWJnGArIQ/iLhUE/UwtprTApXfQWDyNkTCyJRU="
        })
        headers = {
        'Content-Type': 'application/json'
        }

        response_token = requests.request("POST", url_token, headers=headers, data=payload)
        
        data = json.loads(response_token.content)

        return 'Bearer ' + ( data['token_De_Acesso'])



    def endpoint_prepedido_delete (body):
        url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidos/Excluir"
        headers = {
                'Content-Type': 'application/json',
                'Authorization': str(token())
        }
        
        response = requests.request("DELETE", url, headers=headers, data=body)
        
        return response.status_code

    resposta_itens = endpoint_prepedido_delete(js_resultado)
    print('resposta solicitação delete:' + str(resposta_itens))

# IMPORTANDO NOVOS PRÉ-PEDIDOS

def base():
    try:
       pasta = '/mnt/biprivado/12 - Diversos/sugestao_vendas'

       arquivos = [(os.path.join(pasta, arquivo), os.path.getctime(os.path.join(pasta, arquivo))) for arquivo in os.listdir(pasta) if arquivo.endswith('.xlsx')]
       arquivos.sort(key=lambda x: x[1], reverse=True)

       df_base = None
       df = None

       if len(arquivos) > 0:
              caminho_completo, _ = arquivos[0] 
              df_base = pd.read_excel(caminho_completo, sheet_name='Planilha1')
              df = pd.read_excel(caminho_completo, sheet_name='Planilha1')

       if df_base is not None:
              pass

       df_base = df_base[['FILIAL', 'CODSUPERVISOR','Codigo', 'Data Início', 'Data Final']]\
              .sort_values(by='Codigo')\
              .drop_duplicates(subset=['Codigo', 'Data Início', 'Data Final', 'CODSUPERVISOR', 'FILIAL'])

       df = pd.read_excel(caminho_completo, sheet_name='Planilha1')
              
       if df is not None:
              pass

       num_linhas = len(df_base)

       df = pd.read_excel(caminho_completo, sheet_name='Planilha1')

       #LER DATA DE HOJE E TRAZER NÚMERO CORRESPONDENTE

       data_referencia = dt.datetime(2023,1,1)
       data_atual = dt.datetime.now()
       dia = (data_atual - data_referencia).days + 1

       #CRIAR E ADICIONAR COLUNAS COM VARIÁVEIS FIXAS E O ID(DATA)

       vMesDia = [dia] * num_linhas
       vcor = ['#0e3cde'] * num_linhas
       vordenaritens = [''] * num_linhas
       vordenaritenspopup = ['S'] * num_linhas
       vsequenciaSC = list(range(1, num_linhas + 1))

       df_base['MesDia'] = vMesDia
       df_base['cor'] = vcor
       df_base['ordenaritens'] = vordenaritens
       df_base['ordenaritenspopup'] = vordenaritenspopup
       df_base['sequenciaSC'] = vsequenciaSC
       df_base['descricao'] = 'Pre-pedido via API com cor - Cliente ' + df_base['Codigo'].astype(str)

       df_base['ID'] = df_base['MesDia'].astype(str) + df_base['sequenciaSC'].astype(str)

       #RENOMEAR CONFORME A JOB DO PENTAHO

       df_base = df_base.rename(columns={'Codigo': 'codcli', 'FILIAL': 'codfilial', 'ID':'codigo', 'Data Início': 'dtinicio', 'Data Final': 'dtfim', 'CODSUPERVISOR': 'codsupervisor'})

       df_base['dtinicio'] = df_base['dtinicio'].dt.strftime('%Y-%m-%d')

       df_base['dtfim'] = df_base['dtfim'].dt.strftime('%Y-%m-%d')


        #IDENTIFICAR CADA REQUEST, ORGANIZAR EM DATAFRAMES, E TRANSFORMAR EM JSON 

       df_cabecalho = df_base[['codigo','descricao', 'dtinicio', 'dtfim', 'cor', 'ordenaritens', 'ordenaritenspopup']]
       js_cabecalho = df_cabecalho.to_json(orient = 'records')

       df_filial = df_base[['codigo', 'codfilial']]
       df_filial = df_filial.rename(columns={'codigo':'codprepedido'})
       js_filial = df_filial.to_json(orient = 'records')

       df_supervisor = df_base[['codigo', 'codsupervisor']]
       df_supervisor = df_supervisor.rename(columns={'codigo':'codprepedido'})
       js_supervisor = df_supervisor.to_json(orient = 'records')

       df_cliente = df_base[['codigo', 'codcli']]
       df_cliente = df_cliente.rename(columns={'codigo':'codprepedido'})
       js_cliente = df_cliente.to_json(orient = 'records')

       df_itens = df[['Codigo', 'CODPROD', 'Soma de Qt Max Pedid 60d']]\
               .sort_values(by='Codigo')\
               .rename(columns={'Codigo': 'codcli', 'CODPROD': 'codprod', 'Soma de Qt Max Pedid 60d': 'quantidade'})\
               .merge(df_cliente, on=['codcli','codcli'], how='inner')\
               .sort_values(by='codcli', ascending=True)\
               .drop(columns=['codcli'])
        
       df_itens = df_itens[['codprepedido', 'codprod', 'quantidade']].astype(str)
       js_itens = df_itens.to_json(orient='records')

        #GERANDO TOKEN PARA OS ENDPOINTS

       def token():

                url_token = "https://intext-hmg.solucoesmaxima.com.br:81/api/v3/Login"

                payload = json.dumps({
                "login": "WV8QKSJFPRAC2Wh/jkkKIMi5MpOJ7A89PoXL412anX4=",
                "password": "XC9D2SWJnGArIQ/iLhUE/UwtprTApXfQWDyNkTCyJRU="
                })
                headers = {
                'Content-Type': 'application/json'
                }

                response_token = requests.request("POST", url_token, headers=headers, data=payload)
                
                data = json.loads(response_token.content)

                return 'Bearer ' + ( data['token_De_Acesso'])

        #ALIMENTANDO OS ENDPOINTS COM OS JSON GERADOS

       def endpoint_cabecalho(body):
                url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidos"
                headers = {
                        'Content-Type': 'application/json',
                        'Authorization': str(token())
                }

                response = requests.request("POST", url, headers=headers, data=body)
                return response.status_code
                
       resposta_cabecalho = endpoint_cabecalho(js_cabecalho)
       print('resposta solicitação cabecalho:' + str(resposta_cabecalho))



       def endpoint_supervisor(body1):
                url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosSupervisores"
                headers = {
                        'Content-Type': 'application/json',
                        'Authorization': str(token())
                }

                response = requests.request("POST", url, headers=headers, data=body1)
                
                return response.status_code

       resposta_supervisor = endpoint_supervisor(js_supervisor)
       print('resposta solicitação supervisor:' + str(resposta_supervisor))



       def endpoint_filial(body2):
                url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosFiliais"
                headers = {
                        'Content-Type': 'application/json',
                        'Authorization': str(token())
                }

                response = requests.request("POST", url, headers=headers, data=body2)
                
                return response.status_code

       resposta_filial = endpoint_filial(js_filial)
       print('resposta solicitação filial:' + str(resposta_filial))



       def endpoint_cliente (body3):
                url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosClientes"
                headers = {
                        'Content-Type': 'application/json',
                        'Authorization': str(token())
                }

                response = requests.request("POST", url, headers=headers, data=body3)
                
                return response.status_code

       resposta_cliente = endpoint_cliente(js_cliente)
       print('resposta solicitação cliente:' + str(resposta_cliente))



       def endpoint_itens (body4):
                url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosItens"
                headers = {
                        'Content-Type': 'application/json',
                        'Authorization': str(token())
                }

                response = requests.request("POST", url, headers=headers, data=body4)
                
                return response.status_code

       resposta_itens = endpoint_itens(js_itens)
       print('resposta solicitação itens:' + str(resposta_itens))

    except Exception as e:
        email_falha(str(e))
    


if __name__ == "__main__":
        exclusao()
        base()


##########################################################


local_tz = pendulum.timezone('America/Fortaleza')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 2, tzinfo=local_tz),  
    'retries': 0, 
    'retry_delay': timedelta(minutes=5),  
}

with DAG(
    'MAXIMA',
    default_args=default_args,
    description='Agrupamento de dados dos pré pedidos ao Máxima',
    schedule='0 6 * * 2,5', 
    catchup=False,
    max_active_runs=1,
    tags=['comercial']
) as dag: 

    exclusao1 = PythonOperator(
        task_id='exclusão',
        python_callable=exclusao,
        dag=dag,
    )


    ingestão = PythonOperator(
        task_id='ingestão',
        python_callable=base,
        op_args=[],
        provide_context=True,
        dag=dag,
    )

   

    exclusao1 >> ingestão
    
    

