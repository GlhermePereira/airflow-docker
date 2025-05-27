from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

def coletar_dados():
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    dados = response.json()

    df = pd.DataFrame(dados)
    df["nome_tamanho"] = df["name"].apply(len)
    media_nome = df["nome_tamanho"].mean()

    resultado = {
        "media_tamanho_nome": media_nome,
        "total_usuarios": len(df)
    }

    # Exporta o resultado para CSV na pasta do DAG
    df_resultado = pd.DataFrame([resultado])
    df_resultado.to_csv("/opt/airflow/dags/resultado.csv", index=False)

# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG com agendamento automático diário
with DAG(
    dag_id="coletar_dados_api",
    default_args=default_args,
    schedule_interval='@daily',  # Executa automaticamente todos os dias à meia-noite
    catchup=False,
    description="Coleta e processa dados de uma API pública",
) as dag:

    tarefa = PythonOperator(
        task_id="extrair_e_processar",
        python_callable=coletar_dados,
    )
