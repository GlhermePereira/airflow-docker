from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
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

    pd.DataFrame([resultado]).to_csv("/opt/airflow/dags/resultado.csv", index=False)

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG("coletar_dados_api", schedule_interval=None, catchup=False, default_args=default_args) as dag:
    tarefa = PythonOperator(
        task_id="extrair_e_processar",
        python_callable=coletar_dados,
    )

