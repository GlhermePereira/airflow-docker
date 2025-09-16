from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests

# =====================
# Funções
# =====================

def extrair_dados():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": "bitcoin,ethereum,cardano"}
    response = requests.get(url, params=params)
    dados = response.json()
    df = pd.DataFrame(dados)
    df.to_csv("/opt/airflow/dags/crypto_raw.csv", index=False)

def transformar_dados():
    df = pd.read_csv("/opt/airflow/dags/crypto_raw.csv")
    df["price_change_ratio"] = df["high_24h"] / df["low_24h"]
    df["is_profitable"] = df["price_change_ratio"] > 1.05
    df.to_csv("/opt/airflow/dags/crypto_transformed.csv", index=False)


def carregar_dados():
    df = pd.read_csv("/opt/airflow/dags/crypto_transformed.csv")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql("cryptos", engine, schema="etl", if_exists="append", index=False)


# =====================
# DAG
# =====================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_pipeline_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Pipeline ETL para dados de criptomoedas da CoinGecko com Postgres",
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extrair_dados,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transformar_dados,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=carregar_dados,
    )

    extract_task >> transform_task >> load_task
