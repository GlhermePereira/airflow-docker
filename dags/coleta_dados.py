from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import text

# =====================
# Configurações
# =====================
COINS = ["bitcoin", "ethereum", "cardano"]
VS_CURRENCY = "usd"

# =====================
# Funções auxiliares
# =====================
def extrair_dados(**kwargs):
    print("=== EXTRAÇÃO ===")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": VS_CURRENCY, "ids": ",".join(COINS)}
    
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    dados = response.json()
    
    if not dados:
        raise ValueError("A API retornou dados vazios!")
    
    df = pd.DataFrame(dados)
    print("Dados extraídos:")
    print(df.head())
    
    # Passa DataFrame para próxima tarefa via XCom
    kwargs['ti'].xcom_push(key='raw_df', value=df.to_dict(orient='records'))

def transformar_dados(**kwargs):
    print("=== TRANSFORMAÇÃO ===")
    ti = kwargs['ti']
    raw_records = ti.xcom_pull(key='raw_df', task_ids='extract')
    df = pd.DataFrame(raw_records)
    
    print("Dados antes da transformação:")
    print(df.head())
    
    # Seleciona apenas colunas relevantes
    df = df[[
        "id", "symbol", "name",
        "current_price", "market_cap", "total_volume",
        "high_24h", "low_24h",
        "price_change_percentage_24h"
    ]]
    
    # Colunas derivadas
    df["price_change_ratio"] = df["high_24h"] / df["low_24h"]
    df["is_profitable"] = df["price_change_ratio"] > 1.05
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].astype(str)
        ti.xcom_push(key='transformed_df', value=df.to_dict(orient='records'))


        
    # Garantir tipos corretos
    df["is_profitable"] = df["is_profitable"].astype(bool)
    numeric_cols = ["current_price","market_cap","total_volume","high_24h","low_24h","price_change_percentage_24h","price_change_ratio"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    print("Dados transformados:")
    print(df.head())
    
    # Passa DataFrame transformado para próxima tarefa
    ti.xcom_push(key='transformed_df', value=df.to_dict(orient='records'))

def carregar_dados(**kwargs):
    print("=== CARREGAMENTO ===")
    ti = kwargs['ti']
    transformed_records = ti.xcom_pull(key='transformed_df', task_ids='transform')
    df = pd.DataFrame(transformed_records)
    
    print("Dados a serem inseridos:")
    print(df.head())
    
    # Conectar ao Postgres
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    
    # Criar schema e tabela se não existirem
    with engine.connect() as conn:
        print("Garantindo que schema e tabela existam...")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl.cryptos (
                id TEXT,
                symbol TEXT,
                name TEXT,
                current_price NUMERIC,
                market_cap NUMERIC,
                total_volume NUMERIC,
                high_24h NUMERIC,
                low_24h NUMERIC,
                price_change_percentage_24h NUMERIC,
                price_change_ratio NUMERIC,
                is_profitable BOOLEAN,
                last_updated TIMESTAMP
            )
        """))
    
    # Inserir dados
    print(f"Inserindo {len(df)} linhas no banco...")
    df.to_sql("cryptos", engine, schema="etl", if_exists="append", index=False)
    print("Dados inseridos com sucesso!")

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
    dag_id="crypto_pipeline_postgres_debug",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Pipeline ETL de criptomoedas direto no Postgres com debug no terminal",
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extrair_dados,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transformar_dados,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=carregar_dados,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
