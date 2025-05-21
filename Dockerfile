# Dockerfile
FROM apache/airflow:2.7.1

USER airflow
RUN pip install --no-cache-dir pandas requests
