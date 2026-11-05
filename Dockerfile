FROM apache/airflow:2.10.2-python3.11

RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
       dbt-core==1.8.2 dbt-postgres==1.8.2
