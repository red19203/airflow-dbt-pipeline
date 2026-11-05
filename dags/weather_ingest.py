import os
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import psycopg2
import requests
from airflow.decorators import dag, task

CITIES = {
    "Paris": (48.8566, 2.3522),
    "Casablanca": (33.5731, -7.5898),
    "Lyon": (45.7640, 4.8357),
    "Marseille": (43.2965, 5.3698),
    "Lille": (50.6292, 3.0573),
}

API_URL = "https://api.open-meteo.com/v1/forecast"


def get_connection():
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_HOST"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
        dbname=os.environ["WAREHOUSE_DB"],
    )


@dag(
    dag_id="weather_ingest",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["ingestion"],
)
def weather_ingest():
    @task
    def create_table():
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw.weather_daily (
                    city TEXT NOT NULL,
                    date DATE NOT NULL,
                    temperature_max DOUBLE PRECISION,
                    temperature_min DOUBLE PRECISION,
                    precipitation_sum DOUBLE PRECISION,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (city, date)
                )
                """
            )

    @task
    def extract():
        rows = []
        for city, (lat, lon) in CITIES.items():
            response = requests.get(
                API_URL,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
                    "timezone": "auto",
                    "past_days": 3,
                    "forecast_days": 1,
                },
                timeout=30,
            )
            response.raise_for_status()
            daily = response.json()["daily"]
            for i, day in enumerate(daily["time"]):
                rows.append(
                    {
                        "city": city,
                        "date": day,
                        "temperature_max": daily["temperature_2m_max"][i],
                        "temperature_min": daily["temperature_2m_min"][i],
                        "precipitation_sum": daily["precipitation_sum"][i],
                    }
                )
        return rows

    @task
    def load(rows):
        with get_connection() as conn, conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO raw.weather_daily
                        (city, date, temperature_max, temperature_min, precipitation_sum)
                    VALUES (%(city)s, %(date)s, %(temperature_max)s,
                            %(temperature_min)s, %(precipitation_sum)s)
                    ON CONFLICT (city, date) DO UPDATE SET
                        temperature_max = EXCLUDED.temperature_max,
                        temperature_min = EXCLUDED.temperature_min,
                        precipitation_sum = EXCLUDED.precipitation_sum,
                        loaded_at = now()
                    """,
                    row,
                )
        return len(rows)

    table = create_table()
    data = extract()
    loaded = load(data)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt run --profiles-dir .",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt test --profiles-dir .",
    )

    table >> data
    loaded >> dbt_run >> dbt_test


weather_ingest()
