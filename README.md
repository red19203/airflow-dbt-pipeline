# Weather Data Pipeline - Airflow + dbt ELT Project

## Project Information

**Project Title:** Weather Data Pipeline - Automated ELT with Airflow, dbt and PostgreSQL

**Author:**
* [Reda TAHIRI]

**GitHub Repository:** https://github.com/red19203/airflow-dbt-pipeline

## Project Description

This project is an automated data pipeline that collects daily weather data for five cities, stores it in a PostgreSQL warehouse, and transforms it into analytics-ready tables. Airflow orchestrates the whole flow, dbt handles the transformations and the data quality tests, and GitHub Actions validates the dbt layer on every push.

## Data Source

The data comes from the public Open-Meteo API (no API key required) and covers:

* City (Paris, Casablanca, Lyon, Marseille, Lille)
* Date
* Maximum temperature
* Minimum temperature
* Total precipitation
* Load timestamp

Each run fetches 4 days (3 past days + today) for each city, i.e. 20 rows per execution. The load is idempotent: re-running the DAG updates existing rows instead of creating duplicates.

## Technologies Used

This project is built with:

* Apache Airflow 2.10.2 for orchestration
* dbt-core 1.8.2 for transformations and data tests
* PostgreSQL 16 as the data warehouse
* Python 3.11 (requests, psycopg2) for ingestion
* Docker and Docker Compose for the local environment
* GitHub Actions for continuous integration

## Pipeline Architecture

```
Open-Meteo API --> Airflow (extract + load) --> PostgreSQL (raw) --> dbt (staging --> marts)
```

DAG `weather_ingest`, scheduled `@daily`:

```
create_table --> extract --> load --> dbt_run --> dbt_test
```

## Data Model

* `raw.weather_daily`: raw data loaded by Airflow (primary key: city + date)
* `analytics.stg_weather_daily`: view that cleans the data and adds average temperature and a rainy-day flag (precipitation above 1 mm)
* `analytics.mart_city_weather_summary`: table with one row per city (number of days, date range, average/min/max temperature, total precipitation, rainy days)

## Data Quality

9 dbt tests run after each transformation:

* `not_null` on the source and model key columns
* `unique` on the city in the mart
* `unique` on the city + date pair in the staging model

A failing test fails the `dbt_test` task in Airflow.

## Getting Started

```bash
# Clone the repository
git clone https://github.com/red19203/airflow-dbt-pipeline.git

# Navigate to the project directory
cd airflow-dbt-pipeline

# Build and initialize
docker-compose build
docker-compose up airflow-init

# Start the stack
docker-compose up -d
```

Then open http://localhost:8080 (demo credentials: `admin` / `admin`), enable the `weather_ingest` DAG and trigger it manually.

Check the final table:

```bash
docker-compose exec postgres psql -U airflow -d warehouse -c "SELECT * FROM analytics.mart_city_weather_summary;"
```

## Features

* Daily automated ingestion from a public REST API
* Idempotent loading with `INSERT ... ON CONFLICT DO UPDATE`
* Layered modeling with dbt (staging view, marts table)
* 9 automated data quality tests
* Retries on failure (2 retries, 2 minutes apart)
* Fully containerized environment, one command to start
* CI pipeline with GitHub Actions (dbt run + dbt test on a PostgreSQL service, DAG compilation check)

## Project Structure

```
.
├── dags/weather_ingest.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── schema.yml
│       ├── staging/
│       └── marts/
├── .github/workflows/ci.yml
├── Dockerfile
├── docker-compose.yml
└── init-db.sql
```

## Limitations

* Single DAG and single source, local executor: this is a learning project, not a production setup.
* Credentials are hardcoded for local use only.
* Planned: incremental models, dbt snapshots, failure alerts, city list loaded from a config file.
