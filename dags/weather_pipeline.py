"""Daily ELT pipeline for current weather in Paris and Cotonou."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow.decorators import dag, task

from weather_utils import transform_weather, validate_observation


CITIES = {
    "Paris": {"latitude": 48.8566, "longitude": 2.3522, "timezone": "Europe/Paris"},
    "Cotonou": {"latitude": 6.3703, "longitude": 2.3912, "timezone": "Africa/Porto-Novo"},
}
API_URL = "https://api.open-meteo.com/v1/forecast"
CURRENT_FIELDS = (
    "temperature_2m,relative_humidity_2m,apparent_temperature,"
    "precipitation,wind_speed_10m"
)


@dag(
    dag_id="paris_cotonou_weather",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["portfolio", "weather", "postgres"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    doc_md=__doc__,
)
def weather_pipeline():
    @task
    def create_table() -> None:
        ddl = """
        CREATE SCHEMA IF NOT EXISTS weather;
        CREATE TABLE IF NOT EXISTS weather.observations (
            city TEXT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            temperature_c DOUBLE PRECISION NOT NULL,
            apparent_temperature_c DOUBLE PRECISION NOT NULL,
            humidity_percent INTEGER NOT NULL CHECK (humidity_percent BETWEEN 0 AND 100),
            precipitation_mm DOUBLE PRECISION NOT NULL CHECK (precipitation_mm >= 0),
            wind_speed_kmh DOUBLE PRECISION NOT NULL CHECK (wind_speed_kmh >= 0),
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (city, observed_at)
        );
        """
        with psycopg2.connect(os.environ["WEATHER_DATABASE_URL"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(ddl)

    @task
    def extract_and_transform(city: str, coordinates: dict) -> dict:
        response = requests.get(
            API_URL,
            params={**coordinates, "current": CURRENT_FIELDS},
            timeout=20,
        )
        response.raise_for_status()
        row = transform_weather(response.json(), city)
        validate_observation(row)
        return row

    @task
    def load(row: dict) -> None:
        sql = """
        INSERT INTO weather.observations (
            city, observed_at, temperature_c, apparent_temperature_c,
            humidity_percent, precipitation_mm, wind_speed_kmh,
            latitude, longitude, ingested_at
        ) VALUES (
            %(city)s, %(observed_at)s, %(temperature_c)s,
            %(apparent_temperature_c)s, %(humidity_percent)s,
            %(precipitation_mm)s, %(wind_speed_kmh)s, %(latitude)s,
            %(longitude)s, %(ingested_at)s
        )
        ON CONFLICT (city, observed_at) DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            apparent_temperature_c = EXCLUDED.apparent_temperature_c,
            humidity_percent = EXCLUDED.humidity_percent,
            precipitation_mm = EXCLUDED.precipitation_mm,
            wind_speed_kmh = EXCLUDED.wind_speed_kmh,
            ingested_at = EXCLUDED.ingested_at;
        """
        with psycopg2.connect(os.environ["WEATHER_DATABASE_URL"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, row)

    @task
    def quality_check() -> None:
        sql = """
        SELECT city, COUNT(*)
        FROM weather.observations
        WHERE observed_at >= NOW() - INTERVAL '36 hours'
          AND city IN ('Paris', 'Cotonou')
        GROUP BY city;
        """
        with psycopg2.connect(os.environ["WEATHER_DATABASE_URL"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                counts = dict(cursor.fetchall())
        missing = set(CITIES).difference(counts)
        if missing:
            raise ValueError(f"No recent data for: {', '.join(sorted(missing))}")

    table = create_table()
    loaded = []
    for city, coordinates in CITIES.items():
        observation = extract_and_transform.override(task_id=f"extract_{city.lower()}")(
            city, coordinates
        )
        load_task = load.override(task_id=f"load_{city.lower()}")(observation)
        table >> observation
        loaded.append(load_task)
    loaded >> quality_check()


weather_pipeline()
