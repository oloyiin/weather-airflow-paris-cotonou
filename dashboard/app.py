"""Streamlit dashboard backed by the weather PostgreSQL database."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import requests
import streamlit as st


DATABASE_URL = os.environ["WEATHER_DATABASE_URL"]
AIRFLOW_API_URL = os.environ["AIRFLOW_API_URL"]
DAG_ID = "paris_cotonou_weather"

st.set_page_config(
    page_title="Paris × Cotonou Weather",
    page_icon="🌦️",
    layout="wide",
)

st.markdown(
    """
    <style>
    .stApp { background: linear-gradient(145deg, #f4f8ff 0%, #fffaf2 100%); }
    [data-testid="stMetric"] {
        background: rgba(255,255,255,.82); border: 1px solid #dbe5f0;
        padding: 18px; border-radius: 16px; box-shadow: 0 8px 28px rgba(26,55,88,.07);
    }
    h1, h2, h3 { color: #17324d; }
    .subtitle { color: #5e7184; font-size: 1.08rem; margin-top: -12px; }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_observations() -> pd.DataFrame:
    query = """
        SELECT city, observed_at, temperature_c, apparent_temperature_c,
               humidity_percent, precipitation_mm, wind_speed_kmh, ingested_at
        FROM weather.observations
        ORDER BY observed_at;
    """
    try:
        with psycopg2.connect(DATABASE_URL) as connection:
            return pd.read_sql_query(query, connection)
    except psycopg2.errors.UndefinedTable:
        return pd.DataFrame()


def trigger_pipeline() -> tuple[bool, str]:
    run_id = f"dashboard__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    try:
        response = requests.post(
            f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns",
            auth=(os.environ["AIRFLOW_USERNAME"], os.environ["AIRFLOW_PASSWORD"]),
            json={"dag_run_id": run_id},
            timeout=10,
        )
        response.raise_for_status()
        return True, "Collecte lancée dans Airflow. Actualisez dans quelques secondes."
    except requests.RequestException as error:
        return False, f"Impossible de lancer la collecte : {error}"


title_col, action_col = st.columns([4, 1])
with title_col:
    st.title("Paris × Cotonou")
    st.markdown(
        '<p class="subtitle">Un regard météo entre deux continents, alimenté par Airflow.</p>',
        unsafe_allow_html=True,
    )
with action_col:
    if st.button("↻ Nouvelle collecte", type="primary", use_container_width=True):
        success, message = trigger_pipeline()
        (st.success if success else st.error)(message)

data = load_observations()

if data.empty:
    st.info(
        "Aucune donnée pour le moment. Cliquez sur « Nouvelle collecte », "
        "puis actualisez la page après quelques secondes."
    )
    st.stop()

data["observed_at"] = pd.to_datetime(data["observed_at"], utc=True)
latest = data.sort_values("observed_at").groupby("city", as_index=False).tail(1)

for _, row in latest.sort_values("city", ascending=False).iterrows():
    city_col, temp_col, humidity_col, wind_col, rain_col = st.columns([1.3, 1, 1, 1, 1])
    city_col.subheader("🇫🇷 Paris" if row.city == "Paris" else "🇧🇯 Cotonou")
    temp_col.metric("Température", f"{row.temperature_c:.1f} °C")
    humidity_col.metric("Humidité", f"{row.humidity_percent:.0f} %")
    wind_col.metric("Vent", f"{row.wind_speed_kmh:.1f} km/h")
    rain_col.metric("Précipitations", f"{row.precipitation_mm:.1f} mm")
    st.caption(f"Observation : {row.observed_at.strftime('%d/%m/%Y à %H:%M UTC')}")
    st.divider()

st.subheader("Historique des températures")
chart_data = data.pivot_table(
    index="observed_at", columns="city", values="temperature_c", aggfunc="last"
)
st.line_chart(chart_data, color=["#e36b3d", "#3478b8"])

with st.expander("Voir les données brutes"):
    shown = data.sort_values("observed_at", ascending=False).copy()
    shown.columns = [
        "Ville", "Observation", "Température (°C)", "Ressenti (°C)",
        "Humidité (%)", "Précipitations (mm)", "Vent (km/h)", "Ingestion",
    ]
    st.dataframe(shown, use_container_width=True, hide_index=True)
