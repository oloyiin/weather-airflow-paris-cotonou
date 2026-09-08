"""Pure functions shared by the weather DAG and its unit tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


REQUIRED_CURRENT_FIELDS = {
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
}


def transform_weather(payload: dict[str, Any], city: str) -> dict[str, Any]:
    """Convert an Open-Meteo response into one database-ready observation."""
    current = payload.get("current", {})
    missing = REQUIRED_CURRENT_FIELDS.difference(current)
    if missing:
        raise ValueError(f"Missing weather fields: {', '.join(sorted(missing))}")

    return {
        "city": city,
        "observed_at": current["time"],
        "temperature_c": float(current["temperature_2m"]),
        "apparent_temperature_c": float(current["apparent_temperature"]),
        "humidity_percent": int(current["relative_humidity_2m"]),
        "precipitation_mm": float(current["precipitation"]),
        "wind_speed_kmh": float(current["wind_speed_10m"]),
        "latitude": float(payload["latitude"]),
        "longitude": float(payload["longitude"]),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def validate_observation(row: dict[str, Any]) -> None:
    """Raise an explicit error when an observation is implausible."""
    if not -90 <= row["temperature_c"] <= 60:
        raise ValueError("Temperature is outside the accepted range")
    if not 0 <= row["humidity_percent"] <= 100:
        raise ValueError("Humidity must be between 0 and 100")
    if row["precipitation_mm"] < 0 or row["wind_speed_kmh"] < 0:
        raise ValueError("Precipitation and wind speed cannot be negative")
