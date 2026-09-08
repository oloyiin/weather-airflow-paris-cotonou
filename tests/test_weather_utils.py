import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))

from weather_utils import transform_weather, validate_observation


def sample_payload():
    return {
        "latitude": 48.86,
        "longitude": 2.35,
        "current": {
            "time": "2026-09-08T12:00",
            "temperature_2m": 22.4,
            "relative_humidity_2m": 61,
            "apparent_temperature": 22.1,
            "precipitation": 0.0,
            "wind_speed_10m": 8.5,
        },
    }


def test_transform_weather_creates_database_row():
    row = transform_weather(sample_payload(), "Paris")
    assert row["city"] == "Paris"
    assert row["temperature_c"] == 22.4
    assert row["humidity_percent"] == 61


def test_transform_weather_rejects_missing_fields():
    payload = sample_payload()
    del payload["current"]["temperature_2m"]
    with pytest.raises(ValueError, match="temperature_2m"):
        transform_weather(payload, "Paris")


def test_validation_rejects_bad_humidity():
    row = transform_weather(sample_payload(), "Paris")
    row["humidity_percent"] = 120
    with pytest.raises(ValueError, match="Humidity"):
        validate_observation(row)
