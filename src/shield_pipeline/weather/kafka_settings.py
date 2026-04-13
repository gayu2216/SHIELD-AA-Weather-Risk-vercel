from __future__ import annotations

import os
from pathlib import Path

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
DEFAULT_TOPIC = os.environ.get("WEATHER_KAFKA_TOPIC", "weather.airport.observations")
DEFAULT_GROUP_ID = os.environ.get("WEATHER_KAFKA_GROUP", "shield-weather-consumers")
DEFAULT_OUTPUT_JSONL = Path(os.environ.get("WEATHER_OUTPUT_JSONL", "data/processed/weather_stream.jsonl"))
DEFAULT_OUTPUT_LATEST = Path(os.environ.get("WEATHER_OUTPUT_LATEST", "data/processed/weather_latest_by_airport.json"))
