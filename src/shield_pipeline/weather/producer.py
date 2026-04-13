from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from shield_pipeline.features import TARGET_AIRPORTS
from shield_pipeline.weather.kafka_settings import DEFAULT_BOOTSTRAP, DEFAULT_TOPIC
from shield_pipeline.weather.locations import AIRPORT_LAT_LON
from shield_pipeline.weather.open_meteo import extract_current_payload, fetch_current_for_location


def run_producer_loop(interval_sec: float = 300.0, once: bool = False) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    topic = os.environ.get("WEATHER_KAFKA_TOPIC", DEFAULT_TOPIC)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    while True:
        run_started = datetime.now(timezone.utc).isoformat()
        for airport in TARGET_AIRPORTS:
            if airport not in AIRPORT_LAT_LON:
                continue
            lat, lon = AIRPORT_LAT_LON[airport]
            try:
                raw = fetch_current_for_location(lat, lon)
                payload = extract_current_payload(airport, lat, lon, raw)
                payload["ingested_at_utc"] = run_started
                payload["event_type"] = "weather.observation"
                future = producer.send(topic, key=airport, value=payload)
                future.get(timeout=30)
            except Exception as e:
                print(f"[weather-producer] {airport}: {e}", file=sys.stderr)
        producer.flush()
        if once:
            break
        time.sleep(interval_sec)
