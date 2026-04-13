from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from kafka import KafkaConsumer

from shield_pipeline.weather.kafka_settings import (
    DEFAULT_BOOTSTRAP,
    DEFAULT_GROUP_ID,
    DEFAULT_OUTPUT_JSONL,
    DEFAULT_OUTPUT_LATEST,
    DEFAULT_TOPIC,
)


def run_consumer() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    topic = os.environ.get("WEATHER_KAFKA_TOPIC", DEFAULT_TOPIC)
    group = os.environ.get("WEATHER_KAFKA_GROUP", DEFAULT_GROUP_ID)
    jsonl_path = Path(os.environ.get("WEATHER_OUTPUT_JSONL", str(DEFAULT_OUTPUT_JSONL)))
    latest_path = Path(os.environ.get("WEATHER_OUTPUT_LATEST", str(DEFAULT_OUTPUT_LATEST)))

    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print(f"[weather-consumer] Connected to {bootstrap} topic={topic} group={group}", file=sys.stderr)

    latest: dict[str, dict] = {}
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            latest = {}

    for message in consumer:
        payload = message.value
        if not isinstance(payload, dict):
            continue
        airport = payload.get("airport")
        if isinstance(airport, str):
            latest[airport] = payload

        line = json.dumps(payload, ensure_ascii=False)
        with jsonl_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

        latest_path.write_text(json.dumps(latest, indent=2), encoding="utf-8")
