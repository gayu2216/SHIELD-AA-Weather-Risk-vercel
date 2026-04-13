from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"


def fetch_current_for_location(lat: float, lon: float, timeout: int = 30) -> dict[str, Any]:
    """Current weather from Open-Meteo (no API key)."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,precipitation,weather_code,"
        "wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "ms",
        "timezone": "America/Chicago",
    }
    url = f"{OPEN_METEO_BASE}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "SHIELD-Weather-Pipeline/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def extract_current_payload(airport: str, lat: float, lon: float, api_response: dict[str, Any]) -> dict[str, Any]:
    cur = api_response.get("current") or {}
    return {
        "airport": airport,
        "latitude": lat,
        "longitude": lon,
        "observed_at": cur.get("time"),
        "temperature_c": cur.get("temperature_2m"),
        "relative_humidity_pct": cur.get("relative_humidity_2m"),
        "precipitation_mm": cur.get("precipitation"),
        "weather_code": cur.get("weather_code"),
        "wind_speed_ms": cur.get("wind_speed_10m"),
        "wind_direction_deg": cur.get("wind_direction_10m"),
        "source": "open-meteo",
    }
