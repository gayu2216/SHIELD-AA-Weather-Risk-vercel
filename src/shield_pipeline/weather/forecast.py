from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"

# Open-Meteo free forecast API typically allows up to 16 days; clamp for safety.
FORECAST_DAYS_MAX = 16
FORECAST_DAYS_MIN = 1

DAILY_FIELDS = (
    "weather_code,precipitation_sum,precipitation_probability_max,"
    "wind_speed_10m_max,temperature_2m_max,temperature_2m_min"
)


def fetch_forecast_daily(lat: float, lon: float, forecast_days: int, timeout: int = 45) -> dict[str, Any]:
    """Daily forecast for the next N days (1..16)."""
    days = max(FORECAST_DAYS_MIN, min(int(forecast_days), FORECAST_DAYS_MAX))
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": DAILY_FIELDS,
        "forecast_days": days,
        "wind_speed_unit": "ms",
        "timezone": "auto",
    }
    url = f"{OPEN_METEO_BASE}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "SHIELD-Forecast/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def daily_rows(api_response: dict[str, Any]) -> list[dict[str, Any]]:
    daily = api_response.get("daily") or {}
    times = daily.get("time") or []
    rows: list[dict[str, Any]] = []
    n = len(times)
    keys = [k for k in daily.keys() if k != "time"]
    for i in range(n):
        row = {"date": times[i]}
        for k in keys:
            arr = daily.get(k) or []
            row[k] = arr[i] if i < len(arr) else None
        rows.append(row)
    return rows


def forecast_summary_stats(daily_rows_list: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate stats over the window for scheduling risk hints (0..1 scaled heuristics)."""
    if not daily_rows_list:
        return {
            "days_in_window": 0,
            "max_precip_probability_pct": None,
            "total_precipitation_mm": None,
            "max_wind_speed_ms": None,
            "max_temp_c": None,
            "min_temp_c": None,
            "forecast_disruption_hint": 0.0,
        }

    probs = [r.get("precipitation_probability_max") for r in daily_rows_list if r.get("precipitation_probability_max") is not None]
    precips = [r.get("precipitation_sum") for r in daily_rows_list if r.get("precipitation_sum") is not None]
    winds = [r.get("wind_speed_10m_max") for r in daily_rows_list if r.get("wind_speed_10m_max") is not None]
    tmaxs = [r.get("temperature_2m_max") for r in daily_rows_list if r.get("temperature_2m_max") is not None]
    tmins = [r.get("temperature_2m_min") for r in daily_rows_list if r.get("temperature_2m_min") is not None]

    max_prob = max(probs) if probs else None
    total_p = sum(precips) if precips else None
    max_wind = max(winds) if winds else None
    max_t = max(tmaxs) if tmaxs else None
    min_t = min(tmins) if tmins else None

    # Simple heuristic 0..1: wind (cap 15 m/s), precip prob (already 0-100)
    wind_part = min(1.0, (max_wind or 0) / 15.0) if max_wind is not None else 0.0
    rain_part = min(1.0, (max_prob or 0) / 100.0) if max_prob is not None else 0.0
    hint = round(0.5 * wind_part + 0.5 * rain_part, 4)

    return {
        "days_in_window": len(daily_rows_list),
        "max_precip_probability_pct": max_prob,
        "total_precipitation_mm": round(total_p, 2) if total_p is not None else None,
        "max_wind_speed_ms": max_wind,
        "max_temp_c": max_t,
        "min_temp_c": min_t,
        "forecast_disruption_hint": hint,
    }
