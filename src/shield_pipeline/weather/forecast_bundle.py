from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from shield_pipeline.features import TARGET_AIRPORTS
from shield_pipeline.weather.forecast import (
    FORECAST_DAYS_MAX,
    daily_rows,
    fetch_forecast_daily,
    forecast_summary_stats,
)
from shield_pipeline.weather.locations import AIRPORT_LAT_LON


def _one_airport(airport: str, days: int) -> tuple[str, Any]:
    if airport not in AIRPORT_LAT_LON:
        return airport, {"skipped": True}
    lat, lon = AIRPORT_LAT_LON[airport]
    try:
        raw = fetch_forecast_daily(lat, lon, days)
        rows = daily_rows(raw)
        summary = forecast_summary_stats(rows)
        return airport, {
            "latitude": lat,
            "longitude": lon,
            "daily": rows,
            "summary": summary,
        }
    except Exception as e:
        return airport, {"error": str(e), "latitude": lat, "longitude": lon}


def build_forecast_bundle(forecast_days: int, max_workers: int = 10) -> dict[str, Any]:
    """All target airports: daily rows + summary for scheduling (parallel fetch)."""
    days = max(1, min(int(forecast_days), FORECAST_DAYS_MAX))
    generated = datetime.now(timezone.utc).isoformat()
    airports: dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_one_airport, ap, days): ap for ap in TARGET_AIRPORTS}
        for fut in as_completed(futures):
            key, val = fut.result()
            airports[key] = val

    return {
        "window_days": days,
        "generated_at_utc": generated,
        "source": "open-meteo",
        "airports": airports,
    }
