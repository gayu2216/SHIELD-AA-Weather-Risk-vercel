from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Any

from shield_pipeline.features import TARGET_AIRPORTS
from shield_pipeline.weather.forecast import (
    FORECAST_DAYS_MAX,
    daily_rows,
    fetch_forecast_daily,
    fetch_forecast_daily_hourly,
    forecast_summary_stats,
    hourly_rows,
)
from shield_pipeline.weather.airport_timezones import timezone_for_airport
from shield_pipeline.weather.historical_archive import fetch_archive_hourly_day, hourly_day_rows
from shield_pipeline.weather.locations import AIRPORT_LAT_LON, airport_lat_lon


def _one_airport(airport: str, days: int) -> tuple[str, Any]:
    coords = airport_lat_lon(airport)
    if coords is None:
        return airport, {"skipped": True}
    lat, lon = coords
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


def _one_airport_with_hourly(airport: str, days: int) -> tuple[str, Any]:
    coords = airport_lat_lon(airport)
    if coords is None:
        return airport, {"skipped": True}
    lat, lon = coords
    try:
        raw = fetch_forecast_daily_hourly(lat, lon, days)
        daily = daily_rows(raw)
        hourly = hourly_rows(raw)
        summary = forecast_summary_stats(daily)
        return airport, {
            "latitude": lat,
            "longitude": lon,
            "daily": daily,
            "hourly": hourly,
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


def build_forecast_bundle_for_airports(
    airports_list: list[str],
    forecast_days: int,
    *,
    include_hourly: bool = False,
    max_workers: int = 10,
) -> dict[str, Any]:
    days = max(1, min(int(forecast_days), FORECAST_DAYS_MAX))
    generated = datetime.now(timezone.utc).isoformat()
    airports: dict[str, Any] = {}
    worker = _one_airport_with_hourly if include_hourly else _one_airport

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(worker, ap, days): ap for ap in airports_list}
        for fut in as_completed(futures):
            key, val = fut.result()
            airports[key] = val

    return {
        "window_days": days,
        "generated_at_utc": generated,
        "source": "open-meteo",
        "airports": airports,
    }


def _one_airport_archive_hourly(airport: str, date_iso: str) -> tuple[str, Any]:
    coords = airport_lat_lon(airport)
    if coords is None:
        return airport, {"skipped": True}
    lat, lon = coords
    tz = timezone_for_airport(airport)
    if not tz:
        return airport, {"error": f"Missing timezone for {airport}", "latitude": lat, "longitude": lon}
    try:
        day = fetch_archive_hourly_day(lat, lon, date_iso, tz)
        hourly = hourly_day_rows(day)
        return airport, {
            "latitude": lat,
            "longitude": lon,
            "hourly": hourly,
            "daily": [{"date": date_iso}],
            "summary": {
                "days_in_window": 1,
            },
        }
    except Exception as e:
        return airport, {"error": str(e), "latitude": lat, "longitude": lon}


def build_timepoint_weather_bundle_for_airports(
    airports_list: list[str],
    selected_date: str,
    *,
    max_workers: int = 10,
) -> dict[str, Any]:
    target = date.fromisoformat(selected_date)
    today = datetime.now().date()
    generated = datetime.now(timezone.utc).isoformat()
    airports: dict[str, Any] = {}

    if target < today:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_one_airport_archive_hourly, ap, selected_date): ap for ap in airports_list}
            for fut in as_completed(futures):
                key, val = fut.result()
                airports[key] = val
        return {
            "window_days": 1,
            "generated_at_utc": generated,
            "source": "open-meteo-archive",
            "selected_date": selected_date,
            "airports": airports,
        }

    days_needed = (target - today).days + 2
    if days_needed > FORECAST_DAYS_MAX:
        raise ValueError(
            f"Future date {selected_date} is too far ahead for Open-Meteo forecast. Max supported horizon is {FORECAST_DAYS_MAX} days."
        )

    bundle = build_forecast_bundle_for_airports(
        airports_list,
        forecast_days=max(1, days_needed),
        include_hourly=True,
        max_workers=max_workers,
    )
    bundle["selected_date"] = selected_date
    return bundle
