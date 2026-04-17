"""Open-Meteo Historical Weather API — hourly archive for one airport and calendar day."""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from dataclasses import dataclass
from typing import Any

ARCHIVE_BASE = "https://archive-api.open-meteo.com/v1/archive"
MAX_RETRIES = 4
BASE_BACKOFF_SECONDS = 1.5

HOURLY_FIELDS = (
    "temperature_2m,relative_humidity_2m,dew_point_2m,precipitation,weather_code,"
    "wind_speed_10m,pressure_msl,visibility,cloud_cover_low"
)


@dataclass
class HourlyDay:
    """Hourly series for one (lat, lon, local calendar day)."""

    times: list[str]
    temperature_2m: list[float | None]
    relative_humidity_2m: list[float | None]
    dew_point_2m: list[float | None]
    precipitation: list[float | None]
    weather_code: list[float | None]
    wind_speed_10m: list[float | None]
    pressure_msl: list[float | None]
    visibility: list[float | None]
    cloud_cover_low: list[float | None]


def fetch_archive_hourly_day(
    lat: float,
    lon: float,
    date_iso: str,
    timezone: str,
    *,
    timeout: int = 60,
    user_agent: str = "SHIELD-BTS-Weather/1.0",
) -> HourlyDay:
    """
    `date_iso` = YYYY-MM-DD. Returns hourly arrays aligned to `timezone`.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_iso,
        "end_date": date_iso,
        "hourly": HOURLY_FIELDS,
        "timezone": timezone,
        "wind_speed_unit": "ms",
    }
    url = f"{ARCHIVE_BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data: dict[str, Any] = json.loads(resp.read().decode())
            break
        except HTTPError as e:
            last_error = e
            if e.code != 429 or attempt == MAX_RETRIES - 1:
                raise
            retry_after = e.headers.get("Retry-After")
            try:
                sleep_s = float(retry_after) if retry_after is not None else BASE_BACKOFF_SECONDS * (2 ** attempt)
            except ValueError:
                sleep_s = BASE_BACKOFF_SECONDS * (2 ** attempt)
            time.sleep(max(0.5, sleep_s))
        except URLError as e:
            last_error = e
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(BASE_BACKOFF_SECONDS * (2 ** attempt))
    else:
        if last_error is not None:
            raise last_error
        raise RuntimeError("Open-Meteo archive request failed without an explicit error.")

    hourly = data.get("hourly") or {}
    times = list(hourly.get("time") or [])

    def series(key: str) -> list[float | None]:
        raw = hourly.get(key) or []
        out: list[float | None] = []
        for x in raw:
            if x is None:
                out.append(None)
            else:
                try:
                    out.append(float(x))
                except (TypeError, ValueError):
                    out.append(None)
        return out

    return HourlyDay(
        times=times,
        temperature_2m=series("temperature_2m"),
        relative_humidity_2m=series("relative_humidity_2m"),
        dew_point_2m=series("dew_point_2m"),
        precipitation=series("precipitation"),
        weather_code=series("weather_code"),
        wind_speed_10m=series("wind_speed_10m"),
        pressure_msl=series("pressure_msl"),
        visibility=series("visibility"),
        cloud_cover_low=series("cloud_cover_low"),
    )


def hourly_day_rows(day: HourlyDay) -> list[dict[str, float | str | None]]:
    rows: list[dict[str, float | str | None]] = []
    for i, ts in enumerate(day.times):
        rows.append(
            {
                "time": ts,
                "temperature_2m": day.temperature_2m[i] if i < len(day.temperature_2m) else None,
                "relative_humidity_2m": day.relative_humidity_2m[i] if i < len(day.relative_humidity_2m) else None,
                "dew_point_2m": day.dew_point_2m[i] if i < len(day.dew_point_2m) else None,
                "precipitation": day.precipitation[i] if i < len(day.precipitation) else None,
                "weather_code": day.weather_code[i] if i < len(day.weather_code) else None,
                "wind_speed_10m": day.wind_speed_10m[i] if i < len(day.wind_speed_10m) else None,
                "pressure_msl": day.pressure_msl[i] if i < len(day.pressure_msl) else None,
                "visibility": day.visibility[i] if i < len(day.visibility) else None,
                "cloud_cover_low": day.cloud_cover_low[i] if i < len(day.cloud_cover_low) else None,
            }
        )
    return rows


def nearest_hour_index(times: list[str], target_minutes_from_midnight: int) -> int:
    """Pick hourly row whose local clock time is closest to target minutes."""
    if not times:
        return 0
    best_i = 0
    best_d = 24 * 60
    for i, t in enumerate(times):
        # "2024-06-01T14:00"
        try:
            part = t.split("T", 1)[1]
            h, m = part.split(":")[:2]
            mins = int(h) * 60 + int(m)
        except (IndexError, ValueError):
            continue
        d = abs(mins - target_minutes_from_midnight)
        if d < best_d:
            best_d = d
            best_i = i
    return min(best_i, len(times) - 1)


class ArchiveDayCache:
    """Avoid duplicate HTTP calls for the same (airport, local date)."""

    def __init__(self, sleep_s: float = 0.12) -> None:
        self._cache: dict[tuple[str, str], HourlyDay] = {}
        self._sleep_s = sleep_s

    def get(
        self,
        airport: str,
        lat: float,
        lon: float,
        date_iso: str,
        tz: str,
    ) -> HourlyDay:
        key = (airport.upper(), date_iso)
        if key in self._cache:
            return self._cache[key]
        time.sleep(self._sleep_s)
        day = fetch_archive_hourly_day(lat, lon, date_iso, tz)
        self._cache[key] = day
        return day
