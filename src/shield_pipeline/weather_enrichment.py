"""Add historical weather at origin (departure) and destination (arrival) from Open-Meteo archive."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from shield_pipeline.weather.airport_timezones import timezone_for_airport
from shield_pipeline.weather.historical_archive import ArchiveDayCache, nearest_hour_index
from shield_pipeline.weather.locations import AIRPORT_LAT_LON

WX_ORIGIN_PREFIX = "wx_origin_dep_"
WX_DEST_PREFIX = "wx_dest_arr_"

WX_FIELDS = (
    "temperature_c",
    "relative_humidity_pct",
    "precipitation_mm",
    "weather_code",
    "wind_speed_ms",
)


def all_wx_column_names() -> list[str]:
    o = [f"{WX_ORIGIN_PREFIX}{f}" for f in WX_FIELDS]
    d = [f"{WX_DEST_PREFIX}{f}" for f in WX_FIELDS]
    return o + d


def hhmm_to_minutes(val: Any) -> int | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        x = int(float(val))
    except (TypeError, ValueError):
        return None
    hi = x // 100
    lo = x % 100
    if hi > 23 or lo > 59:
        return None
    return hi * 60 + lo


def pick_dep_hhmm(row: pd.Series) -> Any:
    cancelled = float(row.get("CANCELLED", 0) or 0) != 0
    if not cancelled and pd.notna(row.get("DEP_TIME")):
        return row["DEP_TIME"]
    return row.get("CRS_DEP_TIME")


def pick_arr_hhmm(row: pd.Series) -> Any:
    cancelled = float(row.get("CANCELLED", 0) or 0) != 0
    if not cancelled and pd.notna(row.get("ARR_TIME")):
        return row["ARR_TIME"]
    return row.get("CRS_ARR_TIME")


def fl_date_as_date(row: pd.Series) -> date | None:
    raw = row.get("FL_DATE")
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return None
    ts = pd.to_datetime(raw, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def infer_arrival_local_date(
    dep_date: date,
    dep_minutes: int | None,
    arr_minutes: int | None,
    crs_elapsed: Any,
) -> date:
    """Best-effort local arrival calendar date at destination."""
    if dep_minutes is not None and crs_elapsed is not None and pd.notna(crs_elapsed):
        try:
            el = int(float(crs_elapsed))
            if el > 0:
                dep_dt = datetime.combine(dep_date, datetime.min.time()) + timedelta(minutes=dep_minutes)
                arr_dt = dep_dt + timedelta(minutes=el)
                return arr_dt.date()
        except (TypeError, ValueError):
            pass
    if dep_minutes is None or arr_minutes is None:
        return dep_date
    if arr_minutes < dep_minutes:
        return dep_date + timedelta(days=1)
    return dep_date


def _sample_hourly(day: Any, minutes: int | None) -> dict[str, float | None]:
    if minutes is None or not day.times:
        return {f: None for f in WX_FIELDS}
    idx = nearest_hour_index(day.times, minutes)

    def g(arr: list[float | None]) -> float | None:
        return arr[idx] if idx < len(arr) else None

    return {
        "temperature_c": g(day.temperature_2m),
        "relative_humidity_pct": g(day.relative_humidity_2m),
        "precipitation_mm": g(day.precipitation),
        "weather_code": g(day.weather_code),
        "wind_speed_ms": g(day.wind_speed_10m),
    }


def enrich_chunk(
    df: pd.DataFrame,
    cache: ArchiveDayCache,
    *,
    force: bool = False,
) -> pd.DataFrame:
    """
    Add wx_* columns using Open-Meteo historical hourly data at origin dep and dest arr times.
    Skips rows with unknown airport coordinates or timezone.
    """
    out = df.copy()
    for col in all_wx_column_names():
        if col not in out.columns:
            out[col] = np.nan

    for i in range(len(out)):
        row = out.iloc[i]
        if not force:
            o_done = pd.notna(row.get(f"{WX_ORIGIN_PREFIX}temperature_c"))
            d_done = pd.notna(row.get(f"{WX_DEST_PREFIX}temperature_c"))
            if o_done and d_done:
                continue

        origin = str(row.get("ORIGIN", "") or "").strip().upper()
        dest = str(row.get("DEST", "") or "").strip().upper()
        dep_date = fl_date_as_date(row)
        if dep_date is None:
            continue

        dep_m = hhmm_to_minutes(pick_dep_hhmm(row))
        arr_m = hhmm_to_minutes(pick_arr_hhmm(row))
        arr_date = infer_arrival_local_date(
            dep_date,
            dep_m,
            arr_m,
            row.get("CRS_ELAPSED_TIME"),
        )

        # --- Origin / departure ---
        if (force or pd.isna(out.iloc[i].get(f"{WX_ORIGIN_PREFIX}temperature_c"))) and origin:
            lat_lon = AIRPORT_LAT_LON.get(origin)
            tz = timezone_for_airport(origin)
            if lat_lon and tz:
                date_iso = dep_date.isoformat()
                try:
                    day = cache.get(origin, lat_lon[0], lat_lon[1], date_iso, tz)
                    wx = _sample_hourly(day, dep_m)
                    idx_label = out.index[i]
                    for k, v in wx.items():
                        out.loc[idx_label, f"{WX_ORIGIN_PREFIX}{k}"] = v
                except Exception:
                    pass

        # --- Destination / arrival ---
        if (force or pd.isna(out.iloc[i].get(f"{WX_DEST_PREFIX}temperature_c"))) and dest:
            lat_lon = AIRPORT_LAT_LON.get(dest)
            tz = timezone_for_airport(dest)
            if lat_lon and tz:
                date_iso = arr_date.isoformat()
                try:
                    day = cache.get(dest, lat_lon[0], lat_lon[1], date_iso, tz)
                    wx = _sample_hourly(day, arr_m)
                    idx_label = out.index[i]
                    for k, v in wx.items():
                        out.loc[idx_label, f"{WX_DEST_PREFIX}{k}"] = v
                except Exception:
                    pass

    return out
