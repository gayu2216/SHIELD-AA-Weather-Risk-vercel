from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import joblib
import numpy as np
import pandas as pd

from shield_pipeline.bts_schema import normalize_bts_columns
from shield_pipeline.config import PipelineConfig
from shield_pipeline.scoring import DEFAULT_FLIGHT_TIME, DFW_TURNAROUND_HOURS, FLIGHT_TIMES
from shield_pipeline.weather.airport_timezones import timezone_for_airport
from shield_pipeline.weather.locations import airport_lat_lon

DEFAULT_MODEL_PATH = Path("data/processed/weather_delay_xgb_20k/weather_delay_xgb_pipeline.joblib")


@dataclass(frozen=True)
class RouteStats:
    distance: float
    crs_elapsed_minutes: float


def _as_local_dt(date_str: str, time_str: str, airport: str) -> datetime:
    tz_name = timezone_for_airport(airport)
    if tz_name is None:
        raise ValueError(f"Missing timezone for airport {airport}")
    naive = datetime.fromisoformat(f"{date_str}T{time_str}")
    return naive.replace(tzinfo=ZoneInfo(tz_name))


def _hhmm_from_dt(dt: datetime) -> int:
    return dt.hour * 100 + dt.minute


@lru_cache(maxsize=1)
def load_weather_delay_model(model_path: str | None = None):
    path = Path(model_path or os.environ.get("WEATHER_DELAY_MODEL_PATH", str(DEFAULT_MODEL_PATH)))
    if not path.exists():
        raise FileNotFoundError(f"Missing trained weather-delay model: {path}")
    return joblib.load(path)


@lru_cache(maxsize=1)
def route_lookup() -> dict[tuple[str, str], RouteStats]:
    cfg = PipelineConfig()
    route_stats_path = cfg.route_stats_file
    if route_stats_path.exists():
        grouped = pd.read_csv(route_stats_path)
    else:
        path = cfg.scoped_file
        if not path.exists():
            raise FileNotFoundError(f"Missing route stats file {route_stats_path} and scoped file {path}")

        df = pd.read_csv(path, usecols=lambda c: c in {"ORIGIN", "DEST", "DISTANCE", "CRS_ELAPSED_TIME", "CRSElapsedTime"}, low_memory=False)
        df = normalize_bts_columns(df)
        for col in ["DISTANCE", "CRS_ELAPSED_TIME"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        grouped = (
            df.groupby(["ORIGIN", "DEST"], as_index=False)
            .agg(
                distance=("DISTANCE", "median"),
                crs_elapsed_minutes=("CRS_ELAPSED_TIME", "median"),
            )
            .dropna(subset=["distance", "crs_elapsed_minutes"])
        )
    lookup: dict[tuple[str, str], RouteStats] = {}
    for row in grouped.itertuples(index=False):
        lookup[(str(row.ORIGIN).upper(), str(row.DEST).upper())] = RouteStats(
            distance=float(row.distance),
            crs_elapsed_minutes=float(row.crs_elapsed_minutes),
        )
    return lookup


def route_stats(origin: str, dest: str) -> RouteStats:
    key = (origin.upper(), dest.upper())
    lookup = route_lookup()
    if key in lookup:
        return lookup[key]

    flight_hours = FLIGHT_TIMES.get(origin.upper(), FLIGHT_TIMES.get(dest.upper(), DEFAULT_FLIGHT_TIME))
    return RouteStats(distance=np.nan, crs_elapsed_minutes=flight_hours * 60.0)


def _nearest_hour_row(hourly_rows: list[dict[str, Any]], target_local: datetime) -> dict[str, Any] | None:
    if not hourly_rows:
        return None
    best = None
    best_delta = None
    for row in hourly_rows:
        t = row.get("time")
        if not t:
            continue
        try:
            row_dt = datetime.fromisoformat(str(t))
        except ValueError:
            continue
        delta = abs((row_dt - target_local.replace(tzinfo=None)).total_seconds())
        if best_delta is None or delta < best_delta:
            best = row
            best_delta = delta
    return best


def _wx_features_from_hourly_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            "obs_age_minutes": np.nan,
            "temperature_c": np.nan,
            "dewpoint_c": np.nan,
            "visibility_m": np.nan,
            "ceiling_ft": np.nan,
            "wind_speed_ms": np.nan,
            "sea_level_pressure_hpa": np.nan,
            "precip_1h_flag": np.nan,
            "ifr_flag": np.nan,
        }
    visibility = row.get("visibility")
    cloud_low = row.get("cloud_cover_low")
    ceiling_ft = np.nan
    if cloud_low is not None:
        try:
            ceiling_ft = float(max(300, 22000 - (float(cloud_low) / 100.0) * 18000))
        except (TypeError, ValueError):
            ceiling_ft = np.nan
    try:
        precip_flag = float(float(row.get("precipitation", 0) or 0) > 0)
    except (TypeError, ValueError):
        precip_flag = np.nan
    try:
        vis_val = float(visibility) if visibility is not None else np.nan
    except (TypeError, ValueError):
        vis_val = np.nan
    if np.isnan(vis_val):
        ifr_flag = float(not np.isnan(ceiling_ft) and ceiling_ft < 1000)
    else:
        ifr_flag = float(vis_val < 4800 or (not np.isnan(ceiling_ft) and ceiling_ft < 1000))
    return {
        "obs_age_minutes": 0.0,
        "temperature_c": row.get("temperature_2m"),
        "dewpoint_c": row.get("dew_point_2m"),
        "visibility_m": vis_val,
        "ceiling_ft": ceiling_ft,
        "wind_speed_ms": row.get("wind_speed_10m"),
        "sea_level_pressure_hpa": row.get("pressure_msl"),
        "precip_1h_flag": precip_flag,
        "ifr_flag": ifr_flag,
    }


def _build_leg_feature_row(
    *,
    origin: str,
    dest: str,
    dep_local: datetime,
    arr_local: datetime,
    wx_origin: dict[str, Any],
    wx_dest: dict[str, Any],
) -> pd.DataFrame:
    stats = route_stats(origin, dest)
    row = {
        "MONTH": dep_local.month,
        "DAY_OF_MONTH": dep_local.day,
        "ORIGIN": origin,
        "DEST": dest,
        "DISTANCE": stats.distance,
        "CRS_DEP_TIME": _hhmm_from_dt(dep_local),
        "CRS_ARR_TIME": _hhmm_from_dt(arr_local),
        "CRS_ELAPSED_TIME": stats.crs_elapsed_minutes,
        "noaa_origin_dep_obs_age_minutes": wx_origin["obs_age_minutes"],
        "noaa_origin_dep_temperature_c": wx_origin["temperature_c"],
        "noaa_origin_dep_dewpoint_c": wx_origin["dewpoint_c"],
        "noaa_origin_dep_visibility_m": wx_origin["visibility_m"],
        "noaa_origin_dep_ceiling_ft": wx_origin["ceiling_ft"],
        "noaa_origin_dep_wind_speed_ms": wx_origin["wind_speed_ms"],
        "noaa_origin_dep_sea_level_pressure_hpa": wx_origin["sea_level_pressure_hpa"],
        "noaa_origin_dep_precip_1h_flag": wx_origin["precip_1h_flag"],
        "noaa_origin_dep_ifr_flag": wx_origin["ifr_flag"],
        "noaa_dest_arr_obs_age_minutes": wx_dest["obs_age_minutes"],
        "noaa_dest_arr_temperature_c": wx_dest["temperature_c"],
        "noaa_dest_arr_dewpoint_c": wx_dest["dewpoint_c"],
        "noaa_dest_arr_visibility_m": wx_dest["visibility_m"],
        "noaa_dest_arr_ceiling_ft": wx_dest["ceiling_ft"],
        "noaa_dest_arr_wind_speed_ms": wx_dest["wind_speed_ms"],
        "noaa_dest_arr_sea_level_pressure_hpa": wx_dest["sea_level_pressure_hpa"],
        "noaa_dest_arr_precip_1h_flag": wx_dest["precip_1h_flag"],
        "noaa_dest_arr_ifr_flag": wx_dest["ifr_flag"],
    }
    return pd.DataFrame([row])


def predict_pair_weather_delay(
    *,
    airport_a: str,
    airport_b: str,
    date_str: str,
    time_str: str,
    hourly_by_airport: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    a = airport_a.upper()
    b = airport_b.upper()
    dep_a_local = _as_local_dt(date_str, time_str, a)

    stats_a = route_stats(a, "DFW")
    arr_dfw_local = dep_a_local.astimezone(ZoneInfo(timezone_for_airport("DFW"))) + timedelta(minutes=stats_a.crs_elapsed_minutes)

    dep_b_local = arr_dfw_local + timedelta(hours=DFW_TURNAROUND_HOURS)
    stats_b = route_stats("DFW", b)
    arr_b_local = dep_b_local.astimezone(ZoneInfo(timezone_for_airport(b))) + timedelta(minutes=stats_b.crs_elapsed_minutes)

    row_a_origin = _nearest_hour_row(hourly_by_airport.get(a, []), dep_a_local)
    row_a_dest = _nearest_hour_row(hourly_by_airport.get("DFW", []), arr_dfw_local)
    row_b_origin = _nearest_hour_row(hourly_by_airport.get("DFW", []), dep_b_local)
    row_b_dest = _nearest_hour_row(hourly_by_airport.get(b, []), arr_b_local)

    wx_a_origin = _wx_features_from_hourly_row(row_a_origin)
    wx_a_dest = _wx_features_from_hourly_row(row_a_dest)
    wx_b_origin = _wx_features_from_hourly_row(row_b_origin)
    wx_b_dest = _wx_features_from_hourly_row(row_b_dest)

    leg_a = _build_leg_feature_row(
        origin=a,
        dest="DFW",
        dep_local=dep_a_local,
        arr_local=arr_dfw_local,
        wx_origin=wx_a_origin,
        wx_dest=wx_a_dest,
    )
    leg_b = _build_leg_feature_row(
        origin="DFW",
        dest=b,
        dep_local=dep_b_local,
        arr_local=arr_b_local,
        wx_origin=wx_b_origin,
        wx_dest=wx_b_dest,
    )

    model = load_weather_delay_model()
    delay_a = max(0.0, float(model.predict(leg_a)[0]))
    delay_b = max(0.0, float(model.predict(leg_b)[0]))
    pair_delay = delay_a + delay_b

    return {
        "selected_departure_local": dep_a_local.isoformat(),
        "leg_a_predicted_weather_delay_minutes": round(delay_a, 2),
        "leg_b_predicted_weather_delay_minutes": round(delay_b, 2),
        "pair_predicted_weather_delay_minutes": round(pair_delay, 2),
        # Compatibility aliases for the current UI/API code paths.
        "leg_a_weather_delay_prob": round(delay_a, 2),
        "leg_b_weather_delay_prob": round(delay_b, 2),
        "pair_weather_delay_risk": round(pair_delay, 2),
        "leg_a_schedule": {
            "origin": a,
            "dest": "DFW",
            "departure_local": dep_a_local.isoformat(),
            "arrival_local": arr_dfw_local.isoformat(),
        },
        "leg_b_schedule": {
            "origin": "DFW",
            "dest": b,
            "departure_local": dep_b_local.isoformat(),
            "arrival_local": arr_b_local.isoformat(),
        },
    }


def predict_pairs_weather_delay_batch(
    *,
    pairs_df: pd.DataFrame,
    date_str: str,
    time_str: str,
    hourly_by_airport: dict[str, list[dict[str, Any]]],
) -> pd.DataFrame:
    if pairs_df.empty:
        return pd.DataFrame()

    airports_a = sorted(
        {
            str(v).strip().upper()
            for v in pairs_df["airport_A"].dropna().tolist()
            if str(v).strip()
        }
    )
    airports_b = sorted(
        {
            str(v).strip().upper()
            for v in pairs_df["airport_B"].dropna().tolist()
            if str(v).strip()
        }
    )

    leg_a_rows: list[dict[str, Any]] = []
    leg_b_rows: list[dict[str, Any]] = []
    leg_a_meta: dict[str, dict[str, Any]] = {}
    leg_b_meta: dict[str, dict[str, Any]] = {}

    for a in airports_a:
        dep_a_local = _as_local_dt(date_str, time_str, a)
        stats_a = route_stats(a, "DFW")
        arr_dfw_local = dep_a_local.astimezone(ZoneInfo(timezone_for_airport("DFW"))) + timedelta(minutes=stats_a.crs_elapsed_minutes)
        row_a_origin = _nearest_hour_row(hourly_by_airport.get(a, []), dep_a_local)
        row_a_dest = _nearest_hour_row(hourly_by_airport.get("DFW", []), arr_dfw_local)
        wx_a_origin = _wx_features_from_hourly_row(row_a_origin)
        wx_a_dest = _wx_features_from_hourly_row(row_a_dest)
        leg_a_rows.append(
            _build_leg_feature_row(
                origin=a,
                dest="DFW",
                dep_local=dep_a_local,
                arr_local=arr_dfw_local,
                wx_origin=wx_a_origin,
                wx_dest=wx_a_dest,
            ).iloc[0].to_dict()
        )
        leg_a_meta[a] = {
            "selected_departure_local": dep_a_local.isoformat(),
            "arrival_local": arr_dfw_local.isoformat(),
        }

    for b in airports_b:
        dep_dfw_local = _as_local_dt(date_str, time_str, "DFW")
        stats_b = route_stats("DFW", b)
        arr_b_local = dep_dfw_local.astimezone(ZoneInfo(timezone_for_airport(b))) + timedelta(minutes=stats_b.crs_elapsed_minutes)
        row_b_origin = _nearest_hour_row(hourly_by_airport.get("DFW", []), dep_dfw_local)
        row_b_dest = _nearest_hour_row(hourly_by_airport.get(b, []), arr_b_local)
        wx_b_origin = _wx_features_from_hourly_row(row_b_origin)
        wx_b_dest = _wx_features_from_hourly_row(row_b_dest)
        leg_b_rows.append(
            _build_leg_feature_row(
                origin="DFW",
                dest=b,
                dep_local=dep_dfw_local,
                arr_local=arr_b_local,
                wx_origin=wx_b_origin,
                wx_dest=wx_b_dest,
            ).iloc[0].to_dict()
        )
        leg_b_meta[b] = {
            "departure_local": dep_dfw_local.isoformat(),
            "arrival_local": arr_b_local.isoformat(),
        }

    model = load_weather_delay_model()
    leg_a_df = pd.DataFrame(leg_a_rows)
    leg_b_df = pd.DataFrame(leg_b_rows)
    leg_a_pred = model.predict(leg_a_df) if not leg_a_df.empty else np.array([])
    leg_b_pred = model.predict(leg_b_df) if not leg_b_df.empty else np.array([])

    delay_a_by_airport = {
        airport: round(max(0.0, float(pred)), 2)
        for airport, pred in zip(airports_a, leg_a_pred)
    }
    delay_b_by_airport = {
        airport: round(max(0.0, float(pred)), 2)
        for airport, pred in zip(airports_b, leg_b_pred)
    }

    out = pairs_df.copy().reset_index(drop=True)
    out["airport_A"] = out["airport_A"].astype(str).str.strip().str.upper()
    out["airport_B"] = out["airport_B"].astype(str).str.strip().str.upper()
    out["selected_departure_local"] = out["airport_A"].map(
        {k: v["selected_departure_local"] for k, v in leg_a_meta.items()}
    )
    out["leg_a_predicted_weather_delay_minutes"] = out["airport_A"].map(delay_a_by_airport).fillna(0.0)
    out["leg_b_predicted_weather_delay_minutes"] = out["airport_B"].map(delay_b_by_airport).fillna(0.0)
    out["pair_predicted_weather_delay_minutes"] = (
        out["leg_a_predicted_weather_delay_minutes"] + out["leg_b_predicted_weather_delay_minutes"]
    ).round(2)
    out["leg_a_weather_delay_prob"] = out["leg_a_predicted_weather_delay_minutes"]
    out["leg_b_weather_delay_prob"] = out["leg_b_predicted_weather_delay_minutes"]
    out["pair_weather_delay_risk"] = out["pair_predicted_weather_delay_minutes"]
    out["leg_a_schedule"] = out["airport_A"].map(
        {
            k: {
                "origin": k,
                "dest": "DFW",
                "departure_local": v["selected_departure_local"],
                "arrival_local": v["arrival_local"],
            }
            for k, v in leg_a_meta.items()
        }
    )
    out["leg_b_schedule"] = out["airport_B"].map(
        {
            k: {
                "origin": "DFW",
                "dest": k,
                "departure_local": v["departure_local"],
                "arrival_local": v["arrival_local"],
            }
            for k, v in leg_b_meta.items()
        }
    )
    return out


def airports_for_pair_time_prediction(pairs_df: pd.DataFrame) -> list[str]:
    airports = {"DFW"}
    for col in ["airport_A", "airport_B"]:
        if col in pairs_df.columns:
            airports.update(
                str(v).strip().upper()
                for v in pairs_df[col].dropna().unique().tolist()
                if airport_lat_lon(str(v).strip().upper()) is not None
            )
    return sorted(airports)
