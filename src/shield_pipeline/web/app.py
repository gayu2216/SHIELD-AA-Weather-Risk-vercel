from __future__ import annotations

import json
import math
import os
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from shield_pipeline.config import PipelineConfig
from shield_pipeline.weather.forecast import FORECAST_DAYS_MAX
from shield_pipeline.weather.forecast_bundle import (
    build_forecast_bundle,
    build_timepoint_weather_bundle_for_airports,
)
from shield_pipeline.weather_delay_runtime import (
    airports_for_pair_time_prediction,
    predict_pairs_weather_delay_batch,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"
CFG = PipelineConfig()
XGBOOST_FORBIDDEN_DELAY_MINUTES = float(os.environ.get("XGBOOST_FORBIDDEN_DELAY_MINUTES", "60"))


def _sanitize_json_value(obj: object) -> object:
    """Convert numpy/pandas scalars for Pydantic/FastAPI JSON response."""
    if isinstance(obj, dict):
        return {k: _sanitize_json_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_json_value(v) for v in obj]
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        v = float(obj)
        if math.isnan(v):
            return None
        return v
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def _dataframe_to_jsonable_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    # Avoid numpy.int64/float64 in dicts (Pydantic v2 JSON serialization)
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _parse_month_from_date(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        return date.fromisoformat(date_str).month
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"Invalid date '{date_str}'. Use YYYY-MM-DD.") from e


def _load_pair_catalog() -> pd.DataFrame:
    path = CFG.final_pairs_file
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Missing {path}. Run: PYTHONPATH=src python run_pipeline.py",
        )
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read pair catalog: {e}") from e


def _forecast_airports_for_app(date_str: str | None = None) -> list[str]:
    pairs = _load_pair_catalog()
    if date_str and "month" in pairs.columns:
        month = _parse_month_from_date(date_str)
        month_values = pd.to_numeric(pairs["month"], errors="coerce")
        month_pairs = pairs.loc[month_values == month].copy()
        if not month_pairs.empty:
            return airports_for_pair_time_prediction(month_pairs)
    return airports_for_pair_time_prediction(pairs)


app = FastAPI(title="SHIELD Weather Forecast", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DEFAULT_WINDOWS = [7, 10, 14, 16]


@app.get("/api/windows")
def api_windows() -> dict:
    return {
        "windows": DEFAULT_WINDOWS,
        "max_days": FORECAST_DAYS_MAX,
        "min_days": 1,
        "months": list(range(1, 13)),
        "month_names": [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ],
    }


@app.get("/api/forecast")
def api_forecast(
    days: int = Query(7, ge=1, le=FORECAST_DAYS_MAX),
    date: str | None = Query(None, description="Selected calendar date YYYY-MM-DD"),
    time: str | None = Query(None, description="Selected local departure time HH:MM"),
) -> dict:
    try:
        if date and time:
            bundle = build_timepoint_weather_bundle_for_airports(_forecast_airports_for_app(date), date)
        else:
            bundle = build_forecast_bundle(days)
        if date and time:
            bundle["selected_departure_date"] = date
            bundle["selected_departure_time"] = time
        return bundle
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@app.get("/api/forecast/scheduling-hints")
def api_scheduling_hints(days: int = Query(7, ge=1, le=FORECAST_DAYS_MAX)) -> dict:
    """Lightweight view for blending with pair scheduling: one row per airport."""
    try:
        bundle = build_forecast_bundle(days)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    rows = []
    for code, data in bundle.get("airports", {}).items():
        if not isinstance(data, dict) or "error" in data:
            continue
        summary = data.get("summary") or {}
        hint = summary.get("forecast_disruption_hint")
        rows.append(
            {
                "airport": code,
                "forecast_disruption_hint": hint,
                "max_precip_probability_pct": summary.get("max_precip_probability_pct"),
                "max_wind_speed_ms": summary.get("max_wind_speed_ms"),
            }
        )
    rows.sort(key=lambda r: r.get("airport") or "")
    return {
        "window_days": bundle.get("window_days"),
        "generated_at_utc": bundle.get("generated_at_utc"),
        "hints": rows,
    }


@app.get("/api/risk/pairs")
def api_risk_pairs(
    days: int = Query(7, ge=1, le=FORECAST_DAYS_MAX),
    limit: int | None = Query(
        None,
        description="Max rows to return (omit for all rows; large JSON).",
    ),
    date: str | None = Query(None, description="Selected departure date YYYY-MM-DD for XGBoost time-aware prediction"),
    time: str | None = Query(None, description="Selected first-leg local departure time HH:MM"),
) -> dict:
    """
    XGBoost-only pair classification for the selected departure date/time.
    The pair catalog comes from deterministic outputs, but forbidden/safe decisions are based
    only on the weather-delay XGBoost probabilities for the two legs of A -> DFW -> B.
    """
    if not date or not time:
        raise HTTPException(
            status_code=422,
            detail="Both 'date' (YYYY-MM-DD) and 'time' (HH:MM) are required for XGBoost-only pair decisions.",
        )

    effective_month = _parse_month_from_date(date)
    pairs = _load_pair_catalog()
    rows_before_month = len(pairs)
    if effective_month is not None and "month" in pairs.columns:
        month_values = pd.to_numeric(pairs["month"], errors="coerce")
        pairs = pairs.loc[month_values == effective_month].copy()
    if pairs.empty:
        meta = {
            "forecast_window_days": days,
            "forecast_generated_at_utc": None,
            "selected_departure_date": date,
            "selected_departure_time_local": time,
            "selected_departure_month": effective_month,
            "xgboost_forbidden_delay_minutes": XGBOOST_FORBIDDEN_DELAY_MINUTES,
            "pair_weather_rule": "xgboost_pair_delay_minutes = leg_a_delay_minutes + leg_b_delay_minutes",
            "weather_prediction_mode": "xgboost_delay_minutes_time_aware_only",
            "decision_source": "xgboost_only",
        }
        return {
            "meta": _sanitize_json_value(meta),
            "summary": _sanitize_json_value(
                {
                    "rows": 0,
                    "rows_before_month_filter": rows_before_month,
                    "month_filter": effective_month,
                    "forbidden_count": 0,
                    "returned_rows": 0,
                    "forecast_window_days": None,
                }
            ),
            "pairs": [],
        }

    try:
        needed_airports = airports_for_pair_time_prediction(pairs)
        bundle = build_timepoint_weather_bundle_for_airports(needed_airports, date)
        hourly_by_airport = {
            code: (data.get("hourly") or [])
            for code, data in bundle.get("airports", {}).items()
            if isinstance(data, dict)
        }
        scored = predict_pairs_weather_delay_batch(
            pairs_df=pairs,
            date_str=date,
            time_str=time,
            hourly_by_airport=hourly_by_airport,
        )
        xgb_score = pd.to_numeric(scored["pair_predicted_weather_delay_minutes"], errors="coerce").fillna(0.0).round(2)
        scored["xgboost_pair_risk_score"] = xgb_score
        scored["xgboost_pair_delay_minutes"] = xgb_score
        scored["xgboost_pair_risk_class"] = np.where(
            scored["xgboost_pair_risk_score"] >= XGBOOST_FORBIDDEN_DELAY_MINUTES,
            "Forbidden",
            "Safe",
        )
        scored["forecast_hint_A"] = scored["leg_a_predicted_weather_delay_minutes"]
        scored["forecast_hint_B"] = scored["leg_b_predicted_weather_delay_minutes"]
        scored["pair_forecast_weather_risk"] = scored["pair_predicted_weather_delay_minutes"]
        # Compatibility aliases for the current UI; these now mean XGBoost-only risk.
        scored["integrated_risk_score"] = scored["xgboost_pair_risk_score"]
        scored["integrated_risk_class"] = scored["xgboost_pair_risk_class"]

        meta = {
            "forecast_window_days": bundle.get("window_days"),
            "forecast_generated_at_utc": bundle.get("generated_at_utc"),
            "selected_departure_date": date,
            "selected_departure_time_local": time,
            "selected_departure_month": effective_month,
            "xgboost_forbidden_delay_minutes": XGBOOST_FORBIDDEN_DELAY_MINUTES,
            "pair_weather_rule": "xgboost_pair_delay_minutes = leg_a_delay_minutes + leg_b_delay_minutes",
            "weather_prediction_mode": "xgboost_delay_minutes_time_aware_only",
            "decision_source": "xgboost_only",
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    n = len(scored)
    forbidden = int((scored["xgboost_pair_risk_class"] == "Forbidden").sum()) if n else 0
    out_df = scored if limit is None else scored.head(max(0, limit))

    return {
        "meta": _sanitize_json_value(meta),
        "summary": _sanitize_json_value(
            {
                "rows": n,
                "rows_before_month_filter": rows_before_month,
                "month_filter": effective_month,
                "forbidden_count": forbidden,
                "returned_rows": len(out_df),
                "forecast_window_days": meta.get("forecast_window_days"),
            }
        ),
        "pairs": _dataframe_to_jsonable_records(out_df),
    }


@app.get("/")
def index() -> JSONResponse:
    """API root — React UI runs separately (Vite)."""
    return JSONResponse(
        {
            "service": "SHIELD forecast & risk API",
            "ui_dev": "cd frontend && npm run dev  →  http://127.0.0.1:5173/",
            "api_docs": "/docs",
        }
    )


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
