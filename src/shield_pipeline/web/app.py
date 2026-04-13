from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from shield_pipeline.config import PipelineConfig
from shield_pipeline.integrated_risk import integrate_forecast_into_pairs
from shield_pipeline.weather.forecast import FORECAST_DAYS_MAX
from shield_pipeline.weather.forecast_bundle import build_forecast_bundle

STATIC_DIR = Path(__file__).resolve().parent / "static"
CFG = PipelineConfig()


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
def api_forecast(days: int = Query(7, ge=1, le=FORECAST_DAYS_MAX)) -> dict:
    try:
        return build_forecast_bundle(days)
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
    month: int | None = Query(
        None,
        ge=1,
        le=12,
        description="If set, return pairs for this calendar month only (1–12).",
    ),
    limit: int | None = Query(
        None,
        description="Max rows to return (omit for all rows; large JSON).",
    ),
) -> dict:
    """
    Integrated risk per pair-month: multitask score blended with forecast hints at A and B
    for the selected horizon (`days`). Requires `pairs_multitask_scores.csv` on disk.
    Optional `month` filters to one month’s pair rows.
    """
    path = CFG.multitask_pair_scores_file
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Missing {path}. Run: PYTHONPATH=src python run_multitask.py",
        )
    try:
        pairs = pd.read_csv(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read pairs: {e}") from e

    try:
        integrated, meta = integrate_forecast_into_pairs(pairs, days)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    rows_before_month = len(integrated)
    if month is not None and "month" in integrated.columns:
        m = pd.to_numeric(integrated["month"], errors="coerce")
        integrated = integrated[m == month].copy()

    n = len(integrated)
    forbidden = int((integrated["integrated_risk_class"] == "Forbidden").sum()) if n else 0
    out_df = integrated if limit is None else integrated.head(max(0, limit))

    return {
        "meta": _sanitize_json_value(meta),
        "summary": _sanitize_json_value(
            {
                "rows": n,
                "rows_before_month_filter": rows_before_month,
                "month_filter": month,
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
