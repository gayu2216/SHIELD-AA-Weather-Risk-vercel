from __future__ import annotations

import os
from typing import Any

import numpy as np
import pandas as pd

from shield_pipeline.weather.forecast_bundle import build_forecast_bundle


def forecast_hints_from_bundle(bundle: dict[str, Any]) -> dict[str, float]:
    """Map airport code -> forecast_disruption_hint in [0, 1]."""
    hints: dict[str, float] = {}
    for code, data in (bundle.get("airports") or {}).items():
        if not isinstance(data, dict) or "error" in data:
            hints[code] = 0.0
            continue
        summary = data.get("summary") or {}
        h = summary.get("forecast_disruption_hint")
        try:
            hints[code] = float(h) if h is not None else 0.0
        except (TypeError, ValueError):
            hints[code] = 0.0
    return hints


def integrate_forecast_into_pairs(
    pairs_df: pd.DataFrame,
    forecast_days: int,
    *,
    weight_multitask: float | None = None,
    weight_forecast: float | None = None,
    forbidden_threshold: float | None = None,
    bundle: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    For each pair row, attach forecast hints at A and B for the selected horizon,
    derive pair-level weather risk, and blend with multitask_combined_risk.

    pair_forecast_weather_risk = max(hint_A, hint_B)  (either endpoint can drive disruption)

    integrated_risk_score = w_mt * multitask_combined_risk + w_fc * pair_forecast_weather_risk
    """
    w_mt = weight_multitask if weight_multitask is not None else float(
        os.environ.get("INTEGRATED_WEIGHT_MULTITASK", "0.6")
    )
    w_fc = weight_forecast if weight_forecast is not None else float(
        os.environ.get("INTEGRATED_WEIGHT_FORECAST", "0.4")
    )
    thresh = forbidden_threshold if forbidden_threshold is not None else float(
        os.environ.get("INTEGRATED_FORBIDDEN_THRESHOLD", "0.6")
    )

    s = w_mt + w_fc
    if s <= 0:
        w_mt, w_fc = 0.6, 0.4
    else:
        w_mt, w_fc = w_mt / s, w_fc / s

    if bundle is None:
        bundle = build_forecast_bundle(forecast_days)
    hints = forecast_hints_from_bundle(bundle)

    df = pairs_df.copy()
    if "multitask_combined_risk" not in df.columns:
        raise ValueError("pairs_df must include multitask_combined_risk (run run_multitask.py first).")

    df["forecast_window_days"] = int(bundle.get("window_days") or forecast_days)
    df["forecast_generated_at_utc"] = bundle.get("generated_at_utc", "")

    df["forecast_hint_A"] = df["airport_A"].map(hints).fillna(0.0).astype(float)
    df["forecast_hint_B"] = df["airport_B"].map(hints).fillna(0.0).astype(float)
    df["pair_forecast_weather_risk"] = np.maximum(df["forecast_hint_A"], df["forecast_hint_B"])

    mt = pd.to_numeric(df["multitask_combined_risk"], errors="coerce").fillna(0.0)
    pw = df["pair_forecast_weather_risk"]
    df["integrated_risk_score"] = (w_mt * mt + w_fc * pw).round(4)
    df["integrated_risk_class"] = np.where(df["integrated_risk_score"] >= thresh, "Forbidden", "Safe")

    meta = {
        "forecast_window_days": df["forecast_window_days"].iloc[0] if len(df) else forecast_days,
        "forecast_generated_at_utc": bundle.get("generated_at_utc"),
        "weight_multitask": w_mt,
        "weight_forecast": w_fc,
        "integrated_forbidden_threshold": thresh,
        "pair_weather_rule": "pair_forecast_weather_risk = max(forecast_hint_A, forecast_hint_B)",
    }
    return df, meta


def build_integrated_monthly_rules(integrated_df: pd.DataFrame) -> pd.DataFrame:
    """Per (airport_A, airport_B, month) using integrated scores."""
    g = (
        integrated_df.groupby(["airport_A", "airport_B", "month"], as_index=False)
        .agg(
            integrated_risk_score=("integrated_risk_score", "mean"),
            multitask_combined_risk=("multitask_combined_risk", "mean"),
            pair_forecast_weather_risk=("pair_forecast_weather_risk", "mean"),
            forecast_hint_A=("forecast_hint_A", "mean"),
            forecast_hint_B=("forecast_hint_B", "mean"),
        )
        .sort_values(["month", "integrated_risk_score"], ascending=[True, False])
    )
    thresh = float(os.environ.get("INTEGRATED_FORBIDDEN_THRESHOLD", "0.6"))
    g["integrated_monthly_class"] = np.where(g["integrated_risk_score"] >= thresh, "Forbidden", "Safe")
    return g
