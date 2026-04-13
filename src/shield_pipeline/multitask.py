from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler


FEATURE_COLUMNS = [
    "risk_A",
    "risk_B",
    "pair_risk_score",
    "buffer_risk",
    "SHIELD_pair_score",
    "predicted_delay_hours",
    "total_sequence_hours",
    "duty_buffer_hours",
    "duty_risk_score",
    "month_sin",
    "month_cos",
    "cancel_rate_A",
    "cancel_rate_B",
    "avg_arr_delay_A",
    "avg_arr_delay_B",
    "avg_weather_delay_A",
    "avg_weather_delay_B",
    "weather_delay_rate_A",
    "weather_delay_rate_B",
]


def _add_month_cyclic_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    angle = 2 * np.pi * (out["month"].astype(float) / 12.0)
    out["month_sin"] = np.sin(angle)
    out["month_cos"] = np.cos(angle)
    return out


def _build_pair_training_table(pairs_final: pd.DataFrame, airport_summary: pd.DataFrame) -> pd.DataFrame:
    airport_cols = [
        "ORIGIN",
        "MONTH",
        "cancel_rate",
        "avg_arr_delay",
        "avg_weather_delay",
        "weather_delay_rate",
    ]
    lookup = airport_summary[airport_cols].copy()

    df = pairs_final.copy()
    df = df.merge(
        lookup.rename(
            columns={
                "ORIGIN": "airport_A",
                "MONTH": "month",
                "cancel_rate": "cancel_rate_A",
                "avg_arr_delay": "avg_arr_delay_A",
                "avg_weather_delay": "avg_weather_delay_A",
                "weather_delay_rate": "weather_delay_rate_A",
            }
        ),
        on=["airport_A", "month"],
        how="left",
    )
    df = df.merge(
        lookup.rename(
            columns={
                "ORIGIN": "airport_B",
                "MONTH": "month",
                "cancel_rate": "cancel_rate_B",
                "avg_arr_delay": "avg_arr_delay_B",
                "avg_weather_delay": "avg_weather_delay_B",
                "weather_delay_rate": "weather_delay_rate_B",
            }
        ),
        on=["airport_B", "month"],
        how="left",
    )

    numeric_cols = [
        "cancel_rate_A",
        "cancel_rate_B",
        "avg_arr_delay_A",
        "avg_arr_delay_B",
        "avg_weather_delay_A",
        "avg_weather_delay_B",
        "weather_delay_rate_A",
        "weather_delay_rate_B",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = _add_month_cyclic_features(df)
    return df


def _create_multitask_targets(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["cancel_pressure"] = out[["cancel_rate_A", "cancel_rate_B"]].max(axis=1)
    out["severe_delay_pressure"] = out[
        ["avg_arr_delay_A", "avg_arr_delay_B", "avg_weather_delay_A", "avg_weather_delay_B"]
    ].max(axis=1)
    out["duty_pressure"] = out["duty_risk_score"]

    cancel_threshold = out["cancel_pressure"].quantile(0.75)
    severe_threshold = out["severe_delay_pressure"].quantile(0.75)
    duty_threshold = out["duty_pressure"].quantile(0.75)

    out["target_cancel"] = (out["cancel_pressure"] >= cancel_threshold).astype(int)
    out["target_severe_delay"] = (out["severe_delay_pressure"] >= severe_threshold).astype(int)
    out["target_duty_violation"] = (out["duty_pressure"] >= duty_threshold).astype(int)
    return out


def _fit_predict_probability(df: pd.DataFrame, target_col: str, random_state: int = 42) -> pd.Series:
    x = df[FEATURE_COLUMNS].copy()
    for col in FEATURE_COLUMNS:
        x[col] = pd.to_numeric(x[col], errors="coerce").fillna(0)

    y = df[target_col].astype(int)
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_leaf=8,
        random_state=random_state,
        class_weight="balanced_subsample",
        n_jobs=-1,
    )
    model.fit(x_scaled, y)
    probs = model.predict_proba(x_scaled)[:, 1]
    return pd.Series(probs, index=df.index)


def run_multitask_scoring(
    pairs_final: pd.DataFrame, airport_summary: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = _build_pair_training_table(pairs_final, airport_summary)
    df = _create_multitask_targets(df)

    df["cancel_risk_score"] = _fit_predict_probability(df, "target_cancel")
    df["severe_delay_risk_score"] = _fit_predict_probability(df, "target_severe_delay")
    df["duty_violation_risk_score"] = _fit_predict_probability(df, "target_duty_violation")

    df["multitask_combined_risk"] = (
        0.40 * df["cancel_risk_score"]
        + 0.35 * df["severe_delay_risk_score"]
        + 0.25 * df["duty_violation_risk_score"]
    ).round(4)
    df["multitask_class"] = np.where(df["multitask_combined_risk"] >= 0.60, "Forbidden", "Safe")

    airport_a = df[
        ["airport_A", "month", "cancel_risk_score", "severe_delay_risk_score", "duty_violation_risk_score", "multitask_combined_risk"]
    ].rename(columns={"airport_A": "airport"})
    airport_b = df[
        ["airport_B", "month", "cancel_risk_score", "severe_delay_risk_score", "duty_violation_risk_score", "multitask_combined_risk"]
    ].rename(columns={"airport_B": "airport"})

    airport_month_scores = (
        pd.concat([airport_a, airport_b], ignore_index=True)
        .groupby(["airport", "month"], as_index=False)
        .agg(
            cancel_risk_score=("cancel_risk_score", "mean"),
            severe_delay_risk_score=("severe_delay_risk_score", "mean"),
            duty_violation_risk_score=("duty_violation_risk_score", "mean"),
            multitask_combined_risk=("multitask_combined_risk", "mean"),
        )
        .sort_values(["airport", "month"])
    )

    pair_business_rules = (
        df.groupby(["airport_A", "airport_B"], as_index=False)
        .agg(
            avg_cancel_risk=("cancel_risk_score", "mean"),
            avg_severe_delay_risk=("severe_delay_risk_score", "mean"),
            avg_duty_violation_risk=("duty_violation_risk_score", "mean"),
            avg_multitask_risk=("multitask_combined_risk", "mean"),
            forbidden_months=("multitask_class", lambda s: int((s == "Forbidden").sum())),
        )
        .sort_values(["forbidden_months", "avg_multitask_risk"], ascending=[False, False])
    )
    pair_business_rules["business_rule_class"] = np.where(
        (pair_business_rules["forbidden_months"] >= 3) | (pair_business_rules["avg_multitask_risk"] >= 0.60),
        "Forbidden",
        "Safe",
    )

    monthly_pair_business_rules = (
        df.groupby(["airport_A", "airport_B", "month"], as_index=False)
        .agg(
            cancel_risk_score=("cancel_risk_score", "mean"),
            severe_delay_risk_score=("severe_delay_risk_score", "mean"),
            duty_violation_risk_score=("duty_violation_risk_score", "mean"),
            multitask_combined_risk=("multitask_combined_risk", "mean"),
        )
        .sort_values(["month", "multitask_combined_risk"], ascending=[True, False])
    )
    monthly_pair_business_rules["monthly_business_rule_class"] = np.where(
        monthly_pair_business_rules["multitask_combined_risk"] >= 0.60,
        "Forbidden",
        "Safe",
    )

    score_cols = [
        "cancel_risk_score",
        "severe_delay_risk_score",
        "duty_violation_risk_score",
        "multitask_combined_risk",
    ]
    for col in score_cols:
        df[col] = df[col].round(4)
        airport_month_scores[col] = airport_month_scores[col].round(4)
        monthly_pair_business_rules[col] = monthly_pair_business_rules[col].round(4)
    return df, airport_month_scores, pair_business_rules, monthly_pair_business_rules

