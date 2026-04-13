from __future__ import annotations

import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


ML_FEATURES = [
    "risk_A",
    "risk_B",
    "pair_risk_score",
    "buffer_risk",
    "duty_risk_score",
    "predicted_delay_hours",
    "total_sequence_hours",
    "duty_buffer_hours",
    "month",
]


def _normalize_0_1(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.0, index=series.index)
    return (series - mn) / (mx - mn)


def score_pairs_with_isolation_forest(
    pairs_final: pd.DataFrame,
    contamination: float = 0.15,
    random_state: int = 42,
) -> pd.DataFrame:
    df = pairs_final.copy()
    for col in ML_FEATURES:
        if col not in df.columns:
            raise ValueError(f"Missing ML feature column: {col}")
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    X = df[ML_FEATURES]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=300,
        contamination=contamination,
        random_state=random_state,
    )
    model.fit(X_scaled)

    # decision_function: higher is more normal. Convert to "risk".
    anomaly_score = -model.decision_function(X_scaled)
    df["ml_risk_score"] = _normalize_0_1(pd.Series(anomaly_score, index=df.index)).round(4)

    # model predicts -1 anomalies, 1 inliers
    pred = model.predict(X_scaled)
    df["ml_risk_class"] = pd.Series(pred, index=df.index).map({-1: "Forbidden", 1: "Safe"})
    return df


def build_ml_forbidden_pairs(scored: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        scored.groupby(["airport_A", "airport_B"], as_index=False)
        .agg(
            avg_ml_risk=("ml_risk_score", "mean"),
            forbidden_months=("ml_risk_class", lambda s: int((s == "Forbidden").sum())),
            avg_shield_final=("SHIELD_final_score", "mean"),
            avg_duty_risk=("duty_risk_score", "mean"),
        )
        .sort_values(["forbidden_months", "avg_ml_risk"], ascending=[False, False])
    )
    grouped["ml_pair_class"] = grouped["forbidden_months"].apply(
        lambda x: "Forbidden" if x > 0 else "Safe"
    )
    return grouped

