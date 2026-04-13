from __future__ import annotations

import pandas as pd

from .csp import build_forbidden_pairs


def sweep_thresholds(final_pairs: pd.DataFrame, candidates: list[float]) -> pd.DataFrame:
    rows: list[dict] = []
    for threshold in candidates:
        summary = build_forbidden_pairs(final_pairs, threshold)
        forbidden = summary[summary["risk_class"] == "Forbidden"]
        rows.append(
            {
                "threshold": threshold,
                "forbidden_pairs": int(len(forbidden)),
                "safe_pairs": int((summary["risk_class"] == "Safe").sum()),
                "avg_forbidden_risk": round(float(forbidden["avg_SHIELD_final"].mean()), 6)
                if len(forbidden)
                else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("threshold")

